"""
Unit tests for sources/ebid/dlt_pipeline.py's XML parsing, fetch helpers, and
the ebid_sites/ebid_readings resources. No real API calls — httpx.MockTransport
simulates OneRain's XML responses. Fixtures are trimmed from a real live pull
against the OneRain API (site 1164 / MES_19R).
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest
from dlt.extract.exceptions import ResourceExtractionError

from aqueduct_dagster.shared.config import settings_dir
from aqueduct_dagster.sources.ebid import dlt_pipeline as ebid_dlt_pipeline
from aqueduct_dagster.sources.ebid.dlt_pipeline import (
    _NOT_A_WELL,
    EbidApiError,
    _fetch_dtw_sensor,
    _fetch_readings,
    _fetch_sites,
    _get,
    _iter_site_readings,
    _parse_onerain_xml,
    build_ebid_client,
    ebid_readings,
    ebid_sites,
    ebid_source,
    resolve_ebid_system_key,
)
from tests.conftest import client_with_responses_unauthenticated as _client_with_responses_base

# -- fixtures, trimmed from a live pull against the real OneRain API --

SITE_1164 = {
    "or_site_id": "1164",
    "site_id": "209-0000-M19R",
    "location": "Monitoring Well - MES_19R",
    "latitude_dec": "32.3873900",
    "longitude_dec": "-106.8536900",
}

SITE_1165 = {
    "or_site_id": "1165",
    "site_id": "210-0000-M20R",
    "location": "Monitoring Well - MES_20R",
    "latitude_dec": "32.4026600",
    "longitude_dec": "-106.8486500",
}

SENSOR_1164_DTW = {
    "or_site_id": "1164",
    "or_sensor_id": "4",
    "location": "Monitoring Well - MES_19R",
    "description": "Depth to Water - Surface",
    "sensor_class": "102",
}

SENSOR_OTHER = {
    "or_site_id": "1164",
    "or_sensor_id": "9",
    "location": "Monitoring Well - MES_19R",
    "description": "Battery Voltage",
    "sensor_class": "41",
}

READING_ROW = {
    "or_site_id": "1164",
    "or_sensor_id": "4",
    "receive_time": "2026-11-16 05:09:11",
    "data_time": "2026-11-16 05:09:08",
    "data_value": "-17.37400000",
    "raw_value": "-17.37400000",
    "data_quality": "A",
}


def _onerain_xml(rows: list[dict[str, str]]) -> bytes:
    rows_xml = "".join(
        "<row>" + "".join(f"<{k}>{v}</{k}>" for k, v in row.items()) + "</row>" for row in rows
    )
    return f"<onerain><response><general>{rows_xml}</general></response></onerain>".encode()


def _onerain_error(message: str) -> bytes:
    return f"<onerain><error>{message}</error></onerain>".encode()


def _client_with_responses(
    responses: list[httpx.Response | Exception],
) -> tuple[httpx.Client, list[httpx.Request]]:
    return _client_with_responses_base(responses, base_url="http://onerain.example.org:8080")


DUMMY_CLIENT = MagicMock(spec=httpx.Client)


# -- _parse_onerain_xml --


class TestParseOnerainXml:
    def test_parses_rows_into_dicts(self):
        assert _parse_onerain_xml(_onerain_xml([SITE_1164])) == [SITE_1164]

    def test_raises_on_error_element(self):
        with pytest.raises(EbidApiError, match="bad system_key"):
            _parse_onerain_xml(_onerain_error("bad system_key"))

    def test_raises_on_missing_general_element(self):
        with pytest.raises(EbidApiError, match="malformed"):
            _parse_onerain_xml(b"<onerain><response></response></onerain>")


# -- _get --


class TestGet:
    def test_hits_the_exact_servlet_path_with_no_trailing_slash(self):
        # Regression: httpx.Client.base_url always normalizes with a trailing
        # slash, so the servlet path must be passed explicitly per call — not
        # joined into base_url — or this exact-match path 404s.
        client, calls = _client_with_responses(
            [httpx.Response(200, content=_onerain_xml([SITE_1164]))]
        )
        _get(client, {"method": "GetSiteMetaData"})
        assert calls[0].url.path == "/OneRain/DataAPI"

    def test_returns_rows_on_success(self):
        client, _ = _client_with_responses([httpx.Response(200, content=_onerain_xml([SITE_1164]))])
        rows, err = _get(client, {"method": "GetSiteMetaData"})
        assert rows == [SITE_1164]
        assert err is None

    def test_returns_error_reason_on_onerain_error(self):
        client, _ = _client_with_responses(
            [httpx.Response(200, content=_onerain_error("bad system_key"))]
        )
        rows, err = _get(client, {"method": "GetSiteMetaData"})
        assert rows is None
        assert err == "bad system_key"

    def test_returns_error_reason_after_transient_retries_exhausted(self):
        client, _ = _client_with_responses([httpx.ReadError("reset")] * 3)
        with patch("time.sleep"):
            rows, err = _get(client, {"method": "GetSiteMetaData"})
        assert rows is None
        assert "transient" in err.lower()

    def test_returns_error_reason_on_a_real_http_error(self):
        # OneRain's own errors come back as HTTP 200 (see TestParseOnerainXml),
        # so a real 5xx here means a proxy/gateway problem in front of it —
        # this must degrade like every other source's fetch helper, not raise
        # and abort every other site in the run.
        client, _ = _client_with_responses([httpx.Response(503)])
        rows, err = _get(client, {"method": "GetSiteMetaData"})
        assert rows is None
        assert "503" in err

    def test_does_not_raise_on_non_xml_body(self):
        client, _ = _client_with_responses([httpx.Response(200, content=b"not xml")])
        rows, err = _get(client, {"method": "GetSiteMetaData"})
        assert rows is None
        assert err is not None


# -- _fetch_sites --


class TestFetchSites:
    def test_sends_no_site_filter(self):
        client, calls = _client_with_responses(
            [httpx.Response(200, content=_onerain_xml([SITE_1164]))]
        )
        rows, err = _fetch_sites(client)
        assert rows == [SITE_1164]
        assert calls[0].url.params["method"] == "GetSiteMetaData"
        assert "or_site_id" not in calls[0].url.params


# -- _fetch_dtw_sensor --


class TestFetchDtwSensor:
    def test_returns_the_dtw_sensor_when_present(self):
        client, _ = _client_with_responses(
            [httpx.Response(200, content=_onerain_xml([SENSOR_OTHER, SENSOR_1164_DTW]))]
        )
        sensor, err = _fetch_dtw_sensor(client, "1164")
        assert sensor == SENSOR_1164_DTW
        assert err is None

    def test_returns_none_when_no_sensor_matches(self):
        client, _ = _client_with_responses(
            [httpx.Response(200, content=_onerain_xml([SENSOR_OTHER]))]
        )
        sensor, err = _fetch_dtw_sensor(client, "1164")
        assert sensor is None
        assert "no DTW sensor" in err

    def test_propagates_the_upstream_error(self):
        client, _ = _client_with_responses(
            [httpx.Response(200, content=_onerain_error("bad system_key"))]
        )
        sensor, err = _fetch_dtw_sensor(client, "1164")
        assert sensor is None
        assert err == "bad system_key"

    def test_sends_the_site_id_filter(self):
        client, calls = _client_with_responses(
            [httpx.Response(200, content=_onerain_xml([SENSOR_1164_DTW]))]
        )
        _fetch_dtw_sensor(client, "1164")
        assert calls[0].url.params["method"] == "GetSensorMetaData"
        assert calls[0].url.params["or_site_id"] == "1164"


# -- _fetch_readings --


class TestFetchReadings:
    def test_sends_data_start_in_onerains_format(self):
        client, calls = _client_with_responses(
            [httpx.Response(200, content=_onerain_xml([READING_ROW]))]
        )
        start = int(datetime(2026, 11, 15, 5, 39, 15, tzinfo=UTC).timestamp())
        _fetch_readings(client, "1164", "4", start)
        assert calls[0].url.params["data_start"] == "2026-11-15 05:39:15"
        assert "data_end" not in calls[0].url.params

    def test_sends_data_end_when_given(self):
        client, calls = _client_with_responses(
            [httpx.Response(200, content=_onerain_xml([READING_ROW]))]
        )
        start = int(datetime(2026, 11, 15, 0, 0, 0, tzinfo=UTC).timestamp())
        end = int(datetime(2026, 11, 16, 0, 0, 0, tzinfo=UTC).timestamp())
        _fetch_readings(client, "1164", "4", start, end)
        assert calls[0].url.params["data_end"] == "2026-11-16 00:00:00"


# -- build_ebid_client / resolve_ebid_system_key --


class TestBuildEbidClient:
    def test_sets_system_key_as_a_default_param(self):
        client = build_ebid_client("http://onerain.example.org:8080", "sk-123")
        assert client.params["system_key"] == "sk-123"


class TestResolveEbidSystemKey:
    def test_returns_immediately_when_already_given(self):
        with patch("aqueduct_dagster.sources.ebid.dlt_pipeline.secretmanager") as mock_sm:
            assert resolve_ebid_system_key("given-key", "ignored-secret") == "given-key"
        mock_sm.SecretManagerServiceClient.assert_not_called()

    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline.secretmanager.SecretManagerServiceClient")
    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline.ensure_adc")
    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline.load_config")
    def test_reads_the_bare_value_from_secret_manager(
        self, mock_load_config, mock_ensure_adc, mock_sm_cls
    ):
        mock_load_config.return_value = {
            "destination": {"filesystem": {"gcp_project_number": "12345"}}
        }
        mock_sm = mock_sm_cls.return_value
        mock_sm.access_secret_version.return_value = MagicMock(
            payload=MagicMock(data=b"system-key-value\n")
        )

        assert resolve_ebid_system_key("", "ebid_onerain") == "system-key-value"
        mock_sm.secret_version_path.assert_called_once_with("12345", "ebid_onerain", "latest")
        mock_ensure_adc.assert_called_once()


# -- _iter_site_readings --


def _drive(sites, *, cursors=None, start_ts=None, stats=None):
    cursors = {} if cursors is None else cursors
    stats = {} if stats is None else stats
    start_ts = int(time.time()) - 3600 if start_ts is None else start_ts
    rows = list(_iter_site_readings(DUMMY_CLIENT, start_ts, sites, cursors, stats))
    return rows, cursors, stats


class TestIterSiteReadings:
    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline._fetch_readings")
    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline._fetch_dtw_sensor")
    def test_yields_one_flat_record_per_row(self, mock_sensor, mock_readings):
        mock_sensor.return_value = (SENSOR_1164_DTW, None)
        mock_readings.return_value = ([READING_ROW], None)
        rows, _, _ = _drive([SITE_1164])
        ts = int(
            datetime.strptime("2026-11-16 05:09:08", "%Y-%m-%d %H:%M:%S")
            .replace(tzinfo=UTC)
            .timestamp()
        )
        assert rows == [
            {
                "reading_id": f"1164_4_{ts}",
                "or_site_id": "1164",
                "or_sensor_id": "4",
                "timestamp": ts,
                "value": -17.374,
                "raw_value": -17.374,
                "receive_time": "2026-11-16 05:09:11",
                "data_quality": "A",
            }
        ]

    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline._fetch_readings")
    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline._fetch_dtw_sensor")
    def test_cursor_advances_past_start_ts_on_success(self, mock_sensor, mock_readings):
        mock_sensor.return_value = (SENSOR_1164_DTW, None)
        mock_readings.return_value = ([READING_ROW], None)
        start_ts = int(time.time()) - 3600
        _, cursors, _ = _drive([SITE_1164], start_ts=start_ts)
        assert cursors["1164"] > start_ts

    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline._fetch_readings")
    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline._fetch_dtw_sensor")
    def test_a_boundary_row_is_not_refetched_in_the_next_window(self, mock_sensor, mock_readings):
        # A row lands exactly on window 1's end; if data_end is inclusive,
        # starting window 2 at window_end would re-fetch it. +1 avoids that.
        mock_sensor.return_value = (SENSOR_1164_DTW, None)
        start_ts = int(time.time()) - 15 * 24 * 60 * 60
        window_end = start_ts + 10 * 24 * 60 * 60
        boundary_row = {
            **READING_ROW,
            "data_time": datetime.fromtimestamp(window_end, tz=UTC).strftime("%Y-%m-%d %H:%M:%S"),
        }
        mock_readings.side_effect = [([boundary_row], None), ([], None)]
        rows, cursors, _ = _drive([SITE_1164], start_ts=start_ts)
        assert len(rows) == 1
        assert mock_readings.call_count == 2
        second_call_start = mock_readings.call_args_list[1].args[3]
        assert second_call_start == window_end + 1
        assert cursors["1164"] > window_end

    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline._fetch_readings")
    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline._fetch_dtw_sensor")
    def test_sensor_fetch_error_skips_the_site_without_blocking_others(
        self, mock_sensor, mock_readings
    ):
        mock_sensor.side_effect = [
            (None, "transient network error after 3 attempts: boom"),
            (SENSOR_1164_DTW, None),
        ]
        mock_readings.return_value = ([READING_ROW], None)
        stats: dict = {}
        rows, _, stats = _drive([SITE_1165, SITE_1164], stats=stats)
        assert len(rows) == 1
        assert stats["sites_fetched"] == 1
        assert stats["sites_errored"] == 1
        assert stats["failed_site_ids"] == ["1165"]

    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline._fetch_readings")
    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline._fetch_dtw_sensor")
    def test_non_well_site_is_skipped_not_errored(self, mock_sensor, mock_readings):
        # A site genuinely not a DTW well (e.g. a river gauge) isn't a
        # failure — it must not pollute sites_errored/failed_site_ids, or a
        # real failure could get lost in that permanent, expected noise.
        mock_sensor.side_effect = [(None, _NOT_A_WELL), (SENSOR_1164_DTW, None)]
        mock_readings.return_value = ([READING_ROW], None)
        stats: dict = {}
        rows, _, stats = _drive([SITE_1165, SITE_1164], stats=stats)
        assert len(rows) == 1
        assert stats["sites_fetched"] == 1
        assert stats["sites_skipped"] == 1
        assert stats["sites_errored"] == 0
        assert stats["failed_site_ids"] == []

    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline._fetch_readings")
    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline._fetch_dtw_sensor")
    def test_readings_failure_does_not_advance_the_cursor(self, mock_sensor, mock_readings):
        mock_sensor.return_value = (SENSOR_1164_DTW, None)
        mock_readings.return_value = (None, "HTTP 500")
        cursors: dict = {}
        stats: dict = {}
        rows, cursors, stats = _drive([SITE_1164], cursors=cursors, stats=stats)
        assert rows == []
        assert "1164" not in cursors
        assert stats["sites_errored"] == 1
        assert stats["failed_site_ids"] == ["1164"]

    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline._fetch_readings")
    @patch("aqueduct_dagster.sources.ebid.dlt_pipeline._fetch_dtw_sensor")
    def test_row_cap_resumes_right_after_the_last_row_returned(
        self, mock_sensor, mock_readings, caplog
    ):
        # Distinct timestamps, not 5000 identical ones — those would make the
        # cursor crawl forward 1 second per retry instead of past the batch.
        mock_sensor.return_value = (SENSOR_1164_DTW, None)
        start_ts = int(time.time()) - 3600
        capped_rows = [
            {
                **READING_ROW,
                "data_time": datetime.fromtimestamp(start_ts + i, tz=UTC).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            }
            for i in range(5000)
        ]
        mock_readings.return_value = (capped_rows, None)
        with caplog.at_level("WARNING"):
            rows, cursors, _ = _drive([SITE_1164], start_ts=start_ts)
        assert any("cap" in record.message for record in caplog.records)
        assert len(rows) == 5000
        assert cursors["1164"] == start_ts + 5000


# -- ebid_sites --


class TestEbidSites:
    def test_yields_one_row_per_site(self):
        assert list(ebid_sites(sites=[SITE_1164])) == [
            {
                "or_site_id": "1164",
                "site_id": "209-0000-M19R",
                "location": "Monitoring Well - MES_19R",
                "latitude": 32.38739,
                "longitude": -106.85369,
            }
        ]


# -- ebid_readings — client lifetime --


_ITER_TARGET = "aqueduct_dagster.sources.ebid.dlt_pipeline._iter_site_readings"


class TestEbidReadingsClientLifetime:
    @patch("dlt.current.resource_state", return_value={"site_cursors": {}})
    @patch(_ITER_TARGET)
    def test_closes_the_shared_client_when_the_generator_finishes(self, mock_iter, mock_state):
        mock_iter.return_value = iter([{"reading_id": "x"}])
        client = MagicMock(spec=httpx.Client)
        rows = list(ebid_readings(client=client, start_ts=1000, sites=[SITE_1164]))
        assert rows == [{"reading_id": "x"}]
        client.close.assert_called_once()

    @patch("dlt.current.resource_state", return_value={"site_cursors": {}})
    @patch(_ITER_TARGET, side_effect=RuntimeError("boom"))
    def test_closes_the_client_even_if_fetching_raises(self, mock_iter, mock_state):
        client = MagicMock(spec=httpx.Client)
        # dlt wraps the raised exception as ResourceExtractionError, since
        # ebid_readings is itself a @dlt.resource.
        with pytest.raises(ResourceExtractionError):
            list(ebid_readings(client=client, start_ts=1000, sites=[SITE_1164]))
        client.close.assert_called_once()


# -- ebid_source — config binding --


class TestEbidSourceConfig:
    """@dlt.source(name="ebid") binds these defaults to [sources.ebid] in
    .dlt/config.toml — nothing cross-checks the two, so a typo there silently
    points at the wrong endpoint/secret."""

    @staticmethod
    def _resolved_config() -> dict:
        settings_dir()
        captured: dict = {}

        def _fake_resolve_key(system_key: str, gcp_secret: str) -> str:
            captured["gcp_secret"] = gcp_secret
            return "fake-key"

        def _fake_build_client(api_base_url: str, system_key: str) -> httpx.Client:
            captured["api_base_url"] = api_base_url
            return MagicMock(spec=httpx.Client)

        with (
            patch.object(
                ebid_dlt_pipeline, "resolve_ebid_system_key", side_effect=_fake_resolve_key
            ),
            patch.object(ebid_dlt_pipeline, "build_ebid_client", side_effect=_fake_build_client),
            patch.object(ebid_dlt_pipeline, "_fetch_sites", return_value=([], None)),
        ):
            ebid_source()
        return captured

    def test_reads_ebids_own_secret(self):
        assert self._resolved_config()["gcp_secret"] == "ebid_onerain"

    def test_reads_the_onerain_endpoint(self):
        assert self._resolved_config()["api_base_url"] == "http://onerain.ebid-nm.org:8080"

    def test_exposes_both_resources_under_their_table_names(self):
        settings_dir()
        with (
            patch.object(ebid_dlt_pipeline, "resolve_ebid_system_key", return_value="fake-key"),
            patch.object(
                ebid_dlt_pipeline, "build_ebid_client", return_value=MagicMock(spec=httpx.Client)
            ),
            patch.object(ebid_dlt_pipeline, "_fetch_sites", return_value=([], None)),
        ):
            source = ebid_source()
        assert {resource.name for resource in source.resources.values()} == {
            "ebid_sites",
            "ebid_readings",
        }


class TestEbidSourceClientLeak:
    def test_closes_the_client_when_the_site_fetch_fails(self):
        settings_dir()
        client = MagicMock(spec=httpx.Client)
        with (
            patch.object(ebid_dlt_pipeline, "resolve_ebid_system_key", return_value="fake-key"),
            patch.object(ebid_dlt_pipeline, "build_ebid_client", return_value=client),
            patch.object(ebid_dlt_pipeline, "_fetch_sites", return_value=(None, "bad system_key")),
            pytest.raises(EbidApiError),
        ):
            ebid_source()
        client.close.assert_called_once()
