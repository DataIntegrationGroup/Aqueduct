"""
tests/sources/hydrovu/test_dlt_pipeline.py

Unit tests for the HydroVu dlt pipeline private helpers.
No real API calls — HTTP interactions are simulated via httpx.MockTransport,
so requests go through a real httpx.Client + BearerAuth (exercising real
httpx semantics: raise_for_status, headers, auth_flow) without patching
httpx.get.

Covers:
  _fetch_locations     — success, pagination, error propagation, transient retry
  _fetch_location_data — typed result tuple: success, 404, 5xx, 429, transient errors
  hydrovu_readings     — allowlist filtering, error stats, cursor behaviour

TokenManager/BearerAuth's own behavior (401 refresh-and-retry, token caching)
is covered in tests/shared/test_http.py — the 401 tests here only confirm
_fetch_locations/_fetch_location_data are wired to the client correctly.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from aqueduct_dagster.shared.http import TokenManager
from aqueduct_dagster.sources.hydrovu.dlt_pipeline import (
    _fetch_location_data,
    _fetch_locations,
    hydrovu_readings,
)
from tests.conftest import client_with_responses as _client_with_responses_base
from tests.conftest import make_tm as _make_tm

# ── Fixtures / shared test data ───────────────────────────────────────────────

LOCATIONS_RESPONSE = [
    {
        "id": 4503618672918528,
        "name": "Bartlett-827276",
        "description": "827276",
        "gps": {"latitude": 35.1, "longitude": -106.5},
    }
]

DATA_RESPONSE = {
    "parameters": [
        {
            "parameterId": "33",
            "unitId": "241",
            "readings": [
                {"timestamp": 1780704000, "value": 42.5},
                {"timestamp": 1780707600, "value": 43.0},
            ],
        }
    ]
}


def _client_with_responses(
    responses: list[httpx.Response | Exception], tm: TokenManager | None = None
) -> tuple[httpx.Client, list[httpx.Request]]:
    """Thin wrapper over tests.conftest.client_with_responses fixing base_url
    to HydroVu's mock API root, so call sites below don't repeat it."""
    return _client_with_responses_base(responses, tm=tm, base_url="https://api")


# ── _fetch_locations ──────────────────────────────────────────────────────────


class TestFetchLocations:
    def test_returns_list_on_success(self):
        client, _ = _client_with_responses([httpx.Response(200, json=LOCATIONS_RESPONSE)])
        result = _fetch_locations(client)
        assert result == LOCATIONS_RESPONSE

    def test_sends_empty_start_page_header(self):
        # First request sends X-ISI-Start-Page="" (empty cursor); the response's
        # X-ISI-Next-Page token drives subsequent pages.
        client, calls = _client_with_responses([httpx.Response(200, json=LOCATIONS_RESPONSE)])
        _fetch_locations(client)
        assert calls[0].headers["X-ISI-Start-Page"] == ""

    def test_sends_bearer_token(self):
        client, calls = _client_with_responses(
            [httpx.Response(200, json=LOCATIONS_RESPONSE)], tm=_make_tm("my-token")
        )
        _fetch_locations(client)
        assert calls[0].headers["Authorization"] == "Bearer my-token"

    def test_401_then_success_returns_list(self):
        # BearerAuth's refresh-and-retry-on-401 behavior is unit-tested in
        # tests/shared/test_http.py — this only confirms _fetch_locations is
        # wired to the client (not calling httpx.get directly).
        client, calls = _client_with_responses(
            [httpx.Response(401), httpx.Response(200, json=LOCATIONS_RESPONSE)]
        )
        result = _fetch_locations(client)
        assert result == LOCATIONS_RESPONSE
        assert calls[1].headers["Authorization"] == "Bearer tok-new"

    def test_raises_on_server_error(self):
        client, _ = _client_with_responses([httpx.Response(500)])
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            _fetch_locations(client)
        assert exc_info.value.response.status_code == 500

    def test_hits_correct_endpoint(self):
        client, calls = _client_with_responses([httpx.Response(200, json=LOCATIONS_RESPONSE)])
        _fetch_locations(client)
        assert str(calls[0].url) == "https://api/locations/list"

    def test_paginates_using_next_page_header(self):
        client, calls = _client_with_responses(
            [
                httpx.Response(200, json=[{"id": 1}], headers={"X-ISI-Next-Page": "cursor-2"}),
                httpx.Response(200, json=[{"id": 2}]),
            ]
        )
        result = _fetch_locations(client)
        assert result == [{"id": 1}, {"id": 2}]
        assert calls[0].headers["X-ISI-Start-Page"] == ""
        assert calls[1].headers["X-ISI-Start-Page"] == "cursor-2"

    def test_transient_error_retries_then_succeeds(self):
        client, calls = _client_with_responses(
            [httpx.ReadError("reset"), httpx.Response(200, json=LOCATIONS_RESPONSE)]
        )
        with patch("time.sleep"):
            result = _fetch_locations(client)
        assert result == LOCATIONS_RESPONSE
        assert len(calls) == 2


# ── _fetch_location_data ──────────────────────────────────────────────────────


class TestFetchLocationData:
    def test_returns_data_and_no_error_on_success(self):
        client, _ = _client_with_responses([httpx.Response(200, json=DATA_RESPONSE)])
        data, err = _fetch_location_data(client, 123, 1780704000)
        assert data == DATA_RESPONSE
        assert err is None

    def test_returns_none_none_on_404(self):
        client, _ = _client_with_responses([httpx.Response(404)])
        data, err = _fetch_location_data(client, 123, 1780704000)
        assert data is None
        assert err is None

    def test_404_does_not_raise(self):
        client, _ = _client_with_responses([httpx.Response(404)])
        _fetch_location_data(client, 123, 1780704000)  # must not raise

    def test_returns_error_reason_on_500(self):
        client, _ = _client_with_responses([httpx.Response(500)])
        data, err = _fetch_location_data(client, 123, 1780704000)
        assert data is None
        assert err is not None
        assert "500" in err

    def test_returns_error_reason_on_503(self):
        client, _ = _client_with_responses([httpx.Response(503)])
        data, err = _fetch_location_data(client, 123, 1780704000)
        assert data is None
        assert err is not None
        assert "503" in err

    def test_401_then_success_returns_data(self):
        client, calls = _client_with_responses(
            [httpx.Response(401), httpx.Response(200, json=DATA_RESPONSE)]
        )
        data, err = _fetch_location_data(client, 123, 1780704000)
        assert data == DATA_RESPONSE
        assert err is None
        assert calls[1].headers["Authorization"] == "Bearer tok-new"

    def test_passes_start_time_as_query_param(self):
        client, calls = _client_with_responses([httpx.Response(200, json=DATA_RESPONSE)])
        _fetch_location_data(client, 123, 1780704000)
        assert calls[0].url.params["startTime"] == "1780704000"

    def test_hits_correct_endpoint(self):
        client, calls = _client_with_responses([httpx.Response(200, json=DATA_RESPONSE)])
        _fetch_location_data(client, 123, 1780704000)
        assert calls[0].url.path == "/locations/123/data"

    def test_raises_on_unexpected_4xx(self):
        client, _ = _client_with_responses([httpx.Response(403)])
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            _fetch_location_data(client, 123, 1780704000)
        assert exc_info.value.response.status_code == 403

    def test_transient_error_exhausted_returns_error_reason(self):
        # Handler raises on every attempt — retry_transient exhausts its
        # budget (3 attempts) and _fetch_location_data converts that into an
        # error-reason tuple rather than propagating.
        client, _ = _client_with_responses([httpx.ReadError("reset")] * 3)
        with patch("time.sleep"):
            data, err = _fetch_location_data(client, 123, 1780704000)
        assert data is None
        assert err is not None
        assert "transient" in err.lower()

    def test_429_returns_error_after_exhausted_retries(self):
        client, _ = _client_with_responses([httpx.Response(429)] * 4)
        with patch("time.sleep"):
            data, err = _fetch_location_data(client, 123, 1780704000)
        assert data is None
        assert err is not None
        assert "429" in err

    def test_429_respects_retry_after_header(self):
        client, _ = _client_with_responses([httpx.Response(429, headers={"Retry-After": "30"})] * 4)
        with patch("time.sleep") as mock_sleep:
            _fetch_location_data(client, 123, 1780704000)
        assert mock_sleep.call_args_list[0][0][0] == 30.0

    def test_429_uses_default_backoff_when_no_retry_after(self):
        from aqueduct_dagster.sources.hydrovu.dlt_pipeline import _429_BACKOFF

        client, _ = _client_with_responses([httpx.Response(429)] * 4)
        with patch("time.sleep") as mock_sleep:
            _fetch_location_data(client, 123, 1780704000)
        assert mock_sleep.call_args_list[0][0][0] == _429_BACKOFF

    def test_429_succeeds_after_one_retry(self):
        client, _ = _client_with_responses(
            [
                httpx.Response(429, headers={"Retry-After": "1"}),
                httpx.Response(200, json=DATA_RESPONSE),
            ]
        )
        with patch("time.sleep"):
            data, err = _fetch_location_data(client, 123, 1780704000)
        assert data == DATA_RESPONSE
        assert err is None


# ── hydrovu_readings — location_ids filtering ─────────────────────────────────


_LOCATIONS = [
    {"id": 111, "name": "Well A"},
    {"id": 222, "name": "Well B"},
    {"id": 333, "name": "Well C"},
]

_READINGS_DATA = {
    "parameters": [
        {
            "parameterId": "4",
            "unitId": "35",
            "readings": [{"timestamp": 1_000_000, "value": 10.0}],
        }
    ]
}

_DUMMY_CLIENT = MagicMock(spec=httpx.Client)


class TestHydroVuReadingsFilter:
    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.hydrovu.dlt_pipeline._fetch_location_data")
    def test_only_fetches_allowlisted_locations(self, mock_fetch, _mock_state):
        mock_fetch.return_value = (_READINGS_DATA, None)
        list(
            hydrovu_readings(
                client=_DUMMY_CLIENT,
                start_ts=1000,
                locations=_LOCATIONS,
                location_ids=[111, 222],
            )
        )
        called_ids = {call[0][1] for call in mock_fetch.call_args_list}
        assert called_ids == {111, 222}

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.hydrovu.dlt_pipeline._fetch_location_data")
    def test_skips_locations_not_in_allowlist(self, mock_fetch, _mock_state):
        mock_fetch.return_value = (_READINGS_DATA, None)
        list(
            hydrovu_readings(
                client=_DUMMY_CLIENT,
                start_ts=1000,
                locations=_LOCATIONS,
                location_ids=[111],
            )
        )
        called_ids = {call[0][1] for call in mock_fetch.call_args_list}
        assert 222 not in called_ids
        assert 333 not in called_ids

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.hydrovu.dlt_pipeline._fetch_location_data")
    def test_empty_allowlist_skips_all_locations(self, mock_fetch, _mock_state):
        list(
            hydrovu_readings(
                client=_DUMMY_CLIENT,
                start_ts=1000,
                locations=_LOCATIONS,
                location_ids=[],
            )
        )
        mock_fetch.assert_not_called()


# ── hydrovu_readings — error stats ────────────────────────────────────────────


class TestHydroVuReadingsErrorStats:
    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.hydrovu.dlt_pipeline._fetch_location_data")
    def test_real_error_increments_errored_count(self, mock_fetch, _mock_state):
        mock_fetch.return_value = (None, "HTTP 500")
        stats: dict = {}
        list(
            hydrovu_readings(
                client=_DUMMY_CLIENT,
                start_ts=1000,
                locations=[{"id": 111, "name": "Well A"}],
                location_ids=[111],
                _stats=stats,
            )
        )
        assert stats["locations_errored"] == 1
        assert 111 in stats["failed_location_ids"]

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.hydrovu.dlt_pipeline._fetch_location_data")
    def test_404_does_not_increment_errored_count(self, mock_fetch, _mock_state):
        mock_fetch.return_value = (None, None)
        stats: dict = {}
        list(
            hydrovu_readings(
                client=_DUMMY_CLIENT,
                start_ts=1000,
                locations=[{"id": 111, "name": "Well A"}],
                location_ids=[111],
                _stats=stats,
            )
        )
        assert stats["locations_errored"] == 0
        assert stats["locations_no_data"] == 1
        assert stats["failed_location_ids"] == []

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.hydrovu.dlt_pipeline._fetch_location_data")
    def test_error_does_not_advance_cursor(self, mock_fetch, _mock_state):
        state = {"location_cursors": {"111": 999}}
        with patch("dlt.current.resource_state", return_value=state):
            mock_fetch.return_value = (None, "HTTP 500")
            list(
                hydrovu_readings(
                    client=_DUMMY_CLIENT,
                    start_ts=1000,
                    locations=[{"id": 111, "name": "Well A"}],
                    location_ids=[111],
                )
            )
        assert state["location_cursors"]["111"] == 999  # unchanged

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.hydrovu.dlt_pipeline._fetch_location_data")
    def test_partial_failure_stats(self, mock_fetch, _mock_state):
        # location 111 succeeds, 222 errors
        mock_fetch.side_effect = [(_READINGS_DATA, None), (None, "HTTP 503")]
        stats: dict = {}
        list(
            hydrovu_readings(
                client=_DUMMY_CLIENT,
                start_ts=1000,
                locations=[{"id": 111, "name": "Well A"}, {"id": 222, "name": "Well B"}],
                location_ids=[111, 222],
                _stats=stats,
            )
        )
        assert stats["locations_fetched"] == 1
        assert stats["locations_errored"] == 1
        assert stats["failed_location_ids"] == [222]
