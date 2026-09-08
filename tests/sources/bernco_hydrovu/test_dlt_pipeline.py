"""
tests/sources/bernco_hydrovu/test_dlt_pipeline.py

Unit tests for BernCo's hydrovu_readings and hydrovu_locations resources.

The vendor-level HTTP client these fetch through (fetch_locations,
fetch_location_data, credential resolution, pagination, 404/429/5xx handling) is
shared with PVACD and tested once in tests/sources/test_hydrovu_common.py. What is
tested here is what this module owns: the allowlist that keeps the ~24 non-DTW BernCo
locations out of the readings fetch, and the per-location cursors in dlt resource state.

No real API calls — fetch_location_data is patched at the shared module, which is
where the resource resolves it from, and dlt.current.resource_state() is patched with
a plain dict so the generator can be driven directly.

Fixtures are trimmed from the live responses captured in
docs/sources/bernco_hydrovu.md: SierraVista-966932 (an Aqua TROLL sonde carrying
parameterId="4") and E-94077-1193582VL (a VuLink gateway carrying only diagnostics).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from aqueduct_dagster.shared.config import settings_dir
from aqueduct_dagster.sources.bernco_hydrovu import dlt_pipeline as bernco_dlt_pipeline
from aqueduct_dagster.sources.bernco_hydrovu.dlt_pipeline import (
    bernco_hydrovu_source,
    hydrovu_locations,
    hydrovu_readings,
)

_FETCH_TARGET = "aqueduct_dagster.sources.hydrovu_common.fetch_location_data"

# ── Fixtures ──────────────────────────────────────────────────────────────────

_SONDE_ID = 6255051791532032  # SierraVista-966932 — carries DTW
_VULINK_ID = 4890597735137280  # E-94077-1193582VL — gateway diagnostics, no DTW
_DEFAULT_COORDS_ID = 4657879867523072  # default-969659 — In-Situ factory coordinates

_LOCATIONS = [
    {
        "id": _SONDE_ID,
        "name": "SierraVista-966932",
        "description": "",
        "gps": {"latitude": 35.123, "longitude": -106.353},
    },
    {
        "id": _VULINK_ID,
        "name": "E-94077-1193582VL (Anaya-1)",
        "description": "1193582",
        "gps": {"latitude": 35.062, "longitude": -106.151},
    },
    {
        "id": _DEFAULT_COORDS_ID,
        "name": "default-969659",
        "description": "",
        "gps": {"latitude": 40.588, "longitude": -105.066},
    },
]

# Sonde profile, trimmed to two parameters. The parameter array is deliberately not
# ordered by parameterId — the live API does not order it and nothing may depend on it.
_SONDE_DATA = {
    "locationId": _SONDE_ID,
    "parameters": [
        {
            "parameterId": "2",
            "unitId": "17",
            "customParameter": False,
            "readings": [{"timestamp": 1782346800, "value": 18.035131}],
        },
        {
            "parameterId": "4",
            "unitId": "35",
            "customParameter": False,
            "readings": [
                {"timestamp": 1782346800, "value": 70.972789728},
                {"timestamp": 1782361200, "value": 70.975276896},
            ],
        },
    ],
}

_DUMMY_CLIENT = MagicMock(spec=httpx.Client)


def _drive(
    location_ids: list[int],
    *,
    state: dict | None = None,
    start_ts: int = 1_777_000_000,
    stats: dict | None = None,
    locations: list[dict] | None = None,
    client: httpx.Client | None = None,
) -> list[dict]:
    """Runs hydrovu_readings to exhaustion and returns the rows it yielded."""
    with patch("dlt.current.resource_state", return_value=state or {"location_cursors": {}}):
        return list(
            hydrovu_readings(
                client=client or _DUMMY_CLIENT,
                start_ts=start_ts,
                locations=locations if locations is not None else _LOCATIONS,
                location_ids=location_ids,
                _stats=stats,
            )
        )


# ── hydrovu_locations ─────────────────────────────────────────────────────────


class TestHydroVuLocations:
    def test_flattens_gps_into_lat_lon_columns(self):
        rows = list(hydrovu_locations(locations=_LOCATIONS))
        assert rows[0] == {
            "id": _SONDE_ID,
            "name": "SierraVista-966932",
            "description": "",
            "latitude": 35.123,
            "longitude": -106.353,
        }

    def test_writes_every_location_not_just_the_allowlisted_ones(self):
        # The reference table is the record of what exists in the tenant, so it is
        # deliberately not filtered by the readings allowlist.
        rows = list(hydrovu_locations(locations=_LOCATIONS))
        assert {row["id"] for row in rows} == {_SONDE_ID, _VULINK_ID, _DEFAULT_COORDS_ID}


# ── hydrovu_readings — allowlist ──────────────────────────────────────────────


class TestBerncoReadingsAllowlist:
    @patch(_FETCH_TARGET)
    def test_only_fetches_allowlisted_locations(self, mock_fetch):
        mock_fetch.return_value = (_SONDE_DATA, None)
        _drive([_SONDE_ID])
        assert {call[0][1] for call in mock_fetch.call_args_list} == {_SONDE_ID}

    @patch(_FETCH_TARGET)
    def test_vulink_and_factory_default_locations_are_never_fetched(self, mock_fetch):
        # Both are excluded from the config allowlist: VuLink gateways hold no DTW at
        # all, and the factory-default locations would place a New Mexico observation
        # at In-Situ's Colorado headquarters.
        mock_fetch.return_value = (_SONDE_DATA, None)
        stats: dict = {}
        _drive([_SONDE_ID], stats=stats)
        called = {call[0][1] for call in mock_fetch.call_args_list}
        assert _VULINK_ID not in called
        assert _DEFAULT_COORDS_ID not in called
        assert stats["locations_skipped"] == 2

    @patch(_FETCH_TARGET)
    def test_empty_allowlist_fetches_nothing(self, mock_fetch):
        _drive([])
        mock_fetch.assert_not_called()


# ── hydrovu_readings — row shape ──────────────────────────────────────────────


class TestBerncoReadingsRows:
    @patch(_FETCH_TARGET)
    def test_yields_one_flat_row_per_parameter_reading(self, mock_fetch):
        mock_fetch.return_value = (_SONDE_DATA, None)
        rows = _drive([_SONDE_ID])
        assert len(rows) == 3  # 1 pressure + 2 DTW
        assert rows[1] == {
            "reading_id": f"{_SONDE_ID}_4_1782346800",
            "location_id": _SONDE_ID,
            "timestamp": 1782346800,
            "parameter_id": "4",
            "unit_id": "35",
            "value": 70.972789728,
        }

    @patch(_FETCH_TARGET)
    def test_does_not_filter_by_parameter(self, mock_fetch):
        # The raw zone keeps every parameter the API returned; selecting DTW is the
        # transform's job, so raw stays re-readable if the mapping ever changes.
        mock_fetch.return_value = (_SONDE_DATA, None)
        rows = _drive([_SONDE_ID])
        assert {row["parameter_id"] for row in rows} == {"2", "4"}

    @patch(_FETCH_TARGET)
    def test_location_metadata_is_not_embedded_in_readings(self, mock_fetch):
        mock_fetch.return_value = (_SONDE_DATA, None)
        rows = _drive([_SONDE_ID])
        assert "name" not in rows[0]
        assert "latitude" not in rows[0]


# ── hydrovu_readings — cursors ────────────────────────────────────────────────


class TestBerncoReadingsCursor:
    @patch(_FETCH_TARGET)
    def test_first_run_fetches_from_start_ts(self, mock_fetch):
        mock_fetch.return_value = (_SONDE_DATA, None)
        _drive([_SONDE_ID], start_ts=1_777_000_000)
        assert mock_fetch.call_args[0][2] == 1_777_000_000

    @patch(_FETCH_TARGET)
    def test_cursor_advances_to_max_timestamp_after_success(self, mock_fetch):
        mock_fetch.return_value = (_SONDE_DATA, None)
        state: dict = {"location_cursors": {}}
        _drive([_SONDE_ID], state=state)
        assert state["location_cursors"][str(_SONDE_ID)] == 1782361200

    @patch(_FETCH_TARGET)
    def test_second_run_fetches_only_from_the_stored_cursor(self, mock_fetch):
        mock_fetch.return_value = (_SONDE_DATA, None)
        state = {"location_cursors": {str(_SONDE_ID): 1782361200}}
        _drive([_SONDE_ID], state=state, start_ts=1_777_000_000)
        assert mock_fetch.call_args[0][2] == 1782361200

    @patch(_FETCH_TARGET)
    def test_cursor_never_moves_backwards_below_start_ts(self, mock_fetch):
        # A cursor older than initial_start_date must not widen the fetch window.
        mock_fetch.return_value = (_SONDE_DATA, None)
        state = {"location_cursors": {str(_SONDE_ID): 100}}
        _drive([_SONDE_ID], state=state, start_ts=1_777_000_000)
        assert mock_fetch.call_args[0][2] == 1_777_000_000

    @patch(_FETCH_TARGET)
    def test_error_does_not_advance_cursor(self, mock_fetch):
        mock_fetch.return_value = (None, "HTTP 503")
        state = {"location_cursors": {str(_SONDE_ID): 1782361200}}
        _drive([_SONDE_ID], state=state)
        assert state["location_cursors"][str(_SONDE_ID)] == 1782361200


# ── hydrovu_readings — per-station failure ────────────────────────────────────


class TestBerncoReadingsFailureIsolation:
    @patch(_FETCH_TARGET)
    def test_one_failed_station_does_not_block_the_others(self, mock_fetch):
        # Locations are visited in list order, so the failing one goes first: the
        # station after it must still be fetched and its rows must still land.
        mock_fetch.side_effect = [(None, "HTTP 503"), (_SONDE_DATA, None)]
        stats: dict = {}
        rows = _drive(
            [_VULINK_ID, _SONDE_ID],
            locations=[_LOCATIONS[1], _LOCATIONS[0]],
            stats=stats,
        )
        assert len(rows) == 3
        assert stats["locations_fetched"] == 1
        assert stats["locations_errored"] == 1
        assert stats["failed_location_ids"] == [_VULINK_ID]

    @patch(_FETCH_TARGET)
    def test_404_counts_as_no_data_not_an_error(self, mock_fetch):
        # A dormant BernCo location 404s on a recent startTime while still holding
        # history — expected, and not something to alert on.
        mock_fetch.return_value = (None, None)
        stats: dict = {}
        _drive([_SONDE_ID], stats=stats)
        assert stats["locations_no_data"] == 1
        assert stats["locations_errored"] == 0
        assert stats["failed_location_ids"] == []

    @patch(_FETCH_TARGET)
    def test_stats_cover_every_location(self, mock_fetch):
        mock_fetch.side_effect = [(_SONDE_DATA, None), (None, None)]
        stats: dict = {}
        _drive([_SONDE_ID, _VULINK_ID], stats=stats)
        assert stats == {
            "rows_yielded": 3,
            "locations_fetched": 1,
            "locations_skipped": 1,
            "locations_no_data": 1,
            "locations_errored": 0,
            "failed_location_ids": [],
        }


# ── hydrovu_readings — client lifetime ────────────────────────────────────────


class TestBerncoReadingsClientLifetime:
    @patch(_FETCH_TARGET)
    def test_closes_the_shared_client_when_the_generator_finishes(self, mock_fetch):
        # hydrovu_locations only yields pre-fetched dicts, so this resource owns the
        # shared client's lifetime. The close sits in a finally, which also covers dlt
        # abandoning the generator mid-run — not asserted here, because reaching that
        # path would mean driving dlt's own pipe machinery rather than this code.
        mock_fetch.return_value = (_SONDE_DATA, None)
        client = MagicMock(spec=httpx.Client)
        _drive([_SONDE_ID], client=client)
        client.close.assert_called_once()

    @patch(_FETCH_TARGET)
    def test_closes_the_shared_client_even_when_every_station_fails(self, mock_fetch):
        mock_fetch.return_value = (None, "HTTP 503")
        client = MagicMock(spec=httpx.Client)
        _drive([_SONDE_ID], client=client)
        client.close.assert_called_once()


# ── bernco_hydrovu_source — config binding ────────────────────────────────────


class TestBerncoSourceConfig:
    """
    @dlt.source(name="bernco_hydrovu") is what binds these defaults to the
    [sources.bernco_hydrovu] block of .dlt/config.toml. Nothing cross-checks the two,
    and getting it wrong is silent in the worst way: dlt would fall back to PVACD's
    block, and BernCo's pipeline would authenticate against the wrong tenant and fetch
    the wrong wells into the BernCo dataset.

    Offline — no client is built and no request is made; the values are read straight
    off the call the source makes into build_hydrovu_client.
    """

    @staticmethod
    def _resolved_config() -> dict:
        settings_dir()  # locate .dlt/config.toml without depending on the cwd
        captured: dict = {}

        def _fake_build(
            client_id: str, client_secret: str, gcp_secret: str, api_base_url: str, token_url: str
        ) -> httpx.Client:
            captured.update(gcp_secret=gcp_secret, api_base_url=api_base_url, token_url=token_url)
            return MagicMock(spec=httpx.Client)

        with (
            patch.object(bernco_dlt_pipeline, "build_hydrovu_client", side_effect=_fake_build),
            patch.object(bernco_dlt_pipeline, "fetch_locations", return_value=[]),
        ):
            bernco_hydrovu_source()
        return captured

    def test_reads_berncos_own_secret_not_pvacds(self):
        assert self._resolved_config()["gcp_secret"] == "hydrovu_bernco"

    def test_reads_the_hydrovu_endpoints(self):
        config = self._resolved_config()
        assert config["api_base_url"] == "https://www.hydrovu.com/public-api/v1"
        assert config["token_url"] == "https://hydrovu.com/public-api/oauth/token"

    def test_exposes_both_resources_under_the_shared_table_names(self):
        # Two tenants on one platform use the same table names; the dataset is what
        # separates them. See docs/STORAGE_CONVENTIONS.md.
        settings_dir()
        with (
            patch.object(
                bernco_dlt_pipeline,
                "build_hydrovu_client",
                return_value=MagicMock(spec=httpx.Client),
            ),
            patch.object(bernco_dlt_pipeline, "fetch_locations", return_value=[]),
        ):
            source = bernco_hydrovu_source()
        assert {resource.name for resource in source.resources.values()} == {
            "hydrovu_locations",
            "hydrovu_readings",
        }


# ── bernco_hydrovu_source — client lifetime on a failed location fetch ────────


class TestBerncoSourceClientLeak:
    def test_closes_the_client_when_the_location_list_fetch_fails(self):
        # The source builds the client, and hydrovu_readings normally closes it. If
        # fetch_locations raises, that resource is never constructed, so nobody else
        # would ever close it — a leaked connection pool on every failed run.
        settings_dir()
        client = MagicMock(spec=httpx.Client)
        with (
            patch.object(bernco_dlt_pipeline, "build_hydrovu_client", return_value=client),
            patch.object(
                bernco_dlt_pipeline, "fetch_locations", side_effect=httpx.ReadError("boom")
            ),
            pytest.raises(httpx.ReadError),
        ):
            bernco_hydrovu_source()
        client.close.assert_called_once()
