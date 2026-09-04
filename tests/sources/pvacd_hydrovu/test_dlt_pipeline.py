"""
tests/sources/pvacd_hydrovu/test_dlt_pipeline.py

Unit tests for PVACD's hydrovu_readings resource.

The vendor-level HTTP client this resource fetches through (fetch_locations,
fetch_location_data, credential resolution, pagination, 404/429/5xx handling) is
shared with the other HydroVu tenants and tested once in
tests/sources/test_hydrovu_common.py. What is tested here is what this module owns:
the allowlist, and the per-location cursors living in dlt resource state.

No real API calls — fetch_location_data is patched at the shared module, which is
where the resource resolves it from, and dlt.current.resource_state() is patched with
a plain dict so the generator can be driven directly.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx

from aqueduct_dagster.sources.pvacd_hydrovu.dlt_pipeline import hydrovu_readings

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
    @patch("aqueduct_dagster.sources.hydrovu_common.fetch_location_data")
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
    @patch("aqueduct_dagster.sources.hydrovu_common.fetch_location_data")
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
    @patch("aqueduct_dagster.sources.hydrovu_common.fetch_location_data")
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
    @patch("aqueduct_dagster.sources.hydrovu_common.fetch_location_data")
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
    @patch("aqueduct_dagster.sources.hydrovu_common.fetch_location_data")
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
    @patch("aqueduct_dagster.sources.hydrovu_common.fetch_location_data")
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
    @patch("aqueduct_dagster.sources.hydrovu_common.fetch_location_data")
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
