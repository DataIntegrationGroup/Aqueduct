from unittest.mock import MagicMock, patch

import httpx
import pytest

from aqueduct_dagster.sources.cabq.backfill import _locations_by_id, cabq_backfill_readings

_DUMMY_CLIENT = MagicMock(spec=httpx.Client)

_LOCATIONS = [
    {
        "sys_loc_code": "IW4",
        "loc_name": "LALF GROUNDWATER INJECTION WELL 4",
        "latitude": -106.599332407,
        "longitude": 35.170730266,
    }
]

_READINGS_DATA = [{"measurement_date": 1391079600000, "water_depth": 162.396}]

CABQ_RESULTS = {
    "reading_id": "IW4_1391079600000",
    "location_id": "IW4",
    "location_name": "LALF GROUNDWATER INJECTION WELL 4",
    "latitude": -106.599332407,
    "longitude": 35.170730266,
    "timestamp": 1391079600000,
    "value": 162.396,
}

# -- cabq_backfill_readings --------------


class TestCabqBackfillReadings:
    @patch("aqueduct_dagster.sources.cabq.backfill._fetch_readings_for_location")
    def test_passes_start_and_end_ts_through(self, mock_fetch):
        mock_fetch.return_value = (_READINGS_DATA, None)
        list(
            cabq_backfill_readings(
                client=_DUMMY_CLIENT,
                locations=[_LOCATIONS[0]],
                location_ids=["IW4"],
                start_ts=123,
                end_ts=456,
            )
        )
        _client, _loc_id, start_time, end_time = mock_fetch.call_args[0]
        assert start_time == 123
        assert end_time == 456

    @patch("aqueduct_dagster.sources.cabq.backfill._fetch_readings_for_location")
    def test_yields_flat_rows(self, mock_fetch):
        mock_fetch.return_value = (_READINGS_DATA, None)
        rows = list(
            cabq_backfill_readings(
                client=_DUMMY_CLIENT,
                locations=[_LOCATIONS[0]],
                location_ids=["IW4"],
                start_ts=123,
                end_ts=456,
            )
        )
        assert rows == [CABQ_RESULTS]

    @patch("aqueduct_dagster.sources.cabq.backfill._fetch_readings_for_location")
    def test_skips_location_on_404(self, mock_fetch):
        mock_fetch.return_value = (None, None)
        rows = list(
            cabq_backfill_readings(
                client=_DUMMY_CLIENT,
                locations=[_LOCATIONS[0]],
                location_ids=["IW4"],
                start_ts=123,
                end_ts=456,
            )
        )
        assert rows == []

    @patch("aqueduct_dagster.sources.cabq.backfill._fetch_readings_for_location")
    def test_raises_on_real_fetch_error(self, mock_fetch):
        mock_fetch.return_value = (None, "HTTP 500")
        with pytest.raises(Exception, match="HTTP 500"):
            list(
                cabq_backfill_readings(
                    client=_DUMMY_CLIENT,
                    locations=[_LOCATIONS[0]],
                    location_ids=["IW4"],
                    start_ts=123,
                    end_ts=456,
                )
            )

    @patch("aqueduct_dagster.sources.cabq.backfill._fetch_readings_for_location")
    def test_fetch_error_message_includes_the_chunk_window(self, mock_fetch):
        mock_fetch.return_value = (None, "HTTP 500")
        start_ts = 1767225600
        end_ts = 1769904000
        with pytest.raises(Exception, match=r"2026-01-01.*2026-02-01.*HTTP 500"):
            list(
                cabq_backfill_readings(
                    client=_DUMMY_CLIENT,
                    locations=[_LOCATIONS[0]],
                    location_ids=["IW4"],
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
            )


# -- _locaitons_by_id --------------


def test_locations_by_id_shape():
    result = _locations_by_id(_LOCATIONS)
    assert result["IW4"] == {
        "name": "LALF GROUNDWATER INJECTION WELL 4",
        "latitude": -106.599332407,
        "longitude": 35.170730266,
    }
