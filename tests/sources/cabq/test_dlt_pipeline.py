"""
tests/sources/cabq/test_dlt_pipeline.py

Unit tests for CABQ DLT pipeline.
No real API calls, simulated via httpx.MockTransport.          -
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from aqueduct_dagster.sources.cabq.dlt_pipeline import (
    _fetch_locations,
    _fetch_readings_for_location,
    cabq_readings,
)
from tests.conftest import client_with_responses_unauthenticated as _client_with_responses_base


def _client_with_responses(
    responses: list[httpx.Response | Exception],
) -> tuple[httpx.Client, list[httpx.Request]]:
    return _client_with_responses_base(responses, base_url="https://api")


# -- _fetch_locations --

LOCATIONS_PROCESSED = [
    {
        "sys_loc_code": "IW4",
        "loc_name": "LALF GROUNDWATER INJECTION WELL 4",
        "latitude": -106.599332407,
        "longitude": 35.170730266,
    }
]

LOCATIONS_RESPONSE = {"features": [{"attributes": LOCATIONS_PROCESSED[0]}]}


class TestFetchLocations:
    def test_returns_list_on_success(self):
        client, _ = _client_with_responses([httpx.Response(200, json=LOCATIONS_RESPONSE)])
        result, err = _fetch_locations(client)
        assert result == LOCATIONS_PROCESSED

    def test_raises_on_server_error(self):
        client, _ = _client_with_responses([httpx.Response(500)])
        data, err = _fetch_locations(client)
        assert data is None
        assert err is not None
        assert "500" in err

    def test_hits_correct_endpoint(self):
        client, calls = _client_with_responses([httpx.Response(200, json=LOCATIONS_RESPONSE)])
        _fetch_locations(client)
        assert calls[0].url.path == "/query"
        assert calls[0].url.path == "/query"
        assert calls[0].url.params["f"] == "pjson"
        assert calls[0].url.params["returnDistinctValues"] == "true"
        assert calls[0].url.params["outfields"] == "sys_loc_code,loc_name,latitude,longitude"
        assert calls[0].url.params["where"] == "OBJECTID>0"


# -- _fetch_readings_for_location --

READINGS_PROCESSED = [{"measurement_date": 1391079600000, "water_depth": 162.396}]

READINGS_RESPONSE = {"features": [{"attributes": READINGS_PROCESSED[0]}]}


class TestFetchReadings:
    def test_returns_list_on_success(self):
        client, _ = _client_with_responses([httpx.Response(200, json=READINGS_RESPONSE)])
        result, err = _fetch_readings_for_location(client, loc_id="IW4", start_time=1391079600)
        assert result == READINGS_PROCESSED

    def test_returns_none_on_404(self):
        client, _ = _client_with_responses([httpx.Response(404)])
        data, err = _fetch_readings_for_location(client, loc_id="IW4", start_time=1391079600)
        assert data is None
        assert err is None

    def test_returns_error_reason_on_500(self):
        client, _ = _client_with_responses([httpx.Response(500)])
        data, err = _fetch_readings_for_location(client, loc_id="IW4", start_time=1391079600)
        assert data is None
        assert err is not None
        assert "500" in err

    def test_returns_error_reason_on_503(self):
        client, _ = _client_with_responses([httpx.Response(503)])
        data, err = _fetch_readings_for_location(client, loc_id="IW4", start_time=1391079600)
        assert data is None
        assert err is not None
        assert "503" in err

    def test_hits_correct_endpoint(self):
        client, calls = _client_with_responses([httpx.Response(200, json=READINGS_RESPONSE)])
        start_time = int(
            datetime.strptime("2014-01-30", "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
        )
        _fetch_readings_for_location(client, loc_id="IW4", start_time=start_time)
        assert calls[0].url.path == "/query"
        assert calls[0].url.params["f"] == "pjson"
        assert calls[0].url.params["outfields"] == "measurement_date,water_depth"
        assert (
            calls[0].url.params["where"] == "sys_loc_code='IW4' AND measurement_date>='2014-01-30'"
        )

    def test_endtime_hits_correct_endpoint(self):
        client, calls = _client_with_responses([httpx.Response(200, json=READINGS_RESPONSE)])
        start_time = int(
            datetime.strptime("2014-01-30", "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
        )
        end_time = int(datetime.strptime("2015-02-03", "%Y-%m-%d").replace(tzinfo=UTC).timestamp())
        _fetch_readings_for_location(client, loc_id="IW4", start_time=start_time, end_time=end_time)
        assert calls[0].url.path == "/query"
        assert calls[0].url.params["f"] == "pjson"
        assert calls[0].url.params["outfields"] == "measurement_date,water_depth"
        assert (
            calls[0].url.params["where"]
            == "sys_loc_code='IW4' AND measurement_date>='2014-01-30' AND measurement_date<='2015-02-03'"
        )

    def test_raises_on_unexpected_4xx(self):
        client, _ = _client_with_responses([httpx.Response(403)])
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            _fetch_readings_for_location(client, loc_id="IW4", start_time=1391079600)
        assert exc_info.value.response.status_code == 403


# -- cabq_readings --

CABQ_RESULTS = {
    "reading_id": "IW4_1391079600000",
    "location_id": "IW4",
    "location_name": "LALF GROUNDWATER INJECTION WELL 4",
    "latitude": -106.599332407,
    "longitude": 35.170730266,
    "timestamp": 1391079600000,
    "value": 162.396,
}

DUMMY_CLIENT = MagicMock(spec=httpx.Client)


class TestCabqReadings:
    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.cabq.dlt_pipeline._fetch_readings_for_location")
    @patch("aqueduct_dagster.sources.cabq.dlt_pipeline._fetch_locations")
    def test_returns_cabq_readings(self, mock_fetch_locations, mock_fetch_readings, mock_state):
        mock_fetch_locations.return_value = (LOCATIONS_PROCESSED, None)
        mock_fetch_readings.return_value = (READINGS_PROCESSED, None)
        results = list(cabq_readings(client=DUMMY_CLIENT, start_ts=1000))
        assert mock_fetch_locations.called
        assert mock_fetch_readings.called
        assert mock_fetch_readings.call_args.args.__contains__("IW4")
        assert mock_fetch_readings.call_args.args.__contains__(1000)
        assert results[0] == CABQ_RESULTS

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.cabq.dlt_pipeline._fetch_readings_for_location")
    @patch("aqueduct_dagster.sources.cabq.dlt_pipeline._fetch_locations")
    def test_real_error_increments_errored_count(
        self, mock_fetch_locations, mock_fetch_readings, mock_state
    ):
        mock_fetch_locations.return_value = (LOCATIONS_PROCESSED, None)
        mock_fetch_readings.return_value = (None, "HTTP 500")
        stats: dict = {}
        list(cabq_readings(client=DUMMY_CLIENT, start_ts=1000, _stats=stats))
        assert stats["locations_errored"] == 1

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.cabq.dlt_pipeline._fetch_readings_for_location")
    @patch("aqueduct_dagster.sources.cabq.dlt_pipeline._fetch_locations")
    def test_404_does_not_increment_errored_count(
        self, mock_fetch_locations, mock_fetch_readings, mock_state
    ):
        mock_fetch_locations.return_value = (LOCATIONS_PROCESSED, None)
        mock_fetch_readings.return_value = (None, None)
        stats: dict = {}
        list(cabq_readings(client=DUMMY_CLIENT, start_ts=1000, _stats=stats))
        assert stats["locations_errored"] == 0
        assert stats["locations_no_data"] == 1
        assert stats["failed_location_ids"] == []

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.cabq.dlt_pipeline._fetch_readings_for_location")
    @patch("aqueduct_dagster.sources.cabq.dlt_pipeline._fetch_locations")
    def test_error_does_not_advance_cursor(
        self, mock_fetch_locations, mock_fetch_readings, mock_state
    ):
        mock_fetch_locations.return_value = (LOCATIONS_PROCESSED, None)
        mock_fetch_readings.return_value = (None, "HTTP 500")
        state: dict = {"location_cursors": {"IW4": 1000}}
        with patch("dlt.current.resource_state", return_value=state):
            list(cabq_readings(client=DUMMY_CLIENT, start_ts=1000))
        assert state["location_cursors"] == {"IW4": 1000}  # unchanged

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.cabq.dlt_pipeline._fetch_readings_for_location")
    @patch("aqueduct_dagster.sources.cabq.dlt_pipeline._fetch_locations")
    def test_partial_failure_stats(self, mock_fetch_locations, mock_fetch_readings, mock_state):
        locations = [
            {
                "sys_loc_code": "IW4",
                "loc_name": "LALF GROUNDWATER INJECTION WELL 4",
                "latitude": -106.599332407,
                "longitude": 35.170730266,
            },
            {
                "sys_loc_code": "IW3",
                "loc_name": "LALF GROUNDWATER INJECTION WELL 3",
                "latitude": -106.599332407,
                "longitude": 35.170730266,
            },
        ]
        mock_fetch_locations.return_value = (locations, None)
        # IW4 succeeds, IW3 fails
        mock_fetch_readings.side_effect = [(READINGS_PROCESSED, None), (None, "HTTP 500")]
        stats: dict = {}
        list(cabq_readings(client=DUMMY_CLIENT, start_ts=1000, _stats=stats))
        assert stats["locations_fetched"] == 1
        assert stats["locations_errored"] == 1
        assert stats["failed_location_ids"] == ["IW3"]
