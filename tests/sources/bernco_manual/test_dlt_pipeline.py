"""
Unit tests for sources/bernco_manual/dlt_pipeline.py's fetch helpers and
bernco_manual_readings resource. No real API calls — httpx.MockTransport
simulates responses.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from httpx import Client, Request, Response

from aqueduct_dagster.sources.bernco_manual.dlt_pipeline import (
    _fetch_locations,
    _fetch_readings_for_location,
    bernco_manual_readings,
)
from tests.conftest import client_with_responses_unauthenticated as _client_with_responses_base


def _client_with_responses(
    responses: list[Response | Exception],
) -> tuple[Client, list[Request]]:
    return _client_with_responses_base(responses, base_url="https://api")


# -- _fetch_locations --

LOCATIONS_PROCESSED = [
    {
        "GlobalID": "4f92c895-6b41-42d6-be6b-508ee44812fa",
        "Well_Name": "9-Mile Hill LF",
        "Well_Depth": 740,
        "Well_Casing_Stickup": 0,
        "Well_Location_Latitude": 35.071526,
        "Well_Location_Longitude": -106.778477,
        "NMT_ID": "BC-0364",
    }
]

LOCATIONS_RESPONSE = {"features": [{"attributes": LOCATIONS_PROCESSED[0]}]}


class TestFetchLocations:
    def test_returns_list_on_success(self):
        client, _ = _client_with_responses([Response(200, json=LOCATIONS_RESPONSE)])
        result, err = _fetch_locations(client)
        assert result == LOCATIONS_PROCESSED

    def test_hits_correct_endpoint(self):
        client, calls = _client_with_responses([Response(200, json=LOCATIONS_RESPONSE)])
        _fetch_locations(client)
        assert calls[0].url.path == "/0/query"
        assert calls[0].url.params["f"] == "pjson"
        assert calls[0].url.params["returnDistinctValues"] == "true"
        assert (
            calls[0].url.params["outFields"]
            == "GlobalID,Well_Name,Well_Depth,Well_Casing_Stickup,Well_Location_Latitude,Well_Location_Longitude,NMT_ID"
        )
        assert calls[0].url.params["where"] == "OBJECTID>0"


# -- _fetch_readings_for_location --

READINGS_PROCESSED = [
    {
        "MSRMNT_Date": 1573689600000,
        "Depth_To_Water_At_Msrmnt_Point": 711.11,
        "MeasuringPt": None,
        "Measurement_Method": "unknown",
    }
]

READINGS_RESPONSE = {"features": [{"attributes": READINGS_PROCESSED[0]}]}


class TestFetchReadings:
    def test_returns_list_on_success(self):
        client, _ = _client_with_responses([Response(200, json=READINGS_RESPONSE)])
        result, err = _fetch_readings_for_location(
            client, location_id="4f92c895-6b41-42d6-be6b-508ee44812fa", start_time=1391079600
        )
        assert result == READINGS_PROCESSED

    def test_hits_correct_endpoint(self):
        client, calls = _client_with_responses([Response(200, json=READINGS_RESPONSE)])
        start_time = int(
            datetime.strptime("2019-11-14", "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
        )
        _fetch_readings_for_location(
            client, location_id="4f92c895-6b41-42d6-be6b-508ee44812fa", start_time=start_time
        )
        assert calls[0].url.path == "/1/query"
        assert calls[0].url.params["f"] == "pjson"
        assert (
            calls[0].url.params["outFields"]
            == "MSRMNT_Date,Depth_To_Water_At_Msrmnt_Point,MeasuringPt,Measurement_Method"
        )
        assert (
            calls[0].url.params["where"]
            == "Well_ID='4f92c895-6b41-42d6-be6b-508ee44812fa' AND MSRMNT_Date>='2019-11-14'"
        )

    def test_endtime_hits_correct_endpoint(self):
        client, calls = _client_with_responses([Response(200, json=READINGS_RESPONSE)])
        start_time = int(
            datetime.strptime("2019-11-14", "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
        )
        end_time = int(datetime.strptime("2019-11-15", "%Y-%m-%d").replace(tzinfo=UTC).timestamp())
        _fetch_readings_for_location(
            client,
            location_id="4f92c895-6b41-42d6-be6b-508ee44812fa",
            start_time=start_time,
            end_time=end_time,
        )
        assert calls[0].url.path == "/1/query"
        assert calls[0].url.params["f"] == "pjson"
        assert (
            calls[0].url.params["outFields"]
            == "MSRMNT_Date,Depth_To_Water_At_Msrmnt_Point,MeasuringPt,Measurement_Method"
        )
        assert (
            calls[0].url.params["where"]
            == "Well_ID='4f92c895-6b41-42d6-be6b-508ee44812fa' AND MSRMNT_Date>='2019-11-14' AND MSRMNT_Date<='2019-11-15'"
        )


# -- bernco_manual_readings --

BERNCO_MANUAL_RESULTS = {
    "reading_id": "4f92c895-6b41-42d6-be6b-508ee44812fa_1573689600000",
    "location_id": "4f92c895-6b41-42d6-be6b-508ee44812fa",
    "location_name": "9-Mile Hill LF",
    "well_depth": 740,
    "well_casing_stickup": 0,
    "latitude": 35.071526,
    "longitude": -106.778477,
    "timestamp": 1573689600000,
    "value": 711.11,
    "measurement_point_height": 0,
    "measurement_method": "unknown",
    "alternate_id": "BC-0364",
}

DUMMY_CLIENT = MagicMock(spec=Client)


class TestBerncoManualReadings:
    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.bernco_manual.dlt_pipeline._fetch_readings_for_location")
    def test_returns_bernco_manual_readings(self, mock_fetch_readings, mock_state):
        mock_fetch_readings.return_value = (READINGS_PROCESSED, None)
        results = list(
            bernco_manual_readings(
                client=DUMMY_CLIENT, locations=LOCATIONS_PROCESSED, start_ts=1000
            )
        )
        assert mock_fetch_readings.called
        assert mock_fetch_readings.call_args.args.__contains__(
            "4f92c895-6b41-42d6-be6b-508ee44812fa"
        )
        assert mock_fetch_readings.call_args.args.__contains__(1000)
        assert results[0] == BERNCO_MANUAL_RESULTS

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.bernco_manual.dlt_pipeline._fetch_readings_for_location")
    def test_real_error_increments_errored_count(self, mock_fetch_readings, mock_state):
        mock_fetch_readings.return_value = (None, "HTTP 500")
        stats: dict = {}
        list(
            bernco_manual_readings(
                client=DUMMY_CLIENT, locations=LOCATIONS_PROCESSED, start_ts=1000, _stats=stats
            )
        )
        assert stats["locations_errored"] == 1

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.bernco_manual.dlt_pipeline._fetch_readings_for_location")
    def test_404_does_not_increment_errored_count(self, mock_fetch_readings, mock_state):
        mock_fetch_readings.return_value = (None, None)
        stats: dict = {}
        list(
            bernco_manual_readings(
                client=DUMMY_CLIENT, locations=LOCATIONS_PROCESSED, start_ts=1000, _stats=stats
            )
        )
        assert stats["locations_errored"] == 0
        assert stats["locations_no_data"] == 1
        assert stats["failed_location_ids"] == []

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.bernco_manual.dlt_pipeline._fetch_readings_for_location")
    def test_error_does_not_advance_cursor(self, mock_fetch_readings, mock_state):
        mock_fetch_readings.return_value = (None, "HTTP 500")
        state: dict = {"location_cursors": {"4f92c895-6b41-42d6-be6b-508ee44812fa": 1000}}
        with patch("dlt.current.resource_state", return_value=state):
            list(
                bernco_manual_readings(
                    client=DUMMY_CLIENT, locations=LOCATIONS_PROCESSED, start_ts=1000
                )
            )
        assert state["location_cursors"] == {
            "4f92c895-6b41-42d6-be6b-508ee44812fa": 1000
        }  # unchanged

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.bernco_manual.dlt_pipeline._fetch_readings_for_location")
    def test_partial_failure_stats(self, mock_fetch_readings, mock_state):
        locations = [
            {
                "GlobalID": "4f92c895-6b41-42d6-be6b-508ee44812fa",
                "Well_Name": "9-Mile Hill LF",
                "Well_Depth": 740,
                "Well_Location_Latitude": 35.071526,
                "Well_Location_Longitude": -106.778477,
                "Well_Casing_Stickup": 0,
                "NMT_ID": "BC-0364",
            },
            {
                "GlobalID": "4f92c895-6b41-42d6-be6b-508ee44812fb",
                "Well_Name": "8-Mile Hill LF",
                "Well_Depth": 740,
                "Well_Location_Latitude": 35.071526,
                "Well_Location_Longitude": -106.778477,
                "Well_Casing_Stickup": 0,
                "NMT_ID": "BC-0365",
            },
        ]
        mock_fetch_readings.side_effect = [(READINGS_PROCESSED, None), (None, "HTTP 500")]
        stats: dict = {}
        list(
            bernco_manual_readings(
                client=DUMMY_CLIENT, locations=locations, start_ts=1000, _stats=stats
            )
        )
        assert stats["locations_fetched"] == 1
        assert stats["locations_errored"] == 1
        assert stats["failed_location_ids"] == ["4f92c895-6b41-42d6-be6b-508ee44812fb"]

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.bernco_manual.dlt_pipeline._fetch_readings_for_location")
    def test_partial_failure_stats_null_well_stickup(self, mock_fetch_readings, mock_state):
        locations = [
            {
                "GlobalID": "4f92c895-6b41-42d6-be6b-508ee44812fa",
                "Well_Name": "9-Mile Hill LF",
                "Well_Depth": 740,
                "Well_Location_Latitude": 35.071526,
                "Well_Location_Longitude": -106.778477,
                "Well_Casing_Stickup": None,
                "NMT_ID": "BC-0364",
            },
        ]
        mock_fetch_readings.side_effect = [(READINGS_PROCESSED, None)]
        stats: dict = {}
        list(
            bernco_manual_readings(
                client=DUMMY_CLIENT, locations=locations, start_ts=1000, _stats=stats
            )
        )
        assert stats["locations_errored"] == 1
        assert stats["failed_location_ids"] == ["4f92c895-6b41-42d6-be6b-508ee44812fa"]
