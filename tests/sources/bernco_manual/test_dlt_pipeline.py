from datetime import UTC, datetime

import pytest
from httpx import Client, HTTPStatusError, Request, Response

from aqueduct_dagster.sources.bernco_manual.dlt_pipeline import (
    _fetch_locations,
    _fetch_readings_for_location,
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

    def test_raises_on_server_error(self):
        client, _ = _client_with_responses([Response(500)])
        data, err = _fetch_locations(client)
        assert data is None
        assert err is not None
        assert "500" in err

    def test_hits_correct_endpoint(self):
        client, calls = _client_with_responses([Response(200, json=LOCATIONS_RESPONSE)])
        _fetch_locations(client)
        assert calls[0].url.path == "/0/query"
        assert calls[0].url.params["f"] == "pjson"
        assert calls[0].url.params["returnDistinctValues"] == "true"
        assert (
            calls[0].url.params["outFields"]
            == "GlobalID,Well_Name,Well_Location_Latitude,Well_Location_Longitude,NMT_ID"
        )
        assert calls[0].url.params["where"] == "OBJECTID>0"


# -- _fetch_readings_for_location --

READINGS_PROCESSED = [
    {
        "MSRMNT_Date": 1573689600000,
        "Depth_To_Water_At_Msrmnt_Point": 711.11,
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

    def test_returns_none_on_404(self):
        client, _ = _client_with_responses([Response(404)])
        data, err = _fetch_readings_for_location(
            client, location_id="4f92c895-6b41-42d6-be6b-508ee44812fa", start_time=1391079600
        )
        assert data is None
        assert err is None

    def test_returns_error_reason_on_500(self):
        client, _ = _client_with_responses([Response(500)])
        data, err = _fetch_readings_for_location(
            client, location_id="4f92c895-6b41-42d6-be6b-508ee44812fa", start_time=1391079600
        )
        assert data is None
        assert err is not None
        assert "500" in err

    def test_returns_error_reason_on_503(self):
        client, _ = _client_with_responses([Response(503)])
        data, err = _fetch_readings_for_location(
            client, location_id="4f92c895-6b41-42d6-be6b-508ee44812fa", start_time=1391079600
        )
        assert data is None
        assert err is not None
        assert "503" in err

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
        assert calls[0].url.params["outFields"] == "MSRMNT_Date,Depth_To_Water_At_Msrmnt_Point"
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
        assert calls[0].url.params["outFields"] == "MSRMNT_Date,Depth_To_Water_At_Msrmnt_Point"
        assert (
            calls[0].url.params["where"]
            == "Well_ID='4f92c895-6b41-42d6-be6b-508ee44812fa' AND MSRMNT_Date>='2019-11-14' AND MSRMNT_Date<='2019-11-15'"
        )

    def test_raises_on_unexpected_4xx(self):
        client, _ = _client_with_responses([Response(403)])
        with pytest.raises(HTTPStatusError) as exc_info:
            _fetch_readings_for_location(
                client, location_id="4f92c895-6b41-42d6-be6b-508ee44812fa", start_time=1391079600
            )
        assert exc_info.value.response.status_code == 403
