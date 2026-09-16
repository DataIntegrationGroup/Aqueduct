from httpx import Client, Request, Response

from aqueduct_dagster.sources.bernco_manual.dlt_pipeline import (
    _fetch_locations,
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
