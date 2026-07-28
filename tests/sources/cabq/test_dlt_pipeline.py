"""
tests/sources/cabq/test_dlt_pipeline.py

Unit tests for CABQ DLT pipeline.
No real API calls, simulated via httpx.MockTransport.          -
"""

from __future__ import annotations

import httpx
import pytest

from aqueduct_dagster.sources.cabq.dlt_pipeline import (
    _fetch_locations,
    _fetch_readings_for_location,
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
        result = _fetch_locations(client)
        assert result == LOCATIONS_PROCESSED

    def test_raises_on_server_error(self):
        client, _ = _client_with_responses([httpx.Response(500)])
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            _fetch_locations(client)
        assert exc_info.value.response.status_code == 500

    def test_hits_correct_endpoint(self):
        client, calls = _client_with_responses([httpx.Response(200, json=LOCATIONS_RESPONSE)])
        _fetch_locations(client)
        assert (
            str(calls[0].url)
            == "https://api/query?where=OBJECTID%3E0&outFields=sys_loc_code,loc_name,latitude,longitude&returnDistinctValues=true&f=pjson"
        )


# -- _fetch_readings_for_location

READINGS_PROCESSED = [{"measurement_date": 1391079600000, "water_level": "4927.15"}]

READINGS_RESPONSE = {"features": [{"attributes": READINGS_PROCESSED[0]}]}


class TestFetchReadings:
    def test_returns_list_on_success(self):
        client, _ = _client_with_responses([httpx.Response(200, json=READINGS_RESPONSE)])
        result, err = _fetch_readings_for_location(client, loc_id="IW4", loc_start=1391079600000)
        assert result == READINGS_PROCESSED

    def test_returns_none_on_404(self):
        client, _ = _client_with_responses([httpx.Response(404)])
        data, err = _fetch_readings_for_location(client, loc_id="IW4", loc_start=1391079600000)
        assert data is None
        assert err is None

    def test_returns_error_reason_on_500(self):
        client, _ = _client_with_responses([httpx.Response(500)])
        data, err = _fetch_readings_for_location(client, loc_id="IW4", loc_start=1391079600000)
        assert data is None
        assert err is not None
        assert "500" in err

    def test_returns_error_reason_on_503(self):
        client, _ = _client_with_responses([httpx.Response(503)])
        data, err = _fetch_readings_for_location(client, loc_id="IW4", loc_start=1391079600000)
        assert data is None
        assert err is not None
        assert "503" in err

    def test_hits_correct_endpoint(self):
        client, calls = _client_with_responses([httpx.Response(200, json=READINGS_RESPONSE)])
        _fetch_readings_for_location(client, loc_id="IW4", loc_start=1391079600000)
        assert (
            calls[0].url
            == "https://api/query?where=sys_loc_code%3D'IW4'&outfields=measurement_date,water_level&f=pjson"
        )

    def test_raises_on_unexpected_4xx(self):
        client, _ = _client_with_responses([httpx.Response(403)])
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            _fetch_readings_for_location(client, loc_id="IW4", loc_start=1391079600000)
        assert exc_info.value.response.status_code == 403
