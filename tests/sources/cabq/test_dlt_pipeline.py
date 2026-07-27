"""
tests/sources/cabq/test_dlt_pipeline.py

Unit tests for CabQ DLT pipeline private helpers.
No real API calls, simulated via httpx.MockTransport.

Covers:
    _fetch_locations             -
    _fetch_readings_for_location -
    cabq_readings                -
"""

from __future__ import annotations

import httpx

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


# -- _fetch_readings_for_location

READINGS_PROCESSED = [{"measurement_date": 1391079600000, "water_level": "4927.15"}]

READINGS_RESPONSE = {"features": [{"attributes": READINGS_PROCESSED[0]}]}


class TestFetchReadings:
    def test_returns_list_on_success(self):
        client, _ = _client_with_responses([httpx.Response(200, json=READINGS_RESPONSE)])
        result, err = _fetch_readings_for_location(client, "IW4", loc_start=1391079600000)
        assert result == READINGS_PROCESSED
