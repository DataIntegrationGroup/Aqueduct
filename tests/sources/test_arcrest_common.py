"""
Unit tests for Common ArcREST code implementation.
The focus of these tests is on generic data handling and error handling
"""

import pytest
from httpx import Client, HTTPStatusError, Request, Response

from aqueduct_dagster.sources.arcrest_common import fetch_locations, fetch_readings_for_location
from tests.conftest import client_with_responses_unauthenticated as _client_with_responses_base


def _client_with_responses(
    responses: list[Response | Exception],
) -> tuple[Client, list[Request]]:
    return _client_with_responses_base(responses, base_url="https://api")


SAMPLE_DATA = [
    {
        "some_field": "some_value",
        "some_other_field": "some_other_value",
    }
]

SAMPLE_RESPONSE = {"features": [{"attributes": SAMPLE_DATA[0]}]}

# -- fetch_locations --


class TestFetchLocations:
    def test_returns_list_on_success(self):
        client, _ = _client_with_responses([Response(200, json=SAMPLE_RESPONSE)])
        result, err = fetch_locations(client, "/path", {"some_param": "some_value"})
        assert result == SAMPLE_DATA

    def test_hits_correct_endpoint(self):
        client, calls = _client_with_responses([Response(200, json=SAMPLE_RESPONSE)])
        fetch_locations(client, "/path", {"some_param": "some_value"})
        assert calls[0].url.path == "/path"
        assert calls[0].url.params["some_param"] == "some_value"

    def test_raises_on_server_error(self):
        client, _ = _client_with_responses([Response(500)])
        data, err = fetch_locations(client, "/path", {"some_param": "some_value"})
        assert data is None
        assert err is not None
        assert "500" in err

    def test_raises_on_unexpected_4xx(self):
        client, _ = _client_with_responses([Response(403)])
        with pytest.raises(HTTPStatusError) as exc_info:
            fetch_locations(client, "/path", {"some_param": "some_value"})
        assert exc_info.value.response.status_code == 403


# -- fetch_readings_for_locations --


class TestFetchReadings:
    def test_returns_list_on_success(self):
        client, _ = _client_with_responses([Response(200, json=SAMPLE_RESPONSE)])
        result, err = fetch_readings_for_location(
            client, "/path", {"some_param": "some_value"}, "some_location"
        )
        assert result == SAMPLE_DATA

    def test_hits_correct_endpoint(self):
        client, calls = _client_with_responses([Response(200, json=SAMPLE_RESPONSE)])
        fetch_readings_for_location(client, "/path", {"some_param": "some_value"}, "some_location")
        assert calls[0].url.path == "/path"
        assert calls[0].url.params["some_param"] == "some_value"

    def test_returns_none_on_404(self):
        client, _ = _client_with_responses([Response(404)])
        data, err = fetch_readings_for_location(
            client, "/path", {"some_param": "some_value"}, "some_location"
        )
        assert data is None
        assert err is None

    def test_returns_error_reason_on_500(self):
        client, _ = _client_with_responses([Response(500)])
        data, err = fetch_readings_for_location(
            client, "/path", {"some_param": "some_value"}, "some_location"
        )
        assert data is None
        assert err is not None
        assert "500" in err

    def test_returns_error_reason_on_503(self):
        client, _ = _client_with_responses([Response(503)])
        data, err = fetch_readings_for_location(
            client, "/path", {"some_param": "some_value"}, "some_location"
        )
        assert data is None
        assert err is not None
        assert "503" in err

    def test_raises_on_unexpected_4xx(self):
        client, _ = _client_with_responses([Response(403)])
        with pytest.raises(HTTPStatusError) as exc_info:
            fetch_readings_for_location(
                client, "/path", {"some_param": "some_value"}, "some_location"
            )
        assert exc_info.value.response.status_code == 403
