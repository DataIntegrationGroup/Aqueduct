"""
tests/sources/test_hydrovu_common.py

Unit tests for the shared HydroVu API client (sources/hydrovu_common.py), the
vendor-level code every HydroVu tenant runs through.

No real API calls — HTTP interactions are simulated via httpx.MockTransport,
so requests go through a real httpx.Client + BearerAuth (exercising real
httpx semantics: raise_for_status, headers, auth_flow) without patching
httpx.get.

Covers:
  fetch_locations      — success, pagination, error propagation, transient retry
  fetch_location_data  — typed result tuple: success, 404, 5xx, 429, transient errors
  resolve_hydrovu_credentials / build_hydrovu_client — Secret Manager and auth wiring

iter_location_readings is exercised through each tenant's own resource, in
tests/sources/<tenant>/test_dlt_pipeline.py, since the cursor state it mutates is
owned there.

TokenManager/BearerAuth's own behavior (401 refresh-and-retry, token caching)
is covered in tests/shared/test_http.py — the 401 tests here only confirm
fetch_locations/fetch_location_data are wired to the client correctly.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import httpx
import pytest

from aqueduct_dagster.shared.http import BearerAuth, TokenManager
from aqueduct_dagster.sources.hydrovu_common import (
    build_hydrovu_client,
    fetch_location_data,
    fetch_locations,
    resolve_hydrovu_credentials,
)
from tests.conftest import client_with_responses as _client_with_responses_base
from tests.conftest import make_tm as _make_tm

# ── Fixtures / shared test data ───────────────────────────────────────────────

LOCATIONS_RESPONSE = [
    {
        "id": 4503618672918528,
        "name": "Bartlett-827276",
        "description": "827276",
        "gps": {"latitude": 35.1, "longitude": -106.5},
    }
]

DATA_RESPONSE = {
    "parameters": [
        {
            "parameterId": "33",
            "unitId": "241",
            "readings": [
                {"timestamp": 1780704000, "value": 42.5},
                {"timestamp": 1780707600, "value": 43.0},
            ],
        }
    ]
}


def _client_with_responses(
    responses: list[httpx.Response | Exception], tm: TokenManager | None = None
) -> tuple[httpx.Client, list[httpx.Request]]:
    """Thin wrapper over tests.conftest.client_with_responses fixing base_url
    to HydroVu's mock API root, so call sites below don't repeat it."""
    return _client_with_responses_base(responses, tm=tm, base_url="https://api")


# ── fetch_locations ───────────────────────────────────────────────────────────


class TestFetchLocations:
    def test_returns_list_on_success(self):
        client, _ = _client_with_responses([httpx.Response(200, json=LOCATIONS_RESPONSE)])
        result = fetch_locations(client)
        assert result == LOCATIONS_RESPONSE

    def test_sends_empty_start_page_header(self):
        # First request sends X-ISI-Start-Page="" (empty cursor); the response's
        # X-ISI-Next-Page token drives subsequent pages.
        client, calls = _client_with_responses([httpx.Response(200, json=LOCATIONS_RESPONSE)])
        fetch_locations(client)
        assert calls[0].headers["X-ISI-Start-Page"] == ""

    def test_sends_bearer_token(self):
        client, calls = _client_with_responses(
            [httpx.Response(200, json=LOCATIONS_RESPONSE)], tm=_make_tm("my-token")
        )
        fetch_locations(client)
        assert calls[0].headers["Authorization"] == "Bearer my-token"

    def test_401_then_success_returns_list(self):
        # BearerAuth's refresh-and-retry-on-401 behavior is unit-tested in
        # tests/shared/test_http.py — this only confirms fetch_locations is
        # wired to the client (not calling httpx.get directly).
        client, calls = _client_with_responses(
            [httpx.Response(401), httpx.Response(200, json=LOCATIONS_RESPONSE)]
        )
        result = fetch_locations(client)
        assert result == LOCATIONS_RESPONSE
        assert calls[1].headers["Authorization"] == "Bearer tok-new"

    def test_raises_on_server_error(self):
        client, _ = _client_with_responses([httpx.Response(500)])
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            fetch_locations(client)
        assert exc_info.value.response.status_code == 500

    def test_hits_correct_endpoint(self):
        client, calls = _client_with_responses([httpx.Response(200, json=LOCATIONS_RESPONSE)])
        fetch_locations(client)
        assert str(calls[0].url) == "https://api/locations/list"

    def test_paginates_using_next_page_header(self):
        client, calls = _client_with_responses(
            [
                httpx.Response(200, json=[{"id": 1}], headers={"X-ISI-Next-Page": "cursor-2"}),
                httpx.Response(200, json=[{"id": 2}]),
            ]
        )
        result = fetch_locations(client)
        assert result == [{"id": 1}, {"id": 2}]
        assert calls[0].headers["X-ISI-Start-Page"] == ""
        assert calls[1].headers["X-ISI-Start-Page"] == "cursor-2"

    def test_transient_error_retries_then_succeeds(self):
        client, calls = _client_with_responses(
            [httpx.ReadError("reset"), httpx.Response(200, json=LOCATIONS_RESPONSE)]
        )
        with patch("time.sleep"):
            result = fetch_locations(client)
        assert result == LOCATIONS_RESPONSE
        assert len(calls) == 2


# ── fetch_location_data ──────────────────────────────────────────────────────


class TestFetchLocationData:
    def test_returns_data_and_no_error_on_success(self):
        client, _ = _client_with_responses([httpx.Response(200, json=DATA_RESPONSE)])
        data, err = fetch_location_data(client, 123, 1780704000)
        assert data == DATA_RESPONSE
        assert err is None

    def test_returns_none_none_on_404(self):
        client, _ = _client_with_responses([httpx.Response(404)])
        data, err = fetch_location_data(client, 123, 1780704000)
        assert data is None
        assert err is None

    def test_404_does_not_raise(self):
        client, _ = _client_with_responses([httpx.Response(404)])
        fetch_location_data(client, 123, 1780704000)  # must not raise

    def test_returns_error_reason_on_500(self):
        client, _ = _client_with_responses([httpx.Response(500)])
        data, err = fetch_location_data(client, 123, 1780704000)
        assert data is None
        assert err is not None
        assert "500" in err

    def test_returns_error_reason_on_503(self):
        client, _ = _client_with_responses([httpx.Response(503)])
        data, err = fetch_location_data(client, 123, 1780704000)
        assert data is None
        assert err is not None
        assert "503" in err

    def test_401_then_success_returns_data(self):
        client, calls = _client_with_responses(
            [httpx.Response(401), httpx.Response(200, json=DATA_RESPONSE)]
        )
        data, err = fetch_location_data(client, 123, 1780704000)
        assert data == DATA_RESPONSE
        assert err is None
        assert calls[1].headers["Authorization"] == "Bearer tok-new"

    def test_passes_start_time_as_query_param(self):
        client, calls = _client_with_responses([httpx.Response(200, json=DATA_RESPONSE)])
        fetch_location_data(client, 123, 1780704000)
        assert calls[0].url.params["startTime"] == "1780704000"

    def test_hits_correct_endpoint(self):
        client, calls = _client_with_responses([httpx.Response(200, json=DATA_RESPONSE)])
        fetch_location_data(client, 123, 1780704000)
        assert calls[0].url.path == "/locations/123/data"

    def test_raises_on_unexpected_4xx(self):
        client, _ = _client_with_responses([httpx.Response(403)])
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            fetch_location_data(client, 123, 1780704000)
        assert exc_info.value.response.status_code == 403

    def test_transient_error_exhausted_returns_error_reason(self):
        # Handler raises on every attempt — retry_transient exhausts its
        # budget (3 attempts) and fetch_location_data converts that into an
        # error-reason tuple rather than propagating.
        client, _ = _client_with_responses([httpx.ReadError("reset")] * 3)
        with patch("time.sleep"):
            data, err = fetch_location_data(client, 123, 1780704000)
        assert data is None
        assert err is not None
        assert "transient" in err.lower()

    def test_429_returns_error_after_exhausted_retries(self):
        client, _ = _client_with_responses([httpx.Response(429)] * 4)
        with patch("time.sleep"):
            data, err = fetch_location_data(client, 123, 1780704000)
        assert data is None
        assert err is not None
        assert "429" in err

    def test_429_respects_retry_after_header(self):
        client, _ = _client_with_responses([httpx.Response(429, headers={"Retry-After": "30"})] * 4)
        with patch("time.sleep") as mock_sleep:
            fetch_location_data(client, 123, 1780704000)
        assert mock_sleep.call_args_list[0][0][0] == 30.0

    def test_429_uses_default_backoff_when_no_retry_after(self):
        from aqueduct_dagster.sources.hydrovu_common import _429_BACKOFF

        client, _ = _client_with_responses([httpx.Response(429)] * 4)
        with patch("time.sleep") as mock_sleep:
            fetch_location_data(client, 123, 1780704000)
        assert mock_sleep.call_args_list[0][0][0] == _429_BACKOFF

    def test_429_succeeds_after_one_retry(self):
        client, _ = _client_with_responses(
            [
                httpx.Response(429, headers={"Retry-After": "1"}),
                httpx.Response(200, json=DATA_RESPONSE),
            ]
        )
        with patch("time.sleep"):
            data, err = fetch_location_data(client, 123, 1780704000)
        assert data == DATA_RESPONSE
        assert err is None


# ── fetch_location_data — end_time truncation (used by backfill chunks) ─────


class TestFetchLocationDataEndTime:
    def test_drops_readings_at_or_after_end_time(self):
        data = {
            "parameters": [
                {
                    "parameterId": "4",
                    "unitId": "35",
                    "readings": [
                        {"timestamp": 100, "value": 1.0},
                        {"timestamp": 200, "value": 2.0},
                        {"timestamp": 300, "value": 3.0},
                    ],
                }
            ]
        }
        client, _ = _client_with_responses([httpx.Response(200, json=data)])
        result, err = fetch_location_data(client, 123, start_time=0, end_time=200)
        assert err is None
        assert [r["timestamp"] for r in result["parameters"][0]["readings"]] == [100]

    def test_stops_paginating_once_end_time_reached(self):
        page1 = {
            "parameters": [
                {
                    "parameterId": "4",
                    "unitId": "35",
                    "readings": [
                        {"timestamp": 100, "value": 1.0},
                        {"timestamp": 250, "value": 2.5},
                    ],
                }
            ]
        }
        page2 = {
            "parameters": [
                {"parameterId": "4", "unitId": "35", "readings": [{"timestamp": 400, "value": 4.0}]}
            ]
        }
        client, calls = _client_with_responses(
            [
                httpx.Response(200, json=page1, headers={"X-ISI-Next-Page": "cursor-2"}),
                httpx.Response(200, json=page2),
            ]
        )
        result, err = fetch_location_data(client, 123, start_time=0, end_time=200)
        assert err is None
        assert len(calls) == 1  # never fetched page 2 — reached end_time on page 1
        assert [r["timestamp"] for r in result["parameters"][0]["readings"]] == [100]

    def test_no_end_time_is_unbounded_like_before(self):
        client, calls = _client_with_responses(
            [
                httpx.Response(200, json=DATA_RESPONSE, headers={"X-ISI-Next-Page": "cursor-2"}),
                httpx.Response(200, json={"parameters": []}),
            ]
        )
        result, err = fetch_location_data(client, 123, 1780704000)
        assert err is None
        assert len(calls) == 2  # paginates all the way through, no early stop


# ── resolve_hydrovu_credentials / build_hydrovu_client ────────────────────────


class TestResolveHydroVuCredentials:
    def test_returns_immediately_when_client_id_already_given(self):
        result = resolve_hydrovu_credentials("cid", "csecret", "ignored-secret-name")
        assert result == ("cid", "csecret")

    @patch("aqueduct_dagster.sources.hydrovu_common.secretmanager.SecretManagerServiceClient")
    @patch("aqueduct_dagster.sources.hydrovu_common.ensure_adc")
    @patch("aqueduct_dagster.sources.hydrovu_common.load_config")
    def test_fetches_from_secret_manager_when_client_id_empty(
        self, mock_load_config, mock_ensure_adc, mock_sm_cls
    ):
        mock_load_config.return_value = {
            "destination": {"filesystem": {"gcp_project_number": "12345"}}
        }
        mock_sm = mock_sm_cls.return_value
        mock_sm.secret_version_path.return_value = (
            "projects/12345/secrets/hydrovu_pvacd/versions/latest"
        )
        mock_sm.access_secret_version.return_value = MagicMock(
            payload=MagicMock(
                data=json.dumps({"id": "sm-id", "secret": "sm-secret"}).encode("UTF-8")
            )
        )

        result = resolve_hydrovu_credentials("", "", "hydrovu_pvacd")

        assert result == ("sm-id", "sm-secret")
        mock_sm.secret_version_path.assert_called_once_with("12345", "hydrovu_pvacd", "latest")
        # Secret Manager resolves credentials through ADC, so the bootstrap has to
        # run before the client is constructed — not after, and not never.
        mock_ensure_adc.assert_called_once()


class TestBuildHydroVuClient:
    def test_returns_authenticated_client_without_secret_manager(self):
        client = build_hydrovu_client("cid", "csecret", "ignored", "https://api", "https://token")
        assert isinstance(client, httpx.Client)
        assert str(client.base_url).rstrip("/") == "https://api"
        assert isinstance(client.auth, BearerAuth)


# ── fetch_location_data — merging paginated readings ──────────────────────────


class TestFetchLocationDataPageMerge:
    """
    A readings page covers roughly a two-day block, so any real fetch spans many
    pages and the merge below is what assembles them. It matters more for BernCo
    than PVACD: history reaches back to 2009 and one location logs every minute,
    which is thousands of rows across hundreds of pages.
    """

    def test_merges_readings_for_a_parameter_seen_on_several_pages(self):
        page1 = {
            "parameters": [
                {"parameterId": "4", "unitId": "35", "readings": [{"timestamp": 1, "value": 1.0}]}
            ]
        }
        page2 = {
            "parameters": [
                {"parameterId": "4", "unitId": "35", "readings": [{"timestamp": 2, "value": 2.0}]}
            ]
        }
        client, _ = _client_with_responses(
            [
                httpx.Response(200, json=page1, headers={"X-ISI-Next-Page": "cursor-2"}),
                httpx.Response(200, json=page2),
            ]
        )
        data, err = fetch_location_data(client, 123, 0)
        assert err is None
        assert data is not None
        assert data["parameters"][0]["readings"] == [
            {"timestamp": 1, "value": 1.0},
            {"timestamp": 2, "value": 2.0},
        ]

    def test_appends_a_parameter_that_only_appears_on_a_later_page(self):
        # A location's parameter set changes over time, so a parameter can be absent
        # from the first page and present on a later one. It must not be dropped.
        page1 = {
            "parameters": [
                {"parameterId": "1", "unitId": "1", "readings": [{"timestamp": 1, "value": 9.0}]}
            ]
        }
        page2 = {
            "parameters": [
                {"parameterId": "4", "unitId": "35", "readings": [{"timestamp": 2, "value": 2.0}]}
            ]
        }
        client, _ = _client_with_responses(
            [
                httpx.Response(200, json=page1, headers={"X-ISI-Next-Page": "cursor-2"}),
                httpx.Response(200, json=page2),
            ]
        )
        data, err = fetch_location_data(client, 123, 0)
        assert err is None
        assert data is not None
        assert {p["parameterId"] for p in data["parameters"]} == {"1", "4"}

    def test_http_date_retry_after_falls_back_to_the_default_backoff(self):
        # Retry-After is allowed to be an HTTP-date rather than a number of seconds.
        # float() raises on it, and an unhandled raise here would abort the whole run.
        from aqueduct_dagster.sources.hydrovu_common import _429_BACKOFF

        client, _ = _client_with_responses(
            [
                httpx.Response(429, headers={"Retry-After": "Thu, 01 Jan 2026 00:00:00 GMT"}),
                httpx.Response(200, json=DATA_RESPONSE),
            ]
        )
        with patch("time.sleep") as mock_sleep:
            data, err = fetch_location_data(client, 123, 0)
        assert err is None
        assert data == DATA_RESPONSE
        assert mock_sleep.call_args_list[0][0][0] == _429_BACKOFF
