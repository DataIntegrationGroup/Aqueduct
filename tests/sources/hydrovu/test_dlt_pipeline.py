"""
tests/sources/hydrovu/test_dlt_pipeline.py

Unit tests for the HydroVu dlt pipeline private helpers.
No real API calls — all HTTP interactions are mocked.

Covers:
  _TokenManager        — caching, expiry, force-refresh
  _fetch_locations     — success, 401 retry, error propagation
  _fetch_location_data — typed result tuple: success, 404, 5xx, 429, transient errors, 401 retry
  hydrovu_readings     — allowlist filtering, error stats, cursor behaviour
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import httpx
import pytest

from aqueduct_dagster.sources.hydrovu.dlt_pipeline import (
    _auth_headers,
    _fetch_location_data,
    _fetch_locations,
    _TokenManager,
    hydrovu_readings,
)

# ── Fixtures / shared test data ───────────────────────────────────────────────

TOKEN_RESPONSE = {"access_token": "tok-abc", "expires_in": 3600}

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


def _mock_resp(status_code: int, body=None) -> MagicMock:
    """Build a fake httpx response."""
    resp = MagicMock()
    resp.status_code = status_code
    # Real httpx responses expose headers as a mapping; without this the cursor
    # pagination loop reads a truthy MagicMock for X-ISI-Next-Page and never ends.
    resp.headers = {}
    resp.json.return_value = body if body is not None else {}
    if status_code >= 400:
        resp.raise_for_status.side_effect = Exception(f"HTTP {status_code}")
    else:
        resp.raise_for_status.return_value = None
    return resp


def _make_tm(token: str = "tok-abc") -> _TokenManager:
    """Return a pre-seeded mock _TokenManager."""
    tm = MagicMock(spec=_TokenManager)
    tm.get.return_value = token
    tm.force_refresh.return_value = "tok-new"
    return tm


# ── _auth_headers ─────────────────────────────────────────────────────────────


class TestAuthHeaders:
    def test_includes_bearer_token(self):
        h = _auth_headers("my-token")
        assert h["Authorization"] == "Bearer my-token"

    def test_includes_accept_json(self):
        h = _auth_headers("x")
        assert h["Accept"] == "application/json"


# ── _TokenManager ─────────────────────────────────────────────────────────────


class TestTokenManager:
    def test_get_fetches_token_on_first_call(self):
        with patch("httpx.post", return_value=_mock_resp(200, TOKEN_RESPONSE)) as mock_post:
            tm = _TokenManager("https://token.url", "cid", "csec")
            token = tm.get()
        assert token == "tok-abc"
        mock_post.assert_called_once()

    def test_get_returns_cached_token_on_second_call(self):
        with patch("httpx.post", return_value=_mock_resp(200, TOKEN_RESPONSE)) as mock_post:
            tm = _TokenManager("https://token.url", "cid", "csec")
            tm.get()
            tm.get()
        assert mock_post.call_count == 1

    def test_get_refreshes_when_token_expired(self):
        with patch("httpx.post", return_value=_mock_resp(200, TOKEN_RESPONSE)) as mock_post:
            tm = _TokenManager("https://token.url", "cid", "csec")
            tm.get()
            tm._expires_at = time.monotonic() - 1  # force expiry
            tm.get()
        assert mock_post.call_count == 2

    def test_force_refresh_always_re_fetches(self):
        with patch("httpx.post", return_value=_mock_resp(200, TOKEN_RESPONSE)) as mock_post:
            tm = _TokenManager("https://token.url", "cid", "csec")
            tm.get()
            tm.force_refresh()
        assert mock_post.call_count == 2

    def test_refresh_sends_client_credentials_grant(self):
        with patch("httpx.post", return_value=_mock_resp(200, TOKEN_RESPONSE)) as mock_post:
            tm = _TokenManager("https://token.url", "cid", "csec")
            tm.get()
        data = mock_post.call_args[1]["data"]
        assert data["grant_type"] == "client_credentials"
        assert data["client_id"] == "cid"
        assert data["client_secret"] == "csec"

    def test_expires_at_set_60s_before_ttl(self):
        with patch("httpx.post", return_value=_mock_resp(200, TOKEN_RESPONSE)):
            with patch("time.monotonic", return_value=1000.0):
                tm = _TokenManager("https://token.url", "cid", "csec")
                tm.get()
        # expires_in=3600 → _expires_at = 1000 + 3600 - 60 = 4540
        assert tm._expires_at == pytest.approx(4540.0, abs=1.0)

    def test_missing_expires_in_defaults_to_3600(self):
        body = {"access_token": "tok-xyz"}  # no expires_in field
        with patch("httpx.post", return_value=_mock_resp(200, body)):
            with patch("time.monotonic", return_value=0.0):
                tm = _TokenManager("https://token.url", "cid", "csec")
                tm.get()
        # 0 + 3600 - 60 = 3540
        assert tm._expires_at == pytest.approx(3540.0, abs=1.0)

    def test_raises_on_auth_failure(self):
        with patch("httpx.post", return_value=_mock_resp(401)):
            tm = _TokenManager("https://token.url", "cid", "csec")
            with pytest.raises(Exception, match="HTTP 401"):
                tm.get()


# ── _fetch_locations ──────────────────────────────────────────────────────────


class TestFetchLocations:
    def test_returns_list_on_success(self):
        with patch("httpx.get", return_value=_mock_resp(200, LOCATIONS_RESPONSE)):
            result = _fetch_locations("https://api", _make_tm())
        assert result == LOCATIONS_RESPONSE

    def test_sends_empty_start_page_header(self):
        # First request sends X-ISI-Start-Page="" (empty cursor); the response's
        # X-ISI-Next-Page token drives subsequent pages.
        with patch("httpx.get", return_value=_mock_resp(200, LOCATIONS_RESPONSE)) as mock_get:
            _fetch_locations("https://api", _make_tm())
        headers = mock_get.call_args[1]["headers"]
        assert headers["X-ISI-Start-Page"] == ""

    def test_sends_bearer_token(self):
        with patch("httpx.get", return_value=_mock_resp(200, LOCATIONS_RESPONSE)) as mock_get:
            _fetch_locations("https://api", _make_tm("my-token"))
        headers = mock_get.call_args[1]["headers"]
        assert headers["Authorization"] == "Bearer my-token"

    def test_retries_with_fresh_token_on_401(self):
        tm = _make_tm()
        responses = [_mock_resp(401), _mock_resp(200, LOCATIONS_RESPONSE)]
        with patch("httpx.get", side_effect=responses):
            result = _fetch_locations("https://api", tm)
        tm.force_refresh.assert_called_once()
        assert result == LOCATIONS_RESPONSE

    def test_retry_uses_refreshed_token(self):
        tm = _make_tm()
        responses = [_mock_resp(401), _mock_resp(200, LOCATIONS_RESPONSE)]
        with patch("httpx.get", side_effect=responses) as mock_get:
            _fetch_locations("https://api", tm)
        second_call_headers = mock_get.call_args_list[1][1]["headers"]
        assert second_call_headers["Authorization"] == "Bearer tok-new"

    def test_raises_if_retry_also_returns_401(self):
        responses = [_mock_resp(401), _mock_resp(401)]
        with patch("httpx.get", side_effect=responses):
            with pytest.raises(Exception, match="HTTP 401"):
                _fetch_locations("https://api", _make_tm())

    def test_raises_on_server_error(self):
        with patch("httpx.get", return_value=_mock_resp(500)):
            with pytest.raises(Exception, match="HTTP 500"):
                _fetch_locations("https://api", _make_tm())

    def test_hits_correct_endpoint(self):
        with patch("httpx.get", return_value=_mock_resp(200, LOCATIONS_RESPONSE)) as mock_get:
            _fetch_locations("https://api", _make_tm())
        url = mock_get.call_args[0][0]
        assert url == "https://api/locations/list"


# ── _fetch_location_data ──────────────────────────────────────────────────────


class TestFetchLocationData:
    def test_returns_data_and_no_error_on_success(self):
        with patch("httpx.get", return_value=_mock_resp(200, DATA_RESPONSE)):
            data, err = _fetch_location_data("https://api", 123, 1780704000, _make_tm())
        assert data == DATA_RESPONSE
        assert err is None

    def test_returns_none_none_on_404(self):
        with patch("httpx.get", return_value=_mock_resp(404)):
            data, err = _fetch_location_data("https://api", 123, 1780704000, _make_tm())
        assert data is None
        assert err is None

    def test_404_does_not_raise(self):
        with patch("httpx.get", return_value=_mock_resp(404)):
            _fetch_location_data("https://api", 123, 1780704000, _make_tm())  # must not raise

    def test_returns_error_reason_on_500(self):
        with patch("httpx.get", return_value=_mock_resp(500)):
            data, err = _fetch_location_data("https://api", 123, 1780704000, _make_tm())
        assert data is None
        assert err is not None
        assert "500" in err

    def test_returns_error_reason_on_503(self):
        with patch("httpx.get", return_value=_mock_resp(503)):
            data, err = _fetch_location_data("https://api", 123, 1780704000, _make_tm())
        assert data is None
        assert err is not None
        assert "503" in err

    def test_retries_with_fresh_token_on_401(self):
        tm = _make_tm()
        responses = [_mock_resp(401), _mock_resp(200, DATA_RESPONSE)]
        with patch("httpx.get", side_effect=responses):
            data, err = _fetch_location_data("https://api", 123, 1780704000, tm)
        tm.force_refresh.assert_called_once()
        assert data == DATA_RESPONSE
        assert err is None

    def test_retry_uses_refreshed_token(self):
        tm = _make_tm()
        responses = [_mock_resp(401), _mock_resp(200, DATA_RESPONSE)]
        with patch("httpx.get", side_effect=responses) as mock_get:
            _fetch_location_data("https://api", 123, 1780704000, tm)
        second_call_headers = mock_get.call_args_list[1][1]["headers"]
        assert second_call_headers["Authorization"] == "Bearer tok-new"

    def test_raises_if_retry_also_returns_401(self):
        responses = [_mock_resp(401), _mock_resp(401)]
        with patch("httpx.get", side_effect=responses):
            with pytest.raises(Exception, match="HTTP 401"):
                _fetch_location_data("https://api", 123, 1780704000, _make_tm())

    def test_passes_start_time_as_query_param(self):
        with patch("httpx.get", return_value=_mock_resp(200, DATA_RESPONSE)) as mock_get:
            _fetch_location_data("https://api", 123, 1780704000, _make_tm())
        params = mock_get.call_args[1]["params"]
        assert params["startTime"] == 1780704000

    def test_hits_correct_endpoint(self):
        with patch("httpx.get", return_value=_mock_resp(200, DATA_RESPONSE)) as mock_get:
            _fetch_location_data("https://api", 123, 1780704000, _make_tm())
        url = mock_get.call_args[0][0]
        assert url == "https://api/locations/123/data"

    def test_raises_on_unexpected_4xx(self):
        with patch("httpx.get", return_value=_mock_resp(403)):
            with pytest.raises(Exception, match="HTTP 403"):
                _fetch_location_data("https://api", 123, 1780704000, _make_tm())

    def test_transient_error_exhausted_returns_error_reason(self):
        exc = httpx.ReadError("connection reset")
        with patch("httpx.get", side_effect=exc):
            with patch("time.sleep"):
                data, err = _fetch_location_data("https://api", 123, 1780704000, _make_tm())
        assert data is None
        assert err is not None
        assert "transient" in err.lower()

    def test_401_retry_transient_error_returns_error_reason(self):
        # First request returns 401; then the refresh retry hits a transient error.
        responses = [
            _mock_resp(401),
            httpx.ReadError("reset"),
            httpx.ReadError("reset"),
            httpx.ReadError("reset"),
        ]
        with patch("httpx.get", side_effect=responses):
            with patch("time.sleep"):
                data, err = _fetch_location_data("https://api", 123, 1780704000, _make_tm())
        assert data is None
        assert err is not None
        assert "transient" in err.lower()

    def test_429_returns_error_after_exhausted_retries(self):
        with patch("httpx.get", return_value=_mock_resp(429)):
            with patch("time.sleep"):
                data, err = _fetch_location_data("https://api", 123, 1780704000, _make_tm())
        assert data is None
        assert err is not None
        assert "429" in err

    def test_429_respects_retry_after_header(self):
        resp_429 = _mock_resp(429)
        resp_429.headers = {"Retry-After": "30"}
        with patch("httpx.get", side_effect=[resp_429, resp_429, resp_429, resp_429]):
            with patch("time.sleep") as mock_sleep:
                _fetch_location_data("https://api", 123, 1780704000, _make_tm())
        assert mock_sleep.call_args_list[0][0][0] == 30.0

    def test_429_uses_default_backoff_when_no_retry_after(self):
        from aqueduct_dagster.sources.hydrovu.dlt_pipeline import _429_BACKOFF

        resp_429 = _mock_resp(429)
        resp_429.headers = {}
        with patch("httpx.get", side_effect=[resp_429, resp_429, resp_429, resp_429]):
            with patch("time.sleep") as mock_sleep:
                _fetch_location_data("https://api", 123, 1780704000, _make_tm())
        assert mock_sleep.call_args_list[0][0][0] == _429_BACKOFF

    def test_429_succeeds_after_one_retry(self):
        resp_429 = _mock_resp(429)
        resp_429.headers = {"Retry-After": "1"}
        responses = [resp_429, _mock_resp(200, DATA_RESPONSE)]
        with patch("httpx.get", side_effect=responses):
            with patch("time.sleep"):
                data, err = _fetch_location_data("https://api", 123, 1780704000, _make_tm())
        assert data == DATA_RESPONSE
        assert err is None


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


class TestHydroVuReadingsFilter:
    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.hydrovu.dlt_pipeline._fetch_location_data")
    def test_only_fetches_allowlisted_locations(self, mock_fetch, _mock_state):
        mock_fetch.return_value = (_READINGS_DATA, None)
        list(
            hydrovu_readings(
                api_base_url="https://api",
                start_ts=1000,
                tm=_make_tm(),
                locations=_LOCATIONS,
                location_ids=[111, 222],
            )
        )
        called_ids = {call[0][1] for call in mock_fetch.call_args_list}
        assert called_ids == {111, 222}

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.hydrovu.dlt_pipeline._fetch_location_data")
    def test_skips_locations_not_in_allowlist(self, mock_fetch, _mock_state):
        mock_fetch.return_value = (_READINGS_DATA, None)
        list(
            hydrovu_readings(
                api_base_url="https://api",
                start_ts=1000,
                tm=_make_tm(),
                locations=_LOCATIONS,
                location_ids=[111],
            )
        )
        called_ids = {call[0][1] for call in mock_fetch.call_args_list}
        assert 222 not in called_ids
        assert 333 not in called_ids

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.hydrovu.dlt_pipeline._fetch_location_data")
    def test_empty_allowlist_skips_all_locations(self, mock_fetch, _mock_state):
        list(
            hydrovu_readings(
                api_base_url="https://api",
                start_ts=1000,
                tm=_make_tm(),
                locations=_LOCATIONS,
                location_ids=[],
            )
        )
        mock_fetch.assert_not_called()


# ── hydrovu_readings — error stats ────────────────────────────────────────────


class TestHydroVuReadingsErrorStats:
    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.hydrovu.dlt_pipeline._fetch_location_data")
    def test_real_error_increments_errored_count(self, mock_fetch, _mock_state):
        mock_fetch.return_value = (None, "HTTP 500")
        stats: dict = {}
        list(
            hydrovu_readings(
                api_base_url="https://api",
                start_ts=1000,
                tm=_make_tm(),
                locations=[{"id": 111, "name": "Well A"}],
                location_ids=[111],
                _stats=stats,
            )
        )
        assert stats["locations_errored"] == 1
        assert 111 in stats["failed_location_ids"]

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.hydrovu.dlt_pipeline._fetch_location_data")
    def test_404_does_not_increment_errored_count(self, mock_fetch, _mock_state):
        mock_fetch.return_value = (None, None)
        stats: dict = {}
        list(
            hydrovu_readings(
                api_base_url="https://api",
                start_ts=1000,
                tm=_make_tm(),
                locations=[{"id": 111, "name": "Well A"}],
                location_ids=[111],
                _stats=stats,
            )
        )
        assert stats["locations_errored"] == 0
        assert stats["locations_no_data"] == 1
        assert stats["failed_location_ids"] == []

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.hydrovu.dlt_pipeline._fetch_location_data")
    def test_error_does_not_advance_cursor(self, mock_fetch, _mock_state):
        state = {"location_cursors": {"111": 999}}
        with patch("dlt.current.resource_state", return_value=state):
            mock_fetch.return_value = (None, "HTTP 500")
            list(
                hydrovu_readings(
                    api_base_url="https://api",
                    start_ts=1000,
                    tm=_make_tm(),
                    locations=[{"id": 111, "name": "Well A"}],
                    location_ids=[111],
                )
            )
        assert state["location_cursors"]["111"] == 999  # unchanged

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.hydrovu.dlt_pipeline._fetch_location_data")
    def test_partial_failure_stats(self, mock_fetch, _mock_state):
        # location 111 succeeds, 222 errors
        mock_fetch.side_effect = [(_READINGS_DATA, None), (None, "HTTP 503")]
        stats: dict = {}
        list(
            hydrovu_readings(
                api_base_url="https://api",
                start_ts=1000,
                tm=_make_tm(),
                locations=[{"id": 111, "name": "Well A"}, {"id": 222, "name": "Well B"}],
                location_ids=[111, 222],
                _stats=stats,
            )
        )
        assert stats["locations_fetched"] == 1
        assert stats["locations_errored"] == 1
        assert stats["failed_location_ids"] == [222]
