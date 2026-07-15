"""
tests/shared/test_http.py

Unit tests for shared HTTP infrastructure: retry, token management, and
bearer-token auth. No real API calls — httpx interactions are simulated via
httpx.MockTransport (for BearerAuth / build_authenticated_client, so the real
auth_flow() protocol is exercised) or by patching httpx.post (for
TokenManager, which calls a token endpoint directly, not through a Client).

Covers:
  TokenManager             — caching, expiry, force-refresh (moved verbatim
                              from tests/sources/hydrovu/test_dlt_pipeline.py
                              when TokenManager moved to shared/http.py)
  BearerAuth               — attaches token, refreshes + retries once on 401
  build_authenticated_client — wires base_url, default headers, auth, timeout
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import httpx
import pytest

from aqueduct_dagster.shared.http import (
    BearerAuth,
    TokenManager,
    build_authenticated_client,
)
from tests.conftest import client_with_responses as _client_with_responses
from tests.conftest import make_tm as _make_tm

TOKEN_RESPONSE = {"access_token": "tok-abc", "expires_in": 3600}


def _mock_resp(status_code: int, body=None) -> MagicMock:
    """Build a fake httpx response for patch("httpx.post", ...) — TokenManager
    calls httpx.post directly against a token endpoint, not through a Client,
    so MockTransport doesn't apply here."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = body if body is not None else {}
    if status_code >= 400:
        resp.raise_for_status.side_effect = Exception(f"HTTP {status_code}")
    else:
        resp.raise_for_status.return_value = None
    return resp


# ── TokenManager ───────────────────────────────────────────────────────────────


class TestTokenManager:
    def test_get_fetches_token_on_first_call(self):
        with patch("httpx.post", return_value=_mock_resp(200, TOKEN_RESPONSE)) as mock_post:
            tm = TokenManager("https://token.url", "cid", "csec")
            token = tm.get()
        assert token == "tok-abc"
        mock_post.assert_called_once()

    def test_get_returns_cached_token_on_second_call(self):
        with patch("httpx.post", return_value=_mock_resp(200, TOKEN_RESPONSE)) as mock_post:
            tm = TokenManager("https://token.url", "cid", "csec")
            tm.get()
            tm.get()
        assert mock_post.call_count == 1

    def test_get_refreshes_when_token_expired(self):
        with patch("httpx.post", return_value=_mock_resp(200, TOKEN_RESPONSE)) as mock_post:
            tm = TokenManager("https://token.url", "cid", "csec")
            tm.get()
            tm._expires_at = time.monotonic() - 1  # force expiry
            tm.get()
        assert mock_post.call_count == 2

    def test_force_refresh_always_re_fetches(self):
        with patch("httpx.post", return_value=_mock_resp(200, TOKEN_RESPONSE)) as mock_post:
            tm = TokenManager("https://token.url", "cid", "csec")
            tm.get()
            tm.force_refresh()
        assert mock_post.call_count == 2

    def test_refresh_sends_client_credentials_grant(self):
        with patch("httpx.post", return_value=_mock_resp(200, TOKEN_RESPONSE)) as mock_post:
            tm = TokenManager("https://token.url", "cid", "csec")
            tm.get()
        data = mock_post.call_args[1]["data"]
        assert data["grant_type"] == "client_credentials"
        assert data["client_id"] == "cid"
        assert data["client_secret"] == "csec"

    def test_expires_at_set_60s_before_ttl(self):
        with patch("httpx.post", return_value=_mock_resp(200, TOKEN_RESPONSE)):
            with patch("time.monotonic", return_value=1000.0):
                tm = TokenManager("https://token.url", "cid", "csec")
                tm.get()
        # expires_in=3600 → _expires_at = 1000 + 3600 - 60 = 4540
        assert tm._expires_at == pytest.approx(4540.0, abs=1.0)

    def test_missing_expires_in_defaults_to_3600(self):
        body = {"access_token": "tok-xyz"}  # no expires_in field
        with patch("httpx.post", return_value=_mock_resp(200, body)):
            with patch("time.monotonic", return_value=0.0):
                tm = TokenManager("https://token.url", "cid", "csec")
                tm.get()
        # 0 + 3600 - 60 = 3540
        assert tm._expires_at == pytest.approx(3540.0, abs=1.0)

    def test_raises_on_auth_failure(self):
        with patch("httpx.post", return_value=_mock_resp(401)):
            tm = TokenManager("https://token.url", "cid", "csec")
            with pytest.raises(Exception, match="HTTP 401"):
                tm.get()


# ── BearerAuth ─────────────────────────────────────────────────────────────────


class TestBearerAuth:
    def test_attaches_bearer_token(self):
        client, calls = _client_with_responses([httpx.Response(200)], tm=_make_tm("my-token"))
        client.get("https://api/x")
        assert calls[0].headers["Authorization"] == "Bearer my-token"

    def test_refreshes_and_retries_once_on_401(self):
        tm = _make_tm()
        client, calls = _client_with_responses([httpx.Response(401), httpx.Response(200)], tm=tm)
        resp = client.get("https://api/x")
        assert resp.status_code == 200
        tm.force_refresh.assert_called_once()

    def test_retry_uses_refreshed_token(self):
        client, calls = _client_with_responses([httpx.Response(401), httpx.Response(200)])
        client.get("https://api/x")
        assert len(calls) == 2
        assert calls[1].headers["Authorization"] == "Bearer tok-new"

    def test_still_401_after_retry_is_returned_as_is(self):
        # BearerAuth retries exactly once — a second 401 is handed back to the
        # caller (which typically calls raise_for_status() and raises).
        client, calls = _client_with_responses([httpx.Response(401), httpx.Response(401)])
        resp = client.get("https://api/x")
        assert resp.status_code == 401
        assert len(calls) == 2

    def test_does_not_refresh_on_non_401_error(self):
        tm = _make_tm()
        client, _ = _client_with_responses([httpx.Response(500)], tm=tm)
        client.get("https://api/x")
        tm.force_refresh.assert_not_called()


# ── build_authenticated_client ──────────────────────────────────────────────────


class TestBuildAuthenticatedClient:
    def test_sets_base_url(self):
        client = build_authenticated_client("https://api", _make_tm(), timeout=httpx.Timeout(30))
        assert str(client.base_url) == "https://api"

    def test_sets_accept_header(self):
        client = build_authenticated_client("https://api", _make_tm(), timeout=httpx.Timeout(30))
        assert client.headers["accept"] == "application/json"

    def test_wires_bearer_auth(self):
        client = build_authenticated_client("https://api", _make_tm(), timeout=httpx.Timeout(30))
        assert isinstance(client.auth, BearerAuth)

    def test_sets_timeout(self):
        timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)
        client = build_authenticated_client("https://api", _make_tm(), timeout=timeout)
        assert client.timeout == timeout
