"""
tests/loader/test_frost_auth.py

Unit tests for FROST URL resolution and ID-token auth in loader/frost_auth.py.
Entirely offline — no ID token is ever minted and no request leaves the process;
credentials are stubs and only header/handler state is asserted.

Two tests here guard against silent breakage rather than a bug we have seen:
test_service_accepts_id_token_auth_handler pins the isinstance constraint that
forces IdTokenAuthHandler to subclass AuthHandler, and test_audience_excludes_path
pins the Cloud Run audience rule — getting it wrong yields a 403 that reads as a
missing IAM binding.
"""

from __future__ import annotations

import frost_sta_client as fsc
import pytest
import requests

from aqueduct_dagster.loader import frost_auth
from aqueduct_dagster.loader.frost_auth import (
    ENV_FROST_URL,
    FrostAuthError,
    IdTokenAuthHandler,
    _audience,
    _is_local,
    attach_id_token_auth,
    service_root_url,
)

REMOTE = "https://frost-sensorthings-abc123-uw.a.run.app/FROST-Server"
LOCAL = "http://localhost:8081/FROST-Server"


class _StubCredentials:
    """Mimics google.auth credentials: invalid until refreshed, counts refreshes."""

    def __init__(self, token: str = "tok-1", valid: bool = False) -> None:
        self.token = token
        self.valid = valid
        self.refresh_calls = 0

    def refresh(self, request: object) -> None:
        self.refresh_calls += 1
        self.token = f"tok-{self.refresh_calls}"
        self.valid = True


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Clear the override so a developer's own .env can't influence resolution."""
    monkeypatch.delenv(ENV_FROST_URL, raising=False)


# --- URL resolution -------------------------------------------------------


def test_env_var_beats_config_file(monkeypatch):
    monkeypatch.setenv(ENV_FROST_URL, REMOTE)
    assert service_root_url() == f"{REMOTE}/v1.1"


def test_falls_back_to_config_file():
    # No env var set — the committed .dlt/config.toml default is local docker FROST.
    assert service_root_url() == f"{LOCAL}/v1.1"


def test_version_suffix_appended_once(monkeypatch):
    monkeypatch.setenv(ENV_FROST_URL, f"{REMOTE}/v1.1")
    assert service_root_url() == f"{REMOTE}/v1.1"


def test_trailing_slash_does_not_double_up(monkeypatch):
    monkeypatch.setenv(ENV_FROST_URL, f"{REMOTE}/")
    assert service_root_url() == f"{REMOTE}/v1.1"


def test_empty_env_var_falls_back(monkeypatch):
    # An env var set to "" in a deployment UI must not resolve to a bare "/v1.1".
    monkeypatch.setenv(ENV_FROST_URL, "")
    assert service_root_url() == f"{LOCAL}/v1.1"


@pytest.mark.parametrize(
    "url",
    ["http://localhost:8081/FROST-Server", "http://127.0.0.1:8081", "http://[::1]:8081"],
)
def test_local_hosts_detected(url):
    assert _is_local(url) is True


def test_remote_host_not_local():
    assert _is_local(REMOTE) is False


def test_audience_excludes_path():
    """Cloud Run's aud claim is the origin — never the /FROST-Server/v1.1 path."""
    assert _audience(f"{REMOTE}/v1.1") == "https://frost-sensorthings-abc123-uw.a.run.app"


# --- Attaching auth -------------------------------------------------------


def test_local_url_attaches_no_auth():
    service = fsc.SensorThingsService(f"{LOCAL}/v1.1")
    assert attach_id_token_auth(service, f"{LOCAL}/v1.1") is False
    assert service.auth_handler is None


def test_remote_url_attaches_handler(monkeypatch):
    creds = _StubCredentials()
    captured = {}

    def _fake_fetch(audience):
        captured["audience"] = audience
        return creds

    monkeypatch.setattr(frost_auth, "fetch_id_token_credentials", _fake_fetch)
    monkeypatch.setattr(frost_auth, "ensure_adc", lambda: None)

    service = fsc.SensorThingsService(f"{REMOTE}/v1.1")
    assert attach_id_token_auth(service, f"{REMOTE}/v1.1") is True
    assert isinstance(service.auth_handler, IdTokenAuthHandler)
    assert captured["audience"] == "https://frost-sensorthings-abc123-uw.a.run.app"


def test_remote_url_calls_ensure_adc(monkeypatch):
    """ID tokens come from ADC, so the bootstrap must run before minting."""
    calls = []
    monkeypatch.setattr(frost_auth, "ensure_adc", lambda: calls.append(1))
    monkeypatch.setattr(
        frost_auth, "fetch_id_token_credentials", lambda audience: _StubCredentials()
    )

    attach_id_token_auth(fsc.SensorThingsService(f"{REMOTE}/v1.1"), f"{REMOTE}/v1.1")
    assert calls == [1]


def test_missing_credentials_raise_actionable_error(monkeypatch):
    import google.auth.exceptions

    def _boom(audience):
        raise google.auth.exceptions.DefaultCredentialsError("no ADC")

    monkeypatch.setattr(frost_auth, "ensure_adc", lambda: None)
    monkeypatch.setattr(frost_auth, "fetch_id_token_credentials", _boom)

    with pytest.raises(FrostAuthError) as exc:
        attach_id_token_auth(fsc.SensorThingsService(f"{REMOTE}/v1.1"), f"{REMOTE}/v1.1")

    message = str(exc.value)
    assert "GCP_SERVICE_ACCOUNT_KEY_B64" in message
    assert "application-default login" in message  # names the trap explicitly


def test_service_accepts_id_token_auth_handler():
    """
    SensorThingsService.auth_handler isinstance-checks against AuthHandler.
    If a library upgrade tightens or changes that, this fails loudly here rather
    than as a ValueError inside a production load.
    """
    service = fsc.SensorThingsService(f"{REMOTE}/v1.1")
    service.auth_handler = IdTokenAuthHandler(_StubCredentials(), object())
    assert isinstance(service.auth_handler, IdTokenAuthHandler)


# --- Bearer header behaviour ----------------------------------------------


def _apply(handler: IdTokenAuthHandler) -> requests.PreparedRequest:
    request = requests.Request("GET", f"{REMOTE}/v1.1/Things").prepare()
    return handler.add_auth_header()(request)


def test_header_set_and_credentials_refreshed_when_invalid():
    creds = _StubCredentials(valid=False)
    prepared = _apply(IdTokenAuthHandler(creds, object()))
    assert creds.refresh_calls == 1
    assert prepared.headers["Authorization"] == "Bearer tok-1"


def test_valid_credentials_not_refreshed_again():
    creds = _StubCredentials(token="cached", valid=True)
    prepared = _apply(IdTokenAuthHandler(creds, object()))
    assert creds.refresh_calls == 0
    assert prepared.headers["Authorization"] == "Bearer cached"


def test_expired_credentials_refreshed_mid_run():
    """
    ID tokens last an hour; a backfill can post for longer. The same handler must
    mint a new token once the old one goes invalid, not reuse a stale one.
    """
    creds = _StubCredentials(valid=False)
    handler = IdTokenAuthHandler(creds, object())

    first = _apply(handler)
    assert first.headers["Authorization"] == "Bearer tok-1"

    creds.valid = False  # token expires mid-run
    second = _apply(handler)

    assert creds.refresh_calls == 2
    assert second.headers["Authorization"] == "Bearer tok-2"
