"""
Resolves the FROST service root URL and, when that URL is remote, authenticates
to it with a Google-signed OIDC ID token.

Why: production FROST runs on Cloud Run behind IAM, requiring an
`Authorization: Bearer <id_token>` header; local `docker compose` FROST has no
auth. Both go through this loader, so the auth decision derives from the
resolved host, not a separate mode flag that could drift from the URL:
**localhost gets no auth, anything else gets a token.**

Credentials come from ADC (bootstrapped by shared/gcp_auth.py from
GCP_SERVICE_ACCOUNT_KEY_B64) — no new secret, just a `roles/run.invoker`
binding added to the same service account that already reaches GCS and
Secret Manager.

Two library constraints:
  * `SensorThingsService.auth_handler` requires an `AuthHandler` instance, so
    `IdTokenAuthHandler` subclasses it despite sharing none of its behavior —
    `add_auth_header()`'s return value just needs to satisfy `requests`' `AuthBase`.
  * `shared/http.py`'s `BearerAuth` is `httpx.Auth`; `frost_sta_client` uses
    `requests`. Incompatible protocols, hence a second implementation here.
"""

from __future__ import annotations

import logging
import os
from urllib.parse import urlparse

import google.auth.exceptions
import requests.auth
from frost_sta_client.service.auth_handler import AuthHandler
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.id_token import fetch_id_token_credentials

from aqueduct_dagster.shared.config import load_config
from aqueduct_dagster.shared.gcp_auth import ENV_ADC_PATH, ENV_KEY_B64, ensure_adc

logger = logging.getLogger(__name__)

#: Overrides `[destination.frost] service_root_url` in .dlt/config.toml. Set in
#: Dagster+ on the full deployment; docker-compose.yml already uses this name for
#: FROST's own serviceRootUrl, so one name describes "where FROST lives" everywhere.
ENV_FROST_URL = "FROST_SERVICE_ROOT_URL"

#: frost_sta_client builds entity URLs by appending directly to the service root,
#: so the version segment has to already be there.
_API_VERSION = "v1.1"

#: Hosts treated as a developer's local `docker compose` FROST, which has no auth.
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}


class FrostAuthError(RuntimeError):
    """Raised when a remote FROST URL is configured but no ID token can be minted."""


def service_root_url() -> str:
    """Returns the FROST service root URL, `/v1.1` included. The env var wins
    over .dlt/config.toml, so a deployment can override the committed
    local-docker-compose default without anyone editing (and risking
    committing) the tracked file."""
    url = os.environ.get(ENV_FROST_URL) or load_config()["destination"]["frost"]["service_root_url"]
    url = url.rstrip("/")
    if not url.endswith(f"/{_API_VERSION}"):
        url = f"{url}/{_API_VERSION}"
    return url


def _is_local(url: str) -> bool:
    return (urlparse(url).hostname or "") in _LOCAL_HOSTS


def _audience(url: str) -> str:
    """The `aud` claim Cloud Run expects — service origin, no path. Including
    `/FROST-Server/v1.1` produces a token Cloud Run rejects with a 403
    indistinguishable from a missing IAM binding, so derive it, don't hand-write it."""
    parts = urlparse(url)
    return f"{parts.scheme}://{parts.netloc}"


class _IdTokenBearerAuth(requests.auth.AuthBase):
    """Attaches a Google ID token, refreshing when expired. ID tokens last an
    hour and a backfill can post longer, so a token fetched once at
    construction would fail mid-run. `credentials.valid` is False both
    pre-first-refresh and once within google-auth's expiry threshold, so one
    check covers both."""

    def __init__(self, credentials: object, request: GoogleAuthRequest) -> None:
        self._credentials = credentials
        self._request = request

    def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
        if not self._credentials.valid:  # type: ignore[attr-defined]
            self._credentials.refresh(self._request)  # type: ignore[attr-defined]
        r.headers["Authorization"] = f"Bearer {self._credentials.token}"  # type: ignore[attr-defined]
        return r


class IdTokenAuthHandler(AuthHandler):
    """AuthHandler subclass supplying ID-token auth instead of BasicAuth.
    Subclassing is required (auth_handler's setter does an isinstance check),
    not stylistic — the inherited username/password fields go unused."""

    def __init__(self, credentials: object, request: GoogleAuthRequest) -> None:
        super().__init__()
        self._auth = _IdTokenBearerAuth(credentials, request)

    def add_auth_header(self) -> requests.auth.AuthBase:
        return self._auth


def attach_id_token_auth(service: object, url: str) -> bool:
    """Gives `service` an ID-token auth handler, unless `url` is local. Returns
    True if attached. Raises FrostAuthError when a remote URL has no mintable
    ID token — failing here names the cause, instead of an opaque 403 later."""
    if _is_local(url):
        logger.info("FROST at %s is local — no authentication attached.", url)
        return False

    ensure_adc()
    audience = _audience(url)
    try:
        credentials = fetch_id_token_credentials(audience)
    except google.auth.exceptions.DefaultCredentialsError as exc:
        raise FrostAuthError(
            f"Cannot mint an ID token for {audience}. ID tokens require service "
            f"account credentials: set {ENV_KEY_B64} to a base64-encoded key, or "
            f"point {ENV_ADC_PATH} at a key file. Note that user credentials from "
            f"`gcloud auth application-default login` cannot mint ID tokens."
        ) from exc

    service.auth_handler = IdTokenAuthHandler(credentials, GoogleAuthRequest())  # type: ignore[attr-defined]
    logger.info("FROST at %s is remote — ID token auth attached (aud=%s).", url, audience)
    return True
