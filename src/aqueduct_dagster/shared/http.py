"""
Shared HTTP infrastructure for source ingest pipelines: a bounded retry with
exponential backoff on transient errors, a self-refreshing OAuth2
client-credentials token, and a way to attach it to every request without
rebuilding headers/timeout/base-url per call site.

retry_transient() re-raises once exhausted; wrap it in `except
transient_errors:` for a non-raising fallback (see hydrovu_common.py).

TokenManager + BearerAuth + build_authenticated_client() give a client that
attaches a Bearer token and refreshes-and-retries once on a 401. Only OAuth2
client-credentials is implemented; other schemes need their own Auth subclass.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Generator, Sequence

import httpx

logger = logging.getLogger(__name__)

DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF: tuple[float, ...] = (2.0, 4.0, 8.0)
# TransportError covers read/connect/write/pool timeouts, network errors, and
# protocol errors — anything worth retrying rather than failing the whole run.
TRANSIENT_HTTP_ERRORS: tuple[type[Exception], ...] = (httpx.TransportError,)


def retry_transient[T](
    fn: Callable[[], T],
    *,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff: Sequence[float] = DEFAULT_BACKOFF,
    transient_errors: tuple[type[Exception], ...] = TRANSIENT_HTTP_ERRORS,
    on_retry: Callable[[Exception, int, float], None] | None = None,
) -> T:
    """Calls fn(), retrying up to max_retries times on transient_errors with
    the given backoff schedule, re-raising the final exception once exhausted.

    on_retry(exc, attempt, delay) fires before each sleep (attempt is
    1-indexed) — use it to log with call-site context, since this helper has
    none."""
    for attempt in range(max_retries):
        try:
            return fn()
        except transient_errors as exc:
            if attempt >= max_retries - 1:
                raise
            delay = backoff[attempt]
            if on_retry is not None:
                on_retry(exc, attempt + 1, delay)
            time.sleep(delay)
    raise AssertionError("unreachable: max_retries must be >= 1")


class TokenManager:
    """Fetches and caches an OAuth2 client-credentials token; re-fetches on expiry or 401."""

    def __init__(self, token_url: str, client_id: str, client_secret: str) -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._token: str | None = None
        self._expires_at: float = 0.0

    def get(self) -> str:
        if self._token is None or time.monotonic() >= self._expires_at:
            self._refresh()
        return self._token  # type: ignore[return-value]

    def force_refresh(self) -> str:
        self._refresh()
        return self._token  # type: ignore[return-value]

    def _refresh(self) -> None:
        resp = retry_transient(
            lambda: httpx.post(
                self._token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                },
                timeout=30,
            ),
            on_retry=lambda exc, attempt, delay: logger.warning(
                "Token refresh: transient error (%s) on attempt %d — retrying in %.0fs",
                exc,
                attempt,
                delay,
            ),
        )
        resp.raise_for_status()
        body = resp.json()
        self._token = body["access_token"]
        # Refresh 60 s before actual expiry; default to 55 min if field absent
        ttl = body.get("expires_in", 3600)
        self._expires_at = time.monotonic() + ttl - 60
        logger.info("Token refreshed (expires_in=%ss)", ttl)


class BearerAuth(httpx.Auth):
    """httpx.Auth that attaches a Bearer token and, on a 401, refreshes and
    retries exactly once. auth_flow() yields a request, gets the response
    back, and can yield a corrected one — the Client sends it automatically,
    replacing the hand-written "check 401, force_refresh, re-issue" block
    every call site used to need."""

    def __init__(self, tm: TokenManager) -> None:
        self._tm = tm

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response]:
        request.headers["Authorization"] = f"Bearer {self._tm.get()}"
        response = yield request
        if response.status_code == 401:
            logger.warning("401 on %s — refreshing token and retrying", request.url)
            request.headers["Authorization"] = f"Bearer {self._tm.force_refresh()}"
            yield request


def build_authenticated_client(
    base_url: str, tm: TokenManager, timeout: httpx.Timeout
) -> httpx.Client:
    """httpx.Client with base_url, Accept header, BearerAuth (token attached +
    refreshed-on-401), and timeout preconfigured — call sites just do
    client.get("/path", ...). Callers still need retry_transient() around
    each call for transient network errors; that's a separate concern from
    auth."""
    return httpx.Client(
        base_url=base_url,
        headers={"Accept": "application/json"},
        auth=BearerAuth(tm),
        timeout=timeout,
    )


def build_unauthenticated_client(base_url: str, timeout: httpx.Timeout) -> httpx.Client:
    """httpx.Client with base_url, Accept header, and timeout preconfigured,
    no auth handler — for sources that don't need authentication. Callers
    still need retry_transient() around each call for transient network
    errors."""
    return httpx.Client(
        base_url=base_url,
        headers={"Accept": "application/json"},
        timeout=timeout,
    )
