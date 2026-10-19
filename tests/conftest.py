"""
Shared test helpers for httpx.Client/BearerAuth-based code, used by
tests/shared/test_http.py and the tests/sources/ HydroVu and CKAN tests.
Consolidated here after two files independently grew near-identical
make_tm()/client_with_responses() helpers.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx

from aqueduct_dagster.shared.http import BearerAuth, TokenManager


def make_tm(token: str = "tok-abc") -> TokenManager:
    """Return a pre-seeded mock TokenManager."""
    tm = MagicMock(spec=TokenManager)
    tm.get.return_value = token
    tm.force_refresh.return_value = "tok-new"
    return tm


def client_with_responses(
    responses: list[httpx.Response | Exception],
    tm: TokenManager | None = None,
    base_url: str | httpx.URL = "",
) -> tuple[httpx.Client, list[httpx.Request]]:
    """Real httpx.Client wired to BearerAuth(tm), backed by a MockTransport
    that returns `responses` in order (or raises, for an Exception item) —
    exercises real httpx semantics without patching httpx.get. Returns
    (client, calls), where calls records every request the transport saw."""
    calls: list[httpx.Request] = []
    remaining = iter(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        item = next(remaining)
        if isinstance(item, Exception):
            raise item
        return item

    client = httpx.Client(
        base_url=base_url,
        auth=BearerAuth(tm or make_tm()),
        transport=httpx.MockTransport(handler),
    )
    return client, calls


def client_with_responses_unauthenticated(
    responses: list[httpx.Response | Exception],
    base_url: str | httpx.URL = "",
) -> tuple[httpx.Client, list[httpx.Request]]:
    """Same as client_with_responses(), without BearerAuth."""
    calls: list[httpx.Request] = []
    remaining = iter(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        item = next(remaining)
        if isinstance(item, Exception):
            raise item
        return item

    client = httpx.Client(
        base_url=base_url,
        transport=httpx.MockTransport(handler),
    )
    return client, calls


def client_with_routes(
    routes: dict[str, httpx.Response | Exception],
    base_url: str | httpx.URL = "",
    headers: dict[str, str] | None = None,
) -> tuple[httpx.Client, list[httpx.Request]]:
    """Unauthenticated client whose MockTransport answers by URL path rather
    than call order. For code that fetches several URLs. An unrouted path gets a 404.
    Returns (client, calls)."""
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        item = routes.get(request.url.path)
        if item is None:
            return httpx.Response(404)
        if isinstance(item, Exception):
            raise item
        return httpx.Response(item.status_code, headers=item.headers, content=item.content)

    client = httpx.Client(
        base_url=base_url,
        headers=headers,
        transport=httpx.MockTransport(handler),
    )
    return client, calls
