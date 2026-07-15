"""
tests/conftest.py

Shared test helpers for httpx.Client/BearerAuth-based code, used by both
tests/shared/test_http.py and tests/sources/hydrovu/test_dlt_pipeline.py.
Consolidated here after the two files independently grew near-identical
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
    """
    Builds a real httpx.Client wired to BearerAuth(tm), backed by a
    MockTransport that returns `responses` in order — or raises, if an item
    is an Exception instance (simulates a transient network error).

    Exercises real httpx semantics (raise_for_status, headers, pagination via
    response headers, auth_flow) without patching httpx.get. Returns
    (client, calls) — calls records every request the transport actually saw.
    """
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
