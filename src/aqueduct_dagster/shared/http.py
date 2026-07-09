"""
shared/http.py

Shared HTTP retry helper for source ingest pipelines (dlt_pipeline.py modules).

Every source that fetches over HTTP needs the same shape: call an endpoint,
retry a bounded number of times on transient network errors with exponential
backoff, then give up. Without a shared helper this loop gets hand-rolled
per source (and per call site within a source) — see hydrovu/dlt_pipeline.py
before this module existed, which had it copy-pasted three times.

retry_transient() re-raises the final exception once retries are exhausted.
Call sites that want a non-raising fallback (e.g. returning an (None, reason)
tuple instead of propagating) wrap the call in their own
`except transient_errors:` — see hydrovu/dlt_pipeline.py for the pattern.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence

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
    """
    Calls fn(), retrying up to max_retries times on transient_errors with the
    given backoff schedule (sleeping backoff[attempt] seconds between tries).

    on_retry(exc, attempt, delay) is called before each sleep — attempt is
    1-indexed (the attempt that just failed). Use it to log with call-site
    context (e.g. location id) since this helper has none.

    Re-raises the final exception once max_retries is exhausted.
    """
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
