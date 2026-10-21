import logging
import time
from typing import Any

from httpx import Client, Response, Timeout

from aqueduct_dagster.shared.http import DEFAULT_MAX_RETRIES as _MAX_RETRIES
from aqueduct_dagster.shared.http import TRANSIENT_HTTP_ERRORS as _TRANSIENT_ERRORS
from aqueduct_dagster.shared.http import build_unauthenticated_client, retry_transient

logger = logging.getLogger(__name__)

_429_BACKOFF = 60.0  # seconds to wait on 429 when Retry-After header is absent
_MAX_RATE_LIMIT_RETRIES = 3


def transform_result(data: dict) -> list[dict]:
    """
    Extracts the "attributes" dict from each entry in the CABQ response's "features" list — the only part of the
    response actually used.
    """
    all_attributes: list[dict] = []
    for feature in data["features"]:
        all_attributes.append(feature["attributes"])
    return all_attributes


def build_arcrest_client(api_base_url: str) -> Client:
    """
    Build a httpx client without authentication for us eby arc rest sources
    """
    return build_unauthenticated_client(
        api_base_url, timeout=Timeout(connect=30.0, read=60.0, write=30.0, pool=30.0)
    )


def fetch_locations(
    client: Client, path: str, params: dict
) -> tuple[list[dict] | None, str | None]:
    """
    Fetch a list of locations from the source, performing the query defined in the given params against the given path
    """
    rate_limit_retries = 0
    result: dict[Any, Any] = {}
    while True:

        def _fetch_location_info() -> Response:
            return client.get(path, params=params)

        try:
            resp = retry_transient(
                _fetch_location_info,
                on_retry=lambda exc, attempt, seconds: logger.warning(
                    "Location: error (%s) on attempt %d - retrying in %.0fs", exc, attempt, seconds
                ),
            )
        except _TRANSIENT_ERRORS as err:
            logger.warning(
                "Transient error fetching locations after %d attempts",
                _MAX_RETRIES,
            )
            return None, f"transient network error after {_MAX_RETRIES} attempts: {err}"
        if resp.status_code == 429:
            rate_limit_retries += 1
            if rate_limit_retries > _MAX_RATE_LIMIT_RETRIES:
                return None, f"HTTP 429: rate limited after {_MAX_RATE_LIMIT_RETRIES} retries"
            retry_after = resp.headers.get("Retry-After")
            try:
                delay = float(retry_after) if retry_after else _429_BACKOFF
            except (ValueError, TypeError):
                # Retry-After can be an HTTP-date string ("Thu, 01 Jan ...") — fall back.
                delay = _429_BACKOFF
            logger.warning(
                "Locations: 429 rate limited — waiting %.0fs (attempt %d/%d)",
                delay,
                rate_limit_retries,
                _MAX_RATE_LIMIT_RETRIES,
            )
            time.sleep(delay)
            continue
        if resp.status_code >= 500:
            logger.warning("Location: HTTP %s", resp.status_code)
            return None, f"HTTP {resp.status_code}"
        resp.raise_for_status()
        result = resp.json()
        break
    return transform_result(result), None


def fetch_readings_for_location(
    client: Client, path: str, params: dict, loc_id: str
) -> tuple[list[dict] | None, str | None]:
    """
    Fetch a list of readings for the given location id from the source, performing the query defined in the given
    params against the given path
    """
    rate_limit_retries = 0
    result: dict[Any, Any] = {}
    while True:

        def _fetch_readings() -> Response:
            return client.get(path, params=params)

        try:
            resp = retry_transient(
                _fetch_readings,
                on_retry=lambda exc, attempt, seconds: logger.warning(
                    "Location %s: error (%s) on attempt %d - retrying in %.0fs",
                    loc_id,
                    exc,
                    attempt,
                    seconds,
                ),
            )
        except _TRANSIENT_ERRORS as err:
            logger.warning(
                "Location %s: transient error after %d attempts — skipping",
                loc_id,
                _MAX_RETRIES,
            )
            return None, f"transient network error after {_MAX_RETRIES} attempts: {err}"
        if resp.status_code == 404:
            logger.warning("Location %s: 404 — no data endpoint", loc_id)
            return None, None
        if resp.status_code == 429:
            rate_limit_retries += 1
            if rate_limit_retries > _MAX_RATE_LIMIT_RETRIES:
                return None, f"HTTP 429: rate limited after {_MAX_RATE_LIMIT_RETRIES} retries"
            retry_after = resp.headers.get("Retry-After")
            try:
                delay = float(retry_after) if retry_after else _429_BACKOFF
            except (ValueError, TypeError):
                # Retry-After can be an HTTP-date string ("Thu, 01 Jan ...") — fall back.
                delay = _429_BACKOFF
            logger.warning(
                "Location %s: 429 rate limited — waiting %.0fs (attempt %d/%d)",
                loc_id,
                delay,
                rate_limit_retries,
                _MAX_RATE_LIMIT_RETRIES,
            )
            time.sleep(delay)
            continue
        if resp.status_code >= 500:
            logger.warning("Location %s: HTTP %s — skipping", loc_id, resp.status_code)
            return None, f"HTTP {resp.status_code}"
        resp.raise_for_status()
        result = resp.json()
        break
    rows = transform_result(result)
    return rows, None
