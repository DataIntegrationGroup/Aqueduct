"""
Vendor-level HydroVu API client, shared by every HydroVu tenant. Tenant-agnostic —
takes tenant-specific values as arguments.

API endpoints:
  - Auth:      POST https://hydrovu.com/public-api/oauth/token
  - Locations: GET  https://www.hydrovu.com/public-api/v1/locations/list
  - Readings:  GET  https://www.hydrovu.com/public-api/v1/locations/{id}/data?startTime={unix_ts}
  - Pagination: X-ISI-Start-Page="" on the first request; X-ISI-Next-Page response
                header carries the next cursor; stop when absent or empty.
  - Token refresh: BearerAuth (shared/http.py) handles a 401 automatically.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator

import httpx
from google.cloud import secretmanager

from aqueduct_dagster.shared.config import load_config
from aqueduct_dagster.shared.gcp_auth import ensure_adc
from aqueduct_dagster.shared.http import (
    DEFAULT_MAX_RETRIES as _MAX_RETRIES,
)
from aqueduct_dagster.shared.http import (
    TRANSIENT_HTTP_ERRORS as _TRANSIENT_ERRORS,
)
from aqueduct_dagster.shared.http import (
    TokenManager,
    build_authenticated_client,
    retry_transient,
)

logger = logging.getLogger(__name__)

_LOCATION_TIMEOUT = httpx.Timeout(connect=30.0, read=60.0, write=30.0, pool=30.0)
_429_BACKOFF = 60.0  # seconds to wait on 429 when Retry-After header is absent
_MAX_RATE_LIMIT_RETRIES = 3


def fetch_locations(client: httpx.Client) -> list[dict]:
    """Fetches all locations, walking cursor-based pages (same pattern as location data)."""
    all_locations: list[dict] = []
    page_cursor: str = ""
    page_num = 0

    logger.info("Fetching HydroVu location list")
    while True:
        page_num += 1

        def _fetch_page(cursor: str = page_cursor) -> httpx.Response:
            return client.get("/locations/list", headers={"X-ISI-Start-Page": cursor})

        resp = retry_transient(
            _fetch_page,
            on_retry=lambda exc, attempt, delay: logger.warning(
                "locations/list: transient error (%s) on attempt %d — retrying in %.0fs",
                exc,
                attempt,
                delay,
            ),
        )
        resp.raise_for_status()
        page = resp.json()
        all_locations.extend(page)
        logger.info(
            "Location list page %d: %d locations (running total %d)",
            page_num,
            len(page),
            len(all_locations),
        )

        next_cursor = resp.headers.get("X-ISI-Next-Page", "")
        if not next_cursor:
            break
        page_cursor = next_cursor

    logger.info(
        "Location list complete: %d locations across %d pages", len(all_locations), page_num
    )
    return all_locations


def fetch_location_data(
    client: httpx.Client, location_id: int, start_time: int, end_time: int | None = None
) -> tuple[dict | None, str | None]:
    """Fetches all readings for one location, walking cursor-based pages.

    end_time, if given, stops pagination once a page's readings reach it —
    used by backfill's windowed chunk fetch. Production's normal ingest always
    passes None (unbounded, fetch-to-present).

    A 404 does NOT mean the location has no data endpoint: a dormant location
    that 404s on a recent start_time returns its full history at start_time=0.
    Skip it without advancing the cursor; it recovers on its own if the logger
    comes back.

    Returns (data, None) on success, (None, None) on 404 (not an error), or
    (None, reason) on a real error (non-2xx status, or exhausted retries)."""
    all_data: dict | None = None
    page_cursor: str = ""
    page_num = 0
    path = f"/locations/{location_id}/data"
    params = {"startTime": start_time}
    # Per-location counter, resets for each new location. Intentionally spans all pages
    # of that location's fetch so an issue with a single location doesn't burn the full
    # _MAX_RATE_LIMIT_RETRIES budget across multiple other locations.
    rate_limit_retries = 0

    while True:
        page_num += 1
        logger.info("Location %s: fetching readings page %d", location_id, page_num)

        def _fetch_page(cursor: str = page_cursor) -> httpx.Response:
            return client.get(path, headers={"X-ISI-Start-Page": cursor}, params=params)

        try:
            resp = retry_transient(
                _fetch_page,
                on_retry=lambda exc, attempt, delay: logger.warning(
                    "Location %s: transient error (%s) on attempt %d — retrying in %.0fs",
                    location_id,
                    exc,
                    attempt,
                    delay,
                ),
            )
        except _TRANSIENT_ERRORS as exc:
            logger.warning(
                "Location %s: transient error after %d attempts — skipping",
                location_id,
                _MAX_RETRIES,
            )
            return None, f"transient network error after {_MAX_RETRIES} attempts: {exc}"

        if resp.status_code == 404:
            logger.warning(
                "Location %s: 404 — no data at or after startTime=%s, skipping",
                location_id,
                start_time,
            )
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
                location_id,
                delay,
                rate_limit_retries,
                _MAX_RATE_LIMIT_RETRIES,
            )
            time.sleep(delay)
            continue

        if not resp.is_success:
            logger.warning(
                "Location %s: HTTP %s — skipping (%s)",
                location_id,
                resp.status_code,
                resp.text[:200],
            )
            return None, f"HTTP {resp.status_code}"

        page_data = resp.json()

        reached_end = False
        if end_time is not None:
            for param in page_data.get("parameters", []):
                readings = param.get("readings", [])
                kept = [r for r in readings if r["timestamp"] < end_time]
                if len(kept) < len(readings):
                    reached_end = True
                param["readings"] = kept

        if all_data is None:
            all_data = page_data
        else:
            existing = {p["parameterId"]: p for p in all_data.get("parameters", [])}
            for param in page_data.get("parameters", []):
                pid = param["parameterId"]
                if pid in existing:
                    existing[pid]["readings"].extend(param["readings"])
                else:
                    all_data.setdefault("parameters", []).append(param)

        if reached_end:
            logger.info(
                "Location %s: reached end_time=%s — stopping pagination", location_id, end_time
            )
            break

        next_cursor = resp.headers.get("X-ISI-Next-Page", "")
        if not next_cursor:
            break
        page_cursor = next_cursor

    total = sum(len(p.get("readings", [])) for p in (all_data or {}).get("parameters", []))
    logger.info("Location %s: fetched %d readings across %d pages", location_id, total, page_num)
    return all_data, None


def resolve_hydrovu_credentials(
    client_id: str, client_secret: str, gcp_secret: str
) -> tuple[str, str]:
    """Returns (client_id, client_secret) — fetches from Secret Manager if
    client_id isn't already supplied (tests/local overrides can pass both
    explicitly). gcp_secret names the tenant's secret; payload is always
    {"id": ..., "secret": ...}."""
    if client_id:
        return client_id, client_secret

    project_number = load_config()["destination"]["filesystem"]["gcp_project_number"]

    # Secret Manager authenticates via ADC just like GCS does, so the same
    # bootstrap has to run before the client is constructed. Idempotent.
    ensure_adc()
    sm_client = secretmanager.SecretManagerServiceClient()
    name = sm_client.secret_version_path(project_number, gcp_secret, "latest")
    response = sm_client.access_secret_version(name=name)
    payload = json.loads(response.payload.data.decode("UTF-8"))
    return payload["id"], payload["secret"]


def build_hydrovu_client(
    client_id: str,
    client_secret: str,
    gcp_secret: str,
    api_base_url: str,
    token_url: str,
) -> httpx.Client:
    """Resolves credentials and returns an authenticated httpx.Client, shared by
    each tenant's normal ingest and backfill source so OAuth logic is written once."""
    client_id, client_secret = resolve_hydrovu_credentials(client_id, client_secret, gcp_secret)
    tm = TokenManager(token_url, client_id, client_secret)
    return build_authenticated_client(api_base_url, tm, timeout=_LOCATION_TIMEOUT)


def iter_location_readings(
    client: httpx.Client,
    start_ts: int,
    locations: list[dict],
    location_ids: list[int],
    cursors: dict[str, int],
    stats: dict,
) -> Iterator[dict]:
    """Yields one flat record per (location, parameter, reading) across every
    allowlisted location — the body of each tenant's hydrovu_readings resource.

    location_ids: allowlist from .dlt/config.toml; others are skipped to avoid
      slow 404s.
    cursors: caller's per-location Unix-second cursor dict, mutated in place —
      only advances after a successful fetch, so a failed location retries
      from the same point next run.
    stats: populated at generator end — rows_yielded, locations_fetched/
      skipped/no_data/errored, failed_location_ids.

    Record shape: reading_id, location_id, timestamp (Unix s), parameter_id,
    unit_id, value.

    One failing location never stops the others — errors are counted and logged."""
    _allowed: frozenset[int] = frozenset(location_ids)

    skipped = 0
    fetched = 0
    no_data = 0
    errored = 0
    failed_ids: list[int] = []
    rows_yielded = 0
    for location in locations:
        loc_id = location["id"]
        if loc_id not in _allowed:
            skipped += 1
            continue

        loc_start = max(cursors.get(str(loc_id), 0), start_ts)
        logger.info(
            "Fetching readings for location %s (%s) from Unix timestamp %s",
            loc_id,
            location["name"],
            loc_start,
        )

        data, err = fetch_location_data(client, loc_id, loc_start)
        if err is not None:
            logger.warning(
                "Location %s (%s) failed: %s — cursor not advanced, will retry next run",
                loc_id,
                location["name"],
                err,
            )
            errored += 1
            failed_ids.append(loc_id)
            continue
        if data is None:
            logger.warning("Location %s (%s): no data (404)", loc_id, location["name"])
            no_data += 1
            continue

        fetched += 1
        max_ts = loc_start
        for param in data.get("parameters", []):
            for reading in param.get("readings", []):
                ts = reading["timestamp"]
                if ts > max_ts:
                    max_ts = ts
                rows_yielded += 1
                yield {
                    "reading_id": f"{loc_id}_{param['parameterId']}_{ts}",
                    "location_id": loc_id,
                    "timestamp": ts,
                    "parameter_id": param["parameterId"],
                    "unit_id": param["unitId"],
                    "value": reading["value"],
                }

        # Advance this location's cursor only after a successful fetch.
        # A failed location keeps its old cursor and retries from the same point next run.
        cursors[str(loc_id)] = max_ts

    logger.info(
        "hydrovu_readings extract complete: %d fetched, %d errored, %d no-data, "
        "%d skipped (allowlist), %d rows yielded",
        fetched,
        errored,
        no_data,
        skipped,
        rows_yielded,
    )
    # NOTE: stats is populated here at generator end. If dlt abandons the generator
    # mid-run (pipeline error, KeyboardInterrupt), stats stays empty and the asset
    # falls back to stats.get(..., 0) defaults — metadata shows zeros, no exception raised.
    stats["rows_yielded"] = rows_yielded
    stats["locations_fetched"] = fetched
    stats["locations_skipped"] = skipped
    stats["locations_no_data"] = no_data
    stats["locations_errored"] = errored
    stats["failed_location_ids"] = failed_ids


def location_row(location: dict) -> dict:
    """Flattens one /locations/list object into the hydrovu_locations record
    shape shared by every tenant; id is the join key for hydrovu_readings."""
    return {
        "id": location["id"],
        "name": location["name"],
        "description": location["description"],
        "latitude": location["gps"]["latitude"],
        "longitude": location["gps"]["longitude"],
    }
