"""
sources/hydrovu/dlt_pipeline.py

dlt pipeline for HydroVu raw ingestion.

Two resources returned from hydrovu_source():

  hydrovu_locations  (write_disposition="replace")
    Fetches GET /locations/list on every run and fully replaces the parquet.
    One row per location: id, name, description, latitude, longitude.
    Acts as a reference table — rename in HydroVu → latest name in GCS.
    Written to: gs://<bucket>/raw_pvacd/hydrovu_locations/year={YYYY}/month={MM}/day={DD}/

  hydrovu_readings   (write_disposition="append", per-location incremental cursor)
    Fetches readings per location since that location's last successful fetch.
    Each location has its own cursor in dlt.current.resource_state() — a failed location retries
    from the same point next run rather than being skipped permanently.
    One row per (location, parameter, reading) — location metadata is NOT
    embedded; join to hydrovu_locations on location_id at transform time.
    Written to: gs://<bucket>/raw_pvacd/hydrovu_readings/year={YYYY}/month={MM}/day={DD}/

_TokenManager is created once in hydrovu_source() and passed to both
resources so a single token covers the full run.

This module is NOT a Dagster asset — it is called by defs/assets/ingest_hydrovu.py

dlt destination = filesystem (GCS)
  → GCS is the final destination for the raw data ingested by this pipeline.
    → dlt writes parquet files to GCS and manages the incremental cursor state.

API endpoints confirmed:
  - Auth:      POST https://hydrovu.com/public-api/oauth/token
  - Locations: GET  https://www.hydrovu.com/public-api/v1/locations/list
  - Readings:  GET  https://www.hydrovu.com/public-api/v1/locations/{id}/data?startTime={unix_ts}
  - Pagination: X-ISI-Start-Page="" on first request; response carries X-ISI-Next-Page opaque
                cursor token; pass it verbatim on the next request; stop when absent or empty
  - Token refresh: client credentials tokens have a finite TTL; 401 triggers one automatic retry
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import dlt
import httpx
import toml
from google.cloud import secretmanager

from aqueduct_dagster.shared.http import (
    DEFAULT_MAX_RETRIES as _MAX_RETRIES,
)
from aqueduct_dagster.shared.http import (
    TRANSIENT_HTTP_ERRORS as _TRANSIENT_ERRORS,
)
from aqueduct_dagster.shared.http import retry_transient
from aqueduct_dagster.shared.pipeline import build_source_pipeline

logger = logging.getLogger(__name__)


class _TokenManager:
    """Fetches and caches a client-credentials token; re-fetches on expiry or 401."""

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
        resp = httpx.post(
            self._token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        self._token = body["access_token"]
        # Refresh 60 s before actual expiry; default to 55 min if field absent
        ttl = body.get("expires_in", 3600)
        self._expires_at = time.monotonic() + ttl - 60
        logger.info("HydroVu token refreshed (expires_in=%ss)", ttl)


def _auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _fetch_locations(api_base_url: str, tm: _TokenManager) -> list[dict]:
    """Fetches all locations, walking cursor-based pages (same pattern as location data)."""
    all_locations: list[dict] = []
    page_cursor: str = ""
    page_num = 0

    logger.info("Fetching HydroVu location list")
    while True:
        page_num += 1
        resp = httpx.get(
            f"{api_base_url}/locations/list",
            headers={**_auth_headers(tm.get()), "X-ISI-Start-Page": page_cursor},
            timeout=30,
        )
        if resp.status_code == 401:
            logger.warning("401 on /locations/list — refreshing token and retrying")
            fresh_token = tm.force_refresh()

            def _refetch_list(
                token: str = fresh_token, cursor: str = page_cursor
            ) -> httpx.Response:
                return httpx.get(
                    f"{api_base_url}/locations/list",
                    headers={**_auth_headers(token), "X-ISI-Start-Page": cursor},
                    timeout=30,
                )

            resp = retry_transient(
                _refetch_list,
                on_retry=lambda exc, attempt, delay: logger.warning(
                    "locations/list 401-retry: transient error (%s) on attempt %d"
                    " — retrying in %.0fs",
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


_LOCATION_TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)
_429_BACKOFF = 60.0  # seconds to wait on 429 when Retry-After header is absent
_MAX_RATE_LIMIT_RETRIES = 3


def _fetch_location_data(
    api_base_url: str, location_id: int, start_time: int, tm: _TokenManager
) -> tuple[dict | None, str | None]:
    """
    Fetches all readings for one location, walking cursor-based pages.

    Returns:
      (data, None)   — success
      (None, None)   — HTTP 404: location has no data endpoint (expected, not an error)
      (None, reason) — real error: HTTP 429, 5xx, or exhausted retries

    On 401: refreshes token and retries once (with transient-error protection).
    On 429: respects Retry-After header; falls back to _429_BACKOFF seconds.
            Retries up to _MAX_RATE_LIMIT_RETRIES times, then returns (None, reason).
    On transient network errors: retries up to _MAX_RETRIES times with exponential
            backoff, then returns (None, reason).

    Pagination: X-ISI-Start-Page="" on the first request, then pass the
    X-ISI-Next-Page cursor token from each response verbatim. Stop when
    X-ISI-Next-Page is absent or empty (~20 readings per page, ~2 days each).
    """
    all_data: dict | None = None
    page_cursor: str = ""
    page_num = 0
    # Per-location counter — resets for each new location, intentionally spans all pages
    # of that location's fetch so a single badly-behaved location can't burn the full
    # _MAX_RATE_LIMIT_RETRIES budget across multiple other locations.
    rate_limit_retries = 0

    while True:
        page_num += 1
        logger.info("Location %s: fetching readings page %d", location_id, page_num)
        url = f"{api_base_url}/locations/{location_id}/data"
        params = {"startTime": start_time}

        def _fetch_page(
            u: str = url, cursor: str = page_cursor, p: dict = params
        ) -> httpx.Response:
            return httpx.get(
                u,
                headers={**_auth_headers(tm.get()), "X-ISI-Start-Page": cursor},
                params=p,
                timeout=_LOCATION_TIMEOUT,
            )

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

        if resp.status_code == 401:
            logger.warning("401 on location %s — refreshing token and retrying", location_id)
            fresh_token = tm.force_refresh()

            def _refetch_page(
                u: str = url, token: str = fresh_token, cursor: str = page_cursor, p: dict = params
            ) -> httpx.Response:
                return httpx.get(
                    u,
                    headers={**_auth_headers(token), "X-ISI-Start-Page": cursor},
                    params=p,
                    timeout=_LOCATION_TIMEOUT,
                )

            try:
                resp = retry_transient(
                    _refetch_page,
                    on_retry=lambda exc, attempt, delay: logger.warning(
                        "Location %s: 401-retry transient error (%s) on attempt %d"
                        " — retrying in %.0fs",
                        location_id,
                        exc,
                        attempt,
                        delay,
                    ),
                )
            except _TRANSIENT_ERRORS as exc:
                return (None, f"transient network error after token refresh: {exc}")

        if resp.status_code == 404:
            logger.warning("Location %s: 404 — no data endpoint", location_id)
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

        if resp.status_code >= 500:
            logger.warning("Location %s: HTTP %s — skipping", location_id, resp.status_code)
            return None, f"HTTP {resp.status_code}"

        resp.raise_for_status()

        page_data = resp.json()

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

        next_cursor = resp.headers.get("X-ISI-Next-Page", "")
        if not next_cursor:
            break
        page_cursor = next_cursor

    total = sum(len(p.get("readings", [])) for p in (all_data or {}).get("parameters", []))
    logger.info("Location %s: fetched %d readings across %d pages", location_id, total, page_num)
    return all_data, None


@dlt.source(name="hydrovu")
def hydrovu_source(
    client_id: str = "",
    client_secret: str = "",
    gcp_secret: str = dlt.config.value,
    api_base_url: str = dlt.config.value,
    token_url: str = dlt.config.value,
    initial_start_date: str = dlt.config.value,
    location_ids: list[int] = dlt.config.value,  # noqa: B008
    _stats: dict | None = None,
) -> Any:
    """
    Reads config from dlt.config under [hydrovu].
    Creates a single _TokenManager shared by both resources so the token is
    fetched once and reused across the full run.
    Fetches the location list once and passes it to both resources to avoid
    a redundant second API call.

    location_ids: allowlist of HydroVu location integer IDs to fetch.
      Read from [sources.hydrovu] location_ids in .dlt/config.toml.
      Add or remove IDs there without any code change.

    _stats: optional mutable dict populated with extraction counts after pipeline.run().
      keys: rows_yielded, locations_fetched, locations_skipped, locations_no_data,
            locations_errored, failed_location_ids
    """
    if not client_id:
        config_path = os.path.join(os.getcwd(), ".dlt", "config.toml")
        project_number = toml.load(config_path)["destination"]["filesystem"]["gcp_project_number"]

        client = secretmanager.SecretManagerServiceClient()
        name = client.secret_version_path(project_number, gcp_secret, "latest")
        response = client.access_secret_version(name=name)
        payload = json.loads(response.payload.data.decode("UTF-8"))

        client_id = payload["id"]
        client_secret = payload["secret"]

    start_ts = int(
        datetime.strptime(initial_start_date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
    )
    tm = _TokenManager(token_url, client_id, client_secret)
    locations = _fetch_locations(api_base_url, tm)
    return (
        hydrovu_locations(locations=locations),
        hydrovu_readings(
            api_base_url=api_base_url,
            start_ts=start_ts,
            tm=tm,
            locations=locations,
            location_ids=location_ids,
            _stats=_stats if _stats is not None else {},
        ),
    )


@dlt.resource(
    name="hydrovu_locations",
    write_disposition="replace",
)
def hydrovu_locations(locations: list[dict]) -> Iterator[dict]:
    """
    Yields one record per location from the pre-fetched location list.
    write_disposition="replace" ensures the parquet is fully refreshed on
    every run, so renames or removals in HydroVu are reflected immediately.

    Record shape:
      id          — HydroVu location integer ID (join key for hydrovu_readings)
      name        — well name (e.g. "Bartlett Level Troll")
      description — well number (e.g. "827276")
      latitude, longitude
    """
    logger.info("Extracting hydrovu_locations (full replace)")
    for location in locations:
        yield {
            "id": location["id"],
            "name": location["name"],
            "description": location["description"],
            "latitude": location["gps"]["latitude"],
            "longitude": location["gps"]["longitude"],
        }


@dlt.resource(
    name="hydrovu_readings",
    write_disposition="append",
    primary_key="reading_id",
)
def hydrovu_readings(
    api_base_url: str,
    start_ts: int,
    tm: _TokenManager,
    locations: list[dict],
    location_ids: list[int],
    _stats: dict | None = None,
) -> Iterator[dict]:
    """
    Yields one flat record per (location, parameter, reading).
    Location metadata is NOT embedded — join to hydrovu_locations on location_id.

    location_ids: allowlist of HydroVu location integer IDs to fetch. Locations
      absent from this list are skipped to avoid slow 404s on /locations/{id}/data.
      Managed via [sources.hydrovu] location_ids in .dlt/config.toml.

    Incremental: each location has its own cursor stored in dlt.current.resource_state() under
    "location_cursors". A location's cursor only advances after a successful fetch,
    so a failed location retries from the same point on the next run.
    On first run (or new location), falls back to start_ts from config.
    dlt additionally deduplicates on primary_key=reading_id.

    Record shape:
      reading_id   — "{location_id}_{parameter_id}_{timestamp}"
      location_id  — HydroVu location integer ID (FK → hydrovu_locations.id)
      timestamp    — Unix epoch seconds
      parameter_id — HydroVu param code (e.g. "4"=DTW, "1"=Temperature, "33"=Battery)
      unit_id      — HydroVu unit code (e.g. "35"=feet)
      value        — float measurement
    """
    cursors: dict[str, int] = dlt.current.resource_state().setdefault("location_cursors", {})
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

        data, err = _fetch_location_data(api_base_url, loc_id, loc_start, tm)
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
    # NOTE: _stats is populated here at generator end. If dlt abandons the generator
    # mid-run (pipeline error, KeyboardInterrupt), _stats stays empty and the asset
    # falls back to stats.get(..., 0) defaults — metadata shows zeros, no exception raised.
    if _stats is not None:
        _stats["rows_yielded"] = rows_yielded
        _stats["locations_fetched"] = fetched
        _stats["locations_skipped"] = skipped
        _stats["locations_no_data"] = no_data
        _stats["locations_errored"] = errored
        _stats["failed_location_ids"] = failed_ids


def build_pipeline() -> dlt.Pipeline:
    return build_source_pipeline("pvacd_hydrovu", "raw_pvacd")


def run_pipeline() -> None:
    """Convenience entry point: builds and runs the pipeline with parquet output."""
    pipeline = build_pipeline()
    load_info = pipeline.run(hydrovu_source(), loader_file_format="parquet")
    logger.info("Load complete: %s", load_info)
