"""
pipeline/hydrovu_dlt_pipeline.py

dlt pipeline for HydroVu raw ingestion.

Two resources returned from hydrovu_source():

  hydrovu_locations  (write_disposition="replace")
    Fetches GET /locations/list on every run and fully replaces the parquet.
    One row per location: id, name, description, latitude, longitude.
    Acts as a reference table — rename in HydroVu → latest name in GCS.
    Written to: gs://<bucket>/raw_pvacd/hydrovu_locations/

  hydrovu_readings   (write_disposition="append", incremental cursor)
    Fetches readings per location since the last run's max timestamp.
    One row per (location, parameter, reading) — location metadata is NOT
    embedded; join to hydrovu_locations on location_id at transform time.
    Written to: gs://<bucket>/raw_pvacd/hydrovu_readings/

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

import logging
import time
from datetime import datetime, timezone
from typing import Iterator

import dlt
import httpx

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

    while True:
        resp = httpx.get(
            f"{api_base_url}/locations/list",
            headers={**_auth_headers(tm.get()), "X-ISI-Start-Page": page_cursor},
            timeout=30,
        )
        if resp.status_code == 401:
            logger.warning("401 on /locations/list — refreshing token and retrying")
            resp = httpx.get(
                f"{api_base_url}/locations/list",
                headers={**_auth_headers(tm.force_refresh()), "X-ISI-Start-Page": page_cursor},
                timeout=30,
            )
        resp.raise_for_status()
        all_locations.extend(resp.json())

        next_cursor = resp.headers.get("X-ISI-Next-Page", "")
        if not next_cursor:
            break
        page_cursor = next_cursor

    return all_locations


_LOCATION_TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)
_MAX_RETRIES = 3
_RETRY_BACKOFF = (2.0, 4.0, 8.0)
_TRANSIENT_ERRORS = (httpx.ReadError, httpx.ConnectError, httpx.RemoteProtocolError)


def _fetch_location_data(
    api_base_url: str, location_id: int, start_time: int, tm: _TokenManager
) -> dict | None:
    """
    Fetches all readings for one location, walking cursor-based pages.
    Returns None on 404/500. Retries once on 401 per page.
    Retries up to 3 times on transient network errors (connection reset, etc.)
    with exponential backoff.

    Pagination: X-ISI-Start-Page="" on the first request, then pass the
    X-ISI-Next-Page cursor token from each response verbatim. Stop when
    X-ISI-Next-Page is absent or empty (~20 readings per page, ~2 days each).
    """
    all_data: dict | None = None
    page_cursor: str = ""

    while True:
        url = f"{api_base_url}/locations/{location_id}/data"
        params = {"startTime": start_time}

        resp = None
        for attempt in range(_MAX_RETRIES):
            try:
                resp = httpx.get(
                    url,
                    headers={**_auth_headers(tm.get()), "X-ISI-Start-Page": page_cursor},
                    params=params,
                    timeout=_LOCATION_TIMEOUT,
                )
                break
            except _TRANSIENT_ERRORS as exc:
                if attempt < _MAX_RETRIES - 1:
                    delay = _RETRY_BACKOFF[attempt]
                    logger.warning(
                        "Location %s: transient error (%s) on attempt %d — retrying in %.0fs",
                        location_id, exc, attempt + 1, delay,
                    )
                    time.sleep(delay)
                else:
                    logger.warning(
                        "Location %s: transient error after %d attempts — skipping",
                        location_id, _MAX_RETRIES,
                    )
                    return None

        if resp is None:
            return None

        if resp.status_code == 401:
            logger.warning("401 on location %s — refreshing token and retrying", location_id)
            resp = httpx.get(
                url,
                headers={**_auth_headers(tm.force_refresh()), "X-ISI-Start-Page": page_cursor},
                params=params,
                timeout=_LOCATION_TIMEOUT,
            )
        if resp.status_code in (404, 500):
            logger.warning("Location %s returned %s — skipping", location_id, resp.status_code)
            return None
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
    logger.info("Location %s: fetched %d readings across all pages", location_id, total)
    return all_data


@dlt.source(name="hydrovu")
def hydrovu_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    api_base_url: str = dlt.config.value,
    token_url: str = dlt.config.value,
    initial_start_date: str = dlt.config.value,
):
    """
    Reads credentials and config from dlt.secrets/dlt.config under [hydrovu].
    Creates a single _TokenManager shared by both resources so the token is
    fetched once and reused across the full run.
    """
    start_ts = int(
        datetime.strptime(initial_start_date, "%Y-%m-%d")
        .replace(tzinfo=timezone.utc)
        .timestamp()
    )
    tm = _TokenManager(token_url, client_id, client_secret)
    return (
        hydrovu_locations(api_base_url=api_base_url, tm=tm),
        hydrovu_readings(api_base_url=api_base_url, start_ts=start_ts, tm=tm),
    )


@dlt.resource(
    name="hydrovu_locations",
    write_disposition="replace",
)
def hydrovu_locations(api_base_url: str, tm: _TokenManager) -> Iterator[dict]:
    """
    Yields one record per location from GET /locations/list.
    write_disposition="replace" ensures the parquet is fully refreshed on
    every run, so renames or removals in HydroVu are reflected immediately.

    Record shape:
      id          — HydroVu location integer ID (join key for hydrovu_readings)
      name        — well name (e.g. "Bartlett Level Troll")
      description — well number (e.g. "827276")
      latitude, longitude
    """
    for location in _fetch_locations(api_base_url, tm):
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
    updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
        "timestamp",
        initial_value=0,
    ),
) -> Iterator[dict]:
    """
    Yields one flat record per (location, parameter, reading).
    Location metadata is NOT embedded — join to hydrovu_locations on location_id.

    Incremental: uses updated_at.last_value (max timestamp from last run) as
    startTime for the API call. On first run, falls back to start_ts from config.
    dlt additionally deduplicates on primary_key=reading_id.

    Record shape:
      reading_id   — "{location_id}_{parameter_id}_{timestamp}"
      location_id  — HydroVu location integer ID (FK → hydrovu_locations.id)
      timestamp    — Unix epoch seconds (dlt cursor field)
      parameter_id — HydroVu param code (e.g. "4"=DTW, "1"=Temperature, "33"=Battery)
      unit_id      — HydroVu unit code (e.g. "35"=feet)
      value        — float measurement
    """
    api_start = max(updated_at.last_value or 0, start_ts)
    logger.info("HydroVu fetch starting from Unix timestamp %s", api_start)

    for location in _fetch_locations(api_base_url, tm):
        loc_id = location["id"]
        logger.info("Fetching readings for location %s (%s)", loc_id, location["name"])

        data = _fetch_location_data(api_base_url, loc_id, api_start, tm)
        if data is None:
            continue
        for param in data.get("parameters", []):
            for reading in param.get("readings", []):
                yield {
                    "reading_id": f"{loc_id}_{param['parameterId']}_{reading['timestamp']}",
                    "location_id": loc_id,
                    "timestamp": reading["timestamp"],
                    "parameter_id": param["parameterId"],
                    "unit_id": param["unitId"],
                    "value": reading["value"],
                }


def build_pipeline() -> dlt.Pipeline:
    """
    Returns a configured dlt pipeline writing parquet to GCS.
    Bucket is read from config.toml [destination.filesystem] bucket_url.
    Writes to gs://<bucket>/raw_pvacd/hydrovu_readings/

    Always call pipeline.run(..., loader_file_format="parquet") — the format
    cannot be set reliably via config.toml for the filesystem destination.
    """
    return dlt.pipeline(
        pipeline_name="pvacd_hydrovu",
        destination="filesystem",
        dataset_name="raw_pvacd",
    )


def run_pipeline() -> None:
    """Convenience entry point: builds and runs the pipeline with parquet output."""
    pipeline = build_pipeline()
    load_info = pipeline.run(hydrovu_source(), loader_file_format="parquet")
    logger.info("Load complete: %s", load_info)
