"""
pipeline/hydrovu_dlt_pipeline.py

dlt pipeline for HydroVu raw ingestion.

Follows the same pattern as cabq_dlt_pipeline.py (and all future sources).
  - @dlt.source:    defines the HydroVu source, reads creds from dlt.secrets
  - @dlt.resource:  incremental cursor on timestamp field, yields one flat
                    record per parameter per reading per location
  - build_pipeline(): filesystem destination → GCS under raw/pvacd/

What dlt does here:
  - Calls the HydroVu API and fetches readings per location
  - Handles incremental loading via dlt.sources.incremental (cursor = timestamp)
  - Writes raw parquet to GCS (filesystem destination) under:
      gs://<bucket>/raw/pvacd/hydrovu_readings/
  - Stores cursor state (last fetched timestamp) alongside the data in GCS

This module is NOT a Dagster asset — it is called by defs/assets/ingest_hydrovu.py

dlt destination = filesystem (GCS)
  → GCS is the final destination for the raw data ingested by this pipeline.
    → dlt writes parquet files to GCS and manages the incremental cursor state.

API endpoints confirmed:
  - Auth:      POST https://hydrovu.com/public-api/oauth/token
  - Locations: GET  https://www.hydrovu.com/public-api/v1/locations/list
  - Readings:  GET  https://www.hydrovu.com/public-api/v1/locations/{id}/data?startTime={unix_ts}
  - Pagination: API returns all data on page 0; X-ISI-Start-Page header is ignored by server
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
    """Fetches all locations in a single call (API returns all on page 0)."""
    resp = httpx.get(
        f"{api_base_url}/locations/list",
        headers={**_auth_headers(tm.get()), "X-ISI-Start-Page": "0"},
        timeout=30,
    )
    if resp.status_code == 401:
        logger.warning("401 on /locations/list — refreshing token and retrying")
        resp = httpx.get(
            f"{api_base_url}/locations/list",
            headers={**_auth_headers(tm.force_refresh()), "X-ISI-Start-Page": "0"},
            timeout=30,
        )
    resp.raise_for_status()
    return resp.json()


def _fetch_location_data(
    api_base_url: str, location_id: int, start_time: int, tm: _TokenManager
) -> dict | None:
    """Fetches readings for one location. Returns None on 404/500. Retries once on 401."""
    resp = httpx.get(
        f"{api_base_url}/locations/{location_id}/data",
        headers={**_auth_headers(tm.get()), "X-ISI-Start-Page": "0"},
        params={"startTime": start_time},
        timeout=120,
    )
    if resp.status_code == 401:
        logger.warning("401 on location %s — refreshing token and retrying", location_id)
        resp = httpx.get(
            f"{api_base_url}/locations/{location_id}/data",
            headers={**_auth_headers(tm.force_refresh()), "X-ISI-Start-Page": "0"},
            params={"startTime": start_time},
            timeout=120,
        )
    if resp.status_code in (404, 500):
        logger.warning("Location %s returned %s — skipping", location_id, resp.status_code)
        return None
    resp.raise_for_status()
    return resp.json()


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
    Converts initial_start_date (YYYY-MM-DD) to a Unix timestamp for the API.
    """
    start_ts = int(
        datetime.strptime(initial_start_date, "%Y-%m-%d")
        .replace(tzinfo=timezone.utc)
        .timestamp()
    )
    return hydrovu_readings(
        client_id=client_id,
        client_secret=client_secret,
        api_base_url=api_base_url,
        token_url=token_url,
        start_ts=start_ts,
    )


@dlt.resource(
    name="hydrovu_readings",
    write_disposition="append",
    primary_key="reading_id",
)
def hydrovu_readings(
    client_id: str,
    client_secret: str,
    api_base_url: str,
    token_url: str,
    start_ts: int,
    updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
        "timestamp",
        initial_value=0,
    ),
) -> Iterator[dict]:
    """
    Yields one flat record per (location, parameter, reading).

    Incremental: uses updated_at.last_value (max timestamp from last run) as
    startTime for the API call. On first run, falls back to start_ts from config.
    dlt additionally deduplicates on primary_key=reading_id.

    Record shape:
      reading_id        — "{location_id}_{parameter_id}_{timestamp}"
      location_id       — HydroVu location integer ID
      location_name     — well name (e.g. "Bartlett-827276")
      location_description — well number (e.g. "827276")
      latitude, longitude
      timestamp         — Unix epoch seconds (dlt cursor field)
      parameter_id      — HydroVu param code (e.g. "4" = Depth to Water, "1" = Temperature, "33" = Battery Level)
      unit_id           — HydroVu unit code (e.g. "241" = feet)
      value             — float measurement
    """
    api_start = max(updated_at.last_value or 0, start_ts)
    logger.info("HydroVu fetch starting from Unix timestamp %s", api_start)

    tm = _TokenManager(token_url, client_id, client_secret)

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
                    "location_name": location["name"],
                    "location_description": location["description"],
                    "latitude": location["gps"]["latitude"],
                    "longitude": location["gps"]["longitude"],
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
