"""
sources/cabq/dlt_pipeline.py

dlt pipeline for CABQ raw ingestion.

Follows the same pattern as hydrovu_dlt_pipeline.py.
  - @dlt.source: reads config from dlt.config under [cabq]
  - @dlt.resource: per-location incremental cursor via dlt.current.resource_state()  - build_pipeline(): filesystem destination → GCS under raw_cabq/
  - run_pipeline(): convenience entry point (mirrors hydrovu_dlt_pipeline.run_pipeline)

Add CABQ config block to .dlt/config.toml when wiring up:
  [cabq]
  api_base_url       = "https://..."   # CABQ CKAN base URL
  initial_start_date = "2026-05-01"    # match HydroVu start date
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import dlt
import httpx

from aqueduct_dagster.shared.http import DEFAULT_MAX_RETRIES as _MAX_RETRIES
from aqueduct_dagster.shared.http import TRANSIENT_HTTP_ERRORS as _TRANSIENT_ERRORS
from aqueduct_dagster.shared.http import build_unauthenticated_client, retry_transient
from aqueduct_dagster.shared.pipeline import build_source_pipeline

logger = logging.getLogger(__name__)

_429_BACKOFF = 60.0  # seconds to wait on 429 when Retry-After header is absent
_MAX_RATE_LIMIT_RETRIES = 3


def _transform_result(data: dict) -> list[dict]:
    """
    The structure of data we get back from CABQ is:
    {
        "objectIdFieldName": "OBJECTID",
        "uniqueIdField": {
            "name": "OBJECTID",
            "isSystemMaintained": true
        },
        "globalIdFieldName": "",
        "fields": [ list of fields in attributes ... ],
        "exceededTransferLimit": true,
        "features": [
            {
                "attributes": { ... }
            },{
                "attributes": { ... }
            },{
                ...
            }
        ]
    }
    The content of "attributes" in each entry of "features" list is what we actually want.
    This function simply reads in the response from CABQ and returns a list the JSON objects in each "attributes" field.
    """
    all_attributes: list[dict] = []
    for feature in data["features"]:
        all_attributes.append(feature["attributes"])
    return all_attributes


def _fetch_locations(client: httpx.Client) -> tuple[list[dict] | None, str | None]:
    """
    get location information from CABQ
    format of location:
    {
        "sys_loc_code": str,    *string code for identifying location
        "loc_name": str,        *full human-readable name of location
        "latitude": num,        *latitude coordinate for location
        "longitude": num        *longitude coordinate for location
    }
    """
    rate_limit_retries = 0
    result: dict[Any, Any] = {}
    while True:

        def _fetch_location_info() -> httpx.Response:
            # query for OBJECTID > 0, aka all entries, for unique location info
            return client.get(
                "/query?where=OBJECTID%3E0&outFields=sys_loc_code,loc_name,latitude,longitude&returnDistinctValues=true&f=pjson"
            )

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
    return _transform_result(result), None


def _fetch_readings_for_location(
    client: httpx.Client, loc_id: str, start_time: int, end_time: int | None = None
) -> tuple[list[dict] | None, str | None]:
    """
    get reading information for location from CABQ
    format of location:
    {
        measurement_date: num, *timestamp of when measurement was taken in unix epoch milliseconds
        water_depth: num, *water level in ft msl
    }
    """
    rate_limit_retries = 0
    result: dict[Any, Any] = {}
    while True:

        def _fetch_readings() -> httpx.Response:
            # query for location code = given location id for measurement info
            if end_time is None:
                return client.get(
                    "/query?where=sys_loc_code%3D'"
                    + loc_id
                    + "'+AND+measurement_date%3E%3D'"
                    # take unix timestamp in seconds and produce date in format YYYY-MM-DD
                    + datetime.fromtimestamp(start_time, tz=UTC).strftime("%Y-%m-%d")
                    + "'&outfields=measurement_date,water_depth&f=pjson"
                )
            else:
                return client.get(
                    "/query?where=sys_loc_code%3D'"
                    + loc_id
                    + "'+AND+measurement_date%3E%3D'"
                    # take unix timestamp in seconds and produce date in format YYYY-MM-DD
                    + datetime.fromtimestamp(start_time, tz=UTC).strftime("%Y-%m-%d")
                    + "'+AND+measurement_date%3C%3D'"
                    # take unix timestamp in seconds and produce date in format YYYY-MM-DD
                    + datetime.fromtimestamp(end_time, tz=UTC).strftime("%Y-%m-%d")
                    + "'&outfields=measurement_date,water_depth&f=pjson"
                )

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

    rows = _transform_result(result)
    if end_time is not None:
        # The API's measurement_date query only accepts date literals (see
        # _fetch_readings() above), so it's inclusive of the entire end date —
        # a reading exactly at end_time's midnight instant would otherwise pass
        # this query but violate the strict half-open [start, end) window
        # load_window() (loader/frost_loader.py) enforces. Trim it client-side
        # on the exact millisecond timestamp, mirroring hydrovu_dlt_pipeline's
        # _fetch_location_data end_time handling. measurement_date is epoch
        # milliseconds; end_time is epoch seconds (see cabq_backfill_readings).
        rows = [row for row in rows if row["measurement_date"] < end_time * 1000]
    return rows, None


def build_cabq_client(
    api_base_url: str,
) -> httpx.Client:
    client = build_unauthenticated_client(
        api_base_url, timeout=httpx.Timeout(connect=30.0, read=60.0, write=30.0, pool=30.0)
    )
    return client


@dlt.source(name="cabq")
def cabq_source(
    api_base_url: str = dlt.config.value,
    initial_start_date: str = dlt.config.value,
    _stats: dict | None = None,
) -> Any:
    start_ts = int(
        datetime.strptime(initial_start_date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
    )
    return cabq_readings(client=build_cabq_client(api_base_url), start_ts=start_ts, _stats=_stats)


@dlt.resource(
    name="cabq_readings",
    write_disposition="append",
    primary_key="reading_id",
)
def cabq_readings(
    client: httpx.Client,
    start_ts: int,
    _stats: dict | None = None,
    # dlt detects the incremental cursor via this default — idiomatic, so B008 is expected.
    updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(  # noqa: B008
        "timestamp",
        initial_value=0,
    ),
) -> Iterator[dict]:
    """
    Yields one flat record per reading per location.
    Per-location incremental cursor via dlt.current.resource_state() — same pattern as
    hydrovu_readings. Each station has its own cursor; a failed station retries from the
    same point next run rather than being skipped permanently.

    On first run: fetches from start_ts (derived from initial_start_date in config).
    On subsequent runs: fetches only records newer than each station's cursor.

    Record shape (to define when implementing):
      reading_id   — unique key e.g. "{location_id}_{timestamp}"
      location_id  — CABQ station identifier
      location_name — human-readable name of the location
      latitude     — latitude in decimal degrees
      longitude    — longitude in decimal degrees
      timestamp    — Unix epoch milliseconds
      value        — float measurement
      # add other fields as needed
    """
    cursors: dict[str, int] = dlt.current.resource_state().setdefault("location_cursors", {})
    locations, err = _fetch_locations(client)
    if locations is None:
        logger.error("No locations found")
        client.close()
        return
    if err is not None:
        logger.error("Error fetching locations %s", err)
        client.close()
        return
    try:
        fetched = 0
        no_data = 0
        errored = 0
        failed_ids: list[int] = []
        rows_yielded = 0
        for location in locations:
            loc_id = location["sys_loc_code"]
            loc_start = max(cursors.get(str(loc_id), 0), start_ts)
            logger.info(
                "Fetching readings for location %s (%s) from Unix timestamp %s",
                loc_id,
                location["loc_name"],
                loc_start,
            )
            data, err = _fetch_readings_for_location(client, loc_id, loc_start)
            if err is not None:
                logger.warning(
                    "Location %s (%s) failed: %s — cursor not advanced, will retry next run",
                    loc_id,
                    location["loc_name"],
                    err,
                )
                errored += 1
                failed_ids.append(loc_id)
                continue
            if data is None:
                logger.warning("Location %s (%s): no data (404)", loc_id, location["loc_name"])
                no_data += 1
                continue
            fetched += 1
            max_timestamp = loc_start
            for measurement in data:
                # measurement_date is unix timestamp milliseconds, need to convert to seconds
                timestamp = int(measurement["measurement_date"] / 1000)
                if timestamp > max_timestamp:
                    max_timestamp = timestamp
                rows_yielded += 1
                yield {
                    "reading_id": f"{loc_id}_{measurement['measurement_date']}",
                    "location_id": loc_id,
                    "location_name": location["loc_name"],
                    "latitude": location["latitude"],
                    "longitude": location["longitude"],
                    "timestamp": measurement["measurement_date"],
                    "value": measurement["water_depth"],
                }
            cursors[str(loc_id)] = max_timestamp
        logger.info(
            "cabq readings extract complete: %d fetched, %d errored, %d no-data, %d rows yielded",
            fetched,
            errored,
            no_data,
            rows_yielded,
        )
        if _stats is not None:
            _stats["rows_yielded"] = rows_yielded
            _stats["locations_fetched"] = fetched
            _stats["locations_no_data"] = no_data
            _stats["locations_errored"] = errored
            _stats["failed_location_ids"] = failed_ids
    finally:
        client.close()


def build_pipeline() -> dlt.Pipeline:
    return build_source_pipeline("pvacd_cabq", "raw_cabq")


def run_pipeline() -> None:
    """Convenience entry point: builds and runs the pipeline with parquet output."""
    pipeline = build_pipeline()
    load_info = pipeline.run(cabq_source(), loader_file_format="parquet")
    logger.info("Load complete: %s", load_info)
