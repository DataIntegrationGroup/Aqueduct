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
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import dlt
import httpx

from aqueduct_dagster.shared.http import build_unauthenticated_client, retry_transient
from aqueduct_dagster.shared.pipeline import build_source_pipeline

logger = logging.getLogger(__name__)


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


def _fetch_locations(client: httpx.Client) -> list[dict]:
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

    def _fetch_location_info() -> httpx.Response:
        # query for OBJECTID > 0, aka all entries, for unique location info
        return client.get(
            "/query?where=OBJECTID%3E0&outFields=sys_loc_code,loc_name,latitude,longitude&returnDistinctValues=true&f=pjson"
        )

    resp = retry_transient(
        _fetch_location_info,
        on_retry=lambda exc, attempt, delay: logger.warning("", exec, attempt, delay),
    )
    resp.raise_for_status()
    return _transform_result(resp.json())


def _fetch_readings_for_location(
    client: httpx.Client, loc_id: str, loc_start: int
) -> tuple[list[dict] | None, str | None]:
    """
    get reading information for location from CABQ
    format of location:
    {
        measurement_date: num, *timestamp of when measurement was taken in unix epoch seconds
        water_level: num, *water level in ft msl
    }
    """

    def _fetch_readings() -> httpx.Response:
        # query for location code = given location id for measurement info
        return client.get(
            "/query?where=sys_loc_code%3D'"
            + loc_id
            + "'&outfields=measurement_date,water_level&f=pjson"
        )

    resp = retry_transient(
        _fetch_readings,
        on_retry=lambda exc, attempt, delay: logger.warning("", exec, attempt, delay),
    )
    if resp.status_code == 404:
        logger.warning("Location %s: 404 — no data endpoint", loc_id)
        return None, None
    if resp.status_code >= 500:
        logger.warning("Location %s: HTTP %s — skipping", loc_id, resp.status_code)
        return None, f"HTTP {resp.status_code}"
    resp.raise_for_status()
    return _transform_result(resp.json()), None


@dlt.source(name="cabq")
def cabq_source(
    api_base_url: str = dlt.config.value,
    initial_start_date: str = dlt.config.value,
    _stats: dict | None = None,
) -> Any:
    start_ts = int(
        datetime.strptime(initial_start_date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
    )
    client = build_unauthenticated_client(api_base_url, timeout=httpx.Timeout())
    return cabq_readings(client=client, start_ts=start_ts, _stats=_stats)


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
      timestamp    — Unix epoch seconds
      value        — float measurement
      # add other fields as needed
    """
    cursors: dict[str, int] = dlt.current.resource_state().setdefault("location_cursors", {})
    try:
        fetched = 0
        no_data = 0
        errored = 0
        failed_ids: list[int] = []
        rows_yielded = 0
        locations = _fetch_locations(client)
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
                timestamp = measurement["measurement_date"]
                if timestamp > max_timestamp:
                    max_timestamp = timestamp
                rows_yielded += 1
                yield {
                    "reading_id": f"{loc_id}_{timestamp}",
                    "location_id": loc_id,
                    "timestamp": timestamp,
                    "value": measurement["water_level"],
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
