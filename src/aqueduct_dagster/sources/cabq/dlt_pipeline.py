"""
dlt pipeline for CABQ raw ingestion.

  - @dlt.source: reads config from dlt.config under [sources.cabq]
  - @dlt.resource: per-location incremental cursor via dlt.current.resource_state()
  - build_pipeline(): filesystem destination → GCS under raw_cabq/
  - run_pipeline(): convenience entry point
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import dlt
import httpx

from aqueduct_dagster.shared.pipeline import build_source_pipeline
from aqueduct_dagster.sources.arcrest_common import (
    build_arcrest_client,
    fetch_locations,
    fetch_readings_for_location,
)

logger = logging.getLogger(__name__)


def _fetch_locations(client: httpx.Client) -> tuple[list[dict] | None, str | None]:
    """Fetches location info from CABQ.

    Location shape: sys_loc_code, loc_name, latitude, longitude."""
    path = "/query"
    params = {
        "where": "OBJECTID>0",
        "outFields": "sys_loc_code,loc_name,latitude,longitude",
        "returnDistinctValues": "true",
        "f": "pjson",
    }
    return fetch_locations(client, path, params)


def _fetch_readings_for_location(
    client: httpx.Client, loc_id: str, start_time: int, end_time: int | None = None
) -> tuple[list[dict] | None, str | None]:
    """Fetches readings for one location from CABQ.

    Reading shape: measurement_date (Unix epoch ms), water_depth (ft)."""
    path = "/query"
    query = (
        "sys_loc_code='"
        + loc_id
        + "' AND measurement_date>='"
        + datetime.fromtimestamp(start_time, tz=UTC).strftime("%Y-%m-%d")
        + "'"
    )
    if end_time is not None:
        query += (
            " AND measurement_date<='"
            + datetime.fromtimestamp(end_time, tz=UTC).strftime("%Y-%m-%d")
            + "'"
        )
    params = {
        "where": query,
        "outFields": "measurement_date,water_depth",
        "f": "pjson",
    }
    rows, err = fetch_readings_for_location(client, path, params, loc_id)
    if rows is not None and end_time is not None:
        # The API's measurement_date query only accepts date literals, so it's
        # inclusive of the entire end date — a reading exactly at end_time's
        # midnight instant would otherwise pass this query but violate the
        # strict half-open [start, end) window load_window() enforces. Trim it
        # client-side, mirroring hydrovu_common.py's fetch_location_data()
        # end_time handling. measurement_date is epoch ms; end_time is epoch s.
        rows = [row for row in rows if row["measurement_date"] < end_time * 1000]
    return rows, err


@dlt.source(name="cabq")
def cabq_source(
    api_base_url: str = dlt.config.value,
    initial_start_date: str = dlt.config.value,
    _stats: dict | None = None,
) -> Any:
    """dlt source: computes start_ts from initial_start_date and returns the
    cabq_readings resource (which fetches locations itself)."""
    start_ts = int(
        datetime.strptime(initial_start_date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
    )
    return cabq_readings(
        client=build_arcrest_client(api_base_url), start_ts=start_ts, _stats=_stats
    )


@dlt.resource(
    name="cabq_readings",
    write_disposition="append",
    primary_key="reading_id",
)
def cabq_readings(
    client: httpx.Client,
    start_ts: int,
    _stats: dict | None = None,
) -> Iterator[dict]:
    """Per-location incremental cursor via dlt.current.resource_state() — same
    pattern as hydrovu_readings. Each station has its own cursor; a failed
    station retries from the same point next run rather than being skipped
    permanently.

    First run fetches from start_ts (initial_start_date); later runs fetch
    only what's newer than each station's cursor."""
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
    pipeline = build_pipeline()
    load_info = pipeline.run(cabq_source(), loader_file_format="parquet")
    logger.info("Load complete: %s", load_info)
