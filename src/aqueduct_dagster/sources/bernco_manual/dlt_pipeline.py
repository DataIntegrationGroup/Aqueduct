"""
dlt source for BernCo's manual (ArcREST) well-monitoring data — locations and
readings fetched from an unauthenticated ArcGIS FeatureServer, written to GCS.
"""

import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import dlt
from httpx import Client

from aqueduct_dagster.shared.pipeline import build_source_pipeline
from aqueduct_dagster.sources.arcrest_common import (
    build_arcrest_client,
    fetch_locations,
    fetch_readings_for_location,
)

logger = logging.getLogger(__name__)


def _fetch_locations(client: Client) -> tuple[list[dict] | None, str | None]:
    """Fetches location info from the BernCo ArcREST API.

    Location shape: GlobalID (UUID), Well_Name, Well_Location_Latitude/Longitude,
    NMT_ID (e.g. "BC-0364")."""
    path = "/0/query"
    params = {
        "where": "OBJECTID>0",
        "outFields": "GlobalID,Well_Name,Well_Depth,Well_Casing_Stickup,Well_Location_Latitude,Well_Location_Longitude,NMT_ID",
        "returnDistinctValues": "true",
        "f": "pjson",
    }
    return fetch_locations(client, path, params)


def _fetch_readings_for_location(
    client: Client, location_id: str, start_time: int, end_time: int | None = None
) -> tuple[list[dict] | None, str | None]:
    """Fetches readings for one location from the BernCo ArcREST API.

    Reading shape: MSRMNT_Date (Unix epoch ms), Depth_To_Water_At_Msrmnt_Point (ft)."""
    path = "/1/query"
    query = (
        "Well_ID='"
        + location_id
        + "' AND MSRMNT_Date>='"
        + datetime.fromtimestamp(start_time, tz=UTC).strftime("%Y-%m-%d")
        + "'"
    )
    if end_time is not None:
        query += (
            " AND MSRMNT_Date<='"
            + datetime.fromtimestamp(end_time, tz=UTC).strftime("%Y-%m-%d")
            + "'"
        )
    params = {
        "where": query,
        "outFields": "MSRMNT_Date,Depth_To_Water_At_Msrmnt_Point,MeasuringPt,Measurement_Method",
        "f": "pjson",
    }
    rows, err = fetch_readings_for_location(client, path, params, location_id)
    if rows is not None and end_time is not None:
        # The API's MSRMNT_Date query only accepts date literals, so it's
        # inclusive of the entire end date — a reading exactly at end_time's
        # midnight instant would otherwise pass this query but violate the
        # strict half-open [start, end) window load_window() enforces. Trim it
        # client-side, mirroring hydrovu_common.py's fetch_location_data()
        # end_time handling. MSRMNT_Date is epoch ms; end_time is epoch s.
        rows = [row for row in rows if row["MSRMNT_Date"] < end_time * 1000]
    return rows, err


@dlt.source(name="bernco_manual")
def bernco_manual_source(
    api_base_url: str = dlt.config.value,
    initial_start_date: str = dlt.config.value,
    _stats: dict | None = None,
) -> Any:
    """Fetches the location list once, then composes the two resources below.
    Returns None if locations can't be fetched at all — nothing to yield without them."""
    start_ts = int(
        datetime.strptime(initial_start_date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
    )
    client = build_arcrest_client(api_base_url)
    locations, err = _fetch_locations(client)
    if locations is None:
        logger.error("No locations found")
        client.close()
        return None
    if err is not None:
        logger.error("Error fetching locations %s", err)
        client.close()
        return None
    return (
        bernco_manual_locations(locations=locations),
        bernco_manual_readings(
            client=client, locations=locations, start_ts=start_ts, _stats=_stats
        ),
    )


@dlt.resource(name="bernco_manual_locations", write_disposition="replace")
def bernco_manual_locations(locations: list[dict]) -> Iterator[dict]:
    for location in locations:
        yield {
            "id": location["GlobalID"],
            "name": location["Well_Name"],
            "description": "Location of well where measurements are made",
            "well_depth": location["Well_Depth"],
            "well_casing_stickup": location["Well_Casing_Stickup"],
            "latitude": location["Well_Location_Latitude"],
            "longitude": location["Well_Location_Longitude"],
            "alternate_id": location["NMT_ID"],
        }


@dlt.resource(name="bernco_manual_readings", write_disposition="append", primary_key="reading_id")
def bernco_manual_readings(
    client: Client,
    locations: list[dict],
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
    try:
        fetched = 0
        no_data = 0
        errored = 0
        failed_ids: list[int] = []
        rows_yielded = 0
        for location in locations:
            location_id = location["GlobalID"]
            # well casing stickup is needed for accurate depth measurements, skip location if null and log
            if location["Well_Casing_Stickup"] is None:
                logger.warning("No well casing stickup for location %s, skipping", location_id)
                failed_ids.append(location_id)
                errored += 1
                continue
            loc_start = max(cursors.get(str(location_id), 0), start_ts)
            logger.info(
                "Fetching readings for location %s (%s) from Unix timestamp %s",
                location_id,
                location["Well_Name"],
                loc_start,
            )
            data, err = _fetch_readings_for_location(client, location_id, loc_start)
            if err is not None:
                logger.warning(
                    "Location %s (%s) failed: %s — cursor not advanced, will retry next run",
                    location_id,
                    location["Well_Name"],
                    err,
                )
                errored += 1
                failed_ids.append(location_id)
                continue
            if data is None or len(data) == 0:
                logger.warning(
                    "Location %s (%s): no data (404)", location_id, location["Well_Name"]
                )
                no_data += 1
                continue
            fetched += 1
            max_timestamp = loc_start
            for measurement in data:
                # measurement_date is unix timestamp milliseconds, need to convert to seconds
                timestamp = int(measurement["MSRMNT_Date"] / 1000)
                if timestamp > max_timestamp:
                    max_timestamp = timestamp
                # measuring point might be null, if so then use well casing stickup as standin
                if measurement["MeasuringPt"] is None:
                    mph = location["Well_Casing_Stickup"]
                else:
                    mph = measurement["MeasuringPt"]
                rows_yielded += 1
                yield {
                    "reading_id": f"{location_id}_{measurement['MSRMNT_Date']}",
                    "location_id": location_id,
                    "location_name": location["Well_Name"],
                    "well_depth": location["Well_Depth"],
                    "well_casing_stickup": location["Well_Casing_Stickup"],
                    "latitude": location["Well_Location_Latitude"],
                    "longitude": location["Well_Location_Longitude"],
                    "timestamp": measurement["MSRMNT_Date"],
                    "value": measurement["Depth_To_Water_At_Msrmnt_Point"],
                    "measurement_point_height": mph,
                    "measurement_method": measurement["Measurement_Method"],
                    "alternate_id": location["NMT_ID"],
                }
            cursors[str(location_id)] = max_timestamp
        logger.info(
            "Bernco Manual readings extract complete: %d fetched, %d errored, %d no-data, %d rows yielded",
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
    return build_source_pipeline("bernco_manual", "raw_bernco_manual")


def run_pipeline() -> None:
    pipeline = build_pipeline()
    load_info = pipeline.run(bernco_manual_source, loader_file_format="parquet")
    logger.info("Load complete: %s", load_info)
