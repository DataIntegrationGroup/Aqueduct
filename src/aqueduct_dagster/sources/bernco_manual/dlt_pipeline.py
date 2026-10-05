"""
dlt source for BernCo's manual (ArcREST) well-monitoring data — locations and
readings fetched from an unauthenticated ArcGIS FeatureServer, written to GCS.
"""

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
    """Extracts the "attributes" dict from each entry in the BernCo ArcREST
    response's "features" list — the only part of the response actually used."""
    all_attributes: list[dict] = []
    for feature in data["features"]:
        all_attributes.append(feature["attributes"])
    return all_attributes


def _fetch_locations(client: httpx.Client) -> tuple[list[dict] | None, str | None]:
    """Fetches location info from the BernCo ArcREST API.

    Location shape: GlobalID (UUID), Well_Name, Well_Location_Latitude/Longitude,
    NMT_ID (e.g. "BC-0364")."""
    path = "/0/query"
    params = {
        "where": "OBJECTID>0",
        "outFields": "GlobalID,Well_Name,Well_Location_Latitude,Well_Location_Longitude,NMT_ID",
        "returnDistinctValues": "true",
        "f": "pjson",
    }
    rate_limit_retries = 0
    result: dict[Any, Any] = {}
    while True:

        def _fetch_location_info() -> httpx.Response:
            return client.get(path, params=params)

        try:
            response = retry_transient(
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
        if response.status_code == 429:
            rate_limit_retries += 1
            if rate_limit_retries > _MAX_RATE_LIMIT_RETRIES:
                return None, f"HTTP 429: rate limited after {_MAX_RATE_LIMIT_RETRIES} retries"
            retry_after = response.headers.get("Retry-After")
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
        if response.status_code >= 500:
            logger.warning("Location: HTTP %s", response.status_code)
            return None, f"HTTP {response.status_code}"
        response.raise_for_status()
        result = response.json()
        break
    return _transform_result(result), None


def _fetch_readings_for_location(
    client: httpx.Client, location_id: str, start_time: int, end_time: int | None = None
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
        "outFields": "MSRMNT_Date,Depth_To_Water_At_Msrmnt_Point",
        "f": "pjson",
    }
    rate_limit_retries = 0
    result: dict[Any, Any] = {}
    while True:

        def _fetch_readings() -> httpx.Response:
            return client.get(path, params=params)

        try:
            response = retry_transient(
                _fetch_readings,
                on_retry=lambda exc, attempt, seconds: logger.warning(
                    "Location %s: error (%s) on attempt %d - retrying in %.0fs",
                    location_id,
                    exc,
                    attempt,
                    seconds,
                ),
            )
        except _TRANSIENT_ERRORS as err:
            logger.warning(
                "Location %s: transient error after %d attempts — skipping",
                location_id,
                _MAX_RETRIES,
            )
            return None, f"transient network error after {_MAX_RETRIES} attempts: {err}"
        if response.status_code == 404:
            logger.warning("Location %s: 404 — no data endpoint", location_id)
            return None, None
        if response.status_code == 429:
            rate_limit_retries += 1
            if rate_limit_retries > _MAX_RATE_LIMIT_RETRIES:
                return None, f"HTTP 429: rate limited after {_MAX_RATE_LIMIT_RETRIES} retries"
            retry_after = response.headers.get("Retry-After")
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
        if response.status_code >= 500:
            logger.warning("Location %s: HTTP %s — skipping", location_id, response.status_code)
            return None, f"HTTP {response.status_code}"
        response.raise_for_status()
        result = response.json()
        break
    rows = _transform_result(result)
    if end_time is not None:
        rows = [row for row in rows if row["MSRMNT_Date"] < end_time * 1000]
    return rows, None


def build_bernco_manual_client(api_base_url: str) -> httpx.Client:
    return build_unauthenticated_client(
        api_base_url, timeout=httpx.Timeout(connect=30.0, read=60.0, write=30.0, pool=30.0)
    )


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
    client = build_bernco_manual_client(api_base_url)
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
            "latitude": location["Well_Location_Latitude"],
            "longitude": location["Well_Location_Longitude"],
            "alternate_id": location["NMT_ID"],
        }


@dlt.resource(name="bernco_manual_readings", write_disposition="append", primary_key="reading_id")
def bernco_manual_readings(
    client: httpx.Client,
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
                rows_yielded += 1
                yield {
                    "reading_id": f"{location_id}_{measurement['MSRMNT_Date']}",
                    "location_id": location_id,
                    "location_name": location["Well_Name"],
                    "latitude": location["Well_Location_Latitude"],
                    "longitude": location["Well_Location_Longitude"],
                    "timestamp": measurement["MSRMNT_Date"],
                    "value": measurement["Depth_To_Water_At_Msrmnt_Point"],
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
