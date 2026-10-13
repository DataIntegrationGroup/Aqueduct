import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import dlt
from gcsfs import GCSFileSystem

from aqueduct_dagster.shared.pipeline import build_source_pipeline

logger = logging.getLogger(__name__)


def _fetch_readings(
    client: GCSFileSystem, path: str, start_time: int, end_time: int | None = None
) -> tuple[list[dict] | None, str | None]:
    result: list[dict[Any, Any]] = []
    try:
        files = client.ls(path=path)
        valid_file_names = []
        for file in files:
            file_ts = (
                datetime.strptime(file.split("_")[1].split(".csv")[0], "%m-%d-%y")
                .replace(tzinfo=UTC)
                .timestamp()
            )
            if end_time is not None and file_ts >= end_time:
                continue
            if file_ts >= start_time:
                valid_file_names.append(file)
        for file in valid_file_names:
            with client.open(path=path + "/" + file, mode="r") as f:
                lines = f.readlines()
                # first line defines schema of file, pop and process rest as actual data
                lines.pop(0)
                for line in lines:
                    line_split = line.split(",")
                    result.append(
                        {
                            "depth_to_water": line_split[0],
                            "site_id": line_split[1],
                            "measuring_agency": line_split[2],
                            "timestamp": line_split[3],
                        }
                    )
                f.close()
    except Exception as err:
        logger.warning("Encountered error while reading from gcs: %s", err)
        return None, f"{err}"
    return result, None


def build_roswell_client() -> GCSFileSystem:
    return GCSFileSystem(token="google_default")


@dlt.source(name="roswell")
def roswell_source(
    api_base_url: str = dlt.config.value,
    initial_start_date: str = dlt.config.value,
    _stats: dict | None = None,
) -> Any:
    client = build_roswell_client()
    start_ts = int(
        datetime.strptime(initial_start_date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
    )
    return roswell_readings(
        client=client, api_base_url=api_base_url, start_ts=start_ts, _stats=_stats
    )


@dlt.resource(name="roswell_readings", write_disposition="append", primary_key="reading_id")
def roswell_readings(
    client: GCSFileSystem, api_base_url: str, start_ts: int, _stats: dict | None = None
) -> Iterator[dict]:
    cursors: dict[str, int] = dlt.current.resource_state().setdefault("location_cursors", {})
    fetched = 0
    no_data = 0
    errored = 0
    failed_ids: list[str] = []
    rows_yielded = 0
    # only have one location
    loc_start = max(cursors.get("18", 0), start_ts)
    logger.info(
        "Fetching readings for location %s (%s) from Unix timestamp %s",
        "site",
        "18",
        loc_start,
    )
    data, err = _fetch_readings(client, api_base_url, loc_start)
    if err is not None:
        logger.warning(
            "Location 18 failed: %s — cursor not advanced, will retry next run",
            err,
        )
        errored += 1
        failed_ids.append("18")
    if data is None or len(data) == 0:
        logger.warning(
            "Location 18: no data (404)",
        )
        no_data += 1
    if data is not None:
        fetched += 1
        max_timestamp = loc_start
        for measurement in data:
            timestamp = int(
                datetime.strptime(measurement["timestamp"], "%Y-%m-%dT%h:%m:%s")
                .replace(tzinfo=UTC)
                .timestamp()
            )
            if timestamp > max_timestamp:
                max_timestamp = timestamp
            rows_yielded += 1
            yield {
                "reading_id": f"{measurement['site_id']}_{timestamp}",
                "location_id": measurement["site_id"],
                "location_name": "Site-18",
                "latitude": "33.29592",
                "longitude": "-104.58409",
                "timestamp": timestamp,
                "value": measurement["depth_to_water"],
            }
        cursors["18"] = max_timestamp
    logger.info(
        "City of Roswell readings extract complete: %d fetched, %d errored, %d no-data, %d rows yielded",
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


def build_pipeline() -> dlt.Pipeline:
    return build_source_pipeline("roswell", "raw_roswell")


def run_pipeline() -> None:
    pipeline = build_pipeline()
    load_info = pipeline.run(roswell_source, loader_file_format="parquet")
    logger.info("load complete: %s", load_info)
