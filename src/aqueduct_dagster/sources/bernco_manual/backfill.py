import logging
from collections.abc import Iterator
from datetime import UTC, datetime

import dlt
from gcsfs import GCSFileSystem
from httpx import Client

from aqueduct_dagster.canonical import CanonicalBundle
from aqueduct_dagster.canonical.base_adapter import log_if_adapter_failed
from aqueduct_dagster.loader import FrostLoader
from aqueduct_dagster.shared.backfill import (
    ChunkResult,
    load_bundles_windowed,
    load_source_config,
    run_backfill_ingest,
)
from aqueduct_dagster.shared.gcs import read_parquet_rows_for_load_id
from aqueduct_dagster.sources.bernco_manual.adapter import BerncoManualAdapter
from aqueduct_dagster.sources.bernco_manual.dlt_pipeline import (
    _fetch_locations,
    _fetch_readings_for_location,
    build_bernco_manual_client,
)
from aqueduct_dagster.sources.bernco_manual.transform import GCS_DATASET, _group_rows_by_location

logger = logging.getLogger(__name__)

BACKFILL_PIPELINE_NAME = "bernco_manual_backfill_refetch"
BACKFILL_TABLE_NAME = "bernco_manual_backfill_readings"


@dlt.resource(
    name=BACKFILL_TABLE_NAME,
    write_disposition="append",
    primary_key="reading_id",
)
def bernco_manual_backfill_readings(
    client: Client,
    locations: list[dict],
    location_ids: list[str],
    start_ts: int,
    end_ts: int,
) -> Iterator[dict]:
    allowed = frozenset(location_ids)
    for location in locations:
        location_id = location["GlobalID"]
        if location_id not in allowed:
            continue
        data, err = _fetch_readings_for_location(client, location_id, start_ts, end_ts)
        if err is not None:
            window_start_iso = datetime.fromtimestamp(start_ts, tz=UTC).isoformat()
            window_end_iso = datetime.fromtimestamp(end_ts, tz=UTC).isoformat()
            raise RuntimeError(
                f"Backfill fetch failed for location {location_id} in window {window_start_iso} to {window_end_iso}: {err}"
            )
        if data is None:
            continue  # 404 location has no data
        for measurement in data:
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


def _locations_by_id(locations: list[dict]) -> dict[str, dict]:
    return {
        location["GlobalID"]: {
            "description": "Location of well where measurements are made",
            "latitude": location["Well_Location_Latitude"],
            "longitude": location["Well_Location_Longitude"],
            "alternate_id": location["NMT_ID"],
        }
        for location in locations
    }


def default_backfill_locations() -> list[str]:
    return list(load_source_config("bernco_manual").get("location_ids", []))


def prepare_backfill() -> tuple[Client, list[dict], dict[str, dict]]:
    cfg = load_source_config("bernco_manual")
    client = build_bernco_manual_client(cfg["api_base_url"])
    try:
        locations, err = _fetch_locations(client)
        if locations is None:
            raise RuntimeError(f"Backfill fetch failed to receive locations: {err}")
    except Exception:
        client.close()
        raise
    return client, locations, _locations_by_id(locations)


def run_backfill_chunk(
    *,
    client: Client,
    locations: list[dict],
    locations_by_id: dict[str, dict],
    location_ids: list[str],
    chunk_start: datetime,
    chunk_end: datetime,
    loader: FrostLoader,
    bucket: str,
    fs: GCSFileSystem,
    run_key: str,
) -> ChunkResult:
    start_ts = int(chunk_start.timestamp())
    end_ts = int(chunk_end.timestamp())
    load_id = run_backfill_ingest(
        pipeline_name_prefix=BACKFILL_PIPELINE_NAME,
        dataset=GCS_DATASET,
        run_key=run_key,
        resource=bernco_manual_backfill_readings(
            client=client,
            locations=locations,
            location_ids=location_ids,
            start_ts=start_ts,
            end_ts=end_ts,
        ),
        chunk_start=chunk_start,
        chunk_end=chunk_end,
    )
    if load_id is None:
        return ChunkResult(
            rows_ingested=0, bundles_loaded=0, observations_posted=0, observations_deleted=0
        )
    rows, files_skipped_bad_name = read_parquet_rows_for_load_id(
        bucket, f"{GCS_DATASET}/{BACKFILL_TABLE_NAME}/**/*.parquet", load_id, fs
    )
    records = _group_rows_by_location(rows)
    adapter = BerncoManualAdapter(records)
    bundles: list[CanonicalBundle] = list(adapter.run())
    log_if_adapter_failed(adapter, logger, context=f"Backfill run [{chunk_start}, {chunk_end}]")
    observations_posted, observations_deleted = load_bundles_windowed(
        loader, bundles, chunk_start, chunk_end
    )
    return ChunkResult(
        rows_ingested=len(rows),
        bundles_loaded=len(bundles),
        observations_posted=observations_posted,
        observations_deleted=observations_deleted,
        adapter_failures=adapter.failure_count,
        files_skipped_bad_name=files_skipped_bad_name,
    )
