from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime

import dlt
import gcsfs
import httpx

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
from aqueduct_dagster.sources.cabq.adapter import CabqAdapter
from aqueduct_dagster.sources.cabq.dlt_pipeline import (
    _fetch_locations,
    _fetch_readings_for_location,
    build_cabq_client,
)
from aqueduct_dagster.sources.cabq.transform import GCS_DATASET, _group_rows_by_location

logger = logging.getLogger(__name__)

BACKFILL_PIPELINE_NAME = "pvacd_cabq_backfill_refetch"
BACKFILL_TABLE_NAME = "cabq_backfill_readings"


@dlt.resource(
    name=BACKFILL_TABLE_NAME,
    write_disposition="append",
    primary_key="reading_id",
)
def cabq_backfill_readings(
    client: httpx.Client,
    locations: list[dict],
    location_ids: list[str],
    start_ts: int,
    end_ts: int,
) -> Iterator[dict]:
    allowed = frozenset(location_ids)
    for location in locations:
        loc_id = location["sys_loc_code"]
        if loc_id not in allowed:
            continue

        data, err = _fetch_readings_for_location(client, loc_id, start_ts, end_ts)
        if err is not None:
            window_start_iso = datetime.fromtimestamp(start_ts, tz=UTC).isoformat()
            window_end_iso = datetime.fromtimestamp(end_ts, tz=UTC).isoformat()
            raise RuntimeError(
                f"Backfill fetch failed for location {loc_id} in window {window_start_iso} to {window_end_iso}: {err}"
            )
        if data is None:
            continue  # 404 location has no data

        for measurement in data:
            yield {
                "reading_id": f"{loc_id}_{measurement['measurement_date']}",
                "location_id": loc_id,
                "location_name": location["loc_name"],
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "timestamp": measurement["measurement_date"],
                "value": measurement["water_depth"],
            }


def _locations_by_id(locations: list[dict]) -> dict[int, dict]:
    return {
        loc["sys_loc_code"]: {
            "name": loc["loc_name"],
            "latitude": loc["latitude"],
            "longitude": loc["longitude"],
        }
        for loc in locations
    }


def default_backfill_location_ids() -> list[int]:
    return list(load_source_config("cabq").get("location_ids", []))


def prepare_backfill() -> tuple[httpx.Client, list[dict], dict[int, dict]]:
    cfg = load_source_config("cabq")
    client = build_cabq_client(cfg["api_base_url"])
    try:
        locations, err = _fetch_locations(client)
        if locations is None:
            raise RuntimeError("Backfill fetch failed for to receive locations")
    except Exception:
        client.close()
        raise
    return client, locations, _locations_by_id(locations)


def run_backfill_chunk(
    *,
    client: httpx.Client,
    locations: list[dict],
    locations_by_id: dict[int, dict],
    location_ids: list[str],
    chunk_start: datetime,
    chunk_end: datetime,
    loader: FrostLoader,
    bucket: str,
    fs: gcsfs.GCSFileSystem,
    run_key: str,
) -> ChunkResult:
    start_ts = int(chunk_start.timestamp())
    end_ts = int(chunk_end.timestamp())

    load_id = run_backfill_ingest(
        pipeline_name_prefix=BACKFILL_PIPELINE_NAME,
        dataset=GCS_DATASET,
        run_key=run_key,
        resource=cabq_backfill_readings(
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

    rows = read_parquet_rows_for_load_id(
        bucket, f"{GCS_DATASET}/{BACKFILL_TABLE_NAME}/**/*.parquet", load_id, fs
    )

    records = _group_rows_by_location(rows)
    adapter = CabqAdapter(records)
    bundles: list[CanonicalBundle] = list(adapter.run())
    log_if_adapter_failed(adapter, logger, context=f"Backfill chunk [{chunk_start}, {chunk_end})")

    observations_posted, observations_deleted = load_bundles_windowed(
        loader, bundles, chunk_start, chunk_end
    )

    return ChunkResult(
        rows_ingested=len(rows),
        bundles_loaded=len(bundles),
        observations_posted=observations_posted,
        observations_deleted=observations_deleted,
        adapter_failures=adapter.failure_count,
    )
