"""
sources/bernco_hydrovu/backfill.py

Backfill for BernCo HydroVu — mirrors pvacd_hydrovu/backfill.py.
Writes to its own isolated GCS table (hydrovu_backfill_readings under
raw_bernco_hydrovu) and dlt pipeline state, separate from production.

Not a Dagster asset/op — no Dagster imports. Called per-chunk by the shared
job factory in defs/jobs/backfill.py.

One difference from PVACD: BernCo has two locations with bad device clocks
that report near Unix epoch 0 (see adapter.py). Production's floor
(sentinel_floor(), in transform.py) uses initial_start_date, which only
works there because production never asks for anything older. Backfill
deliberately does ask for older data, so it needs its own fixed floor below
instead — using sentinel_floor() here would silently drop every real
historical reading, not just the bad ones.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime

import dlt
import gcsfs
import httpx

from aqueduct_dagster.canonical.base_adapter import log_if_adapter_failed
from aqueduct_dagster.canonical.canonical_model import CanonicalBundle
from aqueduct_dagster.loader.frost_loader import FrostLoader
from aqueduct_dagster.shared.backfill import (
    ChunkResult,
    load_bundles_windowed,
    load_source_config,
    run_backfill_ingest,
)
from aqueduct_dagster.shared.gcs import read_parquet_rows_for_load_id
from aqueduct_dagster.sources.bernco_hydrovu.adapter import BerncoHydroVuAdapter
from aqueduct_dagster.sources.bernco_hydrovu.transform import GCS_DATASET
from aqueduct_dagster.sources.hydrovu_common import (
    build_hydrovu_client,
    fetch_location_data,
    fetch_locations,
)
from aqueduct_dagster.sources.hydrovu_transform_common import (
    DTW_PARAMETER_ID,
    group_readings_by_location,
)

logger = logging.getLogger(__name__)

BACKFILL_PIPELINE_NAME = "bernco_hydrovu_backfill_refetch"
BACKFILL_TABLE_NAME = "hydrovu_backfill_readings"

# The two bad-clock locations report at/near Unix timestamp 0/960 (adapter.py).
# 1 day past epoch clears both with room to spare, while staying far below any
# real reading (earliest known history: 2009).
SENTINEL_FLOOR = 86400


@dlt.resource(
    name=BACKFILL_TABLE_NAME,
    write_disposition="append",
    primary_key="reading_id",
)
def hydrovu_backfill_readings(
    client: httpx.Client,
    locations: list[dict],
    location_ids: list[int],
    start_ts: int,
    end_ts: int,
) -> Iterator[dict]:
    """One flat row per (location, parameter, reading) in [start_ts, end_ts)."""
    allowed = frozenset(location_ids)
    for location in locations:
        loc_id = location["id"]
        if loc_id not in allowed:
            continue

        data, err = fetch_location_data(client, loc_id, start_ts, end_time=end_ts)
        if err is not None:
            window_start_iso = datetime.fromtimestamp(start_ts, tz=UTC).isoformat()
            window_end_iso = datetime.fromtimestamp(end_ts, tz=UTC).isoformat()
            raise RuntimeError(
                f"Backfill fetch failed for location {loc_id} in window "
                f"[{window_start_iso}, {window_end_iso}): {err}"
            )
        if data is None:
            continue  # 404 — location has no data endpoint

        skipped_out_of_window = 0
        for param in data.get("parameters", []):
            for reading in param.get("readings", []):
                timestamp = reading["timestamp"]
                if not start_ts <= timestamp < end_ts:
                    skipped_out_of_window += 1
                    continue
                yield {
                    "reading_id": f"{loc_id}_{param['parameterId']}_{timestamp}",
                    "location_id": loc_id,
                    "timestamp": timestamp,
                    "parameter_id": param["parameterId"],
                    "unit_id": param["unitId"],
                    "value": reading["value"],
                }
        if skipped_out_of_window:
            logger.warning(
                "Location %s: %d reading(s) outside [%s, %s) discarded — API returned "
                "data outside the requested window",
                loc_id,
                skipped_out_of_window,
                start_ts,
                end_ts,
            )


def _locations_by_id(locations: list[dict]) -> dict[int, dict]:
    """Raw /locations/list response -> {id: {...}} for group_readings_by_location."""
    return {
        loc["id"]: {
            "name": loc["name"],
            "description": loc["description"],
            "latitude": loc["gps"]["latitude"],
            "longitude": loc["gps"]["longitude"],
        }
        for loc in locations
    }


def default_backfill_location_ids() -> list[int]:
    """The daily pipeline's own allowlist ([sources.bernco_hydrovu].location_ids)."""
    return list(load_source_config("bernco_hydrovu").get("location_ids", []))


def prepare_backfill() -> tuple[httpx.Client, list[dict], dict[int, dict]]:
    """One-time client + location list setup, shared by every chunk."""
    cfg = load_source_config("bernco_hydrovu")
    client = build_hydrovu_client("", "", cfg["gcp_secret"], cfg["api_base_url"], cfg["token_url"])
    try:
        locations = fetch_locations(client)
    except Exception:
        client.close()
        raise
    return client, locations, _locations_by_id(locations)


def run_backfill_chunk(
    *,
    client: httpx.Client,
    locations: list[dict],
    locations_by_id: dict[int, dict],
    location_ids: list[int],
    chunk_start: datetime,
    chunk_end: datetime,
    loader: FrostLoader,
    bucket: str,
    fs: gcsfs.GCSFileSystem,
    run_key: str,
) -> ChunkResult:
    """Ingest + transform + load for one calendar-month chunk. Raises on failure."""
    start_ts = int(chunk_start.timestamp())
    end_ts = int(chunk_end.timestamp())

    load_id = run_backfill_ingest(
        pipeline_name_prefix=BACKFILL_PIPELINE_NAME,
        dataset=GCS_DATASET,
        run_key=run_key,
        resource=hydrovu_backfill_readings(
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
        bucket,
        f"{GCS_DATASET}/{BACKFILL_TABLE_NAME}/**/*.parquet",
        load_id,
        fs,
        row_filter=lambda row: row["parameter_id"] == DTW_PARAMETER_ID,
    )

    records = group_readings_by_location(rows, locations_by_id)
    adapter = BerncoHydroVuAdapter(records, min_timestamp=SENTINEL_FLOOR)
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
        files_skipped_bad_name=files_skipped_bad_name,
    )
