"""
Mode A (refetch) backfill for PVACD HydroVu — see docs/BACKFILL_STRATEGY.md §4.2.

Runs ingest -> transform -> load for an explicit entity list and date range,
under isolated dlt pipeline state and its own GCS table (hydrovu_backfill_readings,
not hydrovu_readings) — invisible to the normal scheduled pipeline's glob, so
nothing to coordinate or interfere with.

Not a Dagster asset/op — called per-chunk by defs/jobs/backfill.py's generic
factory, which owns the run config, chunk loop, and checkpointing.
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
from aqueduct_dagster.sources.hydrovu_common import (
    build_hydrovu_client,
    fetch_location_data,
    fetch_locations,
)
from aqueduct_dagster.sources.hydrovu_transform_common import (
    DTW_PARAMETER_ID,
    group_readings_by_location,
)
from aqueduct_dagster.sources.pvacd_hydrovu.adapter import PvacdHydroVuAdapter
from aqueduct_dagster.sources.pvacd_hydrovu.transform import GCS_DATASET

logger = logging.getLogger(__name__)

BACKFILL_PIPELINE_NAME = "pvacd_hydrovu_backfill_refetch"
BACKFILL_TABLE_NAME = "hydrovu_backfill_readings"


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
    """Yields one flat record per (location, parameter, reading) within
    [start_ts, end_ts), for location_ids only. No persisted cursor — explicit
    range every call, never touching dlt.current.resource_state() or
    colliding with production's cursors.

    Raises RuntimeError on a real fetch error (not 404) — a chunk is
    all-or-nothing, so one failed location fails the whole chunk rather than
    checkpointing partial data."""
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

        for param in data.get("parameters", []):
            for reading in param.get("readings", []):
                yield {
                    "reading_id": f"{loc_id}_{param['parameterId']}_{reading['timestamp']}",
                    "location_id": loc_id,
                    "timestamp": reading["timestamp"],
                    "parameter_id": param["parameterId"],
                    "unit_id": param["unitId"],
                    "value": reading["value"],
                }


def _locations_by_id(locations: list[dict]) -> dict[int, dict]:
    """Converts the raw /locations/list response into the {id: {...}} shape
    group_readings_by_location expects, from the already-fetched in-memory
    list — backfill never reads or writes the hydrovu_locations table."""
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
    """Same allowlist the daily pipeline reads from .dlt/config.toml. Called
    once, eagerly, at defs/jobs/backfill.py import time, since the Launchpad
    needs a plain, already-computed default.

    Missing location_ids returns [] ("every location" — see resolve_location_ids);
    a broken .dlt/config.toml still raises, since failing loudly beats silently
    backfilling everything."""
    return list(load_source_config("pvacd_hydrovu").get("location_ids", []))


def prepare_backfill() -> tuple[httpx.Client, list[dict], dict[int, dict]]:
    """One-time setup shared by every chunk: builds the client and fetches the
    location list once, since it doesn't depend on the date range.

    Returns (client, locations, locations_by_id)."""
    cfg = load_source_config("pvacd_hydrovu")
    client = build_hydrovu_client("", "", cfg["gcp_secret"], cfg["api_base_url"], cfg["token_url"])
    try:
        locations = fetch_locations(client)
    except Exception:
        # Mirrors pvacd_hydrovu_source() in dlt_pipeline.py: nothing else holds a
        # reference to this client yet if this raises, so it must close itself.
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
    """Runs ingest + transform + load for one calendar-month chunk — ingest
    writes only to hydrovu_backfill_readings, transform reads back by exact
    load_id match (not a watermark) and runs the same PvacdHydroVuAdapter
    production uses, load deletes-then-reposts the chunk window.

    Raises on any failure — checkpointed only after returning clean, so a
    mid-chunk failure retries cleanly (every stage is idempotent). bucket/fs
    are passed in since the caller already computes them once per run."""
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
    adapter = PvacdHydroVuAdapter(records)
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
