"""
sources/pvacd_hydrovu/backfill.py

Mode A (refetch) backfill for PVACD HydroVu — see docs/BACKFILL_STRATEGY.md §4.2.

Runs ingest -> transform -> load for an explicit entity list and date range,
under fully isolated dlt pipeline state (own pipeline_name, BACKFILL_PIPELINE_NAME)
and its own GCS table (hydrovu_backfill_readings, not hydrovu_readings — see
BACKFILL_TABLE_NAME). Because it's a different table, not just a different
pipeline_name, its files never match production transform.py's
"raw_pvacd_hydrovu/hydrovu_readings/**/*.parquet" glob — the normal scheduled pipeline
cannot see this data at all, so there's nothing to coordinate or interfere with.

Not a Dagster asset or op itself — no Dagster imports here. Called per-chunk
by the generic backfill job factory in defs/jobs/backfill.py, which owns the
run config, chunk loop, and checkpointing.

Reused, unchanged: _group_by_location (transform.py), HydroVuAdapter
(adapter.py). Load (FrostLoader.load_window per datastream) happens inside
shared.backfill.load_bundles_windowed, not here. Only the ingest side
(hydrovu_backfill_readings) and the location_ids/client-setup glue
(default_backfill_location_ids, prepare_backfill) are HydroVu-specific —
mirroring dlt_pipeline.py's existing per-source ingest code, per
BACKFILL_STRATEGY.md §4.2: "requires one new function per source... cannot be
made fully generic." Config-reading itself (load_source_config) is shared,
since it's a plain [sources.<name>] lookup with no HydroVu-specific content.
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
from aqueduct_dagster.sources.pvacd_hydrovu.adapter import HydroVuAdapter
from aqueduct_dagster.sources.pvacd_hydrovu.transform import (
    DTW_PARAMETER_ID,
    GCS_DATASET,
    _group_by_location,
)

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
    """
    Yields one flat record per (location, parameter, reading) within the
    half-open range [start_ts, end_ts), for location_ids only.

    No persisted cursor of any kind — unlike hydrovu_readings, every call is
    fully explicit about its range. Chunk-level completion is tracked
    separately by shared.backfill.BackfillCheckpointStore, at a coarser but
    sufficient granularity, so there's no need for this resource to touch
    dlt.current.resource_state() at all — which is what keeps it from ever
    being confused with (or interfering with) hydrovu_readings' per-location
    production cursors.

    Raises RuntimeError on a real fetch error (not a 404) for any location —
    a chunk is all-or-nothing (see BACKFILL_STRATEGY.md §4.3), so one failed
    location must fail the whole chunk rather than silently yielding partial
    data that would then get checkpointed as complete.
    """
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
    """
    Converts the raw HydroVu /locations/list response into the {id: {...}}
    shape _group_by_location expects — the same shape transform.py's
    _read_locations_from_gcs produces when reading the locations parquet.
    Built directly from the already-fetched in-memory list, so backfill never
    needs to read or write the hydrovu_locations table at all.
    """
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
    """
    Same allowlist the daily pipeline reads from .dlt/config.toml
    ([sources.pvacd_hydrovu].location_ids). Called once, eagerly, at
    defs/jobs/backfill.py import time, since Dagster's Launchpad only shows
    a plain, already-computed default — not a lazily-resolved one.

    No location_ids key configured is not an error (returns [], meaning
    "every location" — see resolve_location_ids): some sources may
    deliberately not curate an allowlist. But .dlt/config.toml itself being
    unreadable/malformed does raise — that's a broken environment, and
    failing Dagster's definitions load loudly beats silently defaulting to
    "backfill everything."
    """
    return list(load_source_config("pvacd_hydrovu").get("location_ids", []))


def prepare_backfill() -> tuple[httpx.Client, list[dict], dict[int, dict]]:
    """
    One-time setup shared by every chunk in a backfill run: builds the
    authenticated client and fetches the location list exactly once — the
    location reference data doesn't depend on the date range, so there's no
    reason to re-fetch it per chunk.

    Returns (client, locations, locations_by_id).
    """
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
    """
    Runs ingest + transform + load for one calendar-month chunk:
      1. Ingest — isolated dlt pipeline run, writing only to
         hydrovu_backfill_readings (never hydrovu_readings).
      2. Transform — reads back exactly this run's rows (by exact load_id
         match, not "since some watermark"), groups by location, and runs
         HydroVuAdapter — the same adapter production uses, unchanged.
      3. Load — shared.backfill.load_bundles_windowed() per datastream:
         delete existing observations in [chunk_start, chunk_end), then repost.

    Raises on any failure in any stage — the caller (defs/jobs/backfill.py)
    only checkpoints a chunk after this returns without raising, so a
    mid-chunk failure retries the whole chunk next launch (safe: every stage
    here is idempotent to repeat).

    bucket/fs are passed in (not derived here) because the caller already
    computes them once before the chunk loop — re-parsing .dlt/config.toml
    and rebuilding the GCS filesystem client on every chunk would be pure
    waste for a multi-month backfill.

    run_key gives this run its own dlt pipeline_name (see
    shared.backfill.build_backfill_pipeline), so two different backfill
    operations can never share dlt's local pending-load state at all.
    """
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

    rows = read_parquet_rows_for_load_id(
        bucket,
        f"{GCS_DATASET}/{BACKFILL_TABLE_NAME}/**/*.parquet",
        load_id,
        fs,
        row_filter=lambda row: row["parameter_id"] == DTW_PARAMETER_ID,
    )

    records = _group_by_location(rows, locations_by_id)
    adapter = HydroVuAdapter(records)
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
