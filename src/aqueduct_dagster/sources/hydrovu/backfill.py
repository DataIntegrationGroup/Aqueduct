"""
sources/hydrovu/backfill.py

Mode A (refetch) backfill for PVACD HydroVu — see docs/BACKFILL_STRATEGY.md §4.2.

Runs ingest -> transform -> load for an explicit entity list and date range,
under fully isolated dlt pipeline state (own pipeline_name, BACKFILL_PIPELINE_NAME)
and its own GCS table (hydrovu_backfill_readings, not hydrovu_readings — see
BACKFILL_TABLE_NAME). Because it's a different table, not just a different
pipeline_name, its files never match production transform.py's
"raw_pvacd/hydrovu_readings/**/*.parquet" glob — the normal scheduled pipeline
cannot see this data at all, so there's nothing to coordinate or interfere with.

Not a Dagster asset or op itself — no Dagster imports here. Called per-chunk
by the generic backfill job factory in defs/jobs/backfill.py, which owns the
run config, chunk loop, and checkpointing.

Reused, unchanged: _group_by_location (transform.py), HydroVuAdapter
(adapter.py), FrostLoader.load_window (loader/frost_loader.py). Only the
ingest side (hydrovu_backfill_readings, and reading .dlt/config.toml directly
since this isn't invoked via @dlt.source injection) is HydroVu-specific —
mirroring dlt_pipeline.py's existing per-source ingest code, per
BACKFILL_STRATEGY.md §4.2: "requires one new function per source... cannot be
made fully generic."
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import dlt
import gcsfs
import httpx
import toml

from aqueduct_dagster.canonical.base_adapter import log_if_adapter_failed
from aqueduct_dagster.canonical.canonical_model import CanonicalBundle
from aqueduct_dagster.loader.frost_loader import FrostLoader, ObservationRecord
from aqueduct_dagster.shared.backfill import ChunkResult
from aqueduct_dagster.shared.gcs import read_parquet_rows_for_load_id
from aqueduct_dagster.shared.pipeline import build_source_pipeline
from aqueduct_dagster.sources.hydrovu.adapter import HydroVuAdapter
from aqueduct_dagster.sources.hydrovu.dlt_pipeline import (
    _fetch_location_data,
    _fetch_locations,
    build_hydrovu_client,
)
from aqueduct_dagster.sources.hydrovu.transform import (
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

        data, err = _fetch_location_data(client, loc_id, start_ts, end_time=end_ts)
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


_UNSAFE_PIPELINE_NAME_CHARS = re.compile(r"[^A-Za-z0-9_-]")


def _sanitize_run_key(run_key: str) -> str:
    """
    run_key becomes part of a local filesystem directory name (dlt's pipeline
    working dir lives at ~/.dlt/pipelines/<pipeline_name>), so anything that
    isn't alphanumeric/-/_ is replaced with _ — an operator-typed run_key
    shouldn't be able to produce a broken or surprising path.
    """
    return _UNSAFE_PIPELINE_NAME_CHARS.sub("_", run_key)


def build_backfill_pipeline(run_key: str) -> dlt.Pipeline:
    """
    Isolated dlt pipeline: one pipeline_name PER run_key (not one shared
    constant), same raw_pvacd dataset as production.

    A distinct pipeline_name per run_key means two different backfill
    operations can never share dlt's local pending-load state at all — a
    pending package left by one (uncleanly terminated) run_key's run can no
    longer be silently finished and returned by an unrelated run_key's
    pipeline.run() call later, since they no longer share the same dlt
    working directory in the first place. drop_pending_packages() (see
    run_backfill_chunk) still guards the narrower case of retrying the exact
    same run_key/chunk.
    """
    return build_source_pipeline(
        f"{BACKFILL_PIPELINE_NAME}_{_sanitize_run_key(run_key)}", GCS_DATASET
    )


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


def _load_hydrovu_config() -> dict[str, Any]:
    """
    Reads [sources.hydrovu] from .dlt/config.toml directly. This module isn't
    invoked via @dlt.source, so it doesn't get dlt.config.value injection like
    hydrovu_source() does — this reads the same section by hand.
    """
    config_path = os.path.join(os.getcwd(), ".dlt", "config.toml")
    return toml.load(config_path)["sources"]["hydrovu"]


def prepare_backfill() -> tuple[httpx.Client, list[dict], dict[int, dict]]:
    """
    One-time setup shared by every chunk in a backfill run: builds the
    authenticated client and fetches the location list exactly once — the
    location reference data doesn't depend on the date range, so there's no
    reason to re-fetch it per chunk.

    Returns (client, locations, locations_by_id).
    """
    cfg = _load_hydrovu_config()
    client = build_hydrovu_client("", "", cfg["gcp_secret"], cfg["api_base_url"], cfg["token_url"])
    try:
        locations = _fetch_locations(client)
    except Exception:
        # Mirrors hydrovu_source() in dlt_pipeline.py: nothing else holds a
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
      3. Load — FrostLoader.load_window() per datastream: delete existing
         observations in [chunk_start, chunk_end), then repost.

    Raises on any failure in any stage — the caller (defs/jobs/backfill.py)
    only checkpoints a chunk after this returns without raising, so a
    mid-chunk failure retries the whole chunk next launch (safe: every stage
    here is idempotent to repeat).

    bucket/fs are passed in (not derived here) because the caller already
    computes them once before the chunk loop — re-parsing .dlt/config.toml
    and rebuilding the GCS filesystem client on every chunk would be pure
    waste for a multi-month backfill.

    run_key gives this run its own dlt pipeline_name (see
    build_backfill_pipeline), so two different backfill operations can never
    share dlt's local pending-load state at all.
    """
    pipeline = build_backfill_pipeline(run_key)
    logger.info(
        "Backfill chunk [%s, %s): using dlt pipeline_name=%s",
        chunk_start,
        chunk_end,
        pipeline.pipeline_name,
    )
    start_ts = int(chunk_start.timestamp())
    end_ts = int(chunk_end.timestamp())

    # Guards the narrower case within this SAME run_key: a package left
    # pending by this exact chunk's own earlier, uncleanly-terminated attempt
    # would otherwise get silently finished and returned by pipeline.run()
    # below INSTEAD of this chunk's real data — dlt's own run() exits early
    # once it finds pending data, without ever calling
    # hydrovu_backfill_readings() at all. This resource has no persisted
    # cursor to lose, so there's nothing to gain by ever resuming pending
    # data — always start clean.
    pipeline.drop_pending_packages()

    load_info = pipeline.run(
        hydrovu_backfill_readings(
            client=client,
            locations=locations,
            location_ids=location_ids,
            start_ts=start_ts,
            end_ts=end_ts,
        ),
        loader_file_format="parquet",
    )
    if not load_info.loads_ids:
        # dlt only creates a load package when there's something new to persist.
        # On this pipeline's very first-ever run, that's always true regardless
        # of data (schema/state bookkeeping alone counts). On every run after
        # that, if this chunk's requested location(s) yielded zero rows — e.g.
        # a bad/nonexistent location_id, or a genuinely empty month — there is
        # nothing new at all, and loads_ids comes back empty. That's a normal
        # outcome, not a failure: report an empty chunk rather than indexing
        # into an empty list.
        logger.info(
            "Backfill chunk [%s, %s): ingest yielded no new data — nothing to transform or load",
            chunk_start,
            chunk_end,
        )
        return ChunkResult(
            rows_ingested=0, bundles_loaded=0, observations_posted=0, observations_deleted=0
        )
    load_id = float(load_info.loads_ids[0])
    logger.info(
        "Backfill chunk [%s, %s): ingest complete, load_id=%s", chunk_start, chunk_end, load_id
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

    observations_posted = 0
    observations_deleted = 0
    for bundle in bundles:
        for datastream in bundle.datastreams:
            ds_id = loader.ensure_datastream(datastream)
            raw_obs = bundle.observations.get(datastream.external_key, [])
            obs_records = [
                ObservationRecord(phenomenon_time=o.phenomenon_time, result=o.result)
                for o in raw_obs
            ]
            result = loader.load_window(
                datastream.external_key, ds_id, obs_records, chunk_start, chunk_end
            )
            observations_posted += result.posted
            observations_deleted += result.deleted

    return ChunkResult(
        rows_ingested=len(rows),
        bundles_loaded=len(bundles),
        observations_posted=observations_posted,
        observations_deleted=observations_deleted,
        adapter_failures=adapter.failure_count,
    )
