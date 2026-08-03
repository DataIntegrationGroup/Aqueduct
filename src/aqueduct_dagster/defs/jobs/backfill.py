"""
defs/jobs/backfill.py

Mode A (refetch) backfill jobs — see docs/BACKFILL_STRATEGY.md §4.2, §4.5.

hydrovu_backfill_refetch is the first of these (ST2DAT-202). The job is built
by a small factory (_make_backfill_refetch_job) parameterized by the same
per-source prepare_backfill()/run_backfill_chunk() shape every source's
backfill.py exposes (see sources/hydrovu/backfill.py) — so a future source
only needs its own sources/<name>/backfill.py plus one more call to the
factory here, no other job-wiring changes. Only HydroVu is wired for now.

Not a Dagster asset job — this is a plain @job of one @op, driven entirely by
run configuration (BackfillRefetchConfig), matching how an operator actually
launches a backfill: via the Dagster Launchpad, not the daily schedule (see
docs/BACKFILL_STRATEGY.md §5.2). No schedule is attached — launched manually,
on demand, same as every backfill job.
"""

import logging
from collections.abc import Callable
from datetime import datetime

import httpx
from dagster import Config, JobDefinition, MetadataValue, OpDefinition, OpExecutionContext, job, op

from aqueduct_dagster.defs.assets.load import build_frost_loader
from aqueduct_dagster.defs.dagster_logging import forward_python_logs_to_dagster
from aqueduct_dagster.shared.backfill import (
    BackfillCheckpointStore,
    ChunkResult,
    month_chunks,
    sum_chunk_results,
)
from aqueduct_dagster.shared.gcs import _gcs_bucket_url, _gcs_filesystem
from aqueduct_dagster.shared.source_registry import SOURCE_REGISTRY
from aqueduct_dagster.sources.hydrovu.backfill import prepare_backfill, run_backfill_chunk

logger = logging.getLogger(__name__)

PrepareBackfillFn = Callable[[], tuple[httpx.Client, list[dict], dict[int, dict]]]
RunBackfillChunkFn = Callable[..., ChunkResult]


class BackfillRefetchConfig(Config):
    """
    Run configuration for a <source>_backfill_refetch job, filled in via the
    Dagster Launchpad — see docs/BACKFILL_STRATEGY.md §5.2.
    """

    location_ids: list[int]
    start_date: str  # "YYYY-MM-DD", inclusive
    end_date: str  # "YYYY-MM-DD", exclusive
    # Identifies this backfill run for checkpoint resume — re-launch with the
    # same run_key to resume a crashed/interrupted run from the last
    # completed chunk; use a different run_key to start an independent run.
    run_key: str
    # Safety default per AGENTS.md: "any large backfill is a reviewed,
    # deliberate action, not a default." Must be explicitly set to false to
    # perform any API call, GCS write, or FROST write.
    dry_run: bool = True


def _make_backfill_refetch_op(
    name: str,
    dataset: str,
    prepare_fn: PrepareBackfillFn,
    run_chunk_fn: RunBackfillChunkFn,
) -> OpDefinition:
    """
    Builds the op behind <name>_backfill_refetch:
      1. Resolves the chunk plan (pure date math — see shared.backfill.month_chunks).
      2. dry_run short-circuits here — logs the plan, makes no API/GCS/FROST calls.
      3. Otherwise processes chunks sequentially, skipping any already
         checkpointed for this run_key, checkpointing each only after its
         ingest + transform + load all succeed (see
         sources/hydrovu/backfill.py's run_backfill_chunk).
    """

    @op(name=f"{name}_backfill_refetch_op")
    def _op(context: OpExecutionContext, config: BackfillRefetchConfig) -> None:
        start = datetime.strptime(config.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(config.end_date, "%Y-%m-%d").date()
        chunks = month_chunks(start, end)

        context.log.info(
            "%s backfill refetch: %d location(s), %d chunk(s) over [%s, %s), "
            "run_key=%s, dry_run=%s",
            name,
            len(config.location_ids),
            len(chunks),
            config.start_date,
            config.end_date,
            config.run_key,
            config.dry_run,
        )
        for chunk_start, chunk_end in chunks:
            context.log.info("  planned chunk: [%s, %s)", chunk_start, chunk_end)

        if config.dry_run:
            context.log.info(
                "dry_run=true — plan resolved above, no API/GCS/FROST calls made. "
                "Re-launch with dry_run: false to execute."
            )
            context.add_output_metadata(
                {
                    "dry_run": MetadataValue.bool(True),
                    "location_count": MetadataValue.int(len(config.location_ids)),
                    "chunks_planned": MetadataValue.int(len(chunks)),
                    "start_date": MetadataValue.text(config.start_date),
                    "end_date": MetadataValue.text(config.end_date),
                }
            )
            return

        bucket = _gcs_bucket_url().replace("gs://", "")
        fs = _gcs_filesystem()
        checkpoints = BackfillCheckpointStore(fs, bucket, dataset, run_key=config.run_key)
        # Separate FROST watermark file from production's (raw_pvacd/_frost_watermarks.json),
        # same isolation principle as the separate GCS raw table (hydrovu_backfill_readings
        # vs hydrovu_readings) — so a backfill run can never race with, or clobber, the
        # daily scheduled pipeline's own watermark state.
        #
        # Known limitation this trades away: if this backfill is repairing an outage gap
        # (BACKFILL_STRATEGY.md §3, category A.3) and production's own ingest cursor also
        # naturally recovers into that same window on its own, both paths can independently
        # post the same underlying readings — FROST observations have no dedup key (§4.4),
        # so that specific overlap can produce duplicates. Every other backfill situation
        # (new entity, extended history, vendor correction) is unaffected, since production's
        # cursor never revisits a window it has already moved past.
        loader = build_frost_loader(context, f"{dataset}_backfill")

        # Forwards the per-location/per-page progress logging that
        # prepare_fn()/run_chunk_fn() emit via stdlib logging (see
        # sources/hydrovu/dlt_pipeline.py's _fetch_locations/_fetch_location_data),
        # plus BackfillCheckpointStore's own logging (shared/backfill.py, logger
        # name "aqueduct_dagster.shared.backfill" — not a descendant of
        # "aqueduct_dagster.sources.{name}", so it needs its own prefix here),
        # into this run's captured log stream.
        chunks_processed = 0
        chunks_skipped = 0
        chunk_results: list[ChunkResult] = []
        with forward_python_logs_to_dagster(
            context, f"aqueduct_dagster.sources.{name}", "aqueduct_dagster.shared", "dlt"
        ):
            client, locations, locations_by_id = prepare_fn()
            try:
                for chunk_start, chunk_end in chunks:
                    if checkpoints.is_complete(chunk_start, chunk_end, config.location_ids):
                        context.log.info(
                            "chunk [%s, %s) already checkpointed complete — skipping",
                            chunk_start,
                            chunk_end,
                        )
                        chunks_skipped += 1
                        continue

                    result = run_chunk_fn(
                        client=client,
                        locations=locations,
                        locations_by_id=locations_by_id,
                        location_ids=config.location_ids,
                        chunk_start=chunk_start,
                        chunk_end=chunk_end,
                        loader=loader,
                        bucket=bucket,
                        fs=fs,
                        run_key=config.run_key,
                    )
                    checkpoints.mark_complete(chunk_start, chunk_end, config.location_ids)
                    chunks_processed += 1
                    chunk_results.append(result)
                    context.log.info(
                        "chunk [%s, %s) complete: rows_ingested=%d bundles_loaded=%d "
                        "observations_posted=%d observations_deleted=%d",
                        chunk_start,
                        chunk_end,
                        result.rows_ingested,
                        result.bundles_loaded,
                        result.observations_posted,
                        result.observations_deleted,
                    )
            finally:
                client.close()

        totals = sum_chunk_results(chunk_results)
        context.add_output_metadata(
            {
                "dry_run": MetadataValue.bool(False),
                "chunks_processed": MetadataValue.int(chunks_processed),
                "chunks_skipped_already_complete": MetadataValue.int(chunks_skipped),
                "rows_ingested": MetadataValue.int(totals.rows_ingested),
                "bundles_loaded": MetadataValue.int(totals.bundles_loaded),
                "observations_posted": MetadataValue.int(totals.observations_posted),
                "observations_deleted": MetadataValue.int(totals.observations_deleted),
            }
        )

    return _op


def _make_backfill_refetch_job(
    name: str,
    dataset: str,
    prepare_fn: PrepareBackfillFn,
    run_chunk_fn: RunBackfillChunkFn,
) -> JobDefinition:
    op_fn = _make_backfill_refetch_op(name, dataset, prepare_fn, run_chunk_fn)

    @job(
        name=f"{name}_backfill_refetch",
        description=f"{name.upper()} Mode A backfill: refetch an explicit entity list "
        "and date range under isolated pipeline state (docs/BACKFILL_STRATEGY.md §4.2).",
    )
    def _job() -> None:
        op_fn()

    return _job


_hydrovu_registry_cfg = next(cfg for cfg in SOURCE_REGISTRY if cfg["name"] == "hydrovu")
hydrovu_backfill_refetch = _make_backfill_refetch_job(
    _hydrovu_registry_cfg["name"],
    _hydrovu_registry_cfg["dataset"],
    prepare_backfill,
    run_backfill_chunk,
)
