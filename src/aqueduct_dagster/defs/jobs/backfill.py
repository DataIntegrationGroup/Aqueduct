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

BackfillRefetchConfig holds every field common to all sources (start_date,
end_date, run_key, dry_run), prefilled with example values, plus validation
(date format/order, and an auto-attached run_key timestamp). location_ids is
also shared, but its default differs per source — a per-source subclass (e.g.
HydroVuBackfillRefetchConfig) only overrides that one field's default, since
that's the only thing a new source needs to customize.
"""

import logging
from collections.abc import Callable
from typing import cast

import httpx
from dagster import Config, JobDefinition, MetadataValue, OpDefinition, OpExecutionContext, job, op
from pydantic import Field, ValidationInfo, field_validator, model_validator

from aqueduct_dagster.defs.assets.load import build_frost_loader
from aqueduct_dagster.defs.dagster_logging import forward_python_logs_to_dagster
from aqueduct_dagster.shared.backfill import (
    BackfillCheckpointStore,
    ChunkResult,
    attach_run_timestamp,
    month_chunks,
    parse_backfill_date,
    resolve_location_ids,
    sum_chunk_results,
    validate_date_order,
)
from aqueduct_dagster.shared.gcs import _gcs_bucket_url, _gcs_filesystem
from aqueduct_dagster.shared.source_registry import SOURCE_REGISTRY
from aqueduct_dagster.sources.hydrovu.backfill import (
    default_backfill_location_ids,
    prepare_backfill,
    run_backfill_chunk,
)

logger = logging.getLogger(__name__)

PrepareBackfillFn = Callable[[], tuple[httpx.Client, list[dict], dict[int, dict]]]
RunBackfillChunkFn = Callable[..., ChunkResult]


class BackfillRefetchConfig(Config):
    """
    Run configuration for a <source>_backfill_refetch job, filled in via the
    Dagster Launchpad — see docs/BACKFILL_STRATEGY.md §5.2. Prefilled with
    example values so a fresh scaffold is already valid and safe to launch
    as-is (dry_run: true).

    Fields here are common to every source. Per-source subclasses (e.g.
    HydroVuBackfillRefetchConfig below) only override location_ids' default;
    everything else is inherited. Validation lives in shared/backfill.py as
    plain, Dagster-free functions so Mode B (replay) can reuse it too.
    """

    location_ids: list[int] = Field(
        default=[],
        description="Location/entity IDs to backfill. Leave empty (the "
        "default) to backfill every location the source's API returns — "
        "resolved and validated at launch time, including during a "
        "dry_run. A per-source subclass may default this to a known "
        "allowlist instead.",
    )
    start_date: str = "2026-01-01"  # "YYYY-MM-DD", inclusive
    end_date: str = "2026-02-01"  # "YYYY-MM-DD", exclusive
    # Re-launch with the same run_key to resume a crashed run from its last
    # completed chunk (see attach_run_timestamp, BackfillCheckpointStore).
    # validate_default=True: pydantic otherwise skips field validators on an
    # untouched default, which would leave every launch on the same bare
    # "example-backfill" with no timestamp ever attached.
    run_key: str = Field(default="example-backfill", validate_default=True)
    # Per AGENTS.md: "any large backfill is a reviewed, deliberate action,
    # not a default." Gates GCS/FROST writes only — not the one live,
    # read-only API call the op always makes (see resolve_location_ids) to
    # resolve/validate location_ids, which happens even when dry_run is true.
    dry_run: bool = True

    @field_validator("start_date", "end_date")
    @classmethod
    def _validate_date_format(cls, value: str, info: ValidationInfo) -> str:
        parse_backfill_date(value, info.field_name or "date")
        return value

    @model_validator(mode="after")
    def _validate_date_order(self) -> "BackfillRefetchConfig":
        validate_date_order(self.start_date, self.end_date)
        return self

    @field_validator("run_key")
    @classmethod
    def _attach_run_timestamp(cls, value: str) -> str:
        return attach_run_timestamp(value)


def _make_backfill_refetch_op(
    name: str,
    dataset: str,
    prepare_fn: PrepareBackfillFn,
    run_chunk_fn: RunBackfillChunkFn,
    config_cls: type[BackfillRefetchConfig] = BackfillRefetchConfig,
) -> OpDefinition:
    """
    Builds the op behind <name>_backfill_refetch:
      1. Resolves the chunk plan (pure date math — see shared.backfill.month_chunks).
      2. Calls prepare_fn() (a read, so this runs even during dry_run) to
         resolve an empty location_ids into every API location, and reject
         any explicitly-listed id the API doesn't recognize.
      3. dry_run short-circuits here — logs the resolved plan, no GCS/FROST calls.
      4. Otherwise processes chunks sequentially, skipping ones already
         checkpointed for this run_key, checkpointing each only after its
         ingest + transform + load succeed (sources/hydrovu/backfill.py's
         run_backfill_chunk).
    """

    @op(name=f"{name}_backfill_refetch_op")
    def _op(context: OpExecutionContext, config: config_cls) -> None:  # type: ignore[valid-type]
        # config_cls is a runtime-varying per-source subclass, which mypy
        # can't check directly — cast to the common base every subclass
        # only adds/overrides location_ids' default on top of.
        cfg = cast(BackfillRefetchConfig, config)
        start = parse_backfill_date(cfg.start_date, "start_date")
        end = parse_backfill_date(cfg.end_date, "end_date")
        chunks = month_chunks(start, end)

        # Forwards prepare_fn()/run_chunk_fn()'s stdlib logging (see
        # sources/hydrovu/dlt_pipeline.py) plus BackfillCheckpointStore's own
        # logger ("aqueduct_dagster.shared.backfill", not a descendant of
        # "aqueduct_dagster.sources.{name}") into this run's log stream.
        # prepare_fn() runs even during dry_run, so this wraps it unconditionally.
        with forward_python_logs_to_dagster(
            context,
            f"aqueduct_dagster.sources.{name}",
            "aqueduct_dagster.shared",
            "aqueduct_dagster.canonical",
            "dlt",
        ):
            client, locations, locations_by_id = prepare_fn()
            try:
                try:
                    location_ids = resolve_location_ids(cfg.location_ids, locations_by_id)
                except ValueError as exc:
                    raise ValueError(f"{name} backfill: {exc}") from exc

                context.log.info(
                    "%s backfill refetch: %d location(s) %s, %d chunk(s) over [%s, %s), "
                    "run_key=%s, dry_run=%s",
                    name,
                    len(location_ids),
                    location_ids,
                    len(chunks),
                    cfg.start_date,
                    cfg.end_date,
                    cfg.run_key,
                    cfg.dry_run,
                )
                for chunk_start, chunk_end in chunks:
                    context.log.info("  planned chunk: [%s, %s)", chunk_start, chunk_end)

                if cfg.dry_run:
                    context.log.info(
                        "dry_run=true — plan resolved above (a live location-list read "
                        "was made; no GCS/FROST calls). Re-launch with dry_run: false to execute."
                    )
                    context.add_output_metadata(
                        {
                            "dry_run": MetadataValue.bool(True),
                            "location_count": MetadataValue.int(len(location_ids)),
                            "chunks_planned": MetadataValue.int(len(chunks)),
                            "start_date": MetadataValue.text(cfg.start_date),
                            "end_date": MetadataValue.text(cfg.end_date),
                        }
                    )
                    return

                bucket = _gcs_bucket_url().replace("gs://", "")
                fs = _gcs_filesystem()
                checkpoints = BackfillCheckpointStore(fs, bucket, dataset, run_key=cfg.run_key)
                # Separate FROST watermark file from production's
                # (raw_pvacd/_frost_watermarks.json), same isolation principle as the
                # separate GCS raw table (hydrovu_backfill_readings vs hydrovu_readings) —
                # so a backfill run can never race with, or clobber, the daily scheduled
                # pipeline's own watermark state.
                #
                # Known limitation this trades away: if this backfill is repairing an
                # outage gap (BACKFILL_STRATEGY.md §3, category A.3) and production's own
                # ingest cursor also naturally recovers into that same window on its own,
                # both paths can independently post the same underlying readings — FROST
                # observations have no dedup key (§4.4), so that specific overlap can
                # produce duplicates. Every other backfill situation (new entity, extended
                # history, vendor correction) is unaffected, since production's cursor
                # never revisits a window it has already moved past.
                loader = build_frost_loader(context, f"{dataset}_backfill")

                chunks_processed = 0
                chunks_skipped = 0
                chunk_results: list[ChunkResult] = []
                for chunk_start, chunk_end in chunks:
                    if checkpoints.is_complete(chunk_start, chunk_end, location_ids):
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
                        location_ids=location_ids,
                        chunk_start=chunk_start,
                        chunk_end=chunk_end,
                        loader=loader,
                        bucket=bucket,
                        fs=fs,
                        run_key=cfg.run_key,
                    )
                    checkpoints.mark_complete(chunk_start, chunk_end, location_ids)
                    chunks_processed += 1
                    chunk_results.append(result)
                    context.log.info(
                        "chunk [%s, %s) complete: rows_ingested=%d bundles_loaded=%d "
                        "observations_posted=%d observations_deleted=%d adapter_failures=%d",
                        chunk_start,
                        chunk_end,
                        result.rows_ingested,
                        result.bundles_loaded,
                        result.observations_posted,
                        result.observations_deleted,
                        result.adapter_failures,
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
                "adapter_failures": MetadataValue.int(totals.adapter_failures),
            }
        )

    return _op


def _make_backfill_refetch_job(
    name: str,
    dataset: str,
    prepare_fn: PrepareBackfillFn,
    run_chunk_fn: RunBackfillChunkFn,
    config_cls: type[BackfillRefetchConfig] = BackfillRefetchConfig,
) -> JobDefinition:
    op_fn = _make_backfill_refetch_op(name, dataset, prepare_fn, run_chunk_fn, config_cls)

    @job(
        name=f"{name}_backfill_refetch",
        description=f"{name.upper()} Mode A backfill: refetch an explicit entity list "
        "and date range under isolated pipeline state (docs/BACKFILL_STRATEGY.md §4.2).",
    )
    def _job() -> None:
        op_fn()

    return _job


class HydroVuBackfillRefetchConfig(BackfillRefetchConfig):
    """
    hydrovu_backfill_refetch's run configuration. Only overrides
    location_ids' default (HydroVu's own known-good allowlist, read at
    import time via default_backfill_location_ids()) — every other field is
    inherited unchanged from BackfillRefetchConfig.
    """

    location_ids: list[int] = Field(
        default=default_backfill_location_ids(),
        description="HydroVu location IDs to backfill. Defaults to the "
        "daily pipeline's own allowlist (.dlt/config.toml "
        "[sources.hydrovu].location_ids). Leave empty to backfill every "
        "location the API returns instead.",
    )


_hydrovu_registry_cfg = next(cfg for cfg in SOURCE_REGISTRY if cfg["name"] == "hydrovu")
hydrovu_backfill_refetch = _make_backfill_refetch_job(
    _hydrovu_registry_cfg["name"],
    _hydrovu_registry_cfg["dataset"],
    prepare_backfill,
    run_backfill_chunk,
    HydroVuBackfillRefetchConfig,
)
