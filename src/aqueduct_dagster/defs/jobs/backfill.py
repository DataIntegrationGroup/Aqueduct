"""
Mode A (refetch) backfill jobs — docs/BACKFILL_STRATEGY.md §4.2, §4.5.

One <source>_backfill_refetch job per source, built from each source's
prepare_backfill()/run_backfill_chunk() (see sources/pvacd_hydrovu/backfill.py).
A plain @job/@op pair, launched manually via the Dagster Launchpad — not the
daily schedule.
"""

import logging
from collections.abc import Callable
from typing import Any, cast

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
from aqueduct_dagster.shared.gcs import BAD_FILENAME_WARNING, _gcs_bucket_url, _gcs_filesystem
from aqueduct_dagster.shared.source_registry import SOURCE_REGISTRY
from aqueduct_dagster.sources.bernco_hydrovu.backfill import (
    default_backfill_location_ids as bernco_hydrovu_default_backfill_location_ids,
)
from aqueduct_dagster.sources.bernco_hydrovu.backfill import (
    prepare_backfill as bernco_hydrovu_prepare_backfill,
)
from aqueduct_dagster.sources.bernco_hydrovu.backfill import (
    run_backfill_chunk as bernco_hydrovu_run_backfill_chunk,
)
from aqueduct_dagster.sources.bernco_manual.backfill import (
    default_backfill_location_ids as bernco_manual_default_backfill_location_ids,
)
from aqueduct_dagster.sources.bernco_manual.backfill import (
    prepare_backfill as bernco_manual_prepare_backfill,
)
from aqueduct_dagster.sources.bernco_manual.backfill import (
    run_backfill_chunk as bernco_manual_run_backfill_chunk,
)
from aqueduct_dagster.sources.cabq.backfill import (
    default_backfill_location_ids as cabq_default_backfill_location_ids,
)
from aqueduct_dagster.sources.cabq.backfill import (
    prepare_backfill as cabq_prepare_backfill,
)
from aqueduct_dagster.sources.cabq.backfill import (
    run_backfill_chunk as cabq_run_backfill_chunk,
)
from aqueduct_dagster.sources.pvacd_hydrovu.backfill import (
    default_backfill_location_ids as pvacd_hydrovu_default_backfill_location_ids,
)
from aqueduct_dagster.sources.pvacd_hydrovu.backfill import (
    prepare_backfill as pvacd_hydrovu_prepare_backfill,
)
from aqueduct_dagster.sources.pvacd_hydrovu.backfill import (
    run_backfill_chunk as pvacd_hydrovu_run_backfill_chunk,
)

logger = logging.getLogger(__name__)

PrepareBackfillFn = Callable[[], tuple[httpx.Client, list[dict], dict[Any, dict]]]
RunBackfillChunkFn = Callable[..., ChunkResult]


class BackfillRefetchConfig[LocationId](Config):
    """Run config for a <source>_backfill_refetch job, via the Dagster Launchpad.
    Prefilled so a fresh scaffold is safe to launch as-is (dry_run: true).
    Per-source subclasses only override location_ids' default."""

    location_ids: list[LocationId] = Field(
        default=[],
        description="Location/entity IDs to backfill. Leave empty (the "
        "default) to backfill every location the source's API returns — "
        "resolved and validated at launch time, including during a "
        "dry_run. A per-source subclass may default this to a known "
        "allowlist instead.",
    )
    start_date: str = "2026-01-01"  # "YYYY-MM-DD", inclusive
    end_date: str = "2026-02-01"  # "YYYY-MM-DD", exclusive
    # Re-launch with the same run_key to resume from the last completed chunk.
    # validate_default=True: without it, pydantic skips validators on an
    # untouched default, so the timestamp would never get attached.
    run_key: str = Field(default="example-backfill", validate_default=True)
    # Gates GCS/FROST writes only — the read-only location-list call still
    # happens even when true.
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
    # [int] is an arbitrary placeholder — only tests omit config_cls; every
    # real source passes its own concrete subclass explicitly.
    config_cls: type[BackfillRefetchConfig] = BackfillRefetchConfig[int],
) -> OpDefinition:
    """Resolves the chunk plan, calls prepare_fn() (runs during dry_run too),
    logs and returns on dry_run, otherwise processes chunks sequentially —
    checkpointing each only after ingest+transform+load succeed."""

    @op(name=f"{name}_backfill_refetch_op")
    def _op(context: OpExecutionContext, config: config_cls) -> None:  # type: ignore[valid-type]
        # mypy can't check the runtime-varying subclass directly — cast to the
        # common base, which every subclass only overrides location_ids' default on.
        cfg = cast(BackfillRefetchConfig, config)
        start = parse_backfill_date(cfg.start_date, "start_date")
        end = parse_backfill_date(cfg.end_date, "end_date")
        chunks = month_chunks(start, end)

        # Forwards stdlib logging into this run's log. The whole "sources" tree
        # is forwarded, not just this source's package, since a fetch can go
        # through a sibling vendor module (hydrovu_common.py), not a descendant.
        with forward_python_logs_to_dagster(
            context,
            "aqueduct_dagster.sources",
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
                # Separate watermark file from production's — a backfill run can
                # never race with or clobber the daily pipeline's watermark state.
                #
                # Trade-off: backfilling an outage gap (§3 A.3) while production's
                # cursor independently recovers the same window can duplicate
                # observations — FROST has no dedup key (§4.4).
                loader = build_frost_loader(context, f"{dataset}_backfill")

                # Each chunk re-globs the whole table, so a bad filename can
                # repeat across chunks — dedupe so it's logged once per run.
                bad_filenames_already_logged: set[str] = set()

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

                    newly_seen_bad_filenames = (
                        result.files_skipped_bad_name - bad_filenames_already_logged
                    )
                    for f in newly_seen_bad_filenames:
                        context.log.warning(BAD_FILENAME_WARNING, f)
                    bad_filenames_already_logged |= newly_seen_bad_filenames
                    context.log.info(
                        "chunk [%s, %s) complete: rows_ingested=%d bundles_loaded=%d "
                        "observations_posted=%d observations_deleted=%d adapter_failures=%d "
                        "files_skipped_bad_name=%d",
                        chunk_start,
                        chunk_end,
                        result.rows_ingested,
                        result.bundles_loaded,
                        result.observations_posted,
                        result.observations_deleted,
                        result.adapter_failures,
                        len(result.files_skipped_bad_name),
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
                "files_skipped_bad_name": MetadataValue.int(len(totals.files_skipped_bad_name)),
            }
        )

    return _op


def _make_backfill_refetch_job(
    name: str,
    dataset: str,
    prepare_fn: PrepareBackfillFn,
    run_chunk_fn: RunBackfillChunkFn,
    # [int] is an arbitrary placeholder — only tests omit config_cls; every
    # real source passes its own concrete subclass explicitly.
    config_cls: type[BackfillRefetchConfig] = BackfillRefetchConfig[int],
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


class PvacdHydroVuBackfillRefetchConfig(BackfillRefetchConfig[int]):
    """Only overrides location_ids' default (PVACD's allowlist). Tenant-scoped,
    not vendor-scoped — a second HydroVu tenant gets its own subclass."""

    location_ids: list[int] = Field(
        default=pvacd_hydrovu_default_backfill_location_ids(),
        description="HydroVu location IDs to backfill. Defaults to the "
        "daily pipeline's own allowlist (.dlt/config.toml "
        "[sources.pvacd_hydrovu].location_ids). Leave empty to backfill every "
        "location the API returns instead.",
    )


_pvacd_hydrovu_registry_cfg = next(cfg for cfg in SOURCE_REGISTRY if cfg["name"] == "pvacd_hydrovu")
pvacd_hydrovu_backfill_refetch = _make_backfill_refetch_job(
    _pvacd_hydrovu_registry_cfg["name"],
    _pvacd_hydrovu_registry_cfg["dataset"],
    pvacd_hydrovu_prepare_backfill,
    pvacd_hydrovu_run_backfill_chunk,
    PvacdHydroVuBackfillRefetchConfig,
)


class CabqBackfillRefetchConfig(BackfillRefetchConfig[str]):
    location_ids: list[str] = Field(
        default=cabq_default_backfill_location_ids(),
        description="CABQ location IDs to backfill. Leave empty to backfill every location the API returns instead.",
    )


_cabq_registry_cfg = next(cfg for cfg in SOURCE_REGISTRY if cfg["name"] == "cabq")
cabq_backfill_refetch = _make_backfill_refetch_job(
    _cabq_registry_cfg["name"],
    _cabq_registry_cfg["dataset"],
    cabq_prepare_backfill,
    cabq_run_backfill_chunk,
    CabqBackfillRefetchConfig,
)


class BerncoHydroVuBackfillRefetchConfig(BackfillRefetchConfig[int]):
    """Only overrides location_ids' default (BernCo's allowlist), same pattern
    as PvacdHydroVuBackfillRefetchConfig."""

    location_ids: list[int] = Field(
        default=bernco_hydrovu_default_backfill_location_ids(),
        description="HydroVu location IDs to backfill. Defaults to the "
        "daily pipeline's own allowlist (.dlt/config.toml "
        "[sources.bernco_hydrovu].location_ids). Leave empty to backfill every "
        "location the API returns instead.",
    )


_bernco_hydrovu_registry_cfg = next(
    cfg for cfg in SOURCE_REGISTRY if cfg["name"] == "bernco_hydrovu"
)
bernco_hydrovu_backfill_refetch = _make_backfill_refetch_job(
    _bernco_hydrovu_registry_cfg["name"],
    _bernco_hydrovu_registry_cfg["dataset"],
    bernco_hydrovu_prepare_backfill,
    bernco_hydrovu_run_backfill_chunk,
    BerncoHydroVuBackfillRefetchConfig,
)


class BerncoManualBackfillRefetchConfig(BackfillRefetchConfig[str]):
    location_ids: list[str] = Field(
        default=bernco_manual_default_backfill_location_ids(),
        description="Bernco Manual location IDs to backfill. Leave empty to backfill every location the API returns instead.",
    )


_bernco_manual_registry_cfg = next(cfg for cfg in SOURCE_REGISTRY if cfg["name"] == "bernco_manual")
bernco_manual_backfill_refetch = _make_backfill_refetch_job(
    _bernco_manual_registry_cfg["name"],
    _bernco_manual_registry_cfg["dataset"],
    bernco_manual_prepare_backfill,
    bernco_manual_run_backfill_chunk,
    BerncoManualBackfillRefetchConfig,
)
