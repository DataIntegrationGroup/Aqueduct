"""
defs/definitions.py

Dagster entry point — all assets, jobs, and schedules registered here.

Two independent pipelines — each can be run and scheduled separately:
  hydrovu_pipeline:  raw_hydrovu_readings → canonical_bundles_hydrovu → frost_load
  cabq_pipeline:     raw_cabq_readings    → canonical_bundles_cabq    → frost_load

Each pipeline has its own schedule with an independent cron.
To add a new source: add its assets module, define a job and schedule below.
"""

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from aqueduct_dagster.defs.assets import (
    ingest_hydrovu,
    ingest_cabq,
    transform_hydrovu,
    transform_cabq,
    load,
)

# ── Load all assets ───────────────────────────────────────────────────────────

all_assets = load_assets_from_modules([
    ingest_hydrovu,
    ingest_cabq,
    transform_hydrovu,
    transform_cabq,
    load,
])

# ── Jobs — one per source ─────────────────────────────────────────────────────

hydrovu_pipeline_job = define_asset_job(
    name="hydrovu_pipeline",
    selection=["raw_hydrovu_readings", "canonical_bundles_hydrovu", "frost_load_hydrovu"],
    description="HydroVu pipeline: ingest → transform → FROST",
)

cabq_pipeline_job = define_asset_job(
    name="cabq_pipeline",
    selection=["raw_cabq_readings", "canonical_bundles_cabq", "frost_load_cabq"],
    description="CABQ pipeline: ingest → transform → FROST",
)

# ── Schedules — independent cron per source ───────────────────────────────────

hydrovu_schedule = ScheduleDefinition(
    name="hydrovu_schedule",
    job=hydrovu_pipeline_job,
    cron_schedule="TODO",  # TODO: set HydroVu update frequency e.g. "0 6 * * *"
)

cabq_schedule = ScheduleDefinition(
    name="cabq_schedule",
    job=cabq_pipeline_job,
    cron_schedule="TODO",  # TODO: set CABQ update frequency e.g. "0 8 * * *"
)

# ── Definitions ───────────────────────────────────────────────────────────────

defs = Definitions(
    assets=all_assets,
    jobs=[hydrovu_pipeline_job, cabq_pipeline_job],
    schedules=[hydrovu_schedule, cabq_schedule],
)
