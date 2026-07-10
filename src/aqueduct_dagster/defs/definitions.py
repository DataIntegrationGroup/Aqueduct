"""
defs/definitions.py

Dagster entry point — all assets, jobs, and schedules registered here.

Two independent pipelines — each can be run and scheduled separately:
  hydrovu_pipeline:  raw_hydrovu_readings → canonical_bundles_hydrovu → frost_load_hydrovu
  cabq_pipeline:     raw_cabq_readings    → canonical_bundles_cabq    → frost_load_cabq

Adding source 3: add one entry to shared/source_registry.py's SOURCE_REGISTRY.
Jobs and schedules are generated automatically — no other changes needed in
this file. defs/assets/load.py reads from the same registry.
"""

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)

from aqueduct_dagster import sources as sources_pkg
from aqueduct_dagster.defs import assets as shared_assets_pkg
from aqueduct_dagster.shared.source_registry import SOURCE_REGISTRY

# ── Load all assets ───────────────────────────────────────────────────────────
# sources/ — per-source ingest + transform assets (auto-discovered)
# defs/assets/ — shared load assets (frost_load_*)

all_assets = [
    *load_assets_from_package_module(sources_pkg),
    *load_assets_from_package_module(shared_assets_pkg),
]

# ── Jobs and schedules — generated from config ────────────────────────────────

_jobs = []
_schedules = []

for _cfg in SOURCE_REGISTRY:
    _n = _cfg["name"]
    _job = define_asset_job(
        name=f"{_n}_pipeline",
        selection=[f"raw_{_n}_readings", f"canonical_bundles_{_n}", f"frost_load_{_n}"],
        description=f"{_n.upper()} pipeline: ingest → transform → FROST",
    )
    _jobs.append(_job)
    _schedules.append(
        ScheduleDefinition(
            name=f"{_n}_schedule",
            job=_job,
            cron_schedule=_cfg["cron"],
        )
    )

# ── Definitions ───────────────────────────────────────────────────────────────

defs = Definitions(
    assets=all_assets,
    jobs=_jobs,
    schedules=_schedules,
)
