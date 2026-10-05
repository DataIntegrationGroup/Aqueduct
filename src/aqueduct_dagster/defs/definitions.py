"""
Dagster entry point — all assets, jobs, and schedules registered here.

Each source in shared/source_registry.py's SOURCE_REGISTRY gets its own
pipeline (ingest → transform → FROST), job, and schedule, generated
automatically below. Adding a source needs only a new registry entry —
defs/assets/load.py reads from the same registry.
"""

from typing import Any

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)

from aqueduct_dagster import sources as sources_pkg
from aqueduct_dagster.defs import assets as shared_assets_pkg
from aqueduct_dagster.defs.jobs.backfill import (
    bernco_hydrovu_backfill_refetch,
    bernco_manual_backfill_refetch,
    cabq_backfill_refetch,
    pvacd_hydrovu_backfill_refetch,
)
from aqueduct_dagster.shared.source_registry import SOURCE_REGISTRY

# ── Load all assets ───────────────────────────────────────────────────────────
# sources/ — per-source ingest + transform assets (auto-discovered)
# defs/assets/ — shared load assets (frost_load_*)

all_assets = [
    *load_assets_from_package_module(sources_pkg),
    *load_assets_from_package_module(shared_assets_pkg),
]

# ── Jobs and schedules — generated from config ────────────────────────────────

_jobs: list[Any] = []
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

# ── Backfill jobs (Mode A refetch) ─────────────────────────────────────────────
# Launched manually via run configuration — no schedule attached, see
# docs/BACKFILL_STRATEGY.md §5.2. One job per source that has its own
# sources/<name>/backfill.py; adding another means one more factory call in
# defs/jobs/backfill.py and one more append below.

_jobs.append(pvacd_hydrovu_backfill_refetch)
_jobs.append(cabq_backfill_refetch)
_jobs.append(bernco_hydrovu_backfill_refetch)
_jobs.append(bernco_manual_backfill_refetch)

# ── Definitions ───────────────────────────────────────────────────────────────

defs = Definitions(
    assets=all_assets,
    jobs=_jobs,
    schedules=_schedules,
)
