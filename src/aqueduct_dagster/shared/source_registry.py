"""
shared/source_registry.py

Single registry of per-source configuration. Both defs/definitions.py (jobs
and schedules) and defs/assets/load.py (FROST load assets) pull their config
from SOURCE_REGISTRY instead of each maintaining their own separate list.

Adding a new source means adding one entry here
Asset names follow the convention: raw_{name}_readings, canonical_bundles_{name},
frost_load_{name} — see sources/<name>/ for the per-source implementation.
"""

from __future__ import annotations

from typing import TypedDict


class SourceConfig(TypedDict):
    name: str  # source key — must match the sources/<name>/ folder and asset naming convention
    dataset: str  # GCS dataset name, raw_<name> — FROST watermark store + transform watermark path
    cron: str  # cron schedule for this source's daily pipeline job


SOURCE_REGISTRY: list[SourceConfig] = [
    {"name": "pvacd_hydrovu", "dataset": "raw_pvacd_hydrovu", "cron": "0 6 * * *"},
    {"name": "bernco_hydrovu", "dataset": "raw_bernco_hydrovu", "cron": "0 7 * * *"},
    {"name": "cabq", "dataset": "raw_cabq", "cron": "0 8 * * *"},
]
