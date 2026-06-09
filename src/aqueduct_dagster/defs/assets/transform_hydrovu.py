"""
defs/assets/transform_hydrovu.py

Dagster asset: canonical_bundles_hydrovu
  - Reads latest raw parquet from GCS (written by raw_hydrovu_readings)
  - Reconstructs per-location records from flat parquet rows
  - Runs HydroVuAdapter to produce CanonicalBundles
  - Passes bundles downstream to frost_load

Upstream:  raw_hydrovu_readings
Downstream: frost_load
"""

from __future__ import annotations

import logging

from dagster import AssetExecutionContext, asset

from aqueduct_dagster.canonical.canonical_model import CanonicalBundle

logger = logging.getLogger(__name__)


@asset(
    name="canonical_bundles_hydrovu",
    group_name="hydrovu",
    description="CanonicalBundles produced by HydroVuAdapter from GCS raw parquet.",
    compute_kind="python",
    deps=["raw_hydrovu_readings"],
)
def canonical_bundles_hydrovu(context: AssetExecutionContext) -> list[CanonicalBundle]:
    """
    Reads raw HydroVu parquet from GCS, reconstructs per-location records,
    and runs HydroVuAdapter to produce CanonicalBundles — one per location.
    """
    # TODO: read parquet from GCS under gs://<bucket>/raw/pvacd/hydrovu_readings/
    # TODO: group rows by location_id and reconstruct adapter records
    # TODO: run HydroVuAdapter to_thing(), to_observations(), _build_datastreams()
    # TODO: return list of CanonicalBundles
    pass
