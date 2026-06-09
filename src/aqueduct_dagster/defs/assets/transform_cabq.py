"""
defs/assets/transform_cabq.py

Dagster asset: canonical_bundles_cabq
  - Reads latest raw parquet from GCS (written by raw_cabq_readings)
  - Reconstructs per-location records from flat parquet rows
  - Runs CabqAdapter to produce CanonicalBundles
  - Passes bundles downstream to frost_load

Upstream:  raw_cabq_readings
Downstream: frost_load
"""

from __future__ import annotations

import logging

from dagster import AssetExecutionContext, asset

from aqueduct_dagster.canonical.canonical_model import CanonicalBundle

logger = logging.getLogger(__name__)


@asset(
    name="canonical_bundles_cabq",
    group_name="cabq",
    description="CanonicalBundles produced by CabqAdapter from GCS raw parquet.",
    compute_kind="python",
    deps=["raw_cabq_readings"],
)
def canonical_bundles_cabq(context: AssetExecutionContext) -> list[CanonicalBundle]:
    """
    Reads raw CABQ parquet from GCS, reconstructs per-location records,
    and runs CabqAdapter to produce CanonicalBundles — one per location.
    """
    # TODO: read parquet from GCS under gs://<bucket>/raw/cabq/cabq_readings/
    # TODO: group rows by location_id and reconstruct adapter records
    # TODO: run CabqAdapter to_thing(), to_observations(), _build_datastreams()
    # TODO: return list of CanonicalBundles
    pass
