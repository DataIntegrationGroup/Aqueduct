"""
defs/assets/cabq/transform.py

Dagster asset: canonical_bundles_cabq
  - Reads raw cabq_readings parquet from GCS (written by raw_cabq_readings)
  - Groups flat rows by location_id into one record per location
  - Runs CabqAdapter to produce CanonicalBundles (one per location)
  - Returns bundles downstream to frost_load_cabq

Incremental reads:
  Follow the same load_id watermark pattern as hydrovu/transform.py, using the
  shared helpers in shared/gcs.py — no need to duplicate this logic:
    - read_transform_watermark(fs, bucket, WATERMARK_PATH) for since_load_id
    - read_new_parquet_rows(bucket, glob_suffix, since_load_id, fs, row_filter=...)
      to read only new parquet files, optionally filtered to the rows this
      source cares about (e.g. a specific parameter/measurement type)
    - Watermark must be written in frost_load_cabq (after FROST success), not here
    - Return a CabqTransformResult dataclass carrying (bundles, max_load_id) so
      the load step can call commit_watermark only on success

Upstream:  raw_cabq_readings
Downstream: frost_load_cabq
"""

import logging
from dataclasses import dataclass

from dagster import AssetExecutionContext, asset

from aqueduct_dagster.canonical.canonical_model import CanonicalBundle
from aqueduct_dagster.sources.cabq.adapter import CabqAdapter  # noqa: F401

logger = logging.getLogger(__name__)

GCS_DATASET = "raw_cabq"
WATERMARK_PATH = f"{GCS_DATASET}/_cabq_transform_watermark.json"


@dataclass
class CabqTransformResult:
    """Carries CanonicalBundles and the GCS load_id watermark to the load step.

    max_load_id is None when there were no new parquet files this run.
    The load step writes the watermark only after FROST confirms success.
    """

    bundles: list[CanonicalBundle]
    max_load_id: float | None


@asset(
    name="canonical_bundles_cabq",
    group_name="cabq",
    description="CanonicalBundles produced by CabqAdapter from GCS raw parquet.",
    compute_kind="python",
    deps=["raw_cabq_readings"],
)
def canonical_bundles_cabq(context: AssetExecutionContext) -> CabqTransformResult:
    """
    Reads raw CABQ parquet from GCS, groups rows by location, and runs
    CabqAdapter to produce CanonicalBundles — one per location.

    When implementing, follow hydrovu/transform.py's canonical_bundles_hydrovu,
    using the shared helpers from shared/gcs.py (do not duplicate them):
      1. bucket_url = _gcs_bucket_url(); bucket = bucket_url.replace("gs://", "")
      2. fs = _gcs_filesystem()
      3. since_load_id = read_transform_watermark(fs, bucket, WATERMARK_PATH)
      4. rows, max_load_id = read_new_parquet_rows(
             bucket, f"{GCS_DATASET}/cabq_readings/**/*.parquet", since_load_id, fs,
         )
      5. group rows by location_id
      6. return CabqTransformResult(bundles=list(CabqAdapter(records).run()), max_load_id=max_load_id)
    """
    # TODO: implement — see docstring above for the pattern to follow
    return CabqTransformResult(bundles=[], max_load_id=None)
