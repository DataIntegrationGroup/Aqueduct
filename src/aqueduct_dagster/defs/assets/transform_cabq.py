"""
defs/assets/transform_cabq.py

Dagster asset: canonical_bundles_cabq
  - Reads raw cabq_readings parquet from GCS (written by raw_cabq_readings)
  - Groups flat rows by location_id into one record per location
  - Runs CabqAdapter to produce CanonicalBundles (one per location)
  - Returns bundles downstream to frost_load_cabq

Incremental reads:
  Follow the same load_id watermark pattern as transform_hydrovu.py:
    - _read_watermark / _write_watermark using raw_cabq/_cabq_transform_watermark.json
    - Only read parquet files with load_id > last watermark
    - Watermark must be written in frost_load_cabq (after FROST success), not here
    - Return a CabqTransformResult dataclass carrying (bundles, max_load_id) so
      the load step can call commit_watermark only on success

Upstream:  raw_cabq_readings
Downstream: frost_load_cabq
"""


import logging

from dagster import AssetExecutionContext, asset

from aqueduct_dagster.adapters.cabq_adapter import CabqAdapter
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
    Reads raw CABQ parquet from GCS, groups rows by location, and runs
    CabqAdapter to produce CanonicalBundles — one per location.

    When implementing, follow transform_hydrovu.canonical_bundles_hydrovu:
      1. bucket_url = _gcs_bucket_url()  (import from transform_hydrovu or duplicate helper)
      2. creds = _gcs_credentials()
      3. since_load_id = _read_watermark(fs, bucket)
      4. rows, max_load_id = read new parquet files from raw_cabq/cabq_readings/
      5. group rows by location_id
      6. return CabqTransformResult(bundles=list(CabqAdapter(records).run()), max_load_id=max_load_id)
    """
    # TODO: implement — see docstring above for the pattern to follow
    return []
