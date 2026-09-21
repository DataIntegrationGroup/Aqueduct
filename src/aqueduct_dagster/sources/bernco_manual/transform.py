from dataclasses import dataclass

from dagster import AssetExecutionContext, asset

from aqueduct_dagster.canonical import CanonicalBundle
from aqueduct_dagster.shared.gcs import transform_watermark_path

GCS_DATASET = "raw_bernco_manual"
WATERMARK_PATH = transform_watermark_path(GCS_DATASET, "bernco_manual")


@dataclass
class BerncoManualTransformResult:
    bundles: list[CanonicalBundle]
    max_load_id: float | None


@asset(
    name="canonical_bundles_bernco_manual",
    group_name="bernco_manual",
    description="CanonicalBundles produced by BerncoManualAdapter from GCS raw parquet.",
    compute_kind="python",
    deps=["raw_bernco_manual_readings"],
)
def canonical_bundles_bernco_manual(context: AssetExecutionContext) -> BerncoManualTransformResult:
    return BerncoManualTransformResult(bundles=[], max_load_id=0)
