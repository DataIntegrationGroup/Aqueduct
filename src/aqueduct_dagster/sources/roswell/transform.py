from dataclasses import dataclass

from dagster import AssetExecutionContext, asset

from aqueduct_dagster.canonical import CanonicalBundle
from aqueduct_dagster.shared.gcs import transform_watermark_path

GCS_DATASET = "raw_roswell"
WATERMARK_PATH = transform_watermark_path(GCS_DATASET, "roswell")


@dataclass
class RoswellTransformResult:
    bundles: list[CanonicalBundle]
    max_load_id: float | None


@asset(
    name="canonical_bundles_roswell",
    group_name="roswell",
    description="CanonicalBundles produced by RoswellAdapter from GCS raw parquet.",
    compute_kind="pythin",
    deps=["raw_roswell_readings"],
)
def canonical_bundles_roswell(context: AssetExecutionContext) -> RoswellTransformResult:
    return RoswellTransformResult(bundles=[], max_load_id=0)
