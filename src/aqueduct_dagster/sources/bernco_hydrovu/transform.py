"""
sources/bernco_hydrovu/transform.py

Dagster asset: canonical_bundles_bernco_hydrovu — NOT YET IMPLEMENTED.

Scaffolding only. The asset exists so the BernCo entry in SOURCE_REGISTRY resolves.
"""

from dataclasses import dataclass

from dagster import AssetExecutionContext, asset

from aqueduct_dagster.canonical.canonical_model import CanonicalBundle
from aqueduct_dagster.shared.gcs import transform_watermark_path

GCS_DATASET = "raw_bernco_hydrovu"
DTW_PARAMETER_ID = "4"
WATERMARK_PATH = transform_watermark_path(GCS_DATASET, "bernco_hydrovu")


@dataclass
class BerncoHydroVuTransformResult:
    """Carries CanonicalBundles and the GCS load_id watermark to the load step.

    max_load_id is None when there were no new parquet files this run.
    The load step writes the watermark only after FROST confirms success,
    so a FROST failure leaves max_load_id unwritten and the next run retries.
    """

    bundles: list[CanonicalBundle]
    max_load_id: float | None


@asset(
    name="canonical_bundles_bernco_hydrovu",
    group_name="bernco_hydrovu",
    description="CanonicalBundles produced from BernCo HydroVu raw parquet. Not yet implemented.",
    compute_kind="python",
    deps=["raw_bernco_hydrovu_readings"],
)
def canonical_bundles_bernco_hydrovu(
    context: AssetExecutionContext,
) -> BerncoHydroVuTransformResult:
    """Placeholder — see the module docstring for what this has to do."""
    raise NotImplementedError(
        "BernCo HydroVu transform is not implemented yet. "
        "raw_bernco_hydrovu_readings can be materialized on its own; the "
        "bernco_hydrovu_schedule should stay stopped until this lands."
    )
