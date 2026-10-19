"""
Dagster asset: canonical_bundles_ebwpc_ckan.

TODO: Scaffolding only.
"""

from dataclasses import dataclass

from dagster import AssetExecutionContext, asset

from aqueduct_dagster.canonical.canonical_model import CanonicalBundle
from aqueduct_dagster.shared.gcs import transform_watermark_path

GCS_DATASET = "raw_ebwpc_ckan"
WATERMARK_PATH = transform_watermark_path(GCS_DATASET, "ebwpc_ckan")


@dataclass
class EbwpcCkanTransformResult:
    """Carries CanonicalBundles and the GCS load_id watermark to the load step.

    max_load_id is None when there were no new parquet files this run.
    The load step writes the watermark only after FROST confirms success,
    so a FROST failure leaves max_load_id unwritten and the next run retries.
    """

    bundles: list[CanonicalBundle]
    max_load_id: float | None


@asset(
    name="canonical_bundles_ebwpc_ckan",
    group_name="ebwpc_ckan",
    description="CanonicalBundles produced from EBWPC CKAN raw parquet. Not yet implemented.",
    compute_kind="python",
    deps=["raw_ebwpc_ckan_readings"],
)
def canonical_bundles_ebwpc_ckan(
    context: AssetExecutionContext,
) -> EbwpcCkanTransformResult:
    """Placeholder — see the module docstring for what this has to do."""
    raise NotImplementedError(
        "EBWPC CKAN transform is not implemented yet. "
        "raw_ebwpc_ckan_readings can be materialized on its own; the "
        "ebwpc_ckan_schedule should stay stopped until this lands."
    )
