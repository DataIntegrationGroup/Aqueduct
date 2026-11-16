"""
Dagster asset: canonical_bundles_ebid — not implemented yet. Deferred to a
follow-up ticket; see docs/sources/ebid.md Open Questions.

Upstream: raw_ebid_readings. Downstream: frost_load_ebid.
"""

from dataclasses import dataclass

from dagster import AssetExecutionContext, asset

from aqueduct_dagster.canonical.canonical_model import CanonicalBundle
from aqueduct_dagster.shared.gcs import transform_watermark_path
from aqueduct_dagster.sources.ebid.adapter import EbidAdapter  # noqa: F401

GCS_DATASET = "raw_ebid"
WATERMARK_PATH = transform_watermark_path(GCS_DATASET, "ebid")


@dataclass
class EbidTransformResult:
    bundles: list[CanonicalBundle]
    max_load_id: float | None


@asset(
    name="canonical_bundles_ebid",
    group_name="ebid",
    description="CanonicalBundles produced by EbidAdapter from GCS raw parquet.",
    compute_kind="python",
    deps=["raw_ebid_readings"],
)
def canonical_bundles_ebid(context: AssetExecutionContext) -> EbidTransformResult:
    # TODO: implement — follow cabq/transform.py's pattern; see
    # docs/sources/ebid.md Open Questions for the remaining team decisions.
    return EbidTransformResult(bundles=[], max_load_id=None)
