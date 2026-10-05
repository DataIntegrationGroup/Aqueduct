"""
Dagster asset: canonical_bundles_pvacd_hydrovu — reads new hydrovu_readings
parquet since the last watermark, filters to DTW rows, joins to the latest
hydrovu_locations, and runs PvacdHydroVuAdapter to produce CanonicalBundles.

Upstream: raw_pvacd_hydrovu_readings. Downstream: frost_load_pvacd_hydrovu.
"""

import logging
from dataclasses import dataclass

from dagster import AssetExecutionContext, asset

from aqueduct_dagster.canonical.base_adapter import log_if_adapter_failed
from aqueduct_dagster.canonical.canonical_model import CanonicalBundle
from aqueduct_dagster.defs.dagster_logging import (
    forward_python_logs_to_dagster,
    read_new_parquet_rows_for_asset,
)
from aqueduct_dagster.shared.gcs import (
    _gcs_bucket_url,
    _gcs_filesystem,
    read_transform_watermark,
    transform_watermark_path,
)
from aqueduct_dagster.sources.hydrovu_transform_common import (
    DTW_PARAMETER_ID,
    group_readings_by_location,
    read_locations_from_gcs,
    transform_metadata,
)
from aqueduct_dagster.sources.pvacd_hydrovu.adapter import PvacdHydroVuAdapter


@dataclass
class HydroVuTransformResult:
    """Carries CanonicalBundles and the GCS load_id watermark to the load step.
    max_load_id is None when there were no new parquet files; the load step
    writes the watermark only after FROST confirms success."""

    bundles: list[CanonicalBundle]
    max_load_id: float | None


logger = logging.getLogger(__name__)

GCS_DATASET = "raw_pvacd_hydrovu"
WATERMARK_PATH = transform_watermark_path(GCS_DATASET, "pvacd_hydrovu")


@asset(
    name="canonical_bundles_pvacd_hydrovu",
    group_name="pvacd_hydrovu",
    description="CanonicalBundles produced by PvacdHydroVuAdapter from GCS raw parquet.",
    compute_kind="python",
    deps=["raw_pvacd_hydrovu_readings"],
)
def canonical_bundles_pvacd_hydrovu(
    context: AssetExecutionContext,
) -> HydroVuTransformResult:
    """Does NOT write the watermark — that happens in frost_load_pvacd_hydrovu
    after FROST confirms success, so a failure there leaves it unadvanced and
    the next run retries the same data."""
    bucket_url = _gcs_bucket_url()
    bucket = bucket_url.replace("gs://", "")

    fs = _gcs_filesystem()

    since_load_id = read_transform_watermark(fs, bucket, WATERMARK_PATH)
    context.log.info(
        "Transform watermark: last_load_id=%s (%s)",
        since_load_id,
        "first run — reading all files" if since_load_id is None else "incremental",
    )

    rows, max_load_id, files_skipped_bad_name = read_new_parquet_rows_for_asset(
        context,
        "pvacd_hydrovu",
        bucket,
        f"{GCS_DATASET}/hydrovu_readings/**/*.parquet",
        since_load_id,
        fs,
        row_filter=lambda row: row["parameter_id"] == DTW_PARAMETER_ID,
    )

    if not rows:
        context.log.info("No new DTW rows — returning empty result (watermark unchanged)")
        context.add_output_metadata(
            transform_metadata(
                dtw_rows_read=0,
                locations_grouped=0,
                bundles_produced=0,
                adapter_failures=0,
                files_skipped_bad_name=files_skipped_bad_name,
                since_load_id=since_load_id,
                max_load_id=max_load_id,
            )
        )
        return HydroVuTransformResult(bundles=[], max_load_id=max_load_id)

    locations = read_locations_from_gcs(bucket_url, GCS_DATASET, fs)
    records = group_readings_by_location(rows, locations)
    context.log.info("Grouped %d new DTW rows into %d location records", len(rows), len(records))

    adapter = PvacdHydroVuAdapter(records)
    with forward_python_logs_to_dagster(
        context,
        "aqueduct_dagster.sources.pvacd_hydrovu",
        "aqueduct_dagster.sources.hydrovu_transform_common",
        "aqueduct_dagster.canonical",
    ):
        bundles = list(adapter.run())
    context.log.info("Produced %d CanonicalBundles", len(bundles))
    log_if_adapter_failed(adapter, context.log)

    context.add_output_metadata(
        transform_metadata(
            dtw_rows_read=len(rows),
            locations_grouped=len(records),
            bundles_produced=len(bundles),
            adapter_failures=adapter.failure_count,
            files_skipped_bad_name=files_skipped_bad_name,
            since_load_id=since_load_id,
            max_load_id=max_load_id,
        )
    )
    return HydroVuTransformResult(bundles=bundles, max_load_id=max_load_id)
