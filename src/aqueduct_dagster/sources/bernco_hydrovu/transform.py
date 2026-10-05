"""
Dagster asset: canonical_bundles_bernco_hydrovu — reads new hydrovu_readings
parquet, filters to DTW rows, joins to hydrovu_locations, drops sentinel-floor
readings (sentinel_floor()), and runs BerncoHydroVuAdapter.

Incremental, not full re-reads: 29 locations report as often as once a
minute, so the raw zone grows without bound.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from dagster import AssetExecutionContext, asset

from aqueduct_dagster.canonical.base_adapter import log_if_adapter_failed
from aqueduct_dagster.canonical.canonical_model import CanonicalBundle
from aqueduct_dagster.defs.dagster_logging import (
    forward_python_logs_to_dagster,
    read_new_parquet_rows_for_asset,
)
from aqueduct_dagster.shared.config import load_config
from aqueduct_dagster.shared.gcs import (
    _gcs_bucket_url,
    _gcs_filesystem,
    read_transform_watermark,
    transform_watermark_path,
)
from aqueduct_dagster.sources.bernco_hydrovu.adapter import BerncoHydroVuAdapter
from aqueduct_dagster.sources.hydrovu_transform_common import (
    DTW_PARAMETER_ID,
    group_readings_by_location,
    read_locations_from_gcs,
    transform_metadata,
)

logger = logging.getLogger(__name__)

GCS_DATASET = "raw_bernco_hydrovu"
WATERMARK_PATH = transform_watermark_path(GCS_DATASET, "bernco_hydrovu")


@dataclass
class BerncoHydroVuTransformResult:
    """Carries CanonicalBundles + GCS load_id watermark to the load step.
    max_load_id is None if no new files; watermark is written only after
    FROST confirms success."""

    bundles: list[CanonicalBundle]
    max_load_id: float | None


def sentinel_floor() -> int:
    """Unix second below which a reading is a bad device clock, not history.
    Read from initial_start_date (not hardcoded) so the floor and the dlt
    cursor can't disagree. A backfill fetching older history must pass its
    own floor instead."""
    raw = load_config()["sources"]["bernco_hydrovu"]["initial_start_date"]
    return int(datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=UTC).timestamp())


@asset(
    name="canonical_bundles_bernco_hydrovu",
    group_name="bernco_hydrovu",
    description="CanonicalBundles produced by BerncoHydroVuAdapter from GCS raw parquet.",
    compute_kind="python",
    deps=["raw_bernco_hydrovu_readings"],
)
def canonical_bundles_bernco_hydrovu(
    context: AssetExecutionContext,
) -> BerncoHydroVuTransformResult:
    """Does NOT write the watermark — frost_load_bernco_hydrovu does, after
    FROST success, so a failure there just retries next run."""
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
        "bernco_hydrovu",
        bucket,
        f"{GCS_DATASET}/hydrovu_readings/**/*.parquet",
        since_load_id,
        fs,
        row_filter=lambda row: row["parameter_id"] == DTW_PARAMETER_ID,
    )

    if not rows:
        context.log.info("No new DTW rows - returning empty result (watermark unchanged)")
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
        return BerncoHydroVuTransformResult(bundles=[], max_load_id=max_load_id)

    locations = read_locations_from_gcs(bucket_url, GCS_DATASET, fs)
    records = group_readings_by_location(rows, locations)
    context.log.info("Grouped %d new DTW rows into %d location records", len(rows), len(records))

    floor = sentinel_floor()
    context.log.info("Sentinel floor: dropping DTW readings before Unix second %d", floor)

    adapter = BerncoHydroVuAdapter(records, min_timestamp=floor)
    with forward_python_logs_to_dagster(
        context,
        "aqueduct_dagster.sources.bernco_hydrovu",
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
    return BerncoHydroVuTransformResult(bundles=bundles, max_load_id=max_load_id)
