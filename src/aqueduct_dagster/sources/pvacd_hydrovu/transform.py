"""
sources/pvacd_hydrovu/transform.py

Dagster asset: canonical_bundles_pvacd_hydrovu
  - Reads only NEW hydrovu_readings parquet from GCS since the last successful run
  - Always reads the latest hydrovu_locations parquet (replace resource — one file)
  - Filters readings to DTW rows only (parameter_id="4")
  - Joins readings to locations on location_id to restore name/lat/lon metadata
  - Runs PvacdHydroVuAdapter to produce CanonicalBundles (one per DTW location)
  - Returns bundles downstream to frost_load_pvacd_hydrovu

Incremental reads (readings only):
  A watermark file (raw_pvacd_hydrovu/_pvacd_hydrovu_transform_watermark.json) in GCS tracks
  the highest dlt load_id processed so far. On each run only readings parquet files
  with a newer load_id are read. The watermark is updated after a successful run.

  load_id is the float Unix timestamp dlt embeds in every parquet filename:
    raw_pvacd_hydrovu/hydrovu_readings/year={YYYY}/month={MM}/day={DD}/{load_id}.{file_id}.parquet
  e.g. raw_pvacd_hydrovu/hydrovu_readings/year=2024/month=06/day=18/1781192390.555875.0.parquet

Locations parquet (hydrovu_locations/) uses write_disposition="replace" so it is
always a single up-to-date file — read fresh on every run, no watermark needed.

Upstream:  raw_pvacd_hydrovu_readings
Downstream: frost_load_pvacd_hydrovu
"""

import logging
from dataclasses import dataclass

from dagster import AssetExecutionContext, asset

from aqueduct_dagster.canonical.base_adapter import log_if_adapter_failed
from aqueduct_dagster.canonical.canonical_model import CanonicalBundle
from aqueduct_dagster.defs.dagster_logging import forward_python_logs_to_dagster
from aqueduct_dagster.shared.gcs import (
    _gcs_bucket_url,
    _gcs_filesystem,
    read_new_parquet_rows,
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

    max_load_id is None when there were no new parquet files this run.
    The load step writes the watermark only after FROST confirms success,
    so a FROST failure leaves max_load_id unwritten and the next run retries.
    """

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
    """
    Reads only new HydroVu parquet from GCS (since last run), filters to DTW
    readings, groups by location, and runs PvacdHydroVuAdapter to produce CanonicalBundles.

    Does NOT write the watermark — that happens in frost_load_pvacd_hydrovu after FROST
    confirms success, so a FROST failure leaves the watermark unadvanced and the
    next run retries the same data.
    """
    bucket_url = _gcs_bucket_url()
    bucket = bucket_url.replace("gs://", "")

    fs = _gcs_filesystem()

    since_load_id = read_transform_watermark(fs, bucket, WATERMARK_PATH)
    context.log.info(
        "Transform watermark: last_load_id=%s (%s)",
        since_load_id,
        "first run — reading all files" if since_load_id is None else "incremental",
    )

    rows, max_load_id = read_new_parquet_rows(
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
            since_load_id=since_load_id,
            max_load_id=max_load_id,
        )
    )
    return HydroVuTransformResult(bundles=bundles, max_load_id=max_load_id)
