"""
sources/cabq/transform.py

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

from dagster import AssetExecutionContext, MetadataValue, asset

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
from aqueduct_dagster.sources.cabq.adapter import CabqAdapter

logger = logging.getLogger(__name__)

GCS_DATASET = "raw_cabq"
WATERMARK_PATH = transform_watermark_path(GCS_DATASET, "cabq")


@dataclass
class CabqTransformResult:
    """Carries CanonicalBundles and the GCS load_id watermark to the load step.

    max_load_id is None when there were no new parquet files this run.
    The load step writes the watermark only after FROST confirms success.
    """

    bundles: list[CanonicalBundle]
    max_load_id: float | None


def _group_rows_by_location(rows: list[dict]) -> list[dict]:
    groups: dict[str, dict] = {}
    for row in rows:
        loc_id = row["location_id"]
        if loc_id not in groups:
            groups[loc_id] = {
                "location_id": loc_id,
                "location_name": row["location_name"],
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "readings": [],
            }
        groups[loc_id]["readings"].append(
            {"timestamp": row["timestamp"] / 1000, "value": float(row["value"])}
        )
    return list(groups.values())


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
    """
    bucket = _gcs_bucket_url().replace("gs://", "")
    fs = _gcs_filesystem()
    since_load_id = read_transform_watermark(fs, bucket, WATERMARK_PATH)
    context.log.info(
        "Transform watermark: last_load_id=%s (%s)",
        since_load_id,
        "first run — reading all files" if since_load_id is None else "incremental",
    )
    rows, max_load_id = read_new_parquet_rows(
        bucket, f"{GCS_DATASET}/cabq_readings/**/*.parquet", since_load_id, fs
    )
    if not rows:
        context.log.info("No new rows — returning empty result (watermark unchanged)")
        context.add_output_metadata(
            {
                "rows_read": MetadataValue.int(0),
                "bundles_produced": MetadataValue.int(0),
                "adapter_failures": MetadataValue.int(0),
                "watermark_before": MetadataValue.text(str(since_load_id)),
                "watermark_after": MetadataValue.text(str(max_load_id)),
            }
        )
        return CabqTransformResult(bundles=[], max_load_id=max_load_id)
    records = _group_rows_by_location(rows)
    context.log.info("Grouped %d new rows into %d location records", len(rows), len(records))
    adapter = CabqAdapter(records)
    with forward_python_logs_to_dagster(
        context, "aqueduct_dagster.sources.cabq", "aqueduct_dagster.canonical"
    ):
        bundles = list(adapter.run())
    context.log.info("Produced %d CanonicalBundles", len(bundles))
    log_if_adapter_failed(adapter, context.log)
    context.add_output_metadata(
        {
            "rows_read": MetadataValue.int(len(rows)),
            "locations_grouped": MetadataValue.int(len(records)),
            "bundles_produced": MetadataValue.int(len(bundles)),
            "adapter_failures": MetadataValue.int(adapter.failure_count),
            "watermark_before": MetadataValue.text(str(since_load_id)),
            "watermark_after": MetadataValue.text(str(max_load_id)),
        }
    )
    return CabqTransformResult(bundles=bundles, max_load_id=max_load_id)
