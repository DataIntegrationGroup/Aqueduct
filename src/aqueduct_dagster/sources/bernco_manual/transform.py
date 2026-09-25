from dataclasses import dataclass

from dagster import AssetExecutionContext, MetadataValue, asset

from aqueduct_dagster.canonical import CanonicalBundle
from aqueduct_dagster.canonical.base_adapter import log_if_adapter_failed
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
from aqueduct_dagster.sources.bernco_manual.adapter import BerncoManualAdapter

GCS_DATASET = "raw_bernco_manual"
WATERMARK_PATH = transform_watermark_path(GCS_DATASET, "bernco_manual")


@dataclass
class BerncoManualTransformResult:
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
                "alternate_id": row["alternate_id"],
                "readings": [],
            }
            if groups[loc_id]["location_name"] is None:
                groups[loc_id]["location_name"] = loc_id
        groups[loc_id]["readings"].append(
            {"timestamp": row["timestamp"] / 1000, "value": row["value"]}
        )
    return list(groups.values())


@asset(
    name="canonical_bundles_bernco_manual",
    group_name="bernco_manual",
    description="CanonicalBundles produced by BerncoManualAdapter from GCS raw parquet.",
    compute_kind="python",
    deps=["raw_bernco_manual_readings"],
)
def canonical_bundles_bernco_manual(context: AssetExecutionContext) -> BerncoManualTransformResult:
    bucket = _gcs_bucket_url().replace("gs://", "")
    fs = _gcs_filesystem()
    since_load_id = read_transform_watermark(fs, bucket, WATERMARK_PATH)
    context.log.info(
        "Transform watermark: last_load_id=%s (%s)",
        since_load_id,
        "first run — reading all files" if since_load_id is None else "incremental",
    )
    rows, max_load_id, files_skipped_bad_name = read_new_parquet_rows_for_asset(
        context,
        "bernco_manual",
        bucket,
        f"{GCS_DATASET}/bernco_manual_readings/**/*.parquet",
        since_load_id,
        fs,
    )
    if not rows:
        context.log.info("No new rows — returning empty result (watermark unchanged)")
        context.add_output_metadata(
            {
                "rows_read": MetadataValue.int(0),
                "bundles_produced": MetadataValue.int(0),
                "adapter_failures": MetadataValue.int(0),
                "files_skipped_bad_name": MetadataValue.int(files_skipped_bad_name),
                "watermark_before": MetadataValue.text(str(since_load_id)),
                "watermark_after": MetadataValue.text(str(max_load_id)),
            }
        )
        return BerncoManualTransformResult(bundles=[], max_load_id=max_load_id)
    records = _group_rows_by_location(rows)
    context.log.info("Grouped %d new rows into %d location records", len(rows), len(records))
    adapter = BerncoManualAdapter(records)
    with forward_python_logs_to_dagster(
        context, "aqueduct_dagster.sources.bernco_manual", "aqueduct_dagster.canonical"
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
            "files_skipped_bad_name": MetadataValue.int(files_skipped_bad_name),
            "watermark_before": MetadataValue.text(str(since_load_id)),
            "watermark_after": MetadataValue.text(str(max_load_id)),
        }
    )
    return BerncoManualTransformResult(bundles=bundles, max_load_id=max_load_id)
