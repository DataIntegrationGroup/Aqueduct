"""
Dagster asset: canonical_bundles_ebwpc_ckan
Reads new ckan_readings parquet since the last watermark, keeps each well file's newest snapshot,
transforms it into the WDI shape with EBWPC's shim (joined to the current ckan_locations and
ckan_resources), and runs EbwpcCkanAdapter to produce CanonicalBundles.

Reads incrementally by the GCS load_id watermark rather than re-reading every
snapshot ever landed. Ingest re-lands a well file in full only when that file
changed, so what's past the watermark is the complete current contents of each
changed well and nothing for unchanged ones.

ckan_locations and ckan_resources are tiny replace tables, read in full every run.
Caveat: a well with no matching location is dropped and counted (sites_without_location),
and isn't revisited until its file changes again.

Upstream: raw_ebwpc_ckan_readings. Downstream: frost_load_ebwpc_ckan.
"""

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
from aqueduct_dagster.sources.ckan_transform_common import (
    group_by_site,
    latest_snapshots,
    read_ckan_table,
    transform_metadata,
)
from aqueduct_dagster.sources.ebwpc_ckan.adapter import (
    EbwpcCkanAdapter,
    ebwpc_locations,
    ebwpc_measurements,
)

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
    description="CanonicalBundles produced by EbwpcCkanAdapter from GCS raw parquet.",
    compute_kind="python",
    deps=["raw_ebwpc_ckan_readings"],
)
def canonical_bundles_ebwpc_ckan(
    context: AssetExecutionContext,
) -> EbwpcCkanTransformResult:
    """Does NOT write the watermark. That happens in frost_load_ebwpc_ckan
    after FROST confirms success, so a failure there leaves it unadvanced and
    the next run retries the same snapshots."""
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
        "ebwpc_ckan",
        bucket,
        f"{GCS_DATASET}/ckan_readings/**/*.parquet",
        since_load_id,
        fs,
    )

    if not rows:
        context.log.info("No new well snapshots — returning empty result (watermark unchanged)")
        context.add_output_metadata(
            transform_metadata(
                files_skipped_bad_name=files_skipped_bad_name,
                since_load_id=since_load_id,
                max_load_id=max_load_id,
            )
        )
        return EbwpcCkanTransformResult(bundles=[], max_load_id=max_load_id)

    with forward_python_logs_to_dagster(
        context,
        "aqueduct_dagster.sources.ebwpc_ckan",
        "aqueduct_dagster.sources.ckan_transform_common",
        "aqueduct_dagster.canonical",
    ):
        snapshot_rows, rows_superseded = latest_snapshots(rows)
        locations = ebwpc_locations(
            read_ckan_table(bucket, GCS_DATASET, "ckan_locations", fs),
            read_ckan_table(bucket, GCS_DATASET, "ckan_resources", fs),
        )
        measurements, rows_dropped = ebwpc_measurements(snapshot_rows)
        records, sites_without_location = group_by_site(measurements, locations)
        context.log.info(
            "Mapped %d row(s) into %d measurement(s) across %d located well(s)",
            len(snapshot_rows),
            len(measurements),
            len(records),
        )

        adapter = EbwpcCkanAdapter(records)
        bundles = list(adapter.run())
    context.log.info("Produced %d CanonicalBundles", len(bundles))
    log_if_adapter_failed(adapter, context.log)

    context.add_output_metadata(
        transform_metadata(
            files_skipped_bad_name=files_skipped_bad_name,
            since_load_id=since_load_id,
            max_load_id=max_load_id,
            rows_read=len(rows),
            rows_superseded=rows_superseded,
            rows_dropped=rows_dropped,
            measurements_mapped=len(measurements),
            sites_without_location=sites_without_location,
            observations_dropped=adapter.dropped,
            bundles=bundles,
            adapter_failures=adapter.failure_count,
        )
    )
    return EbwpcCkanTransformResult(bundles=bundles, max_load_id=max_load_id)
