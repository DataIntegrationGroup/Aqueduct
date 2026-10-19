"""
Dagster asset: raw_ebwpc_ckan_readings. Runs the EBWPC CKAN dlt source,
writing two GCS resources: ckan_locations (replace, every run) and
ckan_readings (append, a full snapshot of each well file that changed).

An unchanged dataset is the normal weekly case: nothing is downloaded but
package_show and the tiny locations file, and the run is green.

First asset in the pipeline. Downstream: canonical_bundles_ebwpc_ckan.
"""

import httpx
from dagster import AssetExecutionContext, Failure, MaterializeResult, MetadataValue, asset

from aqueduct_dagster.defs.dagster_logging import forward_python_logs_to_dagster
from aqueduct_dagster.sources.ckan_common import CkanApiError
from aqueduct_dagster.sources.ebwpc_ckan.dlt_pipeline import build_pipeline, ebwpc_ckan_source


@asset(
    name="raw_ebwpc_ckan_readings",
    group_name="ebwpc_ckan",
    description="Raw EBWPC well files from the NM Water Data Catalog (CKAN), landed in GCS via dlt.",
    compute_kind="dlt",
)
def raw_ebwpc_ckan_readings(context: AssetExecutionContext) -> MaterializeResult:
    """Lands every EBWPC well file whose fingerprint changed since the last run
    — the first run lands them all, which is the backfill."""
    pipeline = build_pipeline()
    context.log.info(
        "Starting EBWPC CKAN dlt extract (pipeline=%s, dataset=%s)",
        pipeline.pipeline_name,
        pipeline.dataset_name,
    )
    stats: dict = {}
    # ckan_common carries the fetch logs, so it needs forwarding alongside this package.
    with forward_python_logs_to_dagster(
        context,
        "aqueduct_dagster.sources.ebwpc_ckan",
        "aqueduct_dagster.sources.ckan_common",
        "dlt",
    ):
        try:
            source = ebwpc_ckan_source(_stats=stats)
        except (httpx.HTTPError, CkanApiError) as exc:
            raise Failure(
                description=(
                    f"EBWPC CKAN package_show failed: {exc}. A 403 usually means Cloudflare "
                    "rejected the x-cf-bypass header — check the Secret Manager value."
                )
            ) from exc
        load_info = pipeline.run(source, loader_file_format="parquet")

    context.log.info("EBWPC CKAN dlt load complete: %s", load_info)

    locations_error: str | None = stats.get("locations_error")
    locations_rows: int = stats.get("locations_rows", 0)
    failed: list[str] = stats.get("failed_resources", [])
    removed: list[str] = stats.get("resources_removed", [])
    # The locations file counts as one more station: total failure means
    # nothing at all could be checked or landed.
    errored = stats.get("resources_errored", 0) + (1 if locations_error else 0)
    ok = (
        stats.get("resources_landed", 0)
        + stats.get("resources_unchanged", 0)
        + stats.get("resources_same_content", 0)
        + (1 if locations_rows else 0)
    )

    if errored > 0:
        context.log.warning(
            "EBWPC CKAN ingest: %d resource(s) errored and will retry next run: %s%s",
            errored,
            failed,
            f" (locations: {locations_error})" if locations_error else "",
        )
    if removed:
        context.log.warning(
            "EBWPC CKAN ingest: resource(s) removed from the dataset: %s — check whether "
            "a replacement was published as a new resource",
            removed,
        )

    if errored > 0 and ok == 0:
        raise Failure(
            description=f"All EBWPC CKAN resources failed ({errored} errored, 0 succeeded)",
            metadata={
                "resources_errored": MetadataValue.int(stats.get("resources_errored", 0)),
                "failed_resources": MetadataValue.json(failed),
                "locations_error": MetadataValue.text(str(locations_error)),
            },
        )

    return MaterializeResult(
        metadata={
            "pipeline_name": MetadataValue.text(pipeline.pipeline_name),
            "dataset_name": MetadataValue.text(pipeline.dataset_name),
            "rows_yielded": MetadataValue.int(stats.get("rows_yielded", 0)),
            "resources_selected": MetadataValue.int(stats.get("resources_selected", 0)),
            "resources_landed": MetadataValue.int(stats.get("resources_landed", 0)),
            "resources_unchanged": MetadataValue.int(stats.get("resources_unchanged", 0)),
            "resources_same_content": MetadataValue.int(stats.get("resources_same_content", 0)),
            "resources_errored": MetadataValue.int(stats.get("resources_errored", 0)),
            "failed_resources": MetadataValue.json(failed),
            "resources_skipped": MetadataValue.json(stats.get("resources_skipped", {})),
            "resources_added": MetadataValue.json(stats.get("resources_added", [])),
            "resources_removed": MetadataValue.json(removed),
            "locations_rows": MetadataValue.int(locations_rows),
            "locations_error": MetadataValue.text(str(locations_error or "")),
            "load_info": MetadataValue.text(str(load_info)),
        }
    )
