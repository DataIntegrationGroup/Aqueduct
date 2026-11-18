"""
Dagster asset: raw_ebid_readings — runs the EBID dlt source, writing
ebid_sites (replace) and ebid_readings (append, per-site incremental).
"""

from dagster import AssetExecutionContext, Failure, MaterializeResult, MetadataValue, asset

from aqueduct_dagster.defs.dagster_logging import forward_python_logs_to_dagster
from aqueduct_dagster.sources.ebid.dlt_pipeline import build_pipeline, ebid_source


@asset(
    name="raw_ebid_readings",
    group_name="ebid",
    description="Raw EBID (OneRain Contrail) readings landed in GCS via dlt.",
    compute_kind="dlt",
)
def raw_ebid_readings(context: AssetExecutionContext) -> MaterializeResult:
    """Incrementally fetches EBID readings into GCS — first run from
    initial_start_date, later runs from each site's own cursor."""
    pipeline = build_pipeline()
    context.log.info(
        "Starting EBID dlt extract (pipeline=%s, dataset=%s)",
        pipeline.pipeline_name,
        pipeline.dataset_name,
    )
    stats: dict = {}
    with forward_python_logs_to_dagster(context, "aqueduct_dagster.sources.ebid", "dlt"):
        load_info = pipeline.run(ebid_source(_stats=stats), loader_file_format="parquet")

    context.log.info("EBID dlt load complete: %s", load_info)

    errored: int = stats.get("sites_errored", 0)
    fetched: int = stats.get("sites_fetched", 0)
    failed_ids: list[str] = stats.get("failed_site_ids", [])

    if errored > 0:
        context.log.warning(
            "EBID ingest: %d site(s) errored and will retry next run: %s",
            errored,
            failed_ids,
        )

    if errored > 0 and fetched == 0:
        raise Failure(
            description=f"All EBID sites failed ({errored} errored, 0 fetched)",
            metadata={
                "sites_errored": MetadataValue.int(errored),
                "sites_fetched": MetadataValue.int(fetched),
                "failed_site_ids": MetadataValue.json(failed_ids),
            },
        )

    return MaterializeResult(
        metadata={
            "pipeline_name": MetadataValue.text(pipeline.pipeline_name),
            "dataset_name": MetadataValue.text(pipeline.dataset_name),
            "rows_yielded": MetadataValue.int(stats.get("rows_yielded", 0)),
            "sites_fetched": MetadataValue.int(fetched),
            "sites_skipped_not_a_well": MetadataValue.int(stats.get("sites_skipped", 0)),
            "sites_errored": MetadataValue.int(errored),
            "failed_site_ids": MetadataValue.text(str(failed_ids)),
            "load_info": MetadataValue.text(str(load_info)),
        }
    )
