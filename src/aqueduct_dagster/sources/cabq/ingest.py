"""
Dagster asset: raw_cabq_readings — runs the CABQ dlt pipeline, fetching from
the CABQ ArcGIS FeatureServer incrementally and writing raw parquet to GCS.

First asset in the pipeline. Downstream: canonical_bundles_cabq.
"""

import logging

from dagster import AssetExecutionContext, Failure, MaterializeResult, MetadataValue, asset

from aqueduct_dagster.defs.dagster_logging import forward_python_logs_to_dagster
from aqueduct_dagster.sources.cabq.dlt_pipeline import build_pipeline, cabq_source

logger = logging.getLogger(__name__)


@asset(
    name="raw_cabq_readings",
    group_name="cabq",
    description="Raw CABQ readings landed in GCS via dlt.",
    compute_kind="dlt",
)
def raw_cabq_readings(context: AssetExecutionContext) -> MaterializeResult:
    """Incrementally fetches CABQ readings into GCS — first run from
    initial_start_date, later runs from the last cursor value."""
    pipeline = build_pipeline()
    stats: dict = {}
    with forward_python_logs_to_dagster(context, "aqueduct_dagster.sources.cabq", "dlt"):
        load_info = pipeline.run(cabq_source(_stats=stats), loader_file_format="parquet")

    context.log.info("CABQ dlt load complete: %s", load_info)

    errored: int = stats.get("locations_errored", 0)
    fetched: int = stats.get("locations_fetched", 0)
    failed_ids: list[int] = stats.get("failed_location_ids", [])

    if errored > 0:
        context.log.warning(
            "CABQ ingest: %d location(s) errored and will retry next run: %s",
            errored,
            failed_ids,
        )

    if errored > 0 and fetched == 0:
        raise Failure(
            description=f"All active CABQ locations failed ({errored} errored, 0 fetched)",
            metadata={
                "locations_errored": MetadataValue.int(errored),
                "locations_fetched": MetadataValue.int(fetched),
                "failed_location_ids": MetadataValue.json(failed_ids),
            },
        )

    return MaterializeResult(
        metadata={
            "pipeline_name": MetadataValue.text(pipeline.pipeline_name),
            "dataset_name": MetadataValue.text(pipeline.dataset_name),
            "locations_fetched": MetadataValue.int(fetched),
            "locations_no_data": MetadataValue.int(stats.get("locations_no_data", 0)),
            "locations_errored": MetadataValue.int(errored),
            "failed_location_ids": MetadataValue.text(str(failed_ids)),
            "load_info": MetadataValue.text(str(load_info)),
        }
    )
