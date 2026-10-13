import logging

from dagster import AssetExecutionContext, Failure, MaterializeResult, MetadataValue, asset

from aqueduct_dagster.defs.dagster_logging import forward_python_logs_to_dagster
from aqueduct_dagster.sources.roswell.dlt_pipeline import build_pipeline, roswell_source

logger = logging.getLogger(__name__)


@asset(
    name="raw_roswell_readings",
    group_name="roswell",
    description="Raw roswell readings landed in GCS via dlt.",
    compute_kind="dlt",
)
def raw_roswell_readings(context: AssetExecutionContext) -> MaterializeResult:
    pipeline = build_pipeline()
    stats: dict = {}
    with forward_python_logs_to_dagster(context, "aqueduct_dagster.sources.roswell", "dlt"):
        load_info = pipeline.run(roswell_source(_stats=stats), loader_file_format="parquet")
    context.log.info("City of Roswell dlt load complete: %s", load_info)
    errored: int = stats.get("locations_errored", 0)
    fetched: int = stats.get("locations_fetched", 0)
    failed_ids: list[str] = stats.get("failed_location_ids", [])
    if errored > 0:
        context.log.warning(
            "City of Roswell ingest: %d location(s) errored and will retry next run: %s",
            errored,
            failed_ids,
        )
    if errored > 0 and fetched == 0:
        raise Failure(
            description=f"All active City of Roswell locations failed ({errored} errored, 0 fetched)",
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
        },
    )
