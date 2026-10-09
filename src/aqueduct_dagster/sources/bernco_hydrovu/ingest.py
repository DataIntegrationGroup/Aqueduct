"""
Dagster asset: raw_bernco_hydrovu_readings — runs the BernCo HydroVu dlt source,
writing two GCS resources: hydrovu_locations (replace, full list every run)
and hydrovu_readings (append, per-location incremental; location metadata
omitted, join on location_id at transform time).

First asset in the pipeline. Downstream: canonical_bundles_bernco_hydrovu.
"""

from dagster import AssetExecutionContext, Failure, MaterializeResult, MetadataValue, asset

from aqueduct_dagster.defs.dagster_logging import forward_python_logs_to_dagster
from aqueduct_dagster.sources.bernco_hydrovu.dlt_pipeline import (
    bernco_hydrovu_source,
    build_pipeline,
)


@asset(
    name="raw_bernco_hydrovu_readings",
    group_name="bernco_hydrovu",
    description="Raw BernCo HydroVu readings landed in GCS via dlt.",
    compute_kind="dlt",
)
def raw_bernco_hydrovu_readings(context: AssetExecutionContext) -> MaterializeResult:
    """Incrementally fetches BernCo HydroVu readings into GCS — first run from
    initial_start_date, later runs from each location's own cursor.

    One bad station doesn't block the rest: some locations erroring just warns
    and still materializes (their cursors don't advance, so the next run
    retries them); every location erroring raises Failure instead, since a
    silent green run would hide an expired credential or dead API."""
    pipeline = build_pipeline()
    context.log.info(
        "Starting BernCo HydroVu dlt extract (pipeline=%s, dataset=%s)",
        pipeline.pipeline_name,
        pipeline.dataset_name,
    )
    stats: dict = {}
    # hydrovu_common carries the fetch logs, so it needs forwarding alongside this package.
    with forward_python_logs_to_dagster(
        context,
        "aqueduct_dagster.sources.bernco_hydrovu",
        "aqueduct_dagster.sources.hydrovu_common",
        "dlt",
    ):
        load_info = pipeline.run(bernco_hydrovu_source(_stats=stats), loader_file_format="parquet")

    context.log.info("BernCo HydroVu dlt load complete: %s", load_info)

    errored: int = stats.get("locations_errored", 0)
    fetched: int = stats.get("locations_fetched", 0)
    failed_ids: list[int] = stats.get("failed_location_ids", [])

    if errored > 0:
        context.log.warning(
            "BernCo HydroVu ingest: %d location(s) errored and will retry next run: %s",
            errored,
            failed_ids,
        )

    if errored > 0 and fetched == 0:
        raise Failure(
            description=f"All active BernCo HydroVu locations failed ({errored} errored, 0 fetched)",
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
            "rows_yielded": MetadataValue.int(stats.get("rows_yielded", 0)),
            "locations_fetched": MetadataValue.int(fetched),
            "locations_skipped_allowlist": MetadataValue.int(stats.get("locations_skipped", 0)),
            "locations_no_data": MetadataValue.int(stats.get("locations_no_data", 0)),
            "locations_errored": MetadataValue.int(errored),
            "failed_location_ids": MetadataValue.text(str(failed_ids)),
            "load_info": MetadataValue.text(str(load_info)),
        }
    )
