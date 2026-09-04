"""
sources/bernco_hydrovu/ingest.py

Dagster asset: raw_bernco_hydrovu_readings
  Runs the BernCo HydroVu dlt source which writes two resources to GCS:

  hydrovu_locations  (replace)  gs://<bucket>/raw_bernco_hydrovu/hydrovu_locations/year={YYYY}/month={MM}/day={DD}/
    Full location list on every run, one row per location.

  hydrovu_readings   (append, per-location incremental)  gs://<bucket>/raw_bernco_hydrovu/hydrovu_readings/year={YYYY}/month={MM}/day={DD}/
    New readings since each location's last successful fetch. One row per (location, parameter, reading).
    Location metadata is omitted; join to hydrovu_locations on location_id at transform time.

This is the FIRST asset in the BernCo HydroVu pipeline. No upstream dependencies.
Downstream: canonical_bundles_bernco_hydrovu (reads both GCS folders)
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
    """
    Runs the dlt pipeline to incrementally fetch BernCo HydroVu readings and
    write them as parquet to the GCS raw zone.

    dlt handles:
      - API authentication
      - Incremental cursor (only fetches new data since last run)
      - Parquet serialisation
      - GCS write
      - Cursor state persistence (stored in GCS next to the data)

    On first run: fetches from initial_start_date (set in dlt config).
    On subsequent runs: fetches only records newer than each location's per-location cursor.

    Failure policy? one bad station must not block the rest
      - Some locations errored, some succeeded: warn, still materialize. Their cursors
        did not advance, so the next run picks up exactly where they left off.
      - Every allowlisted location errored: raise Failure. Nothing landed, and a silent
        green run would hide an expired credential or a dead API.
    """
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
