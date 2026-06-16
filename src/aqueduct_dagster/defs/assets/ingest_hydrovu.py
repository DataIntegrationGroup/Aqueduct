"""
defs/assets/ingest_hydrovu.py

Dagster asset: raw_hydrovu_readings
  Runs the HydroVu dlt source which writes two resources to GCS:

  hydrovu_locations  (replace)  gs://<bucket>/raw_pvacd/hydrovu_locations/
    Full location list on every run — one row per location.

  hydrovu_readings   (append, incremental)  gs://<bucket>/raw_pvacd/hydrovu_readings/
    New readings since the last cursor value — one row per (location, parameter, reading).
    Location metadata is omitted; join to hydrovu_locations on location_id at transform time.

This is the FIRST asset in the HydroVu pipeline. No upstream dependencies.
Downstream: transform_hydrovu (reads both GCS folders)
"""

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from aqueduct_dagster.defs.dagster_logging import forward_python_logs_to_dagster
from aqueduct_dagster.pipeline.hydrovu_dlt_pipeline import build_pipeline, hydrovu_source


@asset(
    name="raw_hydrovu_readings",
    group_name="hydrovu",
    description="Raw HydroVu readings landed in GCS via dlt.",
    compute_kind="dlt",
)
def raw_hydrovu_readings(context: AssetExecutionContext) -> MaterializeResult:
    """
    Runs the dlt pipeline to incrementally fetch HydroVu readings and
    write them as parquet to the GCS raw zone.

    dlt handles:
      - API authentication
      - Incremental cursor (only fetches new data since last run)
      - Parquet serialisation
      - GCS write
      - Cursor state persistence (stored in GCS next to the data)

    On first run: fetches from initial_start_date (set in dlt config).
    On subsequent runs: fetches only records newer than the last cursor value.
    """
    pipeline = build_pipeline()
    context.log.info(
        "Starting HydroVu dlt extract (pipeline=%s, dataset=%s)",
        pipeline.pipeline_name,
        pipeline.dataset_name,
    )
    with forward_python_logs_to_dagster(context, "aqueduct_dagster.pipeline", "dlt"):
        load_info = pipeline.run(hydrovu_source(), loader_file_format="parquet")

    context.log.info("HydroVu dlt load complete: %s", load_info)

    return MaterializeResult(
        metadata={
            "pipeline_name": MetadataValue.text(pipeline.pipeline_name),
            "dataset_name": MetadataValue.text(pipeline.dataset_name),
            "load_info": MetadataValue.text(str(load_info)),
        }
    )
