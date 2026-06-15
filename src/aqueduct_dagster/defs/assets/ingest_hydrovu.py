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

import logging

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from aqueduct_dagster.pipeline.hydrovu_dlt_pipeline import build_pipeline, hydrovu_source

logger = logging.getLogger(__name__)


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
    load_info = pipeline.run(hydrovu_source(), loader_file_format="parquet")

    context.log.info("HydroVu dlt load complete: %s", load_info)

    return MaterializeResult(
        metadata={
            "pipeline_name": MetadataValue.text(pipeline.pipeline_name),
            "dataset_name": MetadataValue.text(pipeline.dataset_name),
            "load_info": MetadataValue.text(str(load_info)),
        }
    )
