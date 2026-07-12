"""
sources/cabq/ingest.py

Dagster asset: raw_cabq_readings
  - Runs the CABQ dlt pipeline
  - Fetches from CABQ CKAN API (incrementally, cursor-based)
  - Writes raw parquet to GCS under gs://<bucket>/raw_cabq/cabq_readings/year={YYYY}/month={MM}/day={DD}/
  - dlt handles incremental cursor, parquet serialisation, GCS write,
    and cursor state persistence alongside the data in GCS

This is the FIRST asset in the CABQ pipeline. No upstream dependencies.
Downstream: transform_cabq (reads from GCS)
"""

import logging

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from aqueduct_dagster.sources.cabq.dlt_pipeline import build_pipeline, cabq_source

logger = logging.getLogger(__name__)


@asset(
    name="raw_cabq_readings",
    group_name="cabq",
    description="Raw CABQ readings landed in GCS via dlt.",
    compute_kind="dlt",
)
def raw_cabq_readings(context: AssetExecutionContext) -> MaterializeResult:
    """
    Runs the dlt pipeline to incrementally fetch CABQ readings and
    write them as parquet to the GCS raw zone.

    dlt handles:
      - Incremental cursor (only fetches new data since last run)
      - Parquet serialisation
      - GCS write
      - Cursor state persistence (stored in GCS next to the data)

    On first run: fetches from initial_start_date (set in dlt config).
    On subsequent runs: fetches only records newer than the last cursor value.
    """
    pipeline = build_pipeline()
    load_info = pipeline.run(cabq_source(), loader_file_format="parquet")

    context.log.info("CABQ dlt load complete: %s", load_info)

    return MaterializeResult(
        metadata={
            "pipeline_name": MetadataValue.text(pipeline.pipeline_name),
            "dataset_name": MetadataValue.text(pipeline.dataset_name),
            "load_info": MetadataValue.text(str(load_info)),
        }
    )
