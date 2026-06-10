"""
defs/assets/ingest_cabq.py

Dagster asset: raw_cabq_readings
  - Runs the CABQ dlt pipeline
  - Fetches from CABQ CKAN API (incrementally, cursor-based)
  - Writes raw parquet to GCS under gs://<bucket>/raw/cabq/cabq_readings/
  - dlt handles incremental cursor, parquet serialisation, GCS write,
    and cursor state persistence alongside the data in GCS

This is the FIRST asset in the CABQ pipeline. No upstream dependencies.
Downstream: transform_cabq (reads from GCS)
"""


import logging

from dagster import AssetExecutionContext, asset

logger = logging.getLogger(__name__)


@asset(
    name="raw_cabq_readings",
    group_name="cabq",
    description="Raw CABQ readings landed in GCS via dlt.",
    compute_kind="dlt",
)
def raw_cabq_readings(context: AssetExecutionContext) -> None:
    """
    Runs the dlt pipeline to incrementally fetch CABQ readings and
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
    # TODO: import build_pipeline and cabq_source from pipeline/cabq_dlt_pipeline.py
    # TODO: run dlt pipeline and log load_info
    pass
