"""
defs/assets/ingest_hydrovu.py

Dagster asset: raw_hydrovu_readings
  - Runs the HydroVu dlt pipeline
  - Fetches from HydroVu API (incrementally, cursor-based)
  - Writes raw parquet to GCS under gs://<bucket>/raw/pvacd/hydrovu_readings/
  - dlt handles incremental cursor, parquet serialisation, GCS write,
    and cursor state persistence alongside the data in GCS

This is the FIRST asset in the HydroVu pipeline. No upstream dependencies.
Downstream: transform_hydrovu (reads from GCS)
"""

from __future__ import annotations

import logging

from dagster import AssetExecutionContext, asset

logger = logging.getLogger(__name__)


@asset(
    name="raw_hydrovu_readings",
    group_name="hydrovu",
    description="Raw HydroVu readings landed in GCS via dlt.",
    compute_kind="dlt",
)
def raw_hydrovu_readings(context: AssetExecutionContext) -> None:
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
    # TODO: import build_pipeline and hydrovu_source from pipeline/hydrovu_dlt_pipeline.py
    # TODO: run dlt pipeline and log load_info
    pass
