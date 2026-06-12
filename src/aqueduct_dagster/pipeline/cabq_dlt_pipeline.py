"""
pipeline/cabq_dlt_pipeline.py

dlt pipeline for CABQ raw ingestion.

Follows the same pattern as hydrovu_dlt_pipeline.py.
  - @dlt.source: reads credentials + config from dlt.secrets/dlt.config under [cabq]
  - @dlt.resource: incremental cursor on timestamp field
  - build_pipeline(): filesystem destination → GCS under raw_cabq/
  - run_pipeline(): convenience entry point (mirrors hydrovu_dlt_pipeline.run_pipeline)

Add CABQ config block to .dlt/config.toml when wiring up:
  [cabq]
  api_base_url       = "https://..."   # CABQ CKAN base URL
  initial_start_date = "2026-05-01"    # match HydroVu start date

Add CABQ secrets block to .dlt/secrets.toml if auth is required:
  [cabq]
  api_key = "..."
"""

from __future__ import annotations

import logging

import dlt

logger = logging.getLogger(__name__)


@dlt.source(name="cabq")
def cabq_source(
    api_base_url: str = dlt.config.value,
    initial_start_date: str = dlt.config.value,
):
    # TODO: parse initial_start_date to a start timestamp (same pattern as hydrovu_source)
    # TODO: return cabq_readings resource
    pass


@dlt.resource(
    name="cabq_readings",
    write_disposition="append",
    primary_key="reading_id",
)
def cabq_readings(
    api_base_url: str,
    start_ts: int,
    updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
        "timestamp",
        initial_value=0,
    ),
):
    """
    Yields one flat record per reading per location.
    Incremental cursor tracks timestamp field (Unix epoch int — same as hydrovu_readings).

    On first run: fetches from start_ts (derived from initial_start_date in config).
    On subsequent runs: fetches only records newer than updated_at.last_value.

    Record shape (to define when implementing):
      reading_id   — unique key e.g. "{location_id}_{timestamp}"
      location_id  — CABQ station identifier
      timestamp    — Unix epoch seconds (dlt cursor field)
      value        — float measurement
      # add other fields as needed
    """
    # TODO: fetch CABQ stations/locations from CKAN API
    # TODO: fetch readings per location using max(updated_at.last_value, start_ts) as start
    # TODO: yield one flat record per reading (no location metadata — join at transform time)
    pass


def build_pipeline() -> dlt.Pipeline:
    """
    Returns a configured dlt pipeline writing parquet to GCS.
    Bucket is read from config.toml [destination.filesystem] bucket_url.
    Writes to gs://<bucket>/raw_cabq/cabq_readings/

    Always call pipeline.run(..., loader_file_format="parquet") — same as HydroVu.
    """
    return dlt.pipeline(
        pipeline_name="pvacd_cabq",
        destination="filesystem",
        dataset_name="raw_cabq",
    )


def run_pipeline() -> None:
    """Convenience entry point: builds and runs the pipeline with parquet output."""
    pipeline = build_pipeline()
    load_info = pipeline.run(cabq_source(), loader_file_format="parquet")
    logger.info("Load complete: %s", load_info)
