"""
pipeline/cabq_dlt_pipeline.py

dlt pipeline for CABQ raw ingestion.

Follows the same pattern as hydrovu_dlt_pipeline.py.
  - @dlt.source: defines the CABQ source
  - @dlt.resource: incremental cursor on timestamp field
  - build_pipeline(): filesystem destination → GCS under raw/cabq/

Add CABQ config block to .dlt/config.toml when wiring up:
  [cabq]
  api_base_url       = "https://cabq-api-url.com"   # TODO
  initial_start_date = "2026-01-01"                 # TODO
"""

from __future__ import annotations

import logging

import dlt

logger = logging.getLogger(__name__)


@dlt.source(name="cabq")
def cabq_source():
    # TODO: read credentials from dlt.secrets["cabq"]
    # TODO: return cabq_readings resource
    pass


@dlt.resource(
    name="cabq_readings",
    write_disposition="append",
    primary_key="reading_id",
)
def cabq_readings(
    updated_at: dlt.sources.incremental[str] = dlt.sources.incremental(
        "timestamp",
        initial_value="2026-01-01",  # TODO: move to dlt config
    ),
):
    """
    Yields one flat record per reading per location.
    Incremental cursor tracks timestamp field.

    On first run: fetches from initial_value.
    On subsequent runs: fetches only records newer than last cursor value.
    """
    # TODO: authenticate against CABQ API
    # TODO: fetch locations
    # TODO: fetch readings per location using updated_at.last_value as start
    # TODO: yield one flat record per reading
    pass


def build_pipeline(bucket_name: str) -> dlt.Pipeline:
    """
    Build and return a configured dlt pipeline writing to GCS.
    Writes parquet to gs://<bucket_name>/raw/cabq/cabq_readings/
    """
    return dlt.pipeline(
        pipeline_name="cabq",
        destination="filesystem",
        dataset_name="raw/cabq",
    )
