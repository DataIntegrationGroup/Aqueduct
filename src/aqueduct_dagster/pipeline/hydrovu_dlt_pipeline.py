"""
pipeline/hydrovu_dlt_pipeline.py

dlt pipeline for HydroVu raw ingestion.

Follows the same pattern as cabq_dlt_pipeline.py (and all future sources).
  - @dlt.source:    defines the HydroVu source, reads creds from dlt.secrets
  - @dlt.resource:  incremental cursor on timestamp field, yields one flat
                    record per parameter per reading per location
  - build_pipeline(): filesystem destination → GCS under raw/pvacd/

What dlt does here:
  - Calls the HydroVu API and fetches readings per location
  - Handles incremental loading via dlt.sources.incremental (cursor = timestamp)
  - Writes raw parquet to GCS (filesystem destination) under:
      gs://<bucket>/raw/pvacd/hydrovu_readings/
  - Stores cursor state (last fetched timestamp) alongside the data in GCS

This module is NOT a Dagster asset — it is called by defs/assets/ingest_hydrovu.py

dlt destination = filesystem (GCS)
  → GCS is the final destination for the raw data ingested by this pipeline.
    → dlt writes parquet files to GCS and manages the incremental cursor state.

"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterator

import dlt

logger = logging.getLogger(__name__)


@dlt.source(name="hydrovu")
def hydrovu_source():
    # TODO: read client_id, client_secret from dlt.secrets["hydrovu"]
    # TODO: read api_base_url, token_url, initial_start_date from dlt.config
    # TODO: return hydrovu_readings resource
    pass


@dlt.resource(
    name="hydrovu_readings",
    write_disposition="append",
    primary_key="reading_id",
)
def hydrovu_readings(
    updated_at: dlt.sources.incremental[str] = dlt.sources.incremental(
        "timestamp",        # field dlt tracks as the cursor
        initial_value="2026-01-01",  # TODO: move to dlt config
    ),
) -> Iterator[dict]:
    # TODO: authenticate against HydroVu API
    # TODO: fetch locations
    # TODO: fetch readings per location using updated_at.last_value as startTime
    # TODO: yield one flat record per parameter per reading
    pass


def build_pipeline(bucket_name: str) -> dlt.Pipeline:
    """
    Returns a configured dlt pipeline writing parquet to GCS.
    Writes to gs://<bucket_name>/raw/pvacd/hydrovu_readings/
    """
    return dlt.pipeline(
        pipeline_name="pvacd_hydrovu",
        destination="filesystem",   
        dataset_name="raw/pvacd",
    )