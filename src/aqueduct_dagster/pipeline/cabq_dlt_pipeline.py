"""
pipeline/cabq_dlt_pipeline.py

dlt pipeline for CABQ raw ingestion.

Follows the same pattern as hydrovu_dlt_pipeline.py.
  - @dlt.source: reads config from dlt.config under [cabq]
  - @dlt.resource: per-location incremental cursor via dlt.current.resource_state()  - build_pipeline(): filesystem destination → GCS under raw_cabq/
  - run_pipeline(): convenience entry point (mirrors hydrovu_dlt_pipeline.run_pipeline)

Add CABQ config block to .dlt/config.toml when wiring up:
  [cabq]
  api_base_url       = "https://..."   # CABQ CKAN base URL
  initial_start_date = "2026-05-01"    # match HydroVu start date
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

import dlt

logger = logging.getLogger(__name__)


@dlt.source(name="cabq")
def cabq_source(
    api_base_url: str = dlt.config.value,
    initial_start_date: str = dlt.config.value,
) -> Any:
    # TODO: parse initial_start_date to a start timestamp (same pattern as hydrovu_source)
    # TODO: return cabq_readings resource
    raise NotImplementedError("cabq_source is not implemented yet")


@dlt.resource(
    name="cabq_readings",
    write_disposition="append",
    primary_key="reading_id",
)
def cabq_readings(
    api_base_url: str,
    start_ts: int,
    # dlt detects the incremental cursor via this default — idiomatic, so B008 is expected.
    updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(  # noqa: B008
        "timestamp",
        initial_value=0,
    ),
) -> Iterator[dict]:
    """
    Yields one flat record per reading per location.
    Per-location incremental cursor via dlt.current.resource_state() — same pattern as
    hydrovu_readings. Each station has its own cursor; a failed station retries from the
    same point next run rather than being skipped permanently.

    On first run: fetches from start_ts (derived from initial_start_date in config).
    On subsequent runs: fetches only records newer than each station's cursor.

    Record shape (to define when implementing):
      reading_id   — unique key e.g. "{location_id}_{timestamp}"
      location_id  — CABQ station identifier
      timestamp    — Unix epoch seconds
      value        — float measurement
      # add other fields as needed
    """
    # TODO: fetch CABQ stations/locations from CKAN API
    # TODO: use dlt.current.resource_state().setdefault("location_cursors", {}) for per-station cursors
    # TODO: fetch readings per station using max(cursors.get(str(station_id), 0), start_ts) as start
    # TODO: advance cursor per station only after successful fetch: cursors[str(station_id)] = max_ts
    # TODO: yield one flat record per reading (no location metadata — join at transform time)
    raise NotImplementedError("cabq_readings is not implemented yet")


def build_pipeline() -> dlt.Pipeline:
    """
    Returns a configured dlt pipeline writing parquet to GCS.
    Bucket is read from config.toml [destination.filesystem] bucket_url.
    Writes to gs://<bucket>/raw_cabq/cabq_readings/year={YYYY}/month={MM}/day={DD}/

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
