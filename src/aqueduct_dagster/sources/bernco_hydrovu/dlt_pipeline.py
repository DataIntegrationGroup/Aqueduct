"""
sources/bernco_hydrovu/dlt_pipeline.py

dlt pipeline for Bernalillo County's HydroVu tenant.

Two resources returned from bernco_hydrovu_source():

  hydrovu_locations  (write_disposition="replace")
    Fetches GET /locations/list on every run and fully replaces the parquet.
    One row per location: id, name, description, latitude, longitude.
    Written to: gs://<bucket>/raw_bernco_hydrovu/hydrovu_locations/year={YYYY}/month={MM}/day={DD}/

  hydrovu_readings   (write_disposition="append", per-location incremental cursor)
    Fetches readings per location since that location's last successful fetch.
    Each location has its own cursor in dlt.current.resource_state(). A failed location
    retries from the same point next run rather than being skipped permanently.
    One row per (location, parameter, reading). Location metadata is NOT
    embedded; join to hydrovu_locations on location_id at transform time.
    Written to: gs://<bucket>/raw_bernco_hydrovu/hydrovu_readings/year={YYYY}/month={MM}/day={DD}/
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import dlt
import httpx

from aqueduct_dagster.shared.pipeline import build_source_pipeline
from aqueduct_dagster.sources.hydrovu_common import (
    build_hydrovu_client,
    fetch_locations,
    iter_location_readings,
    location_row,
)

logger = logging.getLogger(__name__)


@dlt.source(name="bernco_hydrovu")
def bernco_hydrovu_source(
    client_id: str = "",
    client_secret: str = "",
    gcp_secret: str = dlt.config.value,
    api_base_url: str = dlt.config.value,
    token_url: str = dlt.config.value,
    initial_start_date: str = dlt.config.value,
    location_ids: list[int] = dlt.config.value,  # noqa: B008
    _stats: dict | None = None,
) -> Any:
    """
    Reads config from dlt.config under [sources.bernco_hydrovu]. The name= argument
    on the decorator is what binds the two, so it has to stay equal to the source key.

    Creates a single authenticated httpx.Client shared by both resources, so the
    token is fetched once and both requests and auth-retries go through one client
    for the full run. Fetches the location list once and passes it to both resources
    to avoid a redundant second API call.

    location_ids: allowlist of HydroVu location integer IDs to fetch.
      Read from [sources.bernco_hydrovu] location_ids in .dlt/config.toml.
      Add or remove IDs there without any code change.

    _stats: optional mutable dict populated with extraction counts after pipeline.run().
      keys: rows_yielded, locations_fetched, locations_skipped, locations_no_data,
            locations_errored, failed_location_ids
    """
    # Credentials are resolved inside build_hydrovu_client() → resolve_hydrovu_credentials(),
    # so this source does not fetch them itself.
    start_ts = int(
        datetime.strptime(initial_start_date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
    )
    client = build_hydrovu_client(client_id, client_secret, gcp_secret, api_base_url, token_url)
    try:
        locations = fetch_locations(client)
    except Exception:
        # hydrovu_readings (the only other user of this client) never gets
        # constructed if this raises, so it must close the client itself.
        client.close()
        raise
    return (
        hydrovu_locations(locations=locations),
        hydrovu_readings(
            client=client,
            start_ts=start_ts,
            locations=locations,
            location_ids=location_ids,
            _stats=_stats if _stats is not None else {},
        ),
    )


@dlt.resource(
    name="hydrovu_locations",
    write_disposition="replace",
)
def hydrovu_locations(locations: list[dict]) -> Iterator[dict]:
    """
    Yields one record per location from the pre-fetched location list.
    write_disposition="replace" ensures the parquet is fully refreshed on
    every run, so renames or removals in HydroVu are reflected immediately.

    Every location is written here, including the ones the readings allowlist skips —
    this is the reference table, and knowing a location exists is the point of it.

    Record shape is location_row()'s: id, name, description, latitude, longitude.
    """
    logger.info("Extracting hydrovu_locations (full replace)")
    for location in locations:
        yield location_row(location)


@dlt.resource(
    name="hydrovu_readings",
    write_disposition="append",
    primary_key="reading_id",
)
def hydrovu_readings(
    client: httpx.Client,
    start_ts: int,
    locations: list[dict],
    location_ids: list[int],
    _stats: dict | None = None,
) -> Iterator[dict]:
    """
    Yields one flat record per (location, parameter, reading).
    Location metadata is NOT embedded — join to hydrovu_locations on location_id.

    Incremental: each location has its own cursor stored in dlt.current.resource_state()
    under "location_cursors". A location's cursor only advances after a successful fetch,
    so a failed location retries from the same point on the next run.
    On first run (or new location), falls back to start_ts from config.
    dlt additionally deduplicates on primary_key=reading_id.

    The fetch loop, record shape and stats accounting are iter_location_readings()
    in hydrovu_common. What stays here is where the cursors live (dlt resource state)
    and the lifetime of the client.

    Closes the shared client in a finally block once this generator is done.
    """
    try:
        cursors: dict[str, int] = dlt.current.resource_state().setdefault("location_cursors", {})
        yield from iter_location_readings(
            client=client,
            start_ts=start_ts,
            locations=locations,
            location_ids=location_ids,
            cursors=cursors,
            stats=_stats if _stats is not None else {},
        )
    finally:
        client.close()


def build_pipeline() -> dlt.Pipeline:
    return build_source_pipeline("bernco_hydrovu", "raw_bernco_hydrovu")


def run_pipeline() -> None:
    """Convenience entry point: builds and runs the pipeline with parquet output."""
    pipeline = build_pipeline()
    load_info = pipeline.run(bernco_hydrovu_source(), loader_file_format="parquet")
    logger.info("Load complete: %s", load_info)
