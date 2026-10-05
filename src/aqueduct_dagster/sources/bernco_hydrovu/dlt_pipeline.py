"""
dlt pipeline for Bernalillo County's HydroVu tenant. Two resources from
bernco_hydrovu_source(): hydrovu_locations (replace, full list every run) and
hydrovu_readings (append, per-location cursor; location metadata omitted,
join on location_id at transform time).
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
    """Reads config from [sources.bernco_hydrovu] — the @dlt.source name= must
    match the config section key. Builds one client and fetches the location
    list once, shared by both resources.

    location_ids comes from .dlt/config.toml; _stats is populated with
    extraction counts after pipeline.run()."""
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
    """Yields one record per location (location_row() shape); full replace
    every run, so HydroVu renames/removals show up immediately. Every location
    is written here, including ones the readings allowlist skips — this is
    the reference table, so knowing a location exists is the point of it."""
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
    """One flat record per (location, parameter, reading); location metadata
    NOT embedded — join on location_id. Per-location cursor in
    dlt.current.resource_state() advances only after a successful fetch.

    Fetch loop, record shape, and stats live in hydrovu_common's
    iter_location_readings(); this just owns the cursor state and the
    client's lifetime, closed in a finally block."""
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
