"""
dlt pipeline for EBWPC's wells on the NM Water Data Catalog (CKAN). Two
resources from ebwpc_ckan_source(): ckan_locations (replace, the locations
file every run) and ckan_readings (append, one snapshot per changed well
file, per-resource fingerprint cursor). Both share one client.
"""

from __future__ import annotations

import csv
import logging
from collections.abc import Iterator
from typing import Any

import dlt
import httpx

from aqueduct_dagster.shared.pipeline import build_source_pipeline
from aqueduct_dagster.sources.ckan_common import (
    build_ckan_client,
    download_resource,
    iter_resource_snapshots,
    package_show,
    resolve_cf_bypass,
    snapshot_rows,
    split_resources,
)

logger = logging.getLogger(__name__)


@dlt.source(name="ebwpc_ckan")
def ebwpc_ckan_source(
    cf_bypass: str = "",
    gcp_secret: str = dlt.config.value,
    api_base_url: str = dlt.config.value,
    dataset_id: str = dlt.config.value,
    locations_resource_name: str = dlt.config.value,
    _stats: dict | None = None,
) -> Any:
    """Calls package_show and downloads the locations file up front; the well
    files are downloaded lazily by ckan_readings. Raises if package_show
    fails.

    _stats is populated with extraction counts as the resources run."""
    stats = _stats if _stats is not None else {}

    # Raw cells must stay strings. dlt's default detection would type
    # ISO-looking values (resource_last_modified, another agency's ISO dates)
    # as timestamps, and a later non-ISO value would split the column.
    dlt.current.source_schema().remove_type_detection("iso_timestamp")

    client = build_ckan_client(api_base_url, resolve_cf_bypass(cf_bypass, gcp_secret))
    try:
        package = package_show(client, dataset_id)
        locations, wells, skipped = split_resources(
            package.get("resources", []), locations_resource_name
        )
        if skipped:
            logger.warning("Skipping %d resource(s): %s", len(skipped), skipped)
        stats["resources_skipped"] = skipped
        location_rows = _locations_snapshot(client, locations, locations_resource_name, stats)
    except Exception:
        # ckan_readings (which normally closes the client) is never constructed.
        client.close()
        raise

    readings = ckan_readings(client=client, resources=wells, _stats=stats)
    if not location_rows:
        return readings
    return ckan_locations(rows=location_rows), readings


def _locations_snapshot(
    client: httpx.Client, resource: dict | None, resource_name: str, stats: dict
) -> list[dict]:
    """Rows of the locations file, or [] on any failure. The caller then omits
    ckan_locations entirely: a replace with no rows could wipe the table's last
    good snapshot, and a locations failure must not block the readings."""
    rows: list[dict] = []
    error: str | None = None
    if resource is None:
        error = f"no CSV resource named {resource_name!r} in the dataset"
    else:
        body, error = download_resource(client, resource)
        if body is not None:
            try:
                rows = snapshot_rows(resource, body)
            except csv.Error as exc:
                error = f"unreadable CSV: {exc}"
            if not rows and error is None:
                error = "file has no data rows"

    if error is not None:
        logger.warning("Locations not landed (%s) — ckan_locations keeps its last snapshot", error)
        rows = []
    stats["locations_rows"] = len(rows)
    stats["locations_error"] = error
    return rows


@dlt.resource(name="ckan_locations", write_disposition="replace")
def ckan_locations(rows: list[dict]) -> Iterator[dict]:
    """Full replace every run (the file is under 1 KB), so a corrected
    coordinate or a new well shows up immediately."""
    logger.info("Extracting ckan_locations (full replace, %d rows)", len(rows))
    yield from rows


@dlt.resource(name="ckan_readings", write_disposition="append", primary_key="reading_id")
def ckan_readings(
    client: httpx.Client,
    resources: list[dict],
    _stats: dict | None = None,
) -> Iterator[dict]:
    """Per-resource (per-well) fingerprint cursor in dlt.current.resource_state(),
    advanced only after that resource's rows are all yielded."""
    try:
        fingerprints: dict[str, dict] = dlt.current.resource_state().setdefault(
            "resource_fingerprints", {}
        )
        yield from iter_resource_snapshots(
            client=client,
            resources=resources,
            fingerprints=fingerprints,
            stats=_stats if _stats is not None else {},
        )
    finally:
        client.close()


def build_pipeline() -> dlt.Pipeline:
    return build_source_pipeline("ebwpc_ckan", "raw_ebwpc_ckan")


def run_pipeline() -> None:
    pipeline = build_pipeline()
    load_info = pipeline.run(ebwpc_ckan_source(), loader_file_format="parquet")
    logger.info("Load complete: %s", load_info)
