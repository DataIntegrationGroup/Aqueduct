"""
defs/assets/hydrovu/transform.py

Dagster asset: canonical_bundles_hydrovu
  - Reads only NEW hydrovu_readings parquet from GCS since the last successful run
  - Always reads the latest hydrovu_locations parquet (replace resource — one file)
  - Filters readings to DTW rows only (parameter_id="4")
  - Joins readings to locations on location_id to restore name/lat/lon metadata
  - Groups joined rows by location_id into one record per location
  - Runs HydroVuAdapter to produce CanonicalBundles (one per DTW location)
  - Returns bundles downstream to frost_load_hydrovu

Incremental reads (readings only):
  A watermark file (raw_pvacd/_hydrovu_transform_watermark.json) in GCS tracks
  the highest dlt load_id processed so far. On each run only readings parquet files
  with a newer load_id are read. The watermark is updated after a successful run.

  load_id is the float Unix timestamp dlt embeds in every parquet filename:
    raw_pvacd/hydrovu_readings/year={YYYY}/month={MM}/day={DD}/{load_id}.{file_id}.parquet
  e.g. raw_pvacd/hydrovu_readings/year=2024/month=06/day=18/1781192390.555875.0.parquet

Locations parquet (hydrovu_locations/) uses write_disposition="replace" so it is
always a single up-to-date file — read fresh on every run, no watermark needed.

Upstream:  raw_hydrovu_readings
Downstream: frost_load_hydrovu
"""

import logging
import re
from dataclasses import dataclass

import gcsfs
import pyarrow.parquet as pq
from dagster import AssetExecutionContext, MetadataValue, asset

from aqueduct_dagster.canonical.canonical_model import CanonicalBundle
from aqueduct_dagster.defs.assets._gcs import (
    _gcs_bucket_url,
    _gcs_filesystem,
    read_transform_watermark,
)
from aqueduct_dagster.sources.hydrovu.adapter import HydroVuAdapter


@dataclass
class HydroVuTransformResult:
    """Carries CanonicalBundles and the GCS load_id watermark to the load step.

    max_load_id is None when there were no new parquet files this run.
    The load step writes the watermark only after FROST confirms success,
    so a FROST failure leaves max_load_id unwritten and the next run retries.
    """

    bundles: list[CanonicalBundle]
    max_load_id: float | None


logger = logging.getLogger(__name__)

GCS_DATASET = "raw_pvacd"
DTW_PARAMETER_ID = "4"
WATERMARK_PATH = f"{GCS_DATASET}/_hydrovu_transform_watermark.json"


def _load_id_from_filename(path: str) -> float | None:
    """
    Extracts the dlt load_id from a parquet filename.
    Expected format: .../year={YYYY}/month={MM}/day={DD}/{load_id}.{file_id}.parquet
    e.g. raw_pvacd/hydrovu_readings/year=2024/month=06/day=18/1781192390.555875.0.parquet → 1781192390.555875
    """
    name = path.split("/")[-1]
    m = re.match(r"^(\d+\.\d+)\.", name)
    return float(m.group(1)) if m else None


def _read_locations_from_gcs(bucket_url: str, fs: gcsfs.GCSFileSystem) -> dict[int, dict]:
    """
    Reads the hydrovu_locations parquet (write_disposition=replace → always one file).
    Returns a dict keyed by location_id for O(1) join with readings rows.
    """
    bucket = bucket_url.replace("gs://", "")
    pattern = f"{bucket}/{GCS_DATASET}/hydrovu_locations/**/*.parquet"
    files = fs.glob(pattern)
    if not files:
        raise FileNotFoundError(
            f"No locations parquet found at {pattern}. "
            "Ensure raw_hydrovu_readings has run at least once."
        )

    locations: dict[int, dict] = {}
    for f in files:
        with fs.open(f) as fh:
            table = pq.read_table(fh)
            df = table.to_pydict()
            for i in range(len(df["id"])):
                locations[df["id"][i]] = {
                    "name": df["name"][i],
                    "description": df["description"][i],
                    "latitude": df["latitude"][i],
                    "longitude": df["longitude"][i],
                }

    logger.info("Read %d locations from GCS", len(locations))
    return locations


def _read_dtw_rows_from_gcs(
    bucket_url: str,
    since_load_id: float | None,
    fs: gcsfs.GCSFileSystem,
) -> tuple[list[dict], float | None]:
    """
    Reads hydrovu_readings parquet files from GCS, returning only DTW rows.

    If since_load_id is set, files with load_id <= since_load_id are skipped.
    Returns (rows, max_load_id_seen_this_run) — max_load_id is None if no new files.
    """
    bucket = bucket_url.replace("gs://", "")
    pattern = f"{bucket}/{GCS_DATASET}/hydrovu_readings/**/*.parquet"
    all_files = fs.glob(pattern)

    new_files = []
    for f in all_files:
        load_id = _load_id_from_filename(f)
        if load_id is None:
            continue
        if since_load_id is not None and load_id <= since_load_id:
            continue
        new_files.append((load_id, f))

    if not new_files:
        logger.info("No new parquet files since load_id=%s — nothing to process", since_load_id)
        return [], None

    logger.info(
        "Reading %d new parquet file(s) (skipped %d already-processed)",
        len(new_files),
        len(all_files) - len(new_files),
    )

    rows = []
    max_load_id = since_load_id or 0.0
    for load_id, f in new_files:
        with fs.open(f) as fh:
            table = pq.read_table(fh)
            df = table.to_pydict()
            n = len(df["parameter_id"])
            for i in range(n):
                if df["parameter_id"][i] == DTW_PARAMETER_ID:
                    rows.append({k: df[k][i] for k in df})
        max_load_id = max(max_load_id, load_id)

    logger.info("Read %d DTW rows from %d new parquet file(s)", len(rows), len(new_files))
    return rows, max_load_id


def _group_by_location(rows: list[dict], locations: dict[int, dict]) -> list[dict]:
    """
    Groups flat readings rows into one record per location, joining location
    metadata (name, description, lat, lon) from the locations reference dict.
    """
    groups: dict[int, dict] = {}
    for row in rows:
        loc_id = row["location_id"]
        if loc_id not in groups:
            loc = locations.get(loc_id, {})
            groups[loc_id] = {
                "location_id": loc_id,
                "location_name": loc.get("name", ""),
                "location_description": loc.get("description", ""),
                "latitude": loc.get("latitude"),
                "longitude": loc.get("longitude"),
                "readings": [],
            }
        groups[loc_id]["readings"].append(
            {
                "parameter_id": row["parameter_id"],
                "unit_id": row["unit_id"],
                "timestamp": row["timestamp"],
                "value": row["value"],
            }
        )
    return list(groups.values())


@asset(
    name="canonical_bundles_hydrovu",
    group_name="hydrovu",
    description="CanonicalBundles produced by HydroVuAdapter from GCS raw parquet.",
    compute_kind="python",
    deps=["raw_hydrovu_readings"],
)
def canonical_bundles_hydrovu(
    context: AssetExecutionContext,
) -> HydroVuTransformResult:
    """
    Reads only new HydroVu parquet from GCS (since last run), filters to DTW
    readings, groups by location, and runs HydroVuAdapter to produce CanonicalBundles.

    Does NOT write the watermark — that happens in frost_load_hydrovu after FROST
    confirms success, so a FROST failure leaves the watermark unadvanced and the
    next run retries the same data.
    """
    bucket_url = _gcs_bucket_url()
    bucket = bucket_url.replace("gs://", "")

    fs = _gcs_filesystem()

    since_load_id = read_transform_watermark(fs, bucket, WATERMARK_PATH)
    context.log.info(
        "Transform watermark: last_load_id=%s (%s)",
        since_load_id,
        "first run — reading all files" if since_load_id is None else "incremental",
    )

    rows, max_load_id = _read_dtw_rows_from_gcs(bucket_url, since_load_id, fs)

    if not rows:
        context.log.info("No new DTW rows — returning empty result (watermark unchanged)")
        context.add_output_metadata(
            {
                "dtw_rows_read": MetadataValue.int(0),
                "bundles_produced": MetadataValue.int(0),
                "watermark_before": MetadataValue.text(str(since_load_id)),
                "watermark_after": MetadataValue.text(str(max_load_id)),
            }
        )
        return HydroVuTransformResult(bundles=[], max_load_id=max_load_id)

    locations = _read_locations_from_gcs(bucket_url, fs)
    records = _group_by_location(rows, locations)
    context.log.info("Grouped %d new DTW rows into %d location records", len(rows), len(records))

    adapter = HydroVuAdapter(records)
    bundles = list(adapter.run())
    context.log.info("Produced %d CanonicalBundles", len(bundles))

    context.add_output_metadata(
        {
            "dtw_rows_read": MetadataValue.int(len(rows)),
            "locations_grouped": MetadataValue.int(len(records)),
            "bundles_produced": MetadataValue.int(len(bundles)),
            "watermark_before": MetadataValue.text(str(since_load_id)),
            "watermark_after": MetadataValue.text(str(max_load_id)),
        }
    )
    return HydroVuTransformResult(bundles=bundles, max_load_id=max_load_id)
