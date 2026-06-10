"""
defs/assets/transform_hydrovu.py

Dagster asset: canonical_bundles_hydrovu
  - Reads latest raw parquet from GCS (written by raw_hydrovu_readings)
  - Filters to DTW rows only (parameter_id="4")
  - Groups flat rows by location_id into one record per location
  - Runs HydroVuAdapter to produce CanonicalBundles (one per DTW location)
  - Returns bundles downstream to frost_load_hydrovu

Upstream:  raw_hydrovu_readings
Downstream: frost_load_hydrovu

DTW locations in current PVACD dataset (4 of 10):
  Bartlett Level Troll, Cottonwood Level Troll,
  Berrendo-Smith Level Troll, Transwestern Level Troll
"""

import json
import logging
import os

import gcsfs
import pyarrow.parquet as pq
import toml
from dagster import AssetExecutionContext, asset

from aqueduct_dagster.adapters.hydrovu_adapter import HydroVuAdapter
from aqueduct_dagster.canonical.canonical_model import CanonicalBundle

logger = logging.getLogger(__name__)

DTW_PARAMETER_ID = "4"


def _gcs_credentials() -> dict:
    """
    Resolve GCS service account credentials in priority order:
      1. GOOGLE_APPLICATION_CREDENTIALS env var → path to a service account JSON file
      2. .dlt/secrets.toml relative to CWD (works when running `dagster dev` from project root)
    In production, set GOOGLE_APPLICATION_CREDENTIALS to the mounted secret path.
    """
    creds_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_file and os.path.exists(creds_file):
        with open(creds_file) as f:
            return json.load(f)

    secrets_path = os.path.join(os.getcwd(), ".dlt", "secrets.toml")
    if not os.path.exists(secrets_path):
        raise FileNotFoundError(
            "GCS credentials not found. Set GOOGLE_APPLICATION_CREDENTIALS or "
            f"ensure .dlt/secrets.toml exists at {secrets_path}"
        )
    creds = toml.load(secrets_path)["destination"]["filesystem"]["credentials"]
    return {
        "type": "service_account",
        "project_id": creds["project_id"],
        "private_key": creds["private_key"].replace("\\n", "\n"),
        "client_email": creds["client_email"],
        "token_uri": "https://oauth2.googleapis.com/token",
    }


def _read_dtw_rows_from_gcs(bucket_url: str) -> list[dict]:
    """
    Reads all hydrovu_readings parquet files from GCS and returns
    only the DTW rows (parameter_id="4") as a list of dicts.
    """
    creds = _gcs_credentials()
    bucket = bucket_url.replace("gs://", "")
    fs = gcsfs.GCSFileSystem(project=creds["project_id"], token=creds)

    pattern = f"{bucket}/raw_pvacd/hydrovu_readings/*.parquet"
    files = fs.glob(pattern)
    if not files:
        logger.warning("No parquet files found at %s", pattern)
        return []

    rows = []
    for f in files:
        with fs.open(f) as fh:
            table = pq.read_table(fh)
            df = table.to_pydict()
            n = len(df["parameter_id"])
            for i in range(n):
                if df["parameter_id"][i] == DTW_PARAMETER_ID:
                    rows.append({k: df[k][i] for k in df})

    logger.info("Read %d DTW rows from %d parquet file(s)", len(rows), len(files))
    return rows


def _group_by_location(rows: list[dict]) -> list[dict]:
    """
    Groups flat parquet rows into one record per location.
    Each record contains location metadata + list of readings.
    """
    groups: dict[int, dict] = {}
    for row in rows:
        loc_id = row["location_id"]
        if loc_id not in groups:
            groups[loc_id] = {
                "location_id": row["location_id"],
                "location_name": row["location_name"],
                "location_description": row["location_description"],
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "readings": [],
            }
        groups[loc_id]["readings"].append({
            "parameter_id": row["parameter_id"],
            "unit_id": row["unit_id"],
            "timestamp": row["timestamp"],
            "value": row["value"],
        })
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
) -> list[CanonicalBundle]:
    """
    Reads raw HydroVu parquet from GCS, filters to DTW readings,
    groups by location, and runs HydroVuAdapter to produce CanonicalBundles.
    """
    bucket_url = "gs://aqueduct-poc-bravo-pvacd"

    rows = _read_dtw_rows_from_gcs(bucket_url)
    if not rows:
        context.log.warning("No DTW rows found in GCS — returning empty bundle list")
        return []

    records = _group_by_location(rows)
    context.log.info(
        "Grouped %d DTW rows into %d location records", len(rows), len(records)
    )

    adapter = HydroVuAdapter(records)
    bundles = list(adapter.run())

    context.log.info("Produced %d CanonicalBundles", len(bundles))
    return bundles
