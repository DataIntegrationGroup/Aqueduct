"""
shared/gcs.py

Shared GCS helpers for all source transform and load assets.
Source-agnostic — no knowledge of HydroVu, CABQ, or any specific dataset.
"""

import json
import logging
import os
import re
from collections.abc import Callable

import gcsfs
import pyarrow.parquet as pq
import toml

logger = logging.getLogger(__name__)


def _gcs_bucket_url() -> str:
    config_path = os.path.join(os.getcwd(), ".dlt", "config.toml")
    return toml.load(config_path)["destination"]["filesystem"]["bucket_url"]


def _gcs_filesystem(project: str = "") -> gcsfs.GCSFileSystem:
    if project:
        return gcsfs.GCSFileSystem(project=project, token="google_default")
    return gcsfs.GCSFileSystem(token="google_default")


def read_transform_watermark(
    fs: gcsfs.GCSFileSystem, bucket: str, watermark_path: str
) -> float | None:
    """Returns the last processed load_id, or None if no watermark exists yet."""
    wm_path = f"{bucket}/{watermark_path}"
    try:
        with fs.open(wm_path) as f:
            return json.load(f).get("last_load_id")
    except FileNotFoundError:
        return None


def write_transform_watermark(
    fs: gcsfs.GCSFileSystem, bucket: str, watermark_path: str, load_id: float
) -> None:
    wm_path = f"{bucket}/{watermark_path}"
    with fs.open(wm_path, "w") as f:
        json.dump({"last_load_id": load_id}, f)
    logger.info("Transform watermark updated: last_load_id=%s", load_id)


def commit_watermark(watermark_path: str, max_load_id: float) -> None:
    """Write the transform watermark. Called by the load step after FROST confirms success."""
    bucket_url = _gcs_bucket_url()
    fs = _gcs_filesystem()
    write_transform_watermark(fs, bucket_url.replace("gs://", ""), watermark_path, max_load_id)


def _load_id_from_filename(path: str) -> float | None:
    """
    Extracts the dlt load_id from a parquet filename dlt itself writes.
    Expected format: .../year={YYYY}/month={MM}/day={DD}/{load_id}.{file_id}.parquet
    e.g. raw_pvacd/hydrovu_readings/year=2024/month=06/day=18/1781192390.555875.0.parquet → 1781192390.555875
    """
    name = path.split("/")[-1]
    m = re.match(r"^(\d+\.\d+)\.", name)
    return float(m.group(1)) if m else None


def read_new_parquet_rows(
    bucket: str,
    glob_suffix: str,
    since_load_id: float | None,
    fs: gcsfs.GCSFileSystem,
    row_filter: Callable[[dict], bool] | None = None,
) -> tuple[list[dict], float | None]:
    """
    Reads parquet files matching {bucket}/{glob_suffix} with load_id > since_load_id,
    keeping only rows where row_filter(row) is True (all rows if row_filter is None).

    Shared by every source's transform asset for incremental reads — see
    hydrovu/transform.py for the reference usage.

    Returns (rows, max_load_id_seen_this_run) — max_load_id is None if no new files.
    """
    pattern = f"{bucket}/{glob_suffix}"
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

    rows: list[dict] = []
    max_load_id = since_load_id or 0.0
    for load_id, f in new_files:
        with fs.open(f) as fh:
            table = pq.read_table(fh)
            df = table.to_pydict()
            n = len(next(iter(df.values()))) if df else 0
            for i in range(n):
                row = {k: df[k][i] for k in df}
                if row_filter is None or row_filter(row):
                    rows.append(row)
        max_load_id = max(max_load_id, load_id)

    logger.info("Read %d row(s) from %d new parquet file(s)", len(rows), len(new_files))
    return rows, max_load_id
