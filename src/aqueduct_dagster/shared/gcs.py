"""
shared/gcs.py

Shared GCS helpers for all source transform and load assets.
Source-agnostic — no knowledge of HydroVu, CABQ, or any specific dataset.
"""

import json
import logging
import os
import re
import time
from collections.abc import Callable
from typing import Any

import gcsfs
import pyarrow.parquet as pq
import toml

logger = logging.getLogger(__name__)

_SAVE_RETRIES = 3
_SAVE_BACKOFF = (1.0, 2.0, 4.0)


def atomic_write_json_with_retry(fs: gcsfs.GCSFileSystem, path: str, data: dict, log: Any) -> None:
    """
    Writes `data` as JSON to `path` atomically (write to a .tmp object, then
    rename), retrying up to _SAVE_RETRIES times with exponential backoff on
    failure. The live file is never partially overwritten.

    Shared by FrostWatermarkStore (loader/watermark_store.py) and
    BackfillCheckpointStore (shared/backfill.py) — both need the exact same
    durable-write mechanism against GCS.

    `log` must expose .warning(msg, *args) / .error(msg, *args) — both
    Dagster's context.log and a stdlib logging.Logger satisfy this, so
    callers can pass either without adapting it.
    """
    tmp_path = f"{path}.tmp"
    last_exc: Exception | None = None
    for attempt in range(_SAVE_RETRIES):
        try:
            with fs.open(tmp_path, "w") as f:
                json.dump(data, f)
            fs.rename(tmp_path, path)
            return
        except Exception as exc:
            last_exc = exc
            if attempt < _SAVE_RETRIES - 1:
                delay = _SAVE_BACKOFF[attempt]
                log.warning(
                    "Write to %s failed (attempt %d/%d): %s — retrying in %.0fs",
                    path,
                    attempt + 1,
                    _SAVE_RETRIES,
                    exc,
                    delay,
                )
                time.sleep(delay)
    log.error(
        "Write to %s failed after %d attempts — not persisted: %s", path, _SAVE_RETRIES, last_exc
    )
    # mypy can't see that _SAVE_RETRIES >= 1 guarantees the loop above always
    # assigns last_exc at least once before this line is ever reached.
    raise last_exc  # type: ignore[misc]


def _gcs_bucket_url() -> str:
    """Resolve the GCS bucket URL for the current run.

    Prefers the GCS_BUCKET_URL env var, falling back to the committed
    [destination.filesystem] bucket_url in .dlt/config.toml.
    """
    env_url = os.environ.get("GCS_BUCKET_URL")
    if env_url:
        return env_url
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


def transform_watermark_path(dataset: str, source_name: str) -> str:
    """
    The one place that defines the transform-watermark filename convention.

    Both the read side (a source's transform.py, via read_transform_watermark)
    and the write side (defs/assets/load.py, via commit_watermark, driven by
    shared/source_registry.py's SOURCE_REGISTRY) must agree on this exact path —
    call this instead of hand-typing the string in both places, so there's no
    risk of the two drifting apart.
    """
    return f"{dataset}/_{source_name}_transform_watermark.json"


def _load_id_from_filename(path: str) -> float | None:
    """
    Extracts the dlt load_id from a parquet filename dlt itself writes.
    Expected format: .../year={YYYY}/month={MM}/day={DD}/{load_id}.{file_id}.parquet
    e.g. raw_pvacd/hydrovu_readings/year=2024/month=06/day=18/1781192390.555875.0.parquet → 1781192390.555875
    """
    name = path.split("/")[-1]
    m = re.match(r"^(\d+\.\d+)\.", name)
    return float(m.group(1)) if m else None


def _read_parquet_files(
    files: list[str],
    fs: gcsfs.GCSFileSystem,
    row_filter: Callable[[dict], bool] | None,
) -> list[dict]:
    """Reads and concatenates rows from the given parquet files, applying row_filter if given."""
    rows: list[dict] = []
    for f in files:
        with fs.open(f) as fh:
            table = pq.read_table(fh)
            df = table.to_pydict()
            n = len(next(iter(df.values()))) if df else 0
            for i in range(n):
                row = {k: df[k][i] for k in df}
                if row_filter is None or row_filter(row):
                    rows.append(row)
    return rows


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

    rows = _read_parquet_files([f for _, f in new_files], fs, row_filter)
    max_load_id = max([since_load_id or 0.0, *(load_id for load_id, _ in new_files)])

    logger.info("Read %d row(s) from %d new parquet file(s)", len(rows), len(new_files))
    return rows, max_load_id


def read_parquet_rows_for_load_id(
    bucket: str,
    glob_suffix: str,
    load_id: float,
    fs: gcsfs.GCSFileSystem,
    row_filter: Callable[[dict], bool] | None = None,
) -> list[dict]:
    """
    Reads parquet files matching {bucket}/{glob_suffix} whose filename load_id
    is exactly `load_id` — an exact match, unlike read_new_parquet_rows's
    "greater than a watermark" range.

    Used by backfill chunk processing (sources/<name>/backfill.py): a chunk's
    own dlt run produces one known load_id (from LoadInfo.loads_ids), so its
    transform step reads exactly the file(s) that run wrote, rather than
    "everything newer than some watermark" — no dependency on, or risk of
    interference with, any watermark the normal scheduled pipeline tracks.

    Returns the matching rows (empty list if no file has this load_id).
    """
    pattern = f"{bucket}/{glob_suffix}"
    all_files = fs.glob(pattern)

    matching = [f for f in all_files if _load_id_from_filename(f) == load_id]
    if not matching:
        logger.warning("No parquet files found for load_id=%s (pattern=%s)", load_id, pattern)
        return []

    rows = _read_parquet_files(matching, fs, row_filter)
    logger.info(
        "Read %d row(s) from %d parquet file(s) for load_id=%s", len(rows), len(matching), load_id
    )
    return rows
