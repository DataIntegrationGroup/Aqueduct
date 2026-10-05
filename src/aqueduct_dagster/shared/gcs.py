"""
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

from aqueduct_dagster.shared.config import load_config
from aqueduct_dagster.shared.gcp_auth import ensure_adc

logger = logging.getLogger(__name__)

_SAVE_RETRIES = 3
_SAVE_BACKOFF = (1.0, 2.0, 4.0)

# Shared with defs/jobs/backfill.py so the wording can't drift between them.
BAD_FILENAME_WARNING = "Skipping parquet file with unrecognized name: %s"


def atomic_write_json_with_retry(fs: gcsfs.GCSFileSystem, path: str, data: dict, log: Any) -> None:
    """Writes `data` as JSON atomically (tmp + rename) with retry/backoff — the
    live file is never partially overwritten. `log` just needs .warning()/.error()."""
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
    """GCS bucket URL: GCS_BUCKET_URL env var, falling back to .dlt/config.toml's bucket_url."""
    env_url = os.environ.get("GCS_BUCKET_URL")
    if env_url:
        return env_url
    return load_config()["destination"]["filesystem"]["bucket_url"]


def _gcs_filesystem(project: str = "") -> gcsfs.GCSFileSystem:
    ensure_adc()
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
    """Canonical transform-watermark filename — call this instead of hand-typing
    the path so the read/write sides can't drift apart."""
    return f"{dataset}/_{source_name}_transform_watermark.json"


def _load_id_from_filename(path: str) -> float | None:
    """Extracts the dlt load_id from a filename (.../{load_id}.{file_id}.parquet)."""
    name = path.split("/")[-1]
    m = re.match(r"^(\d+\.\d+)\.", name)
    return float(m.group(1)) if m else None


def _partition_files_by_load_id(files: list[str]) -> tuple[list[tuple[float, str]], list[str]]:
    """Splits `files` into (load_id, path) pairs, returning names dlt's
    convention doesn't recognize as skipped_paths rather than crashing.

    Silent by design: read_new_parquet_rows (one call per run) and
    read_parquet_rows_for_load_id (one call per backfill chunk, re-globbing
    the same files) need different logging cadences, so each caller logs
    skipped_paths itself instead of this shared helper doing it once."""
    parsed: list[tuple[float, str]] = []
    skipped_paths: list[str] = []
    for f in files:
        load_id = _load_id_from_filename(f)
        if load_id is None:
            skipped_paths.append(f)
            continue
        parsed.append((load_id, f))
    return parsed, skipped_paths


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
) -> tuple[list[dict], float | None, int]:
    """Reads parquet files matching {bucket}/{glob_suffix} with load_id >
    since_load_id, keeping rows where row_filter(row) is True. A bad filename
    is skipped and counted, never silently dropped.

    Returns (rows, max_load_id_seen_this_run, files_skipped_bad_name)."""
    pattern = f"{bucket}/{glob_suffix}"
    all_files = fs.glob(pattern)

    parsed, skipped_paths = _partition_files_by_load_id(all_files)
    for f in skipped_paths:
        logger.warning(BAD_FILENAME_WARNING, f)
    files_skipped_bad_name = len(skipped_paths)
    new_files = [
        (load_id, f) for load_id, f in parsed if since_load_id is None or load_id > since_load_id
    ]

    if not new_files:
        if files_skipped_bad_name and not parsed:
            logger.warning(
                "No usable new parquet files since load_id=%s — all %d candidate file(s) had "
                "unrecognized names (see warnings above), not genuinely empty",
                since_load_id,
                files_skipped_bad_name,
            )
        elif files_skipped_bad_name:
            logger.info(
                "No new parquet files since load_id=%s — nothing to process "
                "(%d file(s) with unrecognized names also skipped)",
                since_load_id,
                files_skipped_bad_name,
            )
        else:
            logger.info("No new parquet files since load_id=%s — nothing to process", since_load_id)
        return [], None, files_skipped_bad_name

    logger.info(
        "Reading %d new parquet file(s) (skipped %d already-processed)",
        len(new_files),
        len(all_files) - len(new_files) - files_skipped_bad_name,
    )

    rows = _read_parquet_files([f for _, f in new_files], fs, row_filter)
    max_load_id = max([since_load_id or 0.0, *(load_id for load_id, _ in new_files)])

    logger.info("Read %d row(s) from %d new parquet file(s)", len(rows), len(new_files))
    return rows, max_load_id, files_skipped_bad_name


def read_parquet_rows_for_load_id(
    bucket: str,
    glob_suffix: str,
    load_id: float,
    fs: gcsfs.GCSFileSystem,
    row_filter: Callable[[dict], bool] | None = None,
) -> tuple[list[dict], frozenset[str]]:
    """Reads parquet files whose filename load_id exactly matches `load_id` —
    unlike read_new_parquet_rows's "greater than a watermark" range. Used by
    backfill: a chunk's dlt run has one known load_id, so transform reads
    exactly those files.

    A file with an unrecognized name is skipped and returned in skipped_paths,
    not logged here — this re-globs the dataset every chunk, so the caller
    (defs/jobs/backfill.py's op) dedupes and logs across chunks instead.

    Returns (rows, skipped_paths) — rows is empty if no file has this load_id."""
    pattern = f"{bucket}/{glob_suffix}"
    all_files = fs.glob(pattern)

    parsed, skipped_paths = _partition_files_by_load_id(all_files)
    matching = [f for file_load_id, f in parsed if file_load_id == load_id]

    if not matching:
        logger.warning("No parquet files found for load_id=%s (pattern=%s)", load_id, pattern)
        return [], frozenset(skipped_paths)

    rows = _read_parquet_files(matching, fs, row_filter)
    logger.info(
        "Read %d row(s) from %d parquet file(s) for load_id=%s", len(rows), len(matching), load_id
    )
    return rows, frozenset(skipped_paths)
