"""
Source-agnostic helpers for backfill jobs (Mode A refetch) — see
docs/BACKFILL_STRATEGY.md.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Protocol

import dlt
import gcsfs

from aqueduct_dagster.canonical.canonical_model import CanonicalBundle
from aqueduct_dagster.loader.frost_loader import FrostLoader, observation_records_for
from aqueduct_dagster.shared.config import load_config
from aqueduct_dagster.shared.gcs import atomic_write_json_with_retry
from aqueduct_dagster.shared.pipeline import build_source_pipeline

logger = logging.getLogger(__name__)

_DATE_FORMAT = "%Y-%m-%d"
# Matches a trailing _YYYYMMDDTHHMMSSZ segment already appended by attach_run_timestamp().
_RUN_KEY_TIMESTAMP_RE = re.compile(r"_\d{8}T\d{6}Z$")
_UNSAFE_PIPELINE_NAME_CHARS = re.compile(r"[^A-Za-z0-9_-]")


class _Orderable(Protocol):
    """Anything sortable — int, str, and most other natural id types satisfy this."""

    def __lt__(self, other: Any) -> bool: ...


@dataclass
class ChunkResult:
    """Outcome of one backfill chunk; returned by every source's run_backfill_chunk()
    so defs/jobs/backfill.py can aggregate metadata identically regardless of source."""

    rows_ingested: int
    bundles_loaded: int
    observations_posted: int
    observations_deleted: int
    adapter_failures: int = 0
    # Paths, not a count: read_parquet_rows_for_load_id re-globs the whole
    # table every chunk, so a persistently-bad file would otherwise be
    # recounted per chunk. sum_chunk_results dedupes via union instead.
    files_skipped_bad_name: frozenset[str] = frozenset()


def sum_chunk_results(results: list[ChunkResult]) -> ChunkResult:
    """Sums ChunkResults across all chunks in a run, field by field, for run metadata."""
    return ChunkResult(
        rows_ingested=sum(r.rows_ingested for r in results),
        bundles_loaded=sum(r.bundles_loaded for r in results),
        observations_posted=sum(r.observations_posted for r in results),
        observations_deleted=sum(r.observations_deleted for r in results),
        adapter_failures=sum(r.adapter_failures for r in results),
        files_skipped_bad_name=frozenset.union(
            frozenset(), *(r.files_skipped_bad_name for r in results)
        ),
    )


def month_chunks(start: date, end: date) -> list[tuple[datetime, datetime]]:
    """Splits [start, end) into calendar-month chunks (UTC). First/last chunks
    are clipped to start/end exactly, e.g. (2026-01-15, 2026-03-01) ->
    [(01-15, 02-01), (02-01, 03-01)]. Raises ValueError if start >= end."""
    if start >= end:
        raise ValueError(f"start ({start}) must be before end ({end})")

    chunks: list[tuple[datetime, datetime]] = []
    month_cursor = date(start.year, start.month, 1)
    while month_cursor < end:
        month_cursor_next = (
            date(month_cursor.year + 1, 1, 1)
            if month_cursor.month == 12
            else date(month_cursor.year, month_cursor.month + 1, 1)
        )
        chunk_start = max(month_cursor, start)
        chunk_end = min(month_cursor_next, end)
        chunks.append(
            (
                datetime(chunk_start.year, chunk_start.month, chunk_start.day, tzinfo=UTC),
                datetime(chunk_end.year, chunk_end.month, chunk_end.day, tzinfo=UTC),
            )
        )
        month_cursor = month_cursor_next

    return chunks


def parse_backfill_date(value: str, field_name: str) -> date:
    """Parses a "YYYY-MM-DD" date; raises ValueError naming the field on a bad format."""
    try:
        return datetime.strptime(value, _DATE_FORMAT).date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a 'YYYY-MM-DD' date, got {value!r}") from exc


def validate_date_order(start_date: str, end_date: str) -> None:
    """Raises ValueError unless start_date is strictly before end_date (both "YYYY-MM-DD")."""
    start = parse_backfill_date(start_date, "start_date")
    end = parse_backfill_date(end_date, "end_date")
    if start >= end:
        raise ValueError(f"start_date ({start_date}) must be before end_date ({end_date})")


def attach_run_timestamp(run_key: str) -> str:
    """Appends a UTC timestamp to run_key (unless already timestamped) so two
    runs from the same label don't collide, while a resume-launch reusing the
    same run_key still finds its checkpoint."""
    if _RUN_KEY_TIMESTAMP_RE.search(run_key):
        return run_key
    return f"{run_key}_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"


def sanitize_run_key(run_key: str) -> str:
    """Replaces any char outside [A-Za-z0-9_-] with _, for embedding run_key in a filesystem path."""
    return _UNSAFE_PIPELINE_NAME_CHARS.sub("_", run_key)


def load_source_config(source_name: str) -> dict[str, Any]:
    """Reads [sources.<source_name>] from .dlt/config.toml."""
    return load_config()["sources"][source_name]


def build_backfill_pipeline(
    *, pipeline_name_prefix: str, dataset: str, run_key: str
) -> dlt.Pipeline:
    """Isolated dlt pipeline: one pipeline_name per run_key, so two backfill
    runs can never share dlt's local pending-load state."""
    return build_source_pipeline(f"{pipeline_name_prefix}_{sanitize_run_key(run_key)}", dataset)


def run_backfill_ingest(
    *,
    pipeline_name_prefix: str,
    dataset: str,
    run_key: str,
    resource: Any,
    chunk_start: datetime,
    chunk_end: datetime,
) -> float | None:
    """Builds the isolated pipeline, drops any pending package (so a crashed
    attempt's leftovers are never silently resumed), runs `resource`, returns
    load_id or None if nothing new was ingested.

    Assumes `resource` has no persisted cursor of its own — drop_pending_packages()
    runs unconditionally, so a cursor-based resource would have its state
    silently discarded every chunk. Write a dedicated cursor-free resource."""
    pipeline = build_backfill_pipeline(
        pipeline_name_prefix=pipeline_name_prefix, dataset=dataset, run_key=run_key
    )
    logger.info(
        "Backfill chunk [%s, %s): using dlt pipeline_name=%s",
        chunk_start,
        chunk_end,
        pipeline.pipeline_name,
    )
    pipeline.drop_pending_packages()
    load_info = pipeline.run(resource, loader_file_format="parquet")
    if not load_info.loads_ids:
        logger.info("Backfill chunk [%s, %s): ingest yielded no new data", chunk_start, chunk_end)
        return None
    load_id = float(load_info.loads_ids[0])
    logger.info(
        "Backfill chunk [%s, %s): ingest complete, load_id=%s", chunk_start, chunk_end, load_id
    )
    return load_id


def load_bundles_windowed(
    loader: FrostLoader,
    bundles: list[CanonicalBundle],
    window_start: datetime,
    window_end: datetime,
) -> tuple[int, int]:
    """Runs load_window() for every datastream across all bundles. Returns (posted, deleted)."""
    posted = 0
    deleted = 0
    for bundle in bundles:
        for datastream in bundle.datastreams:
            ds_id = loader.ensure_datastream(datastream)
            obs_records = observation_records_for(bundle, datastream)
            result = loader.load_window(
                datastream.external_key, ds_id, obs_records, window_start, window_end
            )
            posted += result.posted
            deleted += result.deleted
    return posted, deleted


def resolve_location_ids[T: _Orderable](
    location_ids: list[T], locations_by_id: dict[T, dict]
) -> list[T]:
    """Empty location_ids means "every location the API returns"; otherwise
    raises ValueError listing any id not in locations_by_id, instead of
    silently backfilling nothing for a typo."""
    if not location_ids:
        return sorted(locations_by_id)
    unknown = sorted(loc_id for loc_id in location_ids if loc_id not in locations_by_id)
    if unknown:
        raise ValueError(
            f"location_id(s) not recognized by the API: {unknown}. Check for typos, "
            "or leave location_ids empty to backfill every location the API returns."
        )
    return location_ids


def chunk_key[T: _Orderable](
    chunk_start: datetime, chunk_end: datetime, location_ids: list[T]
) -> str:
    """Key for a chunk window + entity list. location_ids is part of the key
    so a re-launch with a different entity list is a new chunk, not silently
    skipped as already complete."""
    ids = ",".join(str(i) for i in sorted(location_ids))
    return f"{chunk_start.isoformat()}_{chunk_end.isoformat()}_{ids}"


class BackfillCheckpointStore:
    """GCS-backed record of which chunks a backfill run has completed, keyed by
    run_key — the same run_key resumes from the last completed chunk; a
    different one starts fresh. File: {dataset}/_backfill_checkpoints/{run_key}.json."""

    def __init__(
        self,
        fs: gcsfs.GCSFileSystem,
        bucket: str,
        dataset: str,
        run_key: str,
    ) -> None:
        self._fs = fs
        # Sanitized the same way as build_backfill_pipeline's pipeline_name, so a
        # run_key with a slash/space can't split this into two different identifiers.
        self._path = f"{bucket}/{dataset}/_backfill_checkpoints/{sanitize_run_key(run_key)}.json"
        self._completed: set[str] | None = None

    def _load(self) -> set[str]:
        if self._completed is not None:
            return self._completed
        try:
            with self._fs.open(self._path) as f:
                raw = json.load(f)
            self._completed = set(raw.get("completed_chunks", []))
            logger.info(
                "Loaded backfill checkpoint (%s): %d chunk(s) already complete",
                self._path,
                len(self._completed),
            )
        except (FileNotFoundError, json.JSONDecodeError):
            self._completed = set()
            logger.info("No backfill checkpoint at %s — starting fresh", self._path)
        return self._completed

    def is_complete[T: _Orderable](
        self, chunk_start: datetime, chunk_end: datetime, location_ids: list[T]
    ) -> bool:
        return chunk_key(chunk_start, chunk_end, location_ids) in self._load()

    def mark_complete[T: _Orderable](
        self, chunk_start: datetime, chunk_end: datetime, location_ids: list[T]
    ) -> None:
        completed = self._load()
        completed.add(chunk_key(chunk_start, chunk_end, location_ids))
        self._save(completed)

    def _save(self, completed: set[str]) -> None:
        atomic_write_json_with_retry(
            self._fs, self._path, {"completed_chunks": sorted(completed)}, logger
        )
