"""
shared/backfill.py

Source-agnostic helpers for backfill jobs (Mode A refetch, and later Mode B
replay) — see docs/BACKFILL_STRATEGY.md.

Three responsibilities, all with no knowledge of any one source's API or adapter:

  month_chunks()          splits a requested date range into calendar-month
                           windows, so a wide backfill runs as a sequence of
                           small chunks in one Dagster run instead of one huge
                           call (see BACKFILL_STRATEGY.md §4.3).

  BackfillCheckpointStore  tracks which chunks of a specific backfill run have
                           already completed (ingest + transform + load all
                           succeeded), so re-launching the same run resumes
                           from the last completed chunk rather than
                           restarting the whole range (§4.3).

  ChunkResult              the result shape every source's run_backfill_chunk()
                           (sources/<name>/backfill.py) returns, so the generic
                           job factory (defs/jobs/backfill.py) can report
                           metadata without importing any one source's module.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime

import gcsfs

from aqueduct_dagster.shared.gcs import atomic_write_json_with_retry

logger = logging.getLogger(__name__)


@dataclass
class ChunkResult:
    """
    Outcome of one calendar-month backfill chunk (ingest + transform + load).
    Returned by every source's run_backfill_chunk() (sources/<name>/backfill.py)
    so the job orchestration (defs/jobs/backfill.py) can report metadata and
    aggregate totals identically regardless of source.
    """

    rows_ingested: int
    bundles_loaded: int
    observations_posted: int
    observations_deleted: int
    adapter_failures: int = 0


def sum_chunk_results(results: list[ChunkResult]) -> ChunkResult:
    """Sums ChunkResults across all chunks in a run, field by field, for run metadata."""
    return ChunkResult(
        rows_ingested=sum(r.rows_ingested for r in results),
        bundles_loaded=sum(r.bundles_loaded for r in results),
        observations_posted=sum(r.observations_posted for r in results),
        observations_deleted=sum(r.observations_deleted for r in results),
        adapter_failures=sum(r.adapter_failures for r in results),
    )


def month_chunks(start: date, end: date) -> list[tuple[datetime, datetime]]:
    """
    Splits the half-open range [start, end) into calendar-month chunks,
    returned as UTC datetime bounds.

    The first chunk starts exactly at `start` (not padded back to the 1st of
    its month); the last chunk ends exactly at `end`. Every chunk in between
    is a full calendar month. E.g. month_chunks(2026-01-15, 2026-03-01) ->
    [(2026-01-15, 2026-02-01), (2026-02-01, 2026-03-01)].

    Raises ValueError if start >= end.
    """
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


def chunk_key(chunk_start: datetime, chunk_end: datetime, location_ids: list[int]) -> str:
    """
    Canonical string key for a chunk window + entity list — used by
    BackfillCheckpointStore and logging.

    location_ids is part of the key (not just the date range) so that
    re-launching the same run_key with a different entity list (e.g. adding a
    location that was missing from the first run) is treated as a genuinely
    different chunk, not silently skipped as already complete.
    """
    ids = ",".join(str(i) for i in sorted(location_ids))
    return f"{chunk_start.isoformat()}_{chunk_end.isoformat()}_{ids}"


class BackfillCheckpointStore:
    """
    GCS-backed record of which chunks a specific backfill run has completed.

    Keyed by an operator-supplied `run_key` (e.g. "hydrovu-jan2026-repair") —
    re-launching the job with the same run_key resumes from the last
    completed chunk; a different run_key starts fresh and can be run
    independently (e.g. two unrelated backfills for the same source).

    GCS file: {dataset}/_backfill_checkpoints/{run_key}.json
      {"completed_chunks": ["2026-01-01T00:00:00+00:00_2026-02-01T00:00:00+00:00_111,222", ...]}

    Writes are atomic with retry, via shared/gcs.py's atomic_write_json_with_retry()
    (also used by loader/watermark_store.py's FrostWatermarkStore).
    """

    def __init__(
        self,
        fs: gcsfs.GCSFileSystem,
        bucket: str,
        dataset: str,
        run_key: str,
    ) -> None:
        self._fs = fs
        self._path = f"{bucket}/{dataset}/_backfill_checkpoints/{run_key}.json"
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

    def is_complete(
        self, chunk_start: datetime, chunk_end: datetime, location_ids: list[int]
    ) -> bool:
        return chunk_key(chunk_start, chunk_end, location_ids) in self._load()

    def mark_complete(
        self, chunk_start: datetime, chunk_end: datetime, location_ids: list[int]
    ) -> None:
        completed = self._load()
        completed.add(chunk_key(chunk_start, chunk_end, location_ids))
        self._save(completed)

    def _save(self, completed: set[str]) -> None:
        atomic_write_json_with_retry(
            self._fs, self._path, {"completed_chunks": sorted(completed)}, logger
        )
