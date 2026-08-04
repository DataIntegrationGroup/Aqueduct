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

  parse_backfill_date/validate_date_order/attach_run_timestamp/
  sanitize_run_key/resolve_location_ids
                           run-config validation/resolution shared by every
                           backfill job (dates, run_key, entity list) — kept
                           Dagster- and source-free so Mode B (replay) can
                           reuse it too.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime

import gcsfs

from aqueduct_dagster.shared.gcs import atomic_write_json_with_retry

logger = logging.getLogger(__name__)

_DATE_FORMAT = "%Y-%m-%d"
# Matches a trailing _YYYYMMDDTHHMMSSZ segment already appended by attach_run_timestamp().
_RUN_KEY_TIMESTAMP_RE = re.compile(r"_\d{8}T\d{6}Z$")
_UNSAFE_PIPELINE_NAME_CHARS = re.compile(r"[^A-Za-z0-9_-]")


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


def sum_chunk_results(results: list[ChunkResult]) -> ChunkResult:
    """Sums ChunkResults across all chunks in a run, field by field, for run metadata."""
    return ChunkResult(
        rows_ingested=sum(r.rows_ingested for r in results),
        bundles_loaded=sum(r.bundles_loaded for r in results),
        observations_posted=sum(r.observations_posted for r in results),
        observations_deleted=sum(r.observations_deleted for r in results),
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
    """
    Appends a UTC timestamp to run_key, unless it already ends with one.

    Keeps two runs launched from the same typed label from colliding, while
    letting a resume-launch reuse the exact same (already-timestamped)
    run_key unchanged, so BackfillCheckpointStore finds the same checkpoint.
    """
    if _RUN_KEY_TIMESTAMP_RE.search(run_key):
        return run_key
    return f"{run_key}_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"


def sanitize_run_key(run_key: str) -> str:
    """Replaces any char outside [A-Za-z0-9_-] with _, for embedding run_key in a filesystem path."""
    return _UNSAFE_PIPELINE_NAME_CHARS.sub("_", run_key)


def resolve_location_ids(location_ids: list[int], locations_by_id: dict[int, dict]) -> list[int]:
    """
    Empty location_ids means "every location the API returns" (locations_by_id);
    otherwise raises ValueError listing any id not in locations_by_id, instead
    of silently backfilling nothing for a typo'd/nonexistent id.
    """
    if not location_ids:
        return sorted(locations_by_id)
    unknown = sorted(loc_id for loc_id in location_ids if loc_id not in locations_by_id)
    if unknown:
        raise ValueError(
            f"location_id(s) not recognized by the API: {unknown}. Check for typos, "
            "or leave location_ids empty to backfill every location the API returns."
        )
    return location_ids


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
