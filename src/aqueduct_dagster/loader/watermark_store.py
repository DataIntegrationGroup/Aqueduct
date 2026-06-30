"""
loader/watermark_store.py

Defines the WatermarkStore interface and all concrete implementations.

WatermarkStore tracks the last observation timestamp successfully loaded into
FROST per datastream — used by frost_loader.py to avoid loading duplicates.

How it works:
  - frost_loader calls .get() before loading to find the last loaded timestamp
  - frost_loader calls .set() after each successful chunk to advance the watermark
  - On next run, any observation at or before the watermark is skipped

Without this, a failed FROST load midway through would have no way to
resume — it would either re-load already-loaded observations or skip data.

Implementations:
  FrostWatermarkStore    — GCS-backed, durable across Dagster restarts
  InMemoryWatermarkStore — dev/test only, not durable across runs

GCS watermark file: raw_pvacd/_frost_watermarks.json
  {"pvacd-4745648669458432-dtw": "2026-06-16T18:00:00+00:00", ...}
  One key per datastream. Written after every successful chunk so a partial
  failure resumes from the last successful chunk on the next run.
  On first ever run (no file yet), get() returns None and frost_loader falls
  back to _max_phenomenon_time() to recover the watermark from FROST itself.
"""

from __future__ import annotations

import abc
import json
from datetime import datetime

import gcsfs
from dagster import AssetExecutionContext

_FROST_WATERMARKS_PATH = "raw_pvacd/_frost_watermarks.json"


class WatermarkStore(abc.ABC):
    @abc.abstractmethod
    def get(self, datastream_key: str) -> datetime | None: ...
    @abc.abstractmethod
    def set(self, datastream_key: str, watermark: datetime) -> None: ...


class InMemoryWatermarkStore(WatermarkStore):
    """Dev/test only — not durable across runs."""

    def __init__(self) -> None:
        self._wm: dict[str, datetime] = {}

    def get(self, datastream_key: str) -> datetime | None:
        return self._wm.get(datastream_key)

    def set(self, datastream_key: str, watermark: datetime) -> None:
        self._wm[datastream_key] = watermark


class FrostWatermarkStore(WatermarkStore):
    """
    GCS-backed watermark store.

    Reads the GCS watermark file on the first get() call per run (lazy — runs
    with no new observations skip the GCS read entirely). Writes back to GCS
    immediately after every set() so partial failures resume from the last
    successful chunk on the next run.
    """

    def __init__(
        self,
        context: AssetExecutionContext,
        fs: gcsfs.GCSFileSystem,
        bucket: str,
    ) -> None:
        self._context = context
        self._fs = fs
        self._bucket = bucket
        self._cache: dict[str, datetime] = {}
        self._loaded = False

    def _load(self) -> None:
        """Read GCS watermark file into cache. No-op after first call per run."""
        if self._loaded:
            return
        path = f"{self._bucket}/{_FROST_WATERMARKS_PATH}"
        try:
            with self._fs.open(path) as f:
                raw: dict[str, str] = json.load(f)
            self._cache = {k: datetime.fromisoformat(v) for k, v in raw.items()}
            self._context.log.info("Loaded FROST watermarks from GCS: %d entries", len(self._cache))
        except (FileNotFoundError, json.JSONDecodeError):
            self._context.log.info(
                "No FROST watermark file at %s — first run, starting fresh", path
            )
        self._loaded = True

    def _save(self) -> None:
        """Write current cache to GCS watermark file."""
        path = f"{self._bucket}/{_FROST_WATERMARKS_PATH}"
        with self._fs.open(path, "w") as f:
            json.dump({k: v.isoformat() for k, v in self._cache.items()}, f)

    def get(self, datastream_key: str) -> datetime | None:
        self._load()
        return self._cache.get(datastream_key)

    def set(self, datastream_key: str, watermark: datetime) -> None:
        self._cache[datastream_key] = watermark
        self._save()
        self._context.log.debug(
            "Watermark updated and persisted: datastream=%s ts=%s",
            datastream_key,
            watermark.isoformat(),
        )
