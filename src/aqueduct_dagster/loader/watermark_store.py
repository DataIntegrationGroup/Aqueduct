"""
Defines the WatermarkStore interface and implementations — tracks the last
observation timestamp loaded into FROST per datastream, so frost_loader.py
never reloads duplicates. Without it, a failed FROST load midway through
would have no way to resume cleanly.

GCS watermark file: {dataset}/_frost_watermarks.json
  {"<datastream_external_key>": "2026-06-16T18:00:00+00:00", ...}
On first run (no file yet), get() returns None and frost_loader falls back
to _max_phenomenon_time() to recover the watermark from FROST itself.
"""

from __future__ import annotations

import abc
import json
from datetime import datetime

import gcsfs
from dagster import AssetExecutionContext, OpExecutionContext

from aqueduct_dagster.shared.gcs import atomic_write_json_with_retry

_FROST_WATERMARKS_FILENAME = "_frost_watermarks.json"


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
    """GCS-backed watermark store. Reads the file lazily on the first get()
    per run (runs with no new observations skip the GCS read entirely);
    writes immediately after every set(), atomically with retry
    (atomic_write_json_with_retry), so partial failures resume from the
    last successful chunk."""

    def __init__(
        self,
        context: AssetExecutionContext | OpExecutionContext,
        fs: gcsfs.GCSFileSystem,
        bucket: str,
        dataset: str,
    ) -> None:
        self._context = context
        self._fs = fs
        self._watermarks_path = f"{bucket}/{dataset}/{_FROST_WATERMARKS_FILENAME}"
        self._cache: dict[str, datetime] = {}
        self._loaded = False

    def _load(self) -> None:
        """Read GCS watermark file into cache. No-op after first call per run."""
        if self._loaded:
            return
        try:
            with self._fs.open(self._watermarks_path) as f:
                raw: dict[str, str] = json.load(f)
            self._cache = {k: datetime.fromisoformat(v) for k, v in raw.items()}
            self._context.log.info("Loaded FROST watermarks from GCS: %d entries", len(self._cache))
        except (FileNotFoundError, json.JSONDecodeError):
            self._context.log.info(
                "No FROST watermark file at %s — first run, starting fresh",
                self._watermarks_path,
            )
        self._loaded = True

    def _save(self) -> None:
        """Write cache to GCS atomically (write tmp → rename) with retry."""
        data = {k: v.isoformat() for k, v in self._cache.items()}
        atomic_write_json_with_retry(self._fs, self._watermarks_path, data, self._context.log)

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
