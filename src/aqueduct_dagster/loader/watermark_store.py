"""
loader/watermark_store.py

Tracks the last observation timestamp successfully loaded into FROST
per datastream — used by frost_loader.py to avoid loading duplicates.

How it works:
  - frost_loader calls .get() before loading to find the last loaded timestamp
  - frost_loader calls .set() after each successful chunk to advance the watermark
  - On next run, any observation at or before the watermark is skipped

Without this, a failed FROST load midway through would have no way to
resume — it would either re-load already-loaded observations or skip data.
"""

from __future__ import annotations

from datetime import datetime

from dagster import AssetExecutionContext

from aqueduct_dagster.loader.frost_loader import WatermarkStore


class FrostWatermarkStore(WatermarkStore):

    def __init__(self, context: AssetExecutionContext) -> None:
        self._context = context
        self._cache: dict[str, datetime] = {}

    def get(self, datastream_key: str) -> datetime | None:
         # Check in-memory cache first (within a single run)
        if datastream_key in self._cache:
            return self._cache[datastream_key]
        # TODO: read persisted watermark from Dagster asset metadata / Postgres / GCS
        return None

    def set(self, datastream_key: str, watermark: datetime) -> None:
        self._cache[datastream_key] = watermark
        # TODO: persist watermark to Dagster asset metadata / Postgres / GCS
        self._context.log.debug(
            "Watermark updated: datastream=%s ts=%s", datastream_key, watermark.isoformat()
        )
