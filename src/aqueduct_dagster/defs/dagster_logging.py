"""
Forward stdlib logging into Dagster run logs during asset execution, and
Dagster-aware wrappers around shared/gcs.py's plain-Python I/O helpers that
need that forwarding to make their own logging visible.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from dagster import AssetExecutionContext, OpExecutionContext

from aqueduct_dagster.shared.gcs import read_new_parquet_rows

if TYPE_CHECKING:
    import gcsfs
    from dagster import DagsterLogManager


class _DagsterLogHandler(logging.Handler):
    def __init__(self, dagster_log: DagsterLogManager) -> None:
        super().__init__()
        self._dagster_log = dagster_log

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            if record.levelno >= logging.ERROR:
                self._dagster_log.error(msg)
            elif record.levelno >= logging.WARNING:
                self._dagster_log.warning(msg)
            elif record.levelno >= logging.INFO:
                self._dagster_log.info(msg)
            else:
                self._dagster_log.debug(msg)
        except Exception:
            self.handleError(record)


@contextmanager
def forward_python_logs_to_dagster(
    context: AssetExecutionContext | OpExecutionContext,
    *logger_prefixes: str,
    level: int = logging.INFO,
) -> Iterator[None]:
    """Attach a handler so stdlib loggers emit into the Dagster run log stream."""
    handler = _DagsterLogHandler(context.log)
    handler.setFormatter(logging.Formatter("%(message)s"))

    configured: list[tuple[logging.Logger, int]] = []
    for prefix in logger_prefixes:
        log = logging.getLogger(prefix)
        configured.append((log, log.level))
        log.addHandler(handler)
        log.setLevel(level)

    try:
        yield
    finally:
        for log, previous_level in configured:
            log.removeHandler(handler)
            log.setLevel(previous_level)


def read_new_parquet_rows_for_asset(
    context: AssetExecutionContext,
    source_name: str,
    bucket: str,
    glob_suffix: str,
    since_load_id: float | None,
    fs: gcsfs.GCSFileSystem,
    row_filter: Callable[[dict], bool] | None = None,
) -> tuple[list[dict], float | None, int]:
    """
    Calls read_new_parquet_rows() (shared/gcs.py) with this asset's logger
    forwarded into Dagster's run log, so its files_skipped_bad_name warning
    reaches the run rather than stdlib's root logger.

    shared/gcs.py has to stay Dagster-free, so the forwarding lives here
    instead — shared by every source's transform asset, not duplicated per source.

    Returns (rows, max_load_id_seen_this_run, files_skipped_bad_name).
    """
    with forward_python_logs_to_dagster(
        context, f"aqueduct_dagster.sources.{source_name}", "aqueduct_dagster.shared"
    ):
        return read_new_parquet_rows(bucket, glob_suffix, since_load_id, fs, row_filter=row_filter)
