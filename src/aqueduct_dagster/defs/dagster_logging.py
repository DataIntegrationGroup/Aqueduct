"""Forward stdlib logging into Dagster run logs during asset execution."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

from dagster import AssetExecutionContext


class _DagsterLogHandler(logging.Handler):
    def __init__(self, dagster_log) -> None:
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
    context: AssetExecutionContext,
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
