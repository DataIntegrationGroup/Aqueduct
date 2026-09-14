"""
tests/defs/test_dagster_logging.py

Unit tests for forward_python_logs_to_dagster and read_new_parquet_rows_for_asset.
Unlike tests elsewhere that mock forward_python_logs_to_dagster away, these
exercise the real handler-attach/route/detach mechanism, with a MagicMock
standing in only for AssetExecutionContext — no real Dagster run, no GCS.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from aqueduct_dagster.defs.dagster_logging import (
    forward_python_logs_to_dagster,
    read_new_parquet_rows_for_asset,
)

# ── forward_python_logs_to_dagster ──────────────────────────────────────────────


def test_forwards_warning_to_context_log():
    context = MagicMock()
    test_logger = logging.getLogger("aqueduct_dagster.shared.gcs")

    with forward_python_logs_to_dagster(context, "aqueduct_dagster.shared"):
        test_logger.warning("Skipping parquet file with unrecognized name: %s", "bad.parquet")

    context.log.warning.assert_called_once()
    assert "bad.parquet" in context.log.warning.call_args[0][0]


def test_forwards_info_and_error_at_their_own_levels():
    context = MagicMock()
    test_logger = logging.getLogger("aqueduct_dagster.test_dagster_logging")

    with forward_python_logs_to_dagster(context, "aqueduct_dagster.test_dagster_logging"):
        test_logger.info("info message")
        test_logger.error("error message")

    context.log.info.assert_called_once()
    assert "info message" in context.log.info.call_args[0][0]
    context.log.error.assert_called_once()
    assert "error message" in context.log.error.call_args[0][0]


def test_does_not_forward_loggers_outside_the_given_prefix():
    context = MagicMock()
    unrelated_logger = logging.getLogger("some.other.unrelated.module")

    with forward_python_logs_to_dagster(context, "aqueduct_dagster.shared"):
        unrelated_logger.warning("should not be forwarded")

    context.log.warning.assert_not_called()


def test_handler_is_detached_once_the_with_block_exits():
    """Regression guard: a leaked handler would keep forwarding into a stale run's context.log."""
    context = MagicMock()
    test_logger = logging.getLogger("aqueduct_dagster.shared.gcs")
    handlers_before = list(test_logger.handlers)

    with forward_python_logs_to_dagster(context, "aqueduct_dagster.shared"):
        pass

    assert test_logger.handlers == handlers_before


# ── read_new_parquet_rows_for_asset ─────────────────────────────────────────────


def test_read_new_parquet_rows_for_asset_forwards_bad_filename_warning():
    """The warning read_new_parquet_rows emits must reach context.log, and its
    (rows, max_load_id, files_skipped_bad_name) result must pass through unchanged."""
    context = MagicMock()

    def _fake_read_new_parquet_rows(*args, **kwargs):
        logging.getLogger("aqueduct_dagster.shared.gcs").warning(
            "Skipping parquet file with unrecognized name: %s", "bad.parquet"
        )
        return [{"v": 1}], 100.0, 1

    with patch(
        "aqueduct_dagster.defs.dagster_logging.read_new_parquet_rows",
        side_effect=_fake_read_new_parquet_rows,
    ):
        rows, max_load_id, files_skipped_bad_name = read_new_parquet_rows_for_asset(
            context, "cabq", "bucket", "ds/*.parquet", None, MagicMock()
        )

    assert rows == [{"v": 1}]
    assert max_load_id == 100.0
    assert files_skipped_bad_name == 1
    context.log.warning.assert_called_once()
    assert "bad.parquet" in context.log.warning.call_args[0][0]
