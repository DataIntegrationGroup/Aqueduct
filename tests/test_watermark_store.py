"""
tests/test_watermark_store.py

Unit tests for FrostWatermarkStore and InMemoryWatermarkStore.
All GCS I/O is mocked — no live GCS or Dagster runtime required.
"""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from aqueduct_dagster.loader.watermark_store import (
    _FROST_WATERMARKS_FILENAME,
    FrostWatermarkStore,
    InMemoryWatermarkStore,
)

# ── helpers ─────────────────────────────────────────────────────────────────


def _make_store(gcs_content: dict[str, str] | None = None) -> FrostWatermarkStore:
    """
    Build a FrostWatermarkStore with a mocked gcsfs and Dagster context.
    gcs_content: dict to return on open() — None means FileNotFoundError.
    """
    mock_context = MagicMock()
    mock_context.log = MagicMock()

    mock_fs = MagicMock()
    if gcs_content is None:
        mock_fs.open.side_effect = FileNotFoundError
    else:
        raw = json.dumps(gcs_content)
        mock_fs.open.return_value.__enter__ = lambda _: io.StringIO(raw)
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

    return FrostWatermarkStore(mock_context, mock_fs, "my-bucket", dataset="raw_pvacd"), mock_fs


# ── InMemoryWatermarkStore ───────────────────────────────────────────────────


def test_inmemory_get_returns_none_for_unknown_key():
    store = InMemoryWatermarkStore()
    assert store.get("missing") is None


def test_inmemory_set_then_get():
    store = InMemoryWatermarkStore()
    ts = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    store.set("ds-1", ts)
    assert store.get("ds-1") == ts


# ── FrostWatermarkStore — first run (no GCS file) ────────────────────────────


def test_get_returns_none_when_no_gcs_file():
    store, _ = _make_store(gcs_content=None)
    assert store.get("any-key") is None


def test_corrupt_gcs_file_treated_as_first_run():
    mock_context = MagicMock()
    mock_context.log = MagicMock()
    mock_fs = MagicMock()
    mock_fs.open.return_value.__enter__ = lambda _: io.StringIO("not valid json{{{")
    mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

    store = FrostWatermarkStore(mock_context, mock_fs, "my-bucket", dataset="raw_pvacd")
    assert store.get("any-key") is None
    assert store._loaded is True


def test_load_called_once_on_file_not_found():
    store, mock_fs = _make_store(gcs_content=None)
    store.get("a")
    store.get("b")
    # open() called once for the load attempt, not on every get()
    assert mock_fs.open.call_count == 1


# ── FrostWatermarkStore — existing GCS file ──────────────────────────────────


def test_get_loads_watermark_from_gcs():
    ts_str = "2026-06-16T18:00:00+00:00"
    store, _ = _make_store({"pvacd-123-dtw": ts_str})
    result = store.get("pvacd-123-dtw")
    assert result == datetime.fromisoformat(ts_str)


def test_get_returns_none_for_key_not_in_file():
    store, _ = _make_store({"pvacd-123-dtw": "2026-06-16T18:00:00+00:00"})
    assert store.get("pvacd-999-dtw") is None


def test_gcs_read_happens_only_once_per_run():
    store, mock_fs = _make_store({"k": "2026-06-01T00:00:00+00:00"})
    store.get("k")
    store.get("k")
    store.get("other")
    # First call triggers _load() which calls open(); subsequent get()s hit cache
    assert mock_fs.open.call_count == 1


# ── FrostWatermarkStore — set() ──────────────────────────────────────────────


def test_set_writes_to_gcs_immediately():
    store, mock_fs = _make_store(gcs_content=None)
    mock_fs.open.side_effect = None  # reset FileNotFoundError for _load
    mock_fs.open.return_value.__enter__ = lambda _: io.StringIO("{}")
    mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

    ts = datetime(2026, 6, 20, 10, 0, 0, tzinfo=UTC)

    write_buf = io.StringIO()
    mock_fs.open.return_value.__enter__ = lambda _: write_buf

    store.set("ds-1", ts)

    # open() called at least once for the write
    assert mock_fs.open.called


def test_set_then_get_returns_updated_value():
    store, mock_fs = _make_store(gcs_content=None)
    store._loaded = True  # skip GCS read — simulate first run already checked
    mock_fs.open.side_effect = None
    write_buf = io.StringIO()
    mock_fs.open.return_value.__enter__ = lambda _: write_buf
    mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

    ts = datetime(2026, 6, 20, 10, 0, 0, tzinfo=UTC)
    store.set("ds-1", ts)
    assert store.get("ds-1") == ts


def test_set_writes_to_tmp_then_renames():
    store, mock_fs = _make_store(gcs_content=None)
    mock_fs.open.side_effect = None
    write_buf = io.StringIO()
    mock_fs.open.return_value.__enter__ = lambda _: write_buf
    mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

    ts = datetime(2026, 6, 20, tzinfo=UTC)
    store.set("ds-1", ts)

    tmp_path = f"my-bucket/raw_pvacd/{_FROST_WATERMARKS_FILENAME}.tmp"
    final_path = f"my-bucket/raw_pvacd/{_FROST_WATERMARKS_FILENAME}"
    mock_fs.open.assert_called_with(tmp_path, "w")
    mock_fs.rename.assert_called_once_with(tmp_path, final_path)


def test_save_retries_on_transient_failure():
    store, mock_fs = _make_store(gcs_content=None)
    store._loaded = True
    write_buf = io.StringIO()
    # First open call raises, second succeeds
    mock_fs.open.side_effect = [
        OSError("transient"),
        MagicMock(
            __enter__=lambda _: write_buf,
            __exit__=MagicMock(return_value=False),
        ),
    ]

    ts = datetime(2026, 6, 20, tzinfo=UTC)
    store.set("ds-1", ts)  # should not raise — retry succeeds

    assert mock_fs.open.call_count == 2


def test_save_raises_after_all_retries_exhausted():
    store, mock_fs = _make_store(gcs_content=None)
    store._loaded = True
    mock_fs.open.side_effect = OSError("persistent failure")

    ts = datetime(2026, 6, 20, tzinfo=UTC)
    with pytest.raises(OSError):
        store.set("ds-1", ts)

    assert mock_fs.open.call_count == 3  # _SAVE_RETRIES attempts


def test_multiple_set_calls_accumulate_in_cache():
    store, mock_fs = _make_store(gcs_content=None)
    store._loaded = True  # skip GCS read — simulate first run already checked
    mock_fs.open.side_effect = None
    write_buf = io.StringIO()
    mock_fs.open.return_value.__enter__ = lambda _: write_buf
    mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

    ts1 = datetime(2026, 6, 1, tzinfo=UTC)
    ts2 = datetime(2026, 6, 2, tzinfo=UTC)
    store.set("ds-1", ts1)
    store.set("ds-2", ts2)

    assert store.get("ds-1") == ts1
    assert store.get("ds-2") == ts2
