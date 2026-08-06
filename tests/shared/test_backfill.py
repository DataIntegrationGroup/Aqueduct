"""
tests/shared/test_backfill.py

Unit tests for shared/backfill.py: month_chunks(), BackfillCheckpointStore,
and sum_chunk_results(). All GCS I/O is mocked — no live GCS required.
"""

from __future__ import annotations

import io
import json
from datetime import UTC, date, datetime
from unittest.mock import MagicMock

import pytest

from aqueduct_dagster.shared.backfill import (
    BackfillCheckpointStore,
    ChunkResult,
    chunk_key,
    month_chunks,
    sum_chunk_results,
)

# ── month_chunks ──────────────────────────────────────────────────────────────


class TestMonthChunks:
    def test_single_full_month(self):
        chunks = month_chunks(date(2026, 1, 1), date(2026, 2, 1))
        assert chunks == [
            (datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)),
        ]

    def test_spans_multiple_months(self):
        chunks = month_chunks(date(2026, 1, 1), date(2026, 4, 1))
        assert chunks == [
            (datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)),
            (datetime(2026, 2, 1, tzinfo=UTC), datetime(2026, 3, 1, tzinfo=UTC)),
            (datetime(2026, 3, 1, tzinfo=UTC), datetime(2026, 4, 1, tzinfo=UTC)),
        ]

    def test_mid_month_start_is_not_padded_backward(self):
        chunks = month_chunks(date(2026, 1, 15), date(2026, 3, 1))
        assert chunks == [
            (datetime(2026, 1, 15, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)),
            (datetime(2026, 2, 1, tzinfo=UTC), datetime(2026, 3, 1, tzinfo=UTC)),
        ]

    def test_mid_month_end_truncates_last_chunk(self):
        chunks = month_chunks(date(2026, 1, 1), date(2026, 2, 15))
        assert chunks == [
            (datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)),
            (datetime(2026, 2, 1, tzinfo=UTC), datetime(2026, 2, 15, tzinfo=UTC)),
        ]

    def test_range_within_single_month(self):
        chunks = month_chunks(date(2026, 1, 5), date(2026, 1, 20))
        assert chunks == [
            (datetime(2026, 1, 5, tzinfo=UTC), datetime(2026, 1, 20, tzinfo=UTC)),
        ]

    def test_year_boundary(self):
        chunks = month_chunks(date(2025, 12, 1), date(2026, 2, 1))
        assert chunks == [
            (datetime(2025, 12, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)),
            (datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)),
        ]

    def test_raises_when_start_not_before_end(self):
        with pytest.raises(ValueError):
            month_chunks(date(2026, 1, 1), date(2026, 1, 1))
        with pytest.raises(ValueError):
            month_chunks(date(2026, 2, 1), date(2026, 1, 1))


# ── chunk_key ──────────────────────────────────────────────────────────────────


def test_chunk_key_is_stable_string():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 2, 1, tzinfo=UTC)
    assert (
        chunk_key(start, end, [222, 111])
        == "2026-01-01T00:00:00+00:00_2026-02-01T00:00:00+00:00_111,222"
    )


def test_chunk_key_differs_for_different_location_ids():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 2, 1, tzinfo=UTC)
    assert chunk_key(start, end, [111]) != chunk_key(start, end, [111, 222])


# ── BackfillCheckpointStore ────────────────────────────────────────────────────


def _make_store(gcs_content: dict | None = None) -> tuple[BackfillCheckpointStore, MagicMock]:
    mock_fs = MagicMock()
    if gcs_content is None:
        mock_fs.open.side_effect = FileNotFoundError
    else:
        raw = json.dumps(gcs_content)
        mock_fs.open.return_value.__enter__ = lambda _: io.StringIO(raw)
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

    store = BackfillCheckpointStore(mock_fs, "my-bucket", "raw_pvacd", run_key="hydrovu-jan2026")
    return store, mock_fs


CHUNK_1 = (datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC), [111])
CHUNK_2 = (datetime(2026, 2, 1, tzinfo=UTC), datetime(2026, 3, 1, tzinfo=UTC), [111])


def test_is_complete_false_when_no_checkpoint_file():
    store, _ = _make_store(gcs_content=None)
    assert store.is_complete(*CHUNK_1) is False


def test_is_complete_true_when_chunk_in_file():
    store, _ = _make_store({"completed_chunks": [chunk_key(*CHUNK_1)]})
    assert store.is_complete(*CHUNK_1) is True
    assert store.is_complete(*CHUNK_2) is False


def test_corrupt_checkpoint_file_treated_as_fresh_start():
    mock_fs = MagicMock()
    mock_fs.open.return_value.__enter__ = lambda _: io.StringIO("not valid json{{{")
    mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)
    store = BackfillCheckpointStore(mock_fs, "my-bucket", "raw_pvacd", run_key="r")
    assert store.is_complete(*CHUNK_1) is False


def test_load_happens_once_per_run():
    store, mock_fs = _make_store(gcs_content=None)
    store.is_complete(*CHUNK_1)
    store.is_complete(*CHUNK_2)
    assert mock_fs.open.call_count == 1


def test_mark_complete_writes_tmp_then_renames():
    store, mock_fs = _make_store(gcs_content=None)
    write_buf = io.StringIO()
    mock_fs.open.side_effect = None
    mock_fs.open.return_value.__enter__ = lambda _: write_buf
    mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

    store.mark_complete(*CHUNK_1)

    tmp_path = "my-bucket/raw_pvacd/_backfill_checkpoints/hydrovu-jan2026.json.tmp"
    final_path = "my-bucket/raw_pvacd/_backfill_checkpoints/hydrovu-jan2026.json"
    mock_fs.open.assert_called_with(tmp_path, "w")
    mock_fs.rename.assert_called_once_with(tmp_path, final_path)


def test_mark_complete_then_is_complete_reflects_update():
    store, mock_fs = _make_store(gcs_content=None)
    write_buf = io.StringIO()
    mock_fs.open.side_effect = None
    mock_fs.open.return_value.__enter__ = lambda _: write_buf
    mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

    store.mark_complete(*CHUNK_1)

    assert store.is_complete(*CHUNK_1) is True
    assert store.is_complete(*CHUNK_2) is False


def test_mark_complete_with_one_location_list_does_not_cover_a_different_list():
    """
    Regression test: re-launching the same run_key with an expanded/changed
    location_ids list for the same date range must not be silently skipped
    as already complete — that would mean a newly added location never gets
    backfilled for a month already checkpointed under the old list.
    """
    store, mock_fs = _make_store(gcs_content=None)
    write_buf = io.StringIO()
    mock_fs.open.side_effect = None
    mock_fs.open.return_value.__enter__ = lambda _: write_buf
    mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

    chunk_start, chunk_end, _ = CHUNK_1
    store.mark_complete(chunk_start, chunk_end, [111])

    assert store.is_complete(chunk_start, chunk_end, [111]) is True
    assert store.is_complete(chunk_start, chunk_end, [111, 222]) is False


def test_mark_complete_accumulates_multiple_chunks():
    store, mock_fs = _make_store(gcs_content=None)
    write_buf = io.StringIO()
    mock_fs.open.side_effect = None
    mock_fs.open.return_value.__enter__ = lambda _: write_buf
    mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

    store.mark_complete(*CHUNK_1)
    store.mark_complete(*CHUNK_2)

    assert store.is_complete(*CHUNK_1) is True
    assert store.is_complete(*CHUNK_2) is True


def test_save_retries_on_transient_failure():
    store, mock_fs = _make_store(gcs_content=None)
    store._completed = set()
    write_buf = io.StringIO()
    mock_fs.open.side_effect = [
        OSError("transient"),
        MagicMock(
            __enter__=lambda _: write_buf,
            __exit__=MagicMock(return_value=False),
        ),
    ]

    store.mark_complete(*CHUNK_1)  # should not raise — retry succeeds
    assert mock_fs.open.call_count == 2


def test_save_raises_after_all_retries_exhausted():
    store, mock_fs = _make_store(gcs_content=None)
    store._completed = set()
    mock_fs.open.side_effect = OSError("persistent failure")

    with pytest.raises(OSError):
        store.mark_complete(*CHUNK_1)

    assert mock_fs.open.call_count == 3  # _SAVE_RETRIES attempts


# ── sum_chunk_results ────────────────────────────────────────────────────────


def test_sum_chunk_results_adds_fields_across_chunks():
    results = [
        ChunkResult(
            rows_ingested=10,
            bundles_loaded=2,
            observations_posted=10,
            observations_deleted=0,
            adapter_failures=1,
        ),
        ChunkResult(
            rows_ingested=5,
            bundles_loaded=1,
            observations_posted=5,
            observations_deleted=3,
            adapter_failures=2,
        ),
    ]
    totals = sum_chunk_results(results)
    assert totals == ChunkResult(
        rows_ingested=15,
        bundles_loaded=3,
        observations_posted=15,
        observations_deleted=3,
        adapter_failures=3,
    )


def test_sum_chunk_results_empty_list_is_all_zero():
    totals = sum_chunk_results([])
    assert totals == ChunkResult(
        rows_ingested=0, bundles_loaded=0, observations_posted=0, observations_deleted=0
    )
