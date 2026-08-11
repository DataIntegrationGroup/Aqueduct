"""
tests/shared/test_backfill.py

Unit tests for shared/backfill.py: month_chunks(), BackfillCheckpointStore,
sum_chunk_results(), the run-config helpers (parse_backfill_date,
validate_date_order, attach_run_timestamp, resolve_location_ids), and the
per-chunk ingest/load helpers (load_source_config, build_backfill_pipeline,
run_backfill_ingest, load_bundles_windowed) every source's run_backfill_chunk()
reuses. All GCS/dlt/FROST I/O is mocked — no live services required.
"""

from __future__ import annotations

import io
import json
import re
from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest

from aqueduct_dagster.canonical.canonical_model import CanonicalBundle, CanonicalObservation
from aqueduct_dagster.loader.frost_loader import LoadResult
from aqueduct_dagster.shared.backfill import (
    BackfillCheckpointStore,
    ChunkResult,
    attach_run_timestamp,
    build_backfill_pipeline,
    chunk_key,
    load_bundles_windowed,
    load_source_config,
    month_chunks,
    parse_backfill_date,
    resolve_location_ids,
    run_backfill_ingest,
    sanitize_run_key,
    sum_chunk_results,
    validate_date_order,
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


# ── parse_backfill_date / validate_date_order ───────────────────────────────────


def test_parse_backfill_date_parses_valid_date():
    assert parse_backfill_date("2026-01-15", "start_date") == date(2026, 1, 15)


@pytest.mark.parametrize("value", ["01/15/2026", "2026-13-01", "not-a-date", ""])
def test_parse_backfill_date_rejects_malformed_input(value):
    with pytest.raises(ValueError, match="start_date must be a 'YYYY-MM-DD' date"):
        parse_backfill_date(value, "start_date")


def test_validate_date_order_passes_when_start_before_end():
    validate_date_order("2026-01-01", "2026-02-01")  # should not raise


@pytest.mark.parametrize(
    ("start", "end"), [("2026-02-01", "2026-01-01"), ("2026-01-01", "2026-01-01")]
)
def test_validate_date_order_rejects_start_not_before_end(start, end):
    with pytest.raises(ValueError, match="must be before"):
        validate_date_order(start, end)


# ── attach_run_timestamp ─────────────────────────────────────────────────────────


def test_attach_run_timestamp_appends_utc_timestamp():
    result = attach_run_timestamp("jan2026-repair")
    assert re.match(r"^jan2026-repair_\d{8}T\d{6}Z$", result)


def test_attach_run_timestamp_leaves_already_timestamped_key_unchanged():
    timestamped = "jan2026-repair_20260101T000000Z"
    assert attach_run_timestamp(timestamped) == timestamped


# ── sanitize_run_key ──────────────────────────────────────────────────────────────


def test_sanitize_run_key_leaves_safe_characters_untouched():
    assert sanitize_run_key("hydrovu-jan2026_repair") == "hydrovu-jan2026_repair"


def test_sanitize_run_key_replaces_unsafe_characters():
    assert sanitize_run_key("hydrovu jan/2026 repair!") == "hydrovu_jan_2026_repair_"


# ── resolve_location_ids ──────────────────────────────────────────────────────────


def test_resolve_location_ids_empty_returns_all_sorted():
    assert resolve_location_ids([], {222: {}, 111: {}}) == [111, 222]


def test_resolve_location_ids_explicit_list_is_returned_unchanged():
    assert resolve_location_ids([222, 111], {111: {}, 222: {}, 333: {}}) == [222, 111]


def test_resolve_location_ids_raises_on_unknown_id():
    with pytest.raises(ValueError, match=r"not recognized.*\[999\]"):
        resolve_location_ids([111, 999], {111: {}})


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


def test_run_key_is_sanitized_in_checkpoint_path():
    """
    Regression test: the checkpoint path must use the same sanitized run_key
    as build_backfill_pipeline's dlt pipeline_name, so a run_key with unsafe
    characters can't split the two identifiers for the same run apart.
    """
    store = BackfillCheckpointStore(
        MagicMock(), "my-bucket", "raw_pvacd", run_key="team/jan-2026 fix"
    )
    assert store._path == "my-bucket/raw_pvacd/_backfill_checkpoints/team_jan-2026_fix.json"


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


# ── load_source_config ────────────────────────────────────────────────────────


@patch("aqueduct_dagster.shared.backfill.load_config")
def test_load_source_config_reads_named_section(mock_load_config):
    mock_load_config.return_value = {"sources": {"hydrovu": {"gcp_secret": "x"}}}
    assert load_source_config("hydrovu") == {"gcp_secret": "x"}


# ── build_backfill_pipeline ────────────────────────────────────────────────────


@patch("aqueduct_dagster.shared.backfill.build_source_pipeline")
def test_build_backfill_pipeline_includes_run_key_in_pipeline_name(mock_build_source_pipeline):
    build_backfill_pipeline(
        pipeline_name_prefix="hydrovu_backfill", dataset="raw_pvacd", run_key="jan-repair"
    )
    args, _kwargs = mock_build_source_pipeline.call_args
    assert args == ("hydrovu_backfill_jan-repair", "raw_pvacd")


@patch("aqueduct_dagster.shared.backfill.build_source_pipeline")
def test_build_backfill_pipeline_sanitizes_run_key(mock_build_source_pipeline):
    """Two different run_keys must never collide into the same pipeline_name."""
    build_backfill_pipeline(
        pipeline_name_prefix="hydrovu_backfill", dataset="raw_pvacd", run_key="jan repair/v2"
    )
    args, _kwargs = mock_build_source_pipeline.call_args
    assert args[0] == "hydrovu_backfill_jan_repair_v2"


# ── run_backfill_ingest ────────────────────────────────────────────────────────

INGEST_CHUNK_START = datetime(2026, 1, 1, tzinfo=UTC)
INGEST_CHUNK_END = datetime(2026, 2, 1, tzinfo=UTC)


def _run_ingest(resource=None, **overrides):
    kwargs = {
        "pipeline_name_prefix": "prefix",
        "dataset": "dataset",
        "run_key": "run-key",
        "resource": resource if resource is not None else object(),
        "chunk_start": INGEST_CHUNK_START,
        "chunk_end": INGEST_CHUNK_END,
        **overrides,
    }
    return run_backfill_ingest(**kwargs)


@patch("aqueduct_dagster.shared.backfill.build_backfill_pipeline")
def test_run_backfill_ingest_drops_pending_packages_before_run(mock_build_pipeline):
    """
    A package left pending by an earlier, uncleanly-terminated run must be
    dropped BEFORE pipeline.run() is called — otherwise dlt would silently
    finish loading that stale package instead of this chunk's real data.
    """
    call_order: list[str] = []
    mock_pipeline = MagicMock()
    mock_pipeline.drop_pending_packages.side_effect = lambda: call_order.append("drop")
    mock_pipeline.run.side_effect = lambda *a, **k: (
        call_order.append("run") or MagicMock(loads_ids=["100.0"])
    )
    mock_build_pipeline.return_value = mock_pipeline

    _run_ingest()

    assert call_order == ["drop", "run"]


@patch("aqueduct_dagster.shared.backfill.build_backfill_pipeline")
def test_run_backfill_ingest_forwards_prefix_dataset_and_run_key(mock_build_pipeline):
    """
    Regression test: run_backfill_ingest must forward its own
    pipeline_name_prefix/dataset/run_key straight through to
    build_backfill_pipeline, not swap or drop any of them.
    """
    mock_pipeline = MagicMock()
    mock_pipeline.run.return_value = MagicMock(loads_ids=["100.0"])
    mock_build_pipeline.return_value = mock_pipeline

    _run_ingest(pipeline_name_prefix="hydrovu_backfill", dataset="raw_pvacd", run_key="jan-repair")

    mock_build_pipeline.assert_called_once_with(
        pipeline_name_prefix="hydrovu_backfill", dataset="raw_pvacd", run_key="jan-repair"
    )


@patch("aqueduct_dagster.shared.backfill.build_backfill_pipeline")
def test_run_backfill_ingest_logs_the_dlt_pipeline_name(mock_build_pipeline, caplog):
    mock_pipeline = MagicMock()
    mock_pipeline.pipeline_name = "prefix_run-key"
    mock_pipeline.run.return_value = MagicMock(loads_ids=["100.0"])
    mock_build_pipeline.return_value = mock_pipeline

    with caplog.at_level("INFO", logger="aqueduct_dagster.shared.backfill"):
        _run_ingest()

    assert "prefix_run-key" in caplog.text


@patch("aqueduct_dagster.shared.backfill.build_backfill_pipeline")
def test_run_backfill_ingest_returns_none_on_empty_loads_ids(mock_build_pipeline):
    mock_pipeline = MagicMock()
    mock_pipeline.run.return_value = MagicMock(loads_ids=[])
    mock_build_pipeline.return_value = mock_pipeline

    assert _run_ingest() is None


@patch("aqueduct_dagster.shared.backfill.build_backfill_pipeline")
def test_run_backfill_ingest_returns_load_id_as_float(mock_build_pipeline):
    mock_pipeline = MagicMock()
    mock_pipeline.run.return_value = MagicMock(loads_ids=["1781192390.555875"])
    mock_build_pipeline.return_value = mock_pipeline

    assert _run_ingest() == 1781192390.555875


# ── load_bundles_windowed ──────────────────────────────────────────────────────


class _StubFrostLoader:
    def __init__(self) -> None:
        self.ensure_calls: list = []
        self.load_window_calls: list = []
        self._next_ds_id = 0

    def ensure_datastream(self, spec) -> str:
        self.ensure_calls.append(spec)
        self._next_ds_id += 1
        return f"ds-{self._next_ds_id}"

    def load_window(self, datastream_key, datastream_id, records, window_start, window_end):
        self.load_window_calls.append((datastream_key, datastream_id, list(records)))
        result = LoadResult(datastream_key=datastream_key)
        result.posted = len(records)
        result.deleted = 1
        return result


def test_load_bundles_windowed_sums_posted_and_deleted_across_bundles():
    from types import SimpleNamespace

    ds_a = SimpleNamespace(external_key="ds-a")
    ds_b = SimpleNamespace(external_key="ds-b")
    obs = CanonicalObservation(
        phenomenon_time=INGEST_CHUNK_START, result=1.0, datastream_external_key="ds-a"
    )
    bundle_a = CanonicalBundle(datastreams=[ds_a], observations={"ds-a": [obs]})
    bundle_b = CanonicalBundle(datastreams=[ds_b], observations={"ds-b": [obs, obs]})
    loader = _StubFrostLoader()

    posted, deleted = load_bundles_windowed(
        loader,
        [bundle_a, bundle_b],
        INGEST_CHUNK_START,
        INGEST_CHUNK_END,  # type: ignore[arg-type]
    )

    assert posted == 3  # 1 record + 2 records
    assert deleted == 2  # 1 per datastream
    assert len(loader.ensure_calls) == 2


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
