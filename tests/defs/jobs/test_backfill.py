"""
tests/defs/jobs/test_backfill.py

Unit tests for the generic backfill-refetch job factory
(_make_backfill_refetch_job / _make_backfill_refetch_op).

Built and executed with stub prepare_fn/run_chunk_fn (not HydroVu's real
ones) via Dagster's execute_in_process — no live API/GCS/FROST required.
GCS and FROST-loader construction inside the op are mocked.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from aqueduct_dagster.defs.jobs.backfill import _make_backfill_refetch_job

_RUN_CONFIG = {
    "ops": {
        "test_backfill_refetch_op": {
            "config": {
                "location_ids": [111],
                "start_date": "2026-01-01",
                "end_date": "2026-03-01",  # 2 calendar-month chunks
                "run_key": "test-run",
                "dry_run": True,
            }
        }
    }
}


def _run_config(*, dry_run: bool) -> dict:
    cfg = {**_RUN_CONFIG["ops"]["test_backfill_refetch_op"]["config"], "dry_run": dry_run}
    return {"ops": {"test_backfill_refetch_op": {"config": cfg}}}


def _stub_chunk_result(**overrides: int) -> SimpleNamespace:
    defaults = dict(
        rows_ingested=1, bundles_loaded=1, observations_posted=1, observations_deleted=0
    )
    return SimpleNamespace(**{**defaults, **overrides})


def _step_output_metadata(result, op_name: str) -> dict:
    """Extracts the metadata dict attached via context.add_output_metadata()."""
    for event in result.all_events:
        if event.event_type_value == "STEP_OUTPUT" and event.step_key == op_name:
            return {k: v.value for k, v in event.event_specific_data.metadata.items()}
    raise AssertionError(f"no STEP_OUTPUT event found for step {op_name!r}")


# ── dry_run ────────────────────────────────────────────────────────────────────


def test_dry_run_never_calls_prepare_or_run_chunk():
    prepare_fn = MagicMock()
    run_chunk_fn = MagicMock()
    job = _make_backfill_refetch_job("test", "test_dataset", prepare_fn, run_chunk_fn)

    result = job.execute_in_process(run_config=_run_config(dry_run=True))

    assert result.success
    prepare_fn.assert_not_called()
    run_chunk_fn.assert_not_called()


@patch("aqueduct_dagster.defs.jobs.backfill.BackfillCheckpointStore")
def test_dry_run_never_touches_checkpoint_store(mock_checkpoint_cls):
    job = _make_backfill_refetch_job("test", "test_dataset", MagicMock(), MagicMock())
    result = job.execute_in_process(run_config=_run_config(dry_run=True))

    assert result.success
    mock_checkpoint_cls.assert_not_called()


def test_dry_run_attaches_plan_metadata():
    job = _make_backfill_refetch_job("test", "test_dataset", MagicMock(), MagicMock())
    result = job.execute_in_process(run_config=_run_config(dry_run=True))

    metadata = _step_output_metadata(result, "test_backfill_refetch_op")
    assert metadata["dry_run"] is True
    assert metadata["location_count"] == 1
    assert metadata["chunks_planned"] == 2


# ── real run (dry_run=False) ────────────────────────────────────────────────────


@patch("aqueduct_dagster.defs.jobs.backfill.build_frost_loader")
@patch("aqueduct_dagster.defs.jobs.backfill.BackfillCheckpointStore")
@patch("aqueduct_dagster.defs.jobs.backfill._gcs_filesystem")
@patch("aqueduct_dagster.defs.jobs.backfill._gcs_bucket_url")
def test_real_run_calls_prepare_once_and_run_chunk_per_chunk(
    mock_bucket_url, mock_fs, mock_checkpoint_cls, mock_build_loader
):
    mock_bucket_url.return_value = "gs://bucket"
    mock_checkpoint = mock_checkpoint_cls.return_value
    mock_checkpoint.is_complete.return_value = False

    client = MagicMock()
    prepare_fn = MagicMock(return_value=(client, ["loc"], {111: {}}))
    run_chunk_fn = MagicMock(return_value=_stub_chunk_result())

    job = _make_backfill_refetch_job("test", "test_dataset", prepare_fn, run_chunk_fn)
    result = job.execute_in_process(run_config=_run_config(dry_run=False))

    assert result.success
    prepare_fn.assert_called_once()
    assert run_chunk_fn.call_count == 2  # 2 calendar-month chunks in [Jan 1, Mar 1)
    assert mock_checkpoint.mark_complete.call_count == 2
    client.close.assert_called_once()

    metadata = _step_output_metadata(result, "test_backfill_refetch_op")
    assert metadata["dry_run"] is False
    assert metadata["chunks_processed"] == 2
    assert metadata["chunks_skipped_already_complete"] == 0
    assert metadata["rows_ingested"] == 2  # 1 per chunk, 2 chunks
    assert metadata["bundles_loaded"] == 2
    assert metadata["observations_posted"] == 2
    assert metadata["observations_deleted"] == 0


@patch("aqueduct_dagster.defs.jobs.backfill.build_frost_loader")
@patch("aqueduct_dagster.defs.jobs.backfill.BackfillCheckpointStore")
@patch("aqueduct_dagster.defs.jobs.backfill._gcs_filesystem")
@patch("aqueduct_dagster.defs.jobs.backfill._gcs_bucket_url")
def test_frost_watermark_dataset_is_isolated_from_production(
    mock_bucket_url, mock_fs, mock_checkpoint_cls, mock_build_loader
):
    """
    The FROST watermark store must use a distinct dataset from the one the
    normal scheduled pipeline uses, so a backfill run can never race with, or
    silently advance/clobber, production's own per-datastream watermark.
    """
    mock_bucket_url.return_value = "gs://bucket"
    mock_checkpoint_cls.return_value.is_complete.return_value = False

    client = MagicMock()
    prepare_fn = MagicMock(return_value=(client, ["loc"], {111: {}}))
    run_chunk_fn = MagicMock(return_value=_stub_chunk_result())

    job = _make_backfill_refetch_job("test", "raw_pvacd", prepare_fn, run_chunk_fn)
    job.execute_in_process(run_config=_run_config(dry_run=False))

    mock_build_loader.assert_called_once()
    called_dataset = mock_build_loader.call_args[0][1]
    assert called_dataset == "raw_pvacd_backfill"
    assert called_dataset != "raw_pvacd"  # never the same dataset production uses


@patch("aqueduct_dagster.defs.jobs.backfill.forward_python_logs_to_dagster")
@patch("aqueduct_dagster.defs.jobs.backfill.build_frost_loader")
@patch("aqueduct_dagster.defs.jobs.backfill.BackfillCheckpointStore")
@patch("aqueduct_dagster.defs.jobs.backfill._gcs_filesystem")
@patch("aqueduct_dagster.defs.jobs.backfill._gcs_bucket_url")
def test_real_run_forwards_python_logs_with_source_specific_prefix(
    mock_bucket_url, mock_fs, mock_checkpoint_cls, mock_build_loader, mock_log_forward
):
    """
    Regression test: prepare_fn()/run_chunk_fn() emit per-location/per-page
    progress via stdlib logging (see sources/hydrovu/dlt_pipeline.py), which
    only reaches the Dagster run log if forward_python_logs_to_dagster wraps
    the call — this was originally missing, leaving a silent multi-minute gap
    in real backfill runs. Also covers BackfillCheckpointStore's own logger
    ("aqueduct_dagster.shared.backfill"), which needs its own
    "aqueduct_dagster.shared" prefix since it isn't a descendant of
    "aqueduct_dagster.sources.{name}" in the logging hierarchy.
    """
    mock_bucket_url.return_value = "gs://bucket"
    mock_checkpoint_cls.return_value.is_complete.return_value = False

    client = MagicMock()
    prepare_fn = MagicMock(return_value=(client, ["loc"], {111: {}}))
    run_chunk_fn = MagicMock(return_value=_stub_chunk_result())

    job = _make_backfill_refetch_job("test", "test_dataset", prepare_fn, run_chunk_fn)
    result = job.execute_in_process(run_config=_run_config(dry_run=False))

    assert result.success
    mock_log_forward.assert_called_once()
    _context, *prefixes = mock_log_forward.call_args[0]
    assert prefixes == ["aqueduct_dagster.sources.test", "aqueduct_dagster.shared", "dlt"]


@patch("aqueduct_dagster.defs.jobs.backfill.build_frost_loader")
@patch("aqueduct_dagster.defs.jobs.backfill.BackfillCheckpointStore")
@patch("aqueduct_dagster.defs.jobs.backfill._gcs_filesystem")
@patch("aqueduct_dagster.defs.jobs.backfill._gcs_bucket_url")
def test_already_checkpointed_chunk_is_skipped(
    mock_bucket_url, mock_fs, mock_checkpoint_cls, mock_build_loader
):
    mock_bucket_url.return_value = "gs://bucket"
    mock_checkpoint = mock_checkpoint_cls.return_value
    # First chunk (Jan) already done; second (Feb) is not.
    mock_checkpoint.is_complete.side_effect = [True, False]

    client = MagicMock()
    prepare_fn = MagicMock(return_value=(client, ["loc"], {111: {}}))
    run_chunk_fn = MagicMock(return_value=_stub_chunk_result())

    job = _make_backfill_refetch_job("test", "test_dataset", prepare_fn, run_chunk_fn)
    result = job.execute_in_process(run_config=_run_config(dry_run=False))

    assert result.success
    assert run_chunk_fn.call_count == 1  # only the un-checkpointed chunk
    assert mock_checkpoint.mark_complete.call_count == 1

    metadata = _step_output_metadata(result, "test_backfill_refetch_op")
    assert metadata["chunks_processed"] == 1
    assert metadata["chunks_skipped_already_complete"] == 1


@patch("aqueduct_dagster.defs.jobs.backfill.build_frost_loader")
@patch("aqueduct_dagster.defs.jobs.backfill.BackfillCheckpointStore")
@patch("aqueduct_dagster.defs.jobs.backfill._gcs_filesystem")
@patch("aqueduct_dagster.defs.jobs.backfill._gcs_bucket_url")
def test_failed_chunk_is_not_checkpointed_and_job_fails(
    mock_bucket_url, mock_fs, mock_checkpoint_cls, mock_build_loader
):
    mock_bucket_url.return_value = "gs://bucket"
    mock_checkpoint = mock_checkpoint_cls.return_value
    mock_checkpoint.is_complete.return_value = False

    client = MagicMock()
    prepare_fn = MagicMock(return_value=(client, ["loc"], {111: {}}))
    run_chunk_fn = MagicMock(side_effect=RuntimeError("chunk failed"))

    job = _make_backfill_refetch_job("test", "test_dataset", prepare_fn, run_chunk_fn)
    result = job.execute_in_process(run_config=_run_config(dry_run=False), raise_on_error=False)

    assert not result.success
    mock_checkpoint.mark_complete.assert_not_called()
    client.close.assert_called_once()  # cleanup still runs via finally
