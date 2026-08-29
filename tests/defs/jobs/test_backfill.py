"""
tests/defs/jobs/test_backfill.py

Unit tests for the generic backfill-refetch job factory
(_make_backfill_refetch_job / _make_backfill_refetch_op), and for
BackfillRefetchConfig's own validation (dates, run_key timestamping).

Built and executed with stub prepare_fn/run_chunk_fn (not HydroVu's real
ones) via Dagster's execute_in_process — no live API/GCS/FROST required.
GCS and FROST-loader construction inside the op are mocked. prepare_fn() is
now called even during dry_run (a read, not a write — see backfill.py), so
every test configures a realistic return value for it, not a bare MagicMock().
"""

from __future__ import annotations

import re
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from aqueduct_dagster.defs.jobs.backfill import BackfillRefetchConfig, _make_backfill_refetch_job

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


def _run_config(*, dry_run: bool, **overrides: object) -> dict:
    cfg = {
        **_RUN_CONFIG["ops"]["test_backfill_refetch_op"]["config"],
        "dry_run": dry_run,
        **overrides,
    }
    return {"ops": {"test_backfill_refetch_op": {"config": cfg}}}


def _prepare_fn(locations_by_id: dict[int, dict] | None = None) -> MagicMock:
    """A prepare_fn() stub returning (client, locations, locations_by_id)."""
    locations_by_id = {111: {}} if locations_by_id is None else locations_by_id
    client = MagicMock()
    return MagicMock(return_value=(client, list(locations_by_id), locations_by_id))


def _stub_chunk_result(**overrides: int) -> SimpleNamespace:
    defaults = dict(
        rows_ingested=1,
        bundles_loaded=1,
        observations_posted=1,
        observations_deleted=0,
        adapter_failures=0,
    )
    return SimpleNamespace(**{**defaults, **overrides})


def _step_output_metadata(result, op_name: str) -> dict:
    """Extracts the metadata dict attached via context.add_output_metadata()."""
    for event in result.all_events:
        if event.event_type_value == "STEP_OUTPUT" and event.step_key == op_name:
            return {k: v.value for k, v in event.event_specific_data.metadata.items()}
    raise AssertionError(f"no STEP_OUTPUT event found for step {op_name!r}")


def _step_failure_message(result, op_name: str) -> str:
    """Extracts the raised exception's message from a failed step."""
    for event in result.all_events:
        if event.event_type_value == "STEP_FAILURE" and event.step_key == op_name:
            return event.event_specific_data.error.cause.message
    raise AssertionError(f"no STEP_FAILURE event found for step {op_name!r}")


# ── BackfillRefetchConfig validation ────────────────────────────────────────────


def test_config_prefilled_with_example_values():
    config = BackfillRefetchConfig()
    assert config.location_ids == []
    assert config.start_date and config.end_date
    assert config.dry_run is True


def test_default_run_key_is_timestamped_even_when_left_untouched():
    """
    Regression test: pydantic skips field validators on a value the caller
    never supplied, unless validate_default=True — so an operator who leaves
    run_key at its prefilled default (the common case) must still get a
    timestamp attached, not silently keep the bare "example-backfill" label
    every launch would otherwise collide on.
    """
    assert re.match(r"^example-backfill_\d{8}T\d{6}Z$", BackfillRefetchConfig().run_key)


@pytest.mark.parametrize("field", ["start_date", "end_date"])
def test_malformed_date_is_rejected(field):
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        BackfillRefetchConfig(**{field: "01/01/2026"})


def test_start_date_must_be_before_end_date():
    with pytest.raises(ValueError, match="must be before"):
        BackfillRefetchConfig(start_date="2026-03-01", end_date="2026-01-01")


def test_run_key_gets_a_timestamp_attached():
    config = BackfillRefetchConfig(run_key="my-label")
    assert re.match(r"^my-label_\d{8}T\d{6}Z$", config.run_key)


def test_already_timestamped_run_key_is_left_unchanged():
    timestamped = "my-label_20260101T000000Z"
    config = BackfillRefetchConfig(run_key=timestamped)
    assert config.run_key == timestamped


# ── dry_run ────────────────────────────────────────────────────────────────────


def test_dry_run_calls_prepare_but_never_run_chunk():
    prepare_fn = _prepare_fn()
    run_chunk_fn = MagicMock()
    job = _make_backfill_refetch_job("test", "test_dataset", prepare_fn, run_chunk_fn)

    result = job.execute_in_process(run_config=_run_config(dry_run=True))

    assert result.success
    prepare_fn.assert_called_once()
    run_chunk_fn.assert_not_called()


@patch("aqueduct_dagster.defs.jobs.backfill.BackfillCheckpointStore")
def test_dry_run_never_touches_checkpoint_store(mock_checkpoint_cls):
    job = _make_backfill_refetch_job("test", "test_dataset", _prepare_fn(), MagicMock())
    result = job.execute_in_process(run_config=_run_config(dry_run=True))

    assert result.success
    mock_checkpoint_cls.assert_not_called()


def test_dry_run_attaches_plan_metadata():
    job = _make_backfill_refetch_job("test", "test_dataset", _prepare_fn(), MagicMock())
    result = job.execute_in_process(run_config=_run_config(dry_run=True))

    metadata = _step_output_metadata(result, "test_backfill_refetch_op")
    assert metadata["dry_run"] is True
    assert metadata["location_count"] == 1
    assert metadata["chunks_planned"] == 2


def test_dry_run_with_empty_location_ids_resolves_to_all_locations():
    prepare_fn = _prepare_fn({111: {}, 222: {}, 333: {}})
    job = _make_backfill_refetch_job("test", "test_dataset", prepare_fn, MagicMock())

    result = job.execute_in_process(run_config=_run_config(dry_run=True, location_ids=[]))

    assert result.success
    metadata = _step_output_metadata(result, "test_backfill_refetch_op")
    assert metadata["location_count"] == 3


def test_unknown_location_id_fails_the_run_with_source_prefixed_message():
    prepare_fn = _prepare_fn({111: {}})
    job = _make_backfill_refetch_job("test", "test_dataset", prepare_fn, MagicMock())

    result = job.execute_in_process(
        run_config=_run_config(dry_run=True, location_ids=[999]), raise_on_error=False
    )

    assert not result.success
    message = _step_failure_message(result, "test_backfill_refetch_op")
    assert "test backfill:" in message
    assert "[999]" in message


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

    prepare_fn = _prepare_fn()
    client = prepare_fn.return_value[0]
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

    prepare_fn = _prepare_fn()
    run_chunk_fn = MagicMock(return_value=_stub_chunk_result())

    job = _make_backfill_refetch_job("test", "raw_pvacd_hydrovu", prepare_fn, run_chunk_fn)
    job.execute_in_process(run_config=_run_config(dry_run=False))

    mock_build_loader.assert_called_once()
    called_dataset = mock_build_loader.call_args[0][1]
    assert called_dataset == "raw_pvacd_hydrovu_backfill"
    assert called_dataset != "raw_pvacd_hydrovu"  # never the same dataset production uses


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
    progress via stdlib logging (see sources/pvacd_hydrovu/dlt_pipeline.py), which
    only reaches the Dagster run log if forward_python_logs_to_dagster wraps
    the call — this was originally missing, leaving a silent multi-minute gap
    in real backfill runs. Also covers BackfillCheckpointStore's own logger
    ("aqueduct_dagster.shared.backfill") and BaseAdapter's
    ("aqueduct_dagster.canonical.base_adapter"), neither of which is a
    descendant of "aqueduct_dagster.sources.{name}" in the logging hierarchy,
    so each needs its own prefix.
    """
    mock_bucket_url.return_value = "gs://bucket"
    mock_checkpoint_cls.return_value.is_complete.return_value = False

    prepare_fn = _prepare_fn()
    run_chunk_fn = MagicMock(return_value=_stub_chunk_result())

    job = _make_backfill_refetch_job("test", "test_dataset", prepare_fn, run_chunk_fn)
    result = job.execute_in_process(run_config=_run_config(dry_run=False))

    assert result.success
    mock_log_forward.assert_called_once()
    _context, *prefixes = mock_log_forward.call_args[0]
    assert prefixes == [
        "aqueduct_dagster.sources.test",
        "aqueduct_dagster.shared",
        "aqueduct_dagster.canonical",
        "dlt",
    ]


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

    prepare_fn = _prepare_fn()
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

    prepare_fn = _prepare_fn()
    client = prepare_fn.return_value[0]
    run_chunk_fn = MagicMock(side_effect=RuntimeError("chunk failed"))

    job = _make_backfill_refetch_job("test", "test_dataset", prepare_fn, run_chunk_fn)
    result = job.execute_in_process(run_config=_run_config(dry_run=False), raise_on_error=False)

    assert not result.success
    mock_checkpoint.mark_complete.assert_not_called()
    client.close.assert_called_once()  # cleanup still runs via finally
