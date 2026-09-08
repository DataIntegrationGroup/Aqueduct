"""
tests/sources/bernco_hydrovu/test_ingest.py

Unit tests for the raw_bernco_hydrovu_readings Dagster asset.

What is worth testing here is the asset's failure policy, which is the part that
decides whether a human gets paged: a run where some stations failed still
materializes (their cursors did not advance, so the next run retries them), while a
run where every station failed raises, because a green run that landed nothing would
hide an expired credential or a dead API.

Offline: build_pipeline and bernco_hydrovu_source are patched, so no dlt pipeline is
constructed and no GCS or HydroVu call is made. The stats dict the asset reads is
populated by the fake pipeline.run(), standing in for what iter_location_readings
writes at the end of a real extract.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from dagster import Failure, MaterializeResult, build_asset_context

from aqueduct_dagster.sources.bernco_hydrovu.ingest import raw_bernco_hydrovu_readings

_MODULE = "aqueduct_dagster.sources.bernco_hydrovu.ingest"


def _run_asset(stats: dict) -> MaterializeResult:
    """
    Materializes the asset with a fake dlt pipeline whose run() reports `stats`.

    The asset passes its own dict into the source and reads it back after run(),
    so the fake has to copy into that same dict rather than return a new one.
    """
    pipeline = MagicMock()
    pipeline.pipeline_name = "bernco_hydrovu"
    pipeline.dataset_name = "raw_bernco_hydrovu"

    captured: dict = {}

    def _fake_source(_stats: dict | None = None, **_kwargs: object) -> object:
        assert _stats is not None
        captured["stats"] = _stats
        return MagicMock()

    def _fake_run(*_args: object, **_kwargs: object) -> str:
        captured["stats"].update(stats)
        return "LoadInfo(...)"

    pipeline.run.side_effect = _fake_run

    with (
        patch(f"{_MODULE}.build_pipeline", return_value=pipeline),
        patch(f"{_MODULE}.bernco_hydrovu_source", side_effect=_fake_source),
        build_asset_context() as context,
    ):
        return raw_bernco_hydrovu_readings(context)


_ALL_GOOD = {
    "rows_yielded": 42,
    "locations_fetched": 2,
    "locations_skipped": 51,
    "locations_no_data": 0,
    "locations_errored": 0,
    "failed_location_ids": [],
}


class TestMetadata:
    def test_reports_extraction_counts(self):
        result = _run_asset(_ALL_GOOD)
        metadata = {key: value.value for key, value in result.metadata.items()}
        assert metadata["rows_yielded"] == 42
        assert metadata["locations_fetched"] == 2
        assert metadata["locations_skipped_allowlist"] == 51
        assert metadata["locations_no_data"] == 0
        assert metadata["locations_errored"] == 0

    def test_reports_where_the_data_landed(self):
        result = _run_asset(_ALL_GOOD)
        metadata = {key: value.value for key, value in result.metadata.items()}
        assert metadata["pipeline_name"] == "bernco_hydrovu"
        assert metadata["dataset_name"] == "raw_bernco_hydrovu"

    def test_empty_stats_fall_back_to_zeros_rather_than_raising(self):
        # If dlt abandons the resource generator mid-run the stats dict is never
        # populated. Metadata showing zeros is the intended outcome — a KeyError here
        # would turn a partial extract into a hard asset failure.
        result = _run_asset({})
        metadata = {key: value.value for key, value in result.metadata.items()}
        assert metadata["rows_yielded"] == 0
        assert metadata["locations_fetched"] == 0


class TestFailurePolicy:
    def test_partial_failure_still_materializes(self):
        result = _run_asset(
            {
                **_ALL_GOOD,
                "locations_fetched": 1,
                "locations_errored": 1,
                "failed_location_ids": [7],
            }
        )
        metadata = {key: value.value for key, value in result.metadata.items()}
        assert metadata["locations_errored"] == 1
        assert metadata["failed_location_ids"] == "[7]"

    def test_total_failure_raises(self):
        with pytest.raises(Failure) as exc_info:
            _run_asset(
                {
                    **_ALL_GOOD,
                    "locations_fetched": 0,
                    "locations_errored": 2,
                    "failed_location_ids": [7, 9],
                }
            )
        assert "0 fetched" in str(exc_info.value.description)
        assert exc_info.value.metadata["failed_location_ids"].value == [7, 9]

    def test_no_locations_fetched_without_errors_does_not_raise(self):
        # Every allowlisted location returned a 404 (all dormant). Nothing landed, but
        # nothing is broken either, so this must not page anyone.
        result = _run_asset(
            {**_ALL_GOOD, "locations_fetched": 0, "locations_no_data": 2, "rows_yielded": 0}
        )
        metadata = {key: value.value for key, value in result.metadata.items()}
        assert metadata["locations_no_data"] == 2
