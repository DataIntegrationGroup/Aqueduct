"""
Unit tests for raw_ebid_readings' failure policy: partial failure still
materializes, total failure raises. Offline: build_pipeline/ebid_source patched.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from dagster import Failure, MaterializeResult, build_asset_context

from aqueduct_dagster.sources.ebid.ingest import raw_ebid_readings

_MODULE = "aqueduct_dagster.sources.ebid.ingest"


def _run_asset(stats: dict) -> MaterializeResult:
    """Materializes the asset with a fake pipeline whose run() reports `stats`
    by copying into the asset's own dict, not returning a new one."""
    pipeline = MagicMock()
    pipeline.pipeline_name = "ebid"
    pipeline.dataset_name = "raw_ebid"

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
        patch(f"{_MODULE}.ebid_source", side_effect=_fake_source),
        build_asset_context() as context,
    ):
        return raw_ebid_readings(context)


_ALL_GOOD = {
    "rows_yielded": 42,
    "sites_fetched": 6,
    "sites_skipped": 3,
    "sites_errored": 0,
    "failed_site_ids": [],
}


class TestMetadata:
    def test_reports_extraction_counts(self):
        result = _run_asset(_ALL_GOOD)
        metadata = {key: value.value for key, value in result.metadata.items()}
        assert metadata["rows_yielded"] == 42
        assert metadata["sites_fetched"] == 6
        assert metadata["sites_skipped_not_a_well"] == 3
        assert metadata["sites_errored"] == 0

    def test_reports_where_the_data_landed(self):
        result = _run_asset(_ALL_GOOD)
        metadata = {key: value.value for key, value in result.metadata.items()}
        assert metadata["pipeline_name"] == "ebid"
        assert metadata["dataset_name"] == "raw_ebid"

    def test_empty_stats_fall_back_to_zeros_rather_than_raising(self):
        # If dlt abandons the generator mid-run, stats stays empty — zeros
        # here, not a KeyError, is the intended outcome.
        result = _run_asset({})
        metadata = {key: value.value for key, value in result.metadata.items()}
        assert metadata["rows_yielded"] == 0
        assert metadata["sites_fetched"] == 0


class TestFailurePolicy:
    def test_partial_failure_still_materializes(self):
        result = _run_asset(
            {**_ALL_GOOD, "sites_fetched": 8, "sites_errored": 1, "failed_site_ids": ["840"]}
        )
        metadata = {key: value.value for key, value in result.metadata.items()}
        assert metadata["sites_errored"] == 1
        assert metadata["failed_site_ids"] == "['840']"

    def test_total_failure_raises(self):
        with pytest.raises(Failure) as exc_info:
            _run_asset(
                {
                    **_ALL_GOOD,
                    "sites_fetched": 0,
                    "sites_errored": 9,
                    "failed_site_ids": ["1164", "1165"],
                }
            )
        assert "0 fetched" in str(exc_info.value.description)
        assert exc_info.value.metadata["failed_site_ids"].value == ["1164", "1165"]

    def test_no_sites_fetched_without_errors_does_not_raise(self):
        result = _run_asset({**_ALL_GOOD, "sites_fetched": 0, "rows_yielded": 0})
        metadata = {key: value.value for key, value in result.metadata.items()}
        assert metadata["sites_fetched"] == 0
        assert metadata["sites_errored"] == 0
