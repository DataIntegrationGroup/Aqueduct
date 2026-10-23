"""
Unit tests for raw_ebwpc_ckan_readings: its metadata, its failure policy
(partial failure still materializes, total failure and a failed package_show
raise), and one real run into a local file:// bucket proving where the
parquet lands. Offline throughout.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from dagster import Failure, MaterializeResult, build_asset_context

from aqueduct_dagster.sources.ebwpc_ckan.ingest import raw_ebwpc_ckan_readings
from tests.sources.ebwpc_ckan.conftest import (
    PACKAGE_SHOW_PATH,
    WELL_ROWS,
    FakeCatalog,
    parquet_files,
)

_MODULE = "aqueduct_dagster.sources.ebwpc_ckan.ingest"


def _run_asset(stats: dict) -> MaterializeResult:
    """Materializes the asset with a fake dlt pipeline whose run() reports
    `stats`. The asset passes its own dict into the source and reads it back
    after run(), so the fake must copy into that same dict, not return a new one."""
    pipeline = MagicMock()
    pipeline.pipeline_name = "ebwpc_ckan"
    pipeline.dataset_name = "raw_ebwpc_ckan"

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
        patch(f"{_MODULE}.ebwpc_ckan_source", side_effect=_fake_source),
        build_asset_context() as context,
    ):
        return raw_ebwpc_ckan_readings(context)


def _metadata(result: MaterializeResult) -> dict:
    return {key: value.value for key, value in (result.metadata or {}).items()}


_FIRST_RUN = {
    "catalog_resources": 5,
    "rows_yielded": 11,
    "resources_selected": 3,
    "resources_landed": 3,
    "resources_unchanged": 0,
    "resources_same_content": 0,
    "resources_errored": 0,
    "failed_resources": [],
    "resources_skipped": {"E-9407-Archived": "format XLSX: only CSV is ingested"},
    "resources_added": ["E-8428", "E-1639-POD1", "E-9673"],
    "resources_removed": [],
    "locations_rows": 3,
    "locations_error": None,
}
_QUIET_WEEK = {
    **_FIRST_RUN,
    "rows_yielded": 0,
    "resources_landed": 0,
    "resources_unchanged": 3,
    "resources_added": [],
}


class TestMetadata:
    def test_reports_extraction_counts(self):
        metadata = _metadata(_run_asset(_FIRST_RUN))
        assert metadata["catalog_resources"] == 5
        assert metadata["rows_yielded"] == 11
        assert metadata["resources_selected"] == 3
        assert metadata["resources_landed"] == 3
        assert metadata["resources_errored"] == 0
        assert metadata["resources_skipped"] == _FIRST_RUN["resources_skipped"]
        assert metadata["resources_added"] == ["E-8428", "E-1639-POD1", "E-9673"]
        assert metadata["locations_rows"] == 3
        assert metadata["locations_error"] == ""

    def test_reports_where_the_data_landed(self):
        metadata = _metadata(_run_asset(_FIRST_RUN))
        assert metadata["pipeline_name"] == "ebwpc_ckan"
        assert metadata["dataset_name"] == "raw_ebwpc_ckan"

    def test_empty_stats_fall_back_to_zeros_rather_than_raising(self):
        # If dlt abandons the resource generator mid-run the stats dict is never
        # fully populated; zeros are the intended outcome, not a KeyError.
        metadata = _metadata(_run_asset({}))
        assert metadata["rows_yielded"] == 0
        assert metadata["resources_landed"] == 0


class TestFailurePolicy:
    def test_an_unchanged_dataset_is_a_green_run(self):
        # The normal weekly case: nothing downloaded, nothing landed, nothing wrong.
        metadata = _metadata(_run_asset(_QUIET_WEEK))
        assert metadata["resources_unchanged"] == 3
        assert metadata["rows_yielded"] == 0

    def test_partial_failure_still_materializes(self):
        metadata = _metadata(
            _run_asset(
                {
                    **_FIRST_RUN,
                    "resources_landed": 2,
                    "resources_errored": 1,
                    "failed_resources": ["E-8428"],
                }
            )
        )
        assert metadata["resources_errored"] == 1
        assert metadata["failed_resources"] == ["E-8428"]

    def test_failed_locations_alone_still_materializes(self):
        metadata = _metadata(
            _run_asset({**_FIRST_RUN, "locations_rows": 0, "locations_error": "HTTP 500"})
        )
        assert metadata["locations_error"] == "HTTP 500"

    def test_total_failure_raises(self):
        with pytest.raises(Failure) as exc_info:
            _run_asset(
                {
                    **_FIRST_RUN,
                    "resources_landed": 0,
                    "resources_errored": 3,
                    "failed_resources": ["E-8428", "E-1639-POD1", "E-9673"],
                    "locations_rows": 0,
                    "locations_error": "HTTP 503",
                }
            )
        assert "0 succeeded" in str(exc_info.value.description)
        assert exc_info.value.metadata["failed_resources"].value == [
            "E-8428",
            "E-1639-POD1",
            "E-9673",
        ]

    def test_package_show_rejected_by_cloudflare_raises(self, catalog: FakeCatalog):
        catalog.routes[PACKAGE_SHOW_PATH] = httpx.Response(403)
        pipeline = MagicMock(pipeline_name="ebwpc_ckan", dataset_name="raw_ebwpc_ckan")
        with (
            patch(f"{_MODULE}.build_pipeline", return_value=pipeline),
            build_asset_context() as context,
            pytest.raises(Failure) as exc_info,
        ):
            raw_ebwpc_ckan_readings(context)
        assert "x-cf-bypass" in str(exc_info.value.description)
        pipeline.run.assert_not_called()


class TestWhereDataLands:
    def test_writes_parquet_to_the_raw_zone_path(self, catalog: FakeCatalog, local_bucket: Path):
        with build_asset_context() as context:
            metadata = _metadata(raw_ebwpc_ckan_readings(context))

        assert metadata["rows_yielded"] == WELL_ROWS
        assert metadata["resources_landed"] == 3
        assert list(metadata["resources_skipped"]) == ["E-9407-Archived"]
        # gs://<bucket>/raw_<source_key>/<table>/year=/month=/day=/<load_id>.<file_id>.parquet
        for table in ("ckan_readings", "ckan_locations", "ckan_resources"):
            files = parquet_files(local_bucket, table)
            assert files, table
            assert all(path.parent.parent.parent.name.startswith("year=") for path in files)
