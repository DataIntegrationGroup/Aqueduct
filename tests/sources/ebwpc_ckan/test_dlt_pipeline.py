"""
Unit tests for ebwpc_ckan_source and its two resources. The fingerprint
cursor itself is covered agency-agnostically in tests/sources/test_ckan_common.py;
this module covers what EBWPC owns (config block, resource selection, the
locations table, client lifetime) and runs the real dlt pipeline end to end
into a local file:// bucket, so the cursor is proven through real dlt state.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from aqueduct_dagster.shared.config import settings_dir
from aqueduct_dagster.sources.ebwpc_ckan.dlt_pipeline import (
    build_pipeline,
    ckan_readings,
    ebwpc_ckan_source,
)
from tests.sources.ebwpc_ckan.conftest import (
    CATALOG,
    DATASET_ID,
    E_8428,
    E_9407_ARCHIVED,
    LOCATIONS,
    PACKAGE_SHOW_PATH,
    WELL_ROWS,
    WELLS,
    FakeCatalog,
    package_show_response,
    parquet_files,
    path_of,
)


def _source(stats: dict | None = None):
    settings_dir()  # locate .dlt/config.toml without depending on the cwd
    return ebwpc_ckan_source(_stats=stats if stats is not None else {})


def _table_names(source) -> set[str]:
    return {resource.name for resource in source.resources.values()}


# ── ebwpc_ckan_source — config binding and resource selection ─────────────────


class TestSourceConfig:
    """@dlt.source(name="ebwpc_ckan") binds its defaults to [sources.ebwpc_ckan]
    in .dlt/config.toml — nothing else cross-checks the two."""

    def test_reads_its_own_config_block(self, catalog: FakeCatalog):
        _source()
        assert catalog.api_base_urls == [CATALOG]
        assert catalog.gcp_secrets == ["ckan_cf_bypass"]
        package_show = catalog.runs[-1][0]
        assert package_show.url.path == PACKAGE_SHOW_PATH
        # The UUID, not the renamable ebwpc-gw-monitoring slug.
        assert package_show.url.params["id"] == DATASET_ID

    def test_sends_the_bypass_header_on_every_request(self, catalog: FakeCatalog):
        _source()
        assert all(request.headers["x-cf-bypass"] for request in catalog.runs[-1])

    def test_exposes_both_resources_under_platform_table_names(self, catalog: FakeCatalog):
        assert _table_names(_source()) == {"ckan_locations", "ckan_readings"}

    def test_skips_the_xlsx_archive_and_reports_it(self, catalog: FakeCatalog):
        stats: dict = {}
        _source(stats)
        assert list(stats["resources_skipped"]) == [E_9407_ARCHIVED["name"]]
        assert path_of(E_9407_ARCHIVED) not in catalog.paths()

    def test_disables_iso_timestamp_detection_so_raw_cells_stay_strings(self, catalog: FakeCatalog):
        assert "iso_timestamp" not in _source().schema.settings.get("detections", [])


class TestLocations:
    def test_locations_file_is_downloaded_up_front(self, catalog: FakeCatalog):
        stats: dict = {}
        _source(stats)
        assert path_of(LOCATIONS) in catalog.paths()
        assert stats["locations_rows"] == 3
        assert stats["locations_error"] is None

    def test_failed_locations_download_omits_the_table_rather_than_blanking_it(
        self, catalog: FakeCatalog
    ):
        # A replace with no rows could wipe the last good snapshot; the readings
        # must still run.
        catalog.routes[path_of(LOCATIONS)] = httpx.Response(500)
        stats: dict = {}
        assert _table_names(_source(stats)) == {"ckan_readings"}
        assert stats["locations_error"] == "HTTP 500"
        assert stats["locations_rows"] == 0

    def test_missing_locations_resource_omits_the_table(self, catalog: FakeCatalog):
        catalog.routes[PACKAGE_SHOW_PATH] = package_show_response(WELLS)
        stats: dict = {}
        assert _table_names(_source(stats)) == {"ckan_readings"}
        assert "EBWPC Well Locations" in stats["locations_error"]

    def test_header_only_locations_file_omits_the_table(self, catalog: FakeCatalog):
        catalog.routes[path_of(LOCATIONS)] = httpx.Response(200, content=b"NMBG_ID,Datum\r\n")
        stats: dict = {}
        assert _table_names(_source(stats)) == {"ckan_readings"}
        assert stats["locations_error"] == "file has no data rows"


# ── client lifetime ───────────────────────────────────────────────────────────


class TestClientLifetime:
    def test_closes_the_client_when_package_show_fails(self, catalog: FakeCatalog):
        # ckan_readings normally closes it, but is never constructed here.
        catalog.routes[PACKAGE_SHOW_PATH] = httpx.Response(403)
        with pytest.raises(httpx.HTTPStatusError):
            _source()
        assert catalog.clients[-1].is_closed

    @pytest.mark.parametrize("status", [200, 503])
    def test_ckan_readings_closes_the_client_when_it_finishes(
        self, catalog: FakeCatalog, status: int
    ):
        if status != 200:
            for resource in WELLS:
                catalog.routes[path_of(resource)] = httpx.Response(status)
        client = catalog.build_client(CATALOG, "token")
        state: dict = {}
        with patch("dlt.current.resource_state", return_value=state), patch("time.sleep"):
            list(ckan_readings(client=client, resources=WELLS))
        assert client.is_closed

    def test_ckan_readings_keeps_fingerprints_in_resource_state(self, catalog: FakeCatalog):
        state: dict = {}
        with patch("dlt.current.resource_state", return_value=state):
            rows = list(ckan_readings(client=catalog.build_client(CATALOG, "t"), resources=WELLS))
        assert len(rows) == WELL_ROWS
        assert set(state["resource_fingerprints"]) == {resource["id"] for resource in WELLS}


# ── end to end: real dlt into a local file:// bucket ──────────────────────────

_PARQUET_PATH = re.compile(
    r"^raw_ebwpc_ckan/(ckan_readings|ckan_locations)/"
    r"year=\d{4}/month=\d{2}/day=\d{2}/\d+\.\d+\.\w+\.parquet$"
)


def _run() -> dict:
    """One scheduled run: a fresh build_pipeline() + source, as each Dagster run does."""
    stats: dict = {}
    pipeline = build_pipeline()
    pipeline.run(ebwpc_ckan_source(_stats=stats), loader_file_format="parquet")
    return stats


def _read(files: list[Path]) -> pa.Table:
    return pa.concat_tables([pq.read_table(path) for path in files])


class TestEndToEnd:
    def test_first_run_lands_parquet_at_the_raw_zone_path(
        self, catalog: FakeCatalog, local_bucket: Path
    ):
        stats = _run()
        assert stats["resources_landed"] == 3

        readings = parquet_files(local_bucket, "ckan_readings")
        locations = parquet_files(local_bucket, "ckan_locations")
        assert readings and locations
        for path in readings + locations:
            assert _PARQUET_PATH.match(path.relative_to(local_bucket).as_posix())

        table = _read(readings)
        assert table.num_rows == WELL_ROWS
        assert _read(locations).num_rows == 3

    def test_every_source_cell_lands_as_a_string(self, catalog: FakeCatalog, local_bucket: Path):
        _run()
        schema = _read(parquet_files(local_bucket, "ckan_readings")).schema
        for column in (
            "site_id",
            "measurement_date",
            "measurement_time",
            "transducer_depth_bgs",
            "manual_depth_bgs",
            "column_6",  # e-9673's blank trailing header
            "resource_last_modified",  # ISO-looking, but not detected as a timestamp
            "reading_id",
        ):
            assert schema.field(column).type == pa.string(), column

    def test_second_run_fetches_only_what_changed(self, catalog: FakeCatalog, local_bucket: Path):
        _run()
        readings_before = parquet_files(local_bucket, "ckan_readings")
        # Dagster+ runs start on an empty disk: wiping dlt's local working dir
        # proves the cursor comes back from the bucket's _dlt_pipeline_state.
        shutil.rmtree(local_bucket.parent / "dlt")

        stats = _run()
        assert catalog.well_paths() == []
        assert stats["resources_unchanged"] == 3
        assert stats["rows_yielded"] == 0
        assert parquet_files(local_bucket, "ckan_readings") == readings_before
        # Replace keeps exactly one locations snapshot.
        assert len(parquet_files(local_bucket, "ckan_locations")) == 1

        # A re-upload with new content lands that one well again, and only it.
        catalog.routes[PACKAGE_SHOW_PATH] = package_show_response(
            [{**E_8428, "last_modified": "2026-01-01T00:00:00", "size": 1}, *WELLS[1:], LOCATIONS]
        )
        catalog.routes[path_of(E_8428)] = httpx.Response(
            200, content=b"site_id,measurement_date\r\ne-8428,10/1/2025\r\n"
        )
        stats = _run()
        assert catalog.well_paths() == [path_of(E_8428)]
        assert stats["resources_landed"] == 1
        assert stats["rows_yielded"] == 1

    def test_a_failed_well_does_not_block_the_others_and_retries_alone(
        self, catalog: FakeCatalog, local_bucket: Path
    ):
        catalog.routes[path_of(E_8428)] = httpx.Response(500)
        stats = _run()
        assert stats["resources_errored"] == 1
        assert stats["failed_resources"] == ["E-8428"]
        assert stats["resources_landed"] == 2
        sites = set(_read(parquet_files(local_bucket, "ckan_readings"))["site_id"].to_pylist())
        assert sites == {"e-1639-pod1", "e-9673"}

        catalog.routes[path_of(E_8428)] = httpx.Response(200, content=b"site_id\r\ne-8428\r\n")
        stats = _run()
        assert catalog.well_paths() == [path_of(E_8428)]
        assert stats["resources_landed"] == 1
        assert stats["resources_unchanged"] == 2
