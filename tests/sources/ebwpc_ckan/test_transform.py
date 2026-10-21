"""
End-to-end tests for canonical_bundles_ebwpc_ckan: the real dlt pipeline lands
the mapping doc's samples (conftest.py) in a local file:// bucket, then the
asset reads them back through a local filesystem standing in for GCS. Offline.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import fsspec
import httpx
from dagster import build_asset_context

from aqueduct_dagster.canonical.canonical_constants import METRES_TO_FEET
from aqueduct_dagster.shared.gcs import write_transform_watermark
from aqueduct_dagster.sources.ebwpc_ckan.dlt_pipeline import build_pipeline, ebwpc_ckan_source
from aqueduct_dagster.sources.ebwpc_ckan.transform import (
    GCS_DATASET,
    WATERMARK_PATH,
    EbwpcCkanTransformResult,
    canonical_bundles_ebwpc_ckan,
)
from tests.sources.ebwpc_ckan.conftest import (
    E_8428,
    FILES,
    LOCATIONS,
    PACKAGE_SHOW_PATH,
    WELL_ROWS,
    WELLS,
    FakeCatalog,
    package_show_response,
    path_of,
)

_MODULE = "aqueduct_dagster.sources.ebwpc_ckan.transform"
# E-8428 re-uploaded with one new row on top of the published four.
_E_8428_REUPLOADED = FILES[E_8428["id"]] + b"e-8428,10/1/2025,9:00,54.5,55.0\r\n"


def _ingest() -> None:
    build_pipeline().run(ebwpc_ckan_source(_stats={}), loader_file_format="parquet")


def _reupload_e_8428(catalog: FakeCatalog) -> None:
    catalog.routes[PACKAGE_SHOW_PATH] = package_show_response(
        [{**E_8428, "last_modified": "2026-01-01T00:00:00", "size": 1}, *WELLS[1:], LOCATIONS]
    )
    catalog.routes[path_of(E_8428)] = httpx.Response(200, content=_E_8428_REUPLOADED)


def _transform(bucket: Path) -> tuple[EbwpcCkanTransformResult, dict]:
    with (
        patch(f"{_MODULE}._gcs_bucket_url", return_value=str(bucket)),
        patch(f"{_MODULE}._gcs_filesystem", return_value=fsspec.filesystem("file")),
        build_asset_context() as context,
    ):
        result = canonical_bundles_ebwpc_ckan(context)
        metadata = {k: v.value for k, v in context.get_output_metadata("result").items()}
    return result, metadata


def _commit(bucket: Path, max_load_id: float | None) -> None:
    """What frost_load_ebwpc_ckan does after FROST confirms the load."""
    assert max_load_id is not None
    write_transform_watermark(fsspec.filesystem("file"), str(bucket), WATERMARK_PATH, max_load_id)


def _observation_counts(result: EbwpcCkanTransformResult) -> dict[str, int]:
    return {
        key: len(observations)
        for bundle in result.bundles
        for key, observations in bundle.observations.items()
    }


class TestFirstRun:
    def test_maps_every_landed_well(self, catalog: FakeCatalog, local_bucket: Path):
        _ingest()
        result, metadata = _transform(local_bucket)

        assert _observation_counts(result) == {
            "ebwpc-E-8428-dtw": 2,
            "ebwpc-E-8428-dtw-manual": 4,
            "ebwpc-E-1639-POD1-dtw": 2,
            "ebwpc-E-1639-POD1-dtw-manual": 3,
            "ebwpc-E-9673-dtw": 1,
        }
        assert result.max_load_id is not None
        assert metadata["rows_read"] == WELL_ROWS
        assert metadata["rows_superseded"] == 0
        assert metadata["rows_dropped"] == {"no_usable_date": 2}
        assert metadata["measurements_mapped"] == 12
        assert metadata["observations_produced"] == 12
        assert metadata["bundles_produced"] == 3
        assert metadata["sites_without_location"] == {}
        assert metadata["adapter_failures"] == 0
        assert metadata["watermark_before"] == "None"

    def test_reads_elevation_from_ckan_resources(self, catalog: FakeCatalog, local_bucket: Path):
        _ingest()
        result, _ = _transform(local_bucket)
        things = {
            ds.thing.external_key: ds.thing
            for bundle in result.bundles
            for ds in bundle.datastreams
        }
        thing = things["ebwpc-E-8428"]
        assert thing.location.properties["source_specific"]["elevation"]["value"] == (
            6712 / METRES_TO_FEET
        )
        assert thing.properties["source_specific"]["lithology"] == "Madera Formation"

    def test_times_are_utc(self, catalog: FakeCatalog, local_bucket: Path):
        _ingest()
        result, _ = _transform(local_bucket)
        times = [
            obs.phenomenon_time
            for bundle in result.bundles
            for obs in bundle.observations.get("ebwpc-E-8428-dtw", [])
        ]
        assert max(times) == datetime(2024, 4, 26, 15, 18, tzinfo=UTC)


class TestIncremental:
    def test_nothing_new_past_the_watermark_is_an_empty_result(
        self, catalog: FakeCatalog, local_bucket: Path
    ):
        _ingest()
        first, _ = _transform(local_bucket)
        _commit(local_bucket, first.max_load_id)

        _ingest()  # an unchanged week: no new readings land
        result, metadata = _transform(local_bucket)
        assert result.bundles == []
        assert result.max_load_id is None
        assert metadata["rows_read"] == 0
        assert metadata["watermark_before"] == str(first.max_load_id)

    def test_a_reuploaded_well_is_the_only_one_read_again(
        self, catalog: FakeCatalog, local_bucket: Path
    ):
        _ingest()
        first, _ = _transform(local_bucket)
        _commit(local_bucket, first.max_load_id)

        _reupload_e_8428(catalog)
        _ingest()
        result, metadata = _transform(local_bucket)
        assert _observation_counts(result) == {
            "ebwpc-E-8428-dtw": 3,
            "ebwpc-E-8428-dtw-manual": 5,
        }
        assert metadata["rows_read"] == 5
        assert result.max_load_id is not None
        assert first.max_load_id is not None
        assert result.max_load_id > first.max_load_id

    def test_only_the_newest_snapshot_of_a_well_is_mapped(
        self, catalog: FakeCatalog, local_bucket: Path
    ):
        """Both snapshots are past the watermark when the load between them failed."""
        _ingest()
        _reupload_e_8428(catalog)
        _ingest()

        result, metadata = _transform(local_bucket)
        counts = _observation_counts(result)
        assert counts["ebwpc-E-8428-dtw"] == 3
        assert counts["ebwpc-E-8428-dtw-manual"] == 5
        assert metadata["rows_superseded"] == 4  # E-8428's first snapshot
        assert metadata["observations_dropped"] == {}


class TestWatermarkWiring:
    def test_watermark_lives_under_the_dataset(self):
        assert WATERMARK_PATH == f"{GCS_DATASET}/_ebwpc_ckan_transform_watermark.json"
