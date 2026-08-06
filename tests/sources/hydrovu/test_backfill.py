"""
tests/sources/hydrovu/test_backfill.py

Unit tests for sources/hydrovu/backfill.py (Mode A refetch).
No live API/GCS/FROST — all I/O is mocked.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from aqueduct_dagster.loader.frost_loader import LoadResult
from aqueduct_dagster.sources.hydrovu.backfill import (
    BACKFILL_PIPELINE_NAME,
    BACKFILL_TABLE_NAME,
    GCS_DATASET,
    _load_hydrovu_config,
    _locations_by_id,
    _sanitize_run_key,
    build_backfill_pipeline,
    hydrovu_backfill_readings,
    prepare_backfill,
    run_backfill_chunk,
)

_DUMMY_CLIENT = MagicMock(spec=httpx.Client)

_LOCATIONS = [
    {
        "id": 111,
        "name": "Well A",
        "description": "desc-a",
        "gps": {"latitude": 35.1, "longitude": -106.5},
    },
    {
        "id": 222,
        "name": "Well B",
        "description": "desc-b",
        "gps": {"latitude": 35.2, "longitude": -106.6},
    },
]

_READINGS_DATA = {
    "parameters": [
        {
            "parameterId": "4",
            "unitId": "35",
            "readings": [{"timestamp": 1_000_000, "value": 10.0}],
        }
    ]
}

# ── hydrovu_backfill_readings ──────────────────────────────────────────────────


class TestHydroVuBackfillReadings:
    @patch("aqueduct_dagster.sources.hydrovu.backfill._fetch_location_data")
    def test_only_fetches_allowlisted_locations(self, mock_fetch):
        mock_fetch.return_value = (_READINGS_DATA, None)
        list(
            hydrovu_backfill_readings(
                client=_DUMMY_CLIENT,
                locations=_LOCATIONS,
                location_ids=[111],
                start_ts=0,
                end_ts=2_000_000,
            )
        )
        called_ids = {call[0][1] for call in mock_fetch.call_args_list}
        assert called_ids == {111}

    @patch("aqueduct_dagster.sources.hydrovu.backfill._fetch_location_data")
    def test_passes_start_and_end_ts_through(self, mock_fetch):
        mock_fetch.return_value = (_READINGS_DATA, None)
        list(
            hydrovu_backfill_readings(
                client=_DUMMY_CLIENT,
                locations=[_LOCATIONS[0]],
                location_ids=[111],
                start_ts=123,
                end_ts=456,
            )
        )
        _client, _loc_id, start_time = mock_fetch.call_args[0]
        assert start_time == 123
        assert mock_fetch.call_args[1]["end_time"] == 456

    @patch("aqueduct_dagster.sources.hydrovu.backfill._fetch_location_data")
    def test_yields_flat_rows(self, mock_fetch):
        mock_fetch.return_value = (_READINGS_DATA, None)
        rows = list(
            hydrovu_backfill_readings(
                client=_DUMMY_CLIENT,
                locations=[_LOCATIONS[0]],
                location_ids=[111],
                start_ts=0,
                end_ts=2_000_000,
            )
        )
        assert rows == [
            {
                "reading_id": "111_4_1000000",
                "location_id": 111,
                "timestamp": 1_000_000,
                "parameter_id": "4",
                "unit_id": "35",
                "value": 10.0,
            }
        ]

    @patch("aqueduct_dagster.sources.hydrovu.backfill._fetch_location_data")
    def test_skips_location_on_404(self, mock_fetch):
        mock_fetch.return_value = (None, None)
        rows = list(
            hydrovu_backfill_readings(
                client=_DUMMY_CLIENT,
                locations=[_LOCATIONS[0]],
                location_ids=[111],
                start_ts=0,
                end_ts=2_000_000,
            )
        )
        assert rows == []

    @patch("aqueduct_dagster.sources.hydrovu.backfill._fetch_location_data")
    def test_raises_on_real_fetch_error(self, mock_fetch):
        # dlt wraps the generator's exception in its own ResourceExtractionError
        # when iterated directly (as it would be inside pipeline.run()) — match
        # on message content rather than coupling the test to dlt's wrapper type.
        mock_fetch.return_value = (None, "HTTP 500")
        with pytest.raises(Exception, match="HTTP 500"):
            list(
                hydrovu_backfill_readings(
                    client=_DUMMY_CLIENT,
                    locations=[_LOCATIONS[0]],
                    location_ids=[111],
                    start_ts=0,
                    end_ts=2_000_000,
                )
            )

    @patch("aqueduct_dagster.sources.hydrovu.backfill._fetch_location_data")
    def test_fetch_error_message_includes_the_chunk_window(self, mock_fetch):
        """
        An operator glancing at a failed run should immediately see which
        window failed, not just the location id and raw error — the window
        bounds must be readable in the raised message.
        """
        mock_fetch.return_value = (None, "HTTP 500")
        # 2026-01-01T00:00:00Z and 2026-02-01T00:00:00Z, in unix seconds.
        start_ts = 1767225600
        end_ts = 1769904000
        with pytest.raises(Exception, match=r"2026-01-01.*2026-02-01.*HTTP 500"):
            list(
                hydrovu_backfill_readings(
                    client=_DUMMY_CLIENT,
                    locations=[_LOCATIONS[0]],
                    location_ids=[111],
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
            )


# ── _locations_by_id ────────────────────────────────────────────────────────────


def test_locations_by_id_shape():
    result = _locations_by_id(_LOCATIONS)
    assert result[111] == {
        "name": "Well A",
        "description": "desc-a",
        "latitude": 35.1,
        "longitude": -106.5,
    }
    assert result[222]["name"] == "Well B"


# ── _load_hydrovu_config / prepare_backfill ─────────────────────────────────────


@patch("aqueduct_dagster.sources.hydrovu.backfill.toml.load")
def test_load_hydrovu_config_reads_sources_hydrovu_section(mock_toml_load):
    mock_toml_load.return_value = {
        "sources": {
            "hydrovu": {
                "gcp_secret": "hydrovu_pvacd",
                "api_base_url": "https://api",
                "token_url": "https://token",
            }
        }
    }
    cfg = _load_hydrovu_config()
    assert cfg["gcp_secret"] == "hydrovu_pvacd"


@patch("aqueduct_dagster.sources.hydrovu.backfill._fetch_locations")
@patch("aqueduct_dagster.sources.hydrovu.backfill.build_hydrovu_client")
@patch("aqueduct_dagster.sources.hydrovu.backfill._load_hydrovu_config")
def test_prepare_backfill_fetches_locations_once(mock_cfg, mock_build_client, mock_fetch_locations):
    mock_cfg.return_value = {
        "gcp_secret": "hydrovu_pvacd",
        "api_base_url": "https://api",
        "token_url": "https://token",
    }
    mock_build_client.return_value = _DUMMY_CLIENT
    mock_fetch_locations.return_value = _LOCATIONS

    client, locations, locations_by_id = prepare_backfill()

    assert client is _DUMMY_CLIENT
    assert locations == _LOCATIONS
    assert locations_by_id[111]["name"] == "Well A"
    mock_fetch_locations.assert_called_once_with(_DUMMY_CLIENT)


@patch("aqueduct_dagster.sources.hydrovu.backfill._fetch_locations")
@patch("aqueduct_dagster.sources.hydrovu.backfill.build_hydrovu_client")
@patch("aqueduct_dagster.sources.hydrovu.backfill._load_hydrovu_config")
def test_prepare_backfill_closes_client_if_fetch_locations_fails(
    mock_cfg, mock_build_client, mock_fetch_locations
):
    mock_cfg.return_value = {
        "gcp_secret": "hydrovu_pvacd",
        "api_base_url": "https://api",
        "token_url": "https://token",
    }
    client = MagicMock(spec=httpx.Client)
    mock_build_client.return_value = client
    mock_fetch_locations.side_effect = httpx.ReadError("boom")

    with pytest.raises(httpx.ReadError):
        prepare_backfill()

    client.close.assert_called_once()


# ── build_backfill_pipeline / _sanitize_run_key ─────────────────────────────────


def test_sanitize_run_key_leaves_safe_characters_untouched():
    assert _sanitize_run_key("hydrovu-jan2026_repair") == "hydrovu-jan2026_repair"


def test_sanitize_run_key_replaces_unsafe_characters():
    assert _sanitize_run_key("hydrovu jan/2026 repair!") == "hydrovu_jan_2026_repair_"


@patch("aqueduct_dagster.sources.hydrovu.backfill.build_source_pipeline")
def test_build_backfill_pipeline_includes_run_key_in_pipeline_name(mock_build_source_pipeline):
    build_backfill_pipeline("jan-repair")

    args, _kwargs = mock_build_source_pipeline.call_args
    assert args[0] == f"{BACKFILL_PIPELINE_NAME}_jan-repair"
    assert args[1] == GCS_DATASET


@patch("aqueduct_dagster.sources.hydrovu.backfill.build_source_pipeline")
def test_build_backfill_pipeline_sanitizes_run_key(mock_build_source_pipeline):
    """
    Two different run_keys must never produce the same pipeline_name (that
    would defeat the whole point of per-run_key isolation), and an
    operator-typed run_key shouldn't be able to break the local path dlt
    builds from it.
    """
    build_backfill_pipeline("jan repair/v2")

    args, _kwargs = mock_build_source_pipeline.call_args
    assert args[0] == f"{BACKFILL_PIPELINE_NAME}_jan_repair_v2"


# ── run_backfill_chunk ──────────────────────────────────────────────────────────


class _StubFrostLoader:
    """Minimal FrostLoader-shaped stub for run_backfill_chunk tests."""

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


CHUNK_START = datetime(2026, 1, 1, tzinfo=UTC)
CHUNK_END = datetime(2026, 2, 1, tzinfo=UTC)


@patch("aqueduct_dagster.sources.hydrovu.backfill.read_parquet_rows_for_load_id")
@patch("aqueduct_dagster.sources.hydrovu.backfill.build_backfill_pipeline")
def test_run_backfill_chunk_reads_by_exact_load_id_and_loads_bundles(
    mock_build_pipeline, mock_read_rows
):
    mock_pipeline = MagicMock()
    mock_pipeline.run.return_value = MagicMock(loads_ids=["1781192390.555875"])
    mock_build_pipeline.return_value = mock_pipeline
    mock_read_rows.return_value = [
        {
            "location_id": 111,
            "parameter_id": "4",
            "unit_id": "35",
            "timestamp": 1_000_000,
            "value": 10.0,
        }
    ]

    loader = _StubFrostLoader()
    result = run_backfill_chunk(
        client=_DUMMY_CLIENT,
        locations=_LOCATIONS,
        locations_by_id=_locations_by_id(_LOCATIONS),
        location_ids=[111],
        chunk_start=CHUNK_START,
        chunk_end=CHUNK_END,
        loader=loader,  # type: ignore[arg-type]
        bucket="my-bucket",
        fs=MagicMock(),
        run_key="test-run",
    )

    # read_parquet_rows_for_load_id called with the exact load_id from LoadInfo
    args, kwargs = mock_read_rows.call_args
    assert args[0] == "my-bucket"
    assert args[1] == f"{GCS_DATASET}/{BACKFILL_TABLE_NAME}/**/*.parquet"
    assert args[2] == 1781192390.555875

    mock_pipeline.drop_pending_packages.assert_called_once()

    assert result.rows_ingested == 1
    assert result.bundles_loaded == 1
    assert result.observations_posted == 1
    assert result.observations_deleted == 1
    assert len(loader.ensure_calls) == 1
    assert len(loader.load_window_calls) == 1
    ds_key, ds_id, records = loader.load_window_calls[0]
    assert ds_id == "ds-1"
    assert len(records) == 1


@patch("aqueduct_dagster.sources.hydrovu.backfill.read_parquet_rows_for_load_id")
@patch("aqueduct_dagster.sources.hydrovu.backfill.build_backfill_pipeline")
def test_run_backfill_chunk_drops_pending_packages_before_run(mock_build_pipeline, mock_read_rows):
    """
    Regression test: every chunk shares BACKFILL_PIPELINE_NAME, so a package
    left pending by an earlier, uncleanly-terminated run must be dropped
    BEFORE pipeline.run() is called — otherwise dlt would silently finish
    loading that stale package instead of this chunk's real data (dlt's
    run() exits early once it detects pending data, without ever calling
    hydrovu_backfill_readings() at all).
    """
    call_order: list[str] = []
    mock_pipeline = MagicMock()
    mock_pipeline.drop_pending_packages.side_effect = lambda: call_order.append("drop")
    mock_pipeline.run.side_effect = lambda *a, **k: (
        call_order.append("run") or MagicMock(loads_ids=["100.0"])
    )
    mock_build_pipeline.return_value = mock_pipeline
    mock_read_rows.return_value = []

    run_backfill_chunk(
        client=_DUMMY_CLIENT,
        locations=_LOCATIONS,
        locations_by_id=_locations_by_id(_LOCATIONS),
        location_ids=[111],
        chunk_start=CHUNK_START,
        chunk_end=CHUNK_END,
        loader=_StubFrostLoader(),  # type: ignore[arg-type]
        bucket="my-bucket",
        fs=MagicMock(),
        run_key="test-run",
    )

    assert call_order == ["drop", "run"]


@patch("aqueduct_dagster.sources.hydrovu.backfill.read_parquet_rows_for_load_id")
@patch("aqueduct_dagster.sources.hydrovu.backfill.build_backfill_pipeline")
def test_run_backfill_chunk_reports_adapter_failures_without_dropping_good_locations(
    mock_build_pipeline, mock_read_rows
):
    """
    One location's reading has a malformed timestamp (None) — HydroVuAdapter
    raises adapting it, BaseAdapter.run() catches and records it. The other,
    healthy location must still produce a bundle and get loaded; ChunkResult
    must report the failure count rather than silently swallowing it.
    """
    mock_pipeline = MagicMock()
    mock_pipeline.run.return_value = MagicMock(loads_ids=["100.0"])
    mock_build_pipeline.return_value = mock_pipeline
    mock_read_rows.return_value = [
        {
            "location_id": 111,
            "parameter_id": "4",
            "unit_id": "35",
            "timestamp": 1_000_000,
            "value": 10.0,
        },
        {
            "location_id": 222,
            "parameter_id": "4",
            "unit_id": "35",
            "timestamp": None,  # malformed — datetime.fromtimestamp() raises on this
            "value": 5.0,
        },
    ]

    loader = _StubFrostLoader()
    result = run_backfill_chunk(
        client=_DUMMY_CLIENT,
        locations=_LOCATIONS,
        locations_by_id=_locations_by_id(_LOCATIONS),
        location_ids=[111, 222],
        chunk_start=CHUNK_START,
        chunk_end=CHUNK_END,
        loader=loader,  # type: ignore[arg-type]
        bucket="my-bucket",
        fs=MagicMock(),
        run_key="test-run",
    )

    assert result.bundles_loaded == 1
    assert result.adapter_failures == 1
    assert len(loader.load_window_calls) == 1


@patch("aqueduct_dagster.sources.hydrovu.backfill.read_parquet_rows_for_load_id")
@patch("aqueduct_dagster.sources.hydrovu.backfill.build_backfill_pipeline")
def test_run_backfill_chunk_logs_the_dlt_pipeline_name(mock_build_pipeline, mock_read_rows, caplog):
    """
    The dlt pipeline_name isn't shown anywhere in the Dagster Runs UI (that
    table only shows Dagster-level info), so it must be logged explicitly for
    an operator to confirm which pipeline a chunk actually used.
    """
    mock_pipeline = MagicMock()
    mock_pipeline.pipeline_name = "pvacd_hydrovu_backfill_refetch_test-run"
    mock_pipeline.run.return_value = MagicMock(loads_ids=["100.0"])
    mock_build_pipeline.return_value = mock_pipeline
    mock_read_rows.return_value = []

    with caplog.at_level("INFO", logger="aqueduct_dagster.sources.hydrovu.backfill"):
        run_backfill_chunk(
            client=_DUMMY_CLIENT,
            locations=_LOCATIONS,
            locations_by_id=_locations_by_id(_LOCATIONS),
            location_ids=[111],
            chunk_start=CHUNK_START,
            chunk_end=CHUNK_END,
            loader=_StubFrostLoader(),  # type: ignore[arg-type]
            bucket="my-bucket",
            fs=MagicMock(),
            run_key="test-run",
        )

    assert "pvacd_hydrovu_backfill_refetch_test-run" in caplog.text


@patch("aqueduct_dagster.sources.hydrovu.backfill.read_parquet_rows_for_load_id")
@patch("aqueduct_dagster.sources.hydrovu.backfill.build_backfill_pipeline")
def test_run_backfill_chunk_with_no_rows_loads_nothing(mock_build_pipeline, mock_read_rows):
    mock_pipeline = MagicMock()
    mock_pipeline.run.return_value = MagicMock(loads_ids=["100.0"])
    mock_build_pipeline.return_value = mock_pipeline
    mock_read_rows.return_value = []

    loader = _StubFrostLoader()
    result = run_backfill_chunk(
        client=_DUMMY_CLIENT,
        locations=_LOCATIONS,
        locations_by_id=_locations_by_id(_LOCATIONS),
        location_ids=[111],
        chunk_start=CHUNK_START,
        chunk_end=CHUNK_END,
        loader=loader,  # type: ignore[arg-type]
        bucket="my-bucket",
        fs=MagicMock(),
        run_key="test-run",
    )

    assert result.rows_ingested == 0
    assert result.bundles_loaded == 0
    assert result.observations_posted == 0
    assert result.observations_deleted == 0
    assert loader.ensure_calls == []


@patch("aqueduct_dagster.sources.hydrovu.backfill.read_parquet_rows_for_load_id")
@patch("aqueduct_dagster.sources.hydrovu.backfill.build_backfill_pipeline")
def test_run_backfill_chunk_handles_empty_loads_ids_without_crashing(
    mock_build_pipeline, mock_read_rows
):
    """
    Regression test: dlt only creates a load package when there's new data or
    schema/state to persist. On a reused pipeline (every chunk in a backfill
    run shares the same pipeline_name), a chunk whose requested location(s)
    yield zero rows — e.g. a wrong/nonexistent location_id, confirmed via a
    real report — gets back an EMPTY loads_ids list, not a list with one
    entry. Indexing into it unconditionally used to raise IndexError and kill
    the whole chunk.
    """
    mock_pipeline = MagicMock()
    mock_pipeline.run.return_value = MagicMock(loads_ids=[])
    mock_build_pipeline.return_value = mock_pipeline

    loader = _StubFrostLoader()
    result = run_backfill_chunk(
        client=_DUMMY_CLIENT,
        locations=_LOCATIONS,
        locations_by_id=_locations_by_id(_LOCATIONS),
        location_ids=[999999],  # e.g. a mistyped/nonexistent location id
        chunk_start=CHUNK_START,
        chunk_end=CHUNK_END,
        loader=loader,  # type: ignore[arg-type]
        bucket="my-bucket",
        fs=MagicMock(),
        run_key="test-run",
    )

    assert result.rows_ingested == 0
    assert result.bundles_loaded == 0
    assert result.observations_posted == 0
    assert result.observations_deleted == 0
    assert loader.ensure_calls == []
    # No point reading parquet back — there's no load_id to look up.
    mock_read_rows.assert_not_called()
