"""
tests/sources/pvacd_hydrovu/test_backfill.py

Unit tests for sources/pvacd_hydrovu/backfill.py (Mode A refetch).
No live API/GCS/FROST — all I/O is mocked.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from aqueduct_dagster.loader.frost_loader import LoadResult
from aqueduct_dagster.sources.pvacd_hydrovu.backfill import (
    BACKFILL_PIPELINE_NAME,
    BACKFILL_TABLE_NAME,
    GCS_DATASET,
    _locations_by_id,
    default_backfill_location_ids,
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
    @patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill._fetch_location_data")
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

    @patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill._fetch_location_data")
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

    @patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill._fetch_location_data")
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

    @patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill._fetch_location_data")
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

    @patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill._fetch_location_data")
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

    @patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill._fetch_location_data")
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


# ── prepare_backfill ─────────────────────────────────────────────────────────


@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill.load_source_config")
def test_default_backfill_location_ids_reads_the_configured_allowlist(mock_cfg):
    mock_cfg.return_value = {"location_ids": [111, 222]}
    assert default_backfill_location_ids() == [111, 222]


@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill.load_source_config")
def test_default_backfill_location_ids_is_empty_when_key_not_configured(mock_cfg):
    """
    A [sources.pvacd_hydrovu] section with no location_ids key at all is not an
    error — some sources may deliberately not curate an allowlist — so this
    returns [] (meaning "every location", see resolve_location_ids), not a
    KeyError.
    """
    mock_cfg.return_value = {"gcp_secret": "hydrovu_pvacd"}  # no location_ids key
    assert default_backfill_location_ids() == []


@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill.load_source_config")
def test_default_backfill_location_ids_raises_on_missing_config(mock_cfg):
    """
    Raises when .dlt/config.toml itself can't be read at all — a broken
    environment, not an intentional "backfill everything" choice — instead
    of silently falling back to [] and widening a reviewed allowlist into
    "backfill everything" at Dagster's definitions-load time (see
    PvacdHydroVuBackfillRefetchConfig).
    """
    mock_cfg.side_effect = FileNotFoundError("no .dlt/config.toml")
    with pytest.raises(FileNotFoundError):
        default_backfill_location_ids()


@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill._fetch_locations")
@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill.build_hydrovu_client")
@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill.load_source_config")
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


@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill._fetch_locations")
@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill.build_hydrovu_client")
@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill.load_source_config")
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


# build_backfill_pipeline/run_backfill_ingest are now shared, source-agnostic
# logic — tested directly in tests/shared/test_backfill.py.


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


@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill.read_parquet_rows_for_load_id")
@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill.run_backfill_ingest")
def test_run_backfill_chunk_reads_by_exact_load_id_and_loads_bundles(
    mock_run_ingest, mock_read_rows
):
    mock_run_ingest.return_value = 1781192390.555875
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

    # run_backfill_ingest called with this source's pipeline prefix/dataset/run_key
    ingest_kwargs = mock_run_ingest.call_args.kwargs
    assert ingest_kwargs["pipeline_name_prefix"] == BACKFILL_PIPELINE_NAME
    assert ingest_kwargs["dataset"] == GCS_DATASET
    assert ingest_kwargs["run_key"] == "test-run"

    # read_parquet_rows_for_load_id called with the exact load_id run_backfill_ingest returned
    args, kwargs = mock_read_rows.call_args
    assert args[0] == "my-bucket"
    assert args[1] == f"{GCS_DATASET}/{BACKFILL_TABLE_NAME}/**/*.parquet"
    assert args[2] == 1781192390.555875

    assert result.rows_ingested == 1
    assert result.bundles_loaded == 1
    assert result.observations_posted == 1
    assert result.observations_deleted == 1
    assert len(loader.ensure_calls) == 1
    assert len(loader.load_window_calls) == 1
    ds_key, ds_id, records = loader.load_window_calls[0]
    assert ds_id == "ds-1"
    assert len(records) == 1


@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill.read_parquet_rows_for_load_id")
@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill.run_backfill_ingest")
def test_run_backfill_chunk_reports_adapter_failures_without_dropping_good_locations(
    mock_run_ingest, mock_read_rows
):
    """
    One location's reading has a malformed timestamp (None) — HydroVuAdapter
    raises adapting it, BaseAdapter.run() catches and records it. The other,
    healthy location must still produce a bundle and get loaded; ChunkResult
    must report the failure count rather than silently swallowing it.
    """
    mock_run_ingest.return_value = 100.0
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


@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill.read_parquet_rows_for_load_id")
@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill.run_backfill_ingest")
def test_run_backfill_chunk_with_no_rows_loads_nothing(mock_run_ingest, mock_read_rows):
    mock_run_ingest.return_value = 100.0
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


@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill.read_parquet_rows_for_load_id")
@patch("aqueduct_dagster.sources.pvacd_hydrovu.backfill.run_backfill_ingest")
def test_run_backfill_chunk_handles_empty_loads_ids_without_crashing(
    mock_run_ingest, mock_read_rows
):
    """
    Regression test: run_backfill_ingest returns None when the chunk's
    requested location(s) yield zero rows — e.g. a wrong/nonexistent
    location_id — this must return a zero ChunkResult, not crash.
    """
    mock_run_ingest.return_value = None

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
