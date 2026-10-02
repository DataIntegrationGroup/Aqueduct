from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from httpx import Client, ReadError

from aqueduct_dagster.loader import LoadResult
from aqueduct_dagster.sources.bernco_manual.backfill import (
    BACKFILL_PIPELINE_NAME,
    BACKFILL_TABLE_NAME,
    _locations_by_id,
    bernco_manual_backfill_readings,
    default_backfill_location_ids,
    prepare_backfill,
    run_backfill_chunk,
)
from aqueduct_dagster.sources.bernco_manual.transform import GCS_DATASET

_Dummy_Client = MagicMock(spec=Client)

_LOCATIONS = [
    {
        "GlobalID": "4f92c895-6b41-42d6-be6b-508ee44812fa",
        "Well_Name": "9-Mile Hill LF",
        "Well_Location_Latitude": 35.071526,
        "Well_Location_Longitude": -106.778477,
        "NMT_ID": "BC-0364",
    }
]

_READINGS_DATA = [
    {
        "MSRMNT_Date": 1573689600000,
        "Depth_To_Water_At_Msrmnt_Point": 711.11,
    }
]

BERNCO_MANUAL_RESULTS = {
    "reading_id": "4f92c895-6b41-42d6-be6b-508ee44812fa_1573689600000",
    "location_id": "4f92c895-6b41-42d6-be6b-508ee44812fa",
    "location_name": "9-Mile Hill LF",
    "latitude": 35.071526,
    "longitude": -106.778477,
    "timestamp": 1573689600000,
    "value": 711.11,
    "alternate_id": "BC-0364",
}

# -- bernco_manual_backfill_readings --


class TestBerncoManualBackfillReadings:
    @patch("aqueduct_dagster.sources.bernco_manual.backfill._fetch_readings_for_location")
    def test_passes_start_and_end_ts_through(self, mock_fetch):
        mock_fetch.return_value = (_READINGS_DATA, None)
        list(
            bernco_manual_backfill_readings(
                client=_Dummy_Client,
                locations=[_LOCATIONS[0]],
                location_ids=["4f92c895-6b41-42d6-be6b-508ee44812fa"],
                start_ts=123,
                end_ts=456,
            )
        )
        _client, _loc_id, _start_ts, _end_ts = mock_fetch.call_args[0]
        assert _start_ts == 123
        assert _end_ts == 456

    @patch("aqueduct_dagster.sources.bernco_manual.backfill._fetch_readings_for_location")
    def test_yields_flat_rows(self, mock_fetch):
        mock_fetch.return_value = (_READINGS_DATA, None)
        rows = list(
            bernco_manual_backfill_readings(
                client=_Dummy_Client,
                locations=[_LOCATIONS[0]],
                location_ids=["4f92c895-6b41-42d6-be6b-508ee44812fa"],
                start_ts=123,
                end_ts=456,
            )
        )
        assert rows == [BERNCO_MANUAL_RESULTS]

    @patch("aqueduct_dagster.sources.bernco_manual.backfill._fetch_readings_for_location")
    def test_skips_location_on_404(self, mock_fetch):
        mock_fetch.return_value = (None, None)
        rows = list(
            bernco_manual_backfill_readings(
                client=_Dummy_Client,
                locations=[_LOCATIONS[0]],
                location_ids=["4f92c895-6b41-42d6-be6b-508ee44812fa"],
                start_ts=123,
                end_ts=456,
            )
        )
        assert rows == []

    @patch("aqueduct_dagster.sources.bernco_manual.backfill._fetch_readings_for_location")
    def test_raises_on_real_fetch_error(self, mock_fetch):
        mock_fetch.return_value = (None, "HTTP 500")
        with pytest.raises(Exception, match="HTTP 500"):
            list(
                bernco_manual_backfill_readings(
                    client=_Dummy_Client,
                    locations=[_LOCATIONS[0]],
                    location_ids=["4f92c895-6b41-42d6-be6b-508ee44812fa"],
                    start_ts=123,
                    end_ts=456,
                )
            )

    @patch("aqueduct_dagster.sources.bernco_manual.backfill._fetch_readings_for_location")
    def test_fetch_error_message_includes_chunk_window(self, mock_fetch):
        mock_fetch.return_value = (None, "HTTP 500")
        start_ts = 1767225600
        end_ts = 1769904000
        with pytest.raises(Exception, match=r"2026-01-01.*2026-02-01.*HTTP 500"):
            list(
                bernco_manual_backfill_readings(
                    client=_Dummy_Client,
                    locations=[_LOCATIONS[0]],
                    location_ids=["4f92c895-6b41-42d6-be6b-508ee44812fa"],
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
            )


# -- _locations_by_id --


def test_locations_by_id_shape():
    result = _locations_by_id(_LOCATIONS)
    assert result["4f92c895-6b41-42d6-be6b-508ee44812fa"] == {
        "name": "9-Mile Hill LF",
        "description": "Location of well where measurements are made",
        "latitude": 35.071526,
        "longitude": -106.778477,
        "alternate_id": "BC-0364",
    }


# -- prepare_backfill --


class TestBerncoManualBackfillPreparation:
    @patch("aqueduct_dagster.sources.bernco_manual.backfill.load_source_config")
    def test_default_backfill_location_ids_reads_the_configured_allowed_list(self, mock_cfg):
        mock_cfg.return_value = {
            "location_ids": [
                "4f92c895-6b41-42d6-be6b-508ee44812fa",
                "4f92c895-6b41-42d6-be6b-508ee44812fb",
            ],
        }
        assert default_backfill_location_ids() == [
            "4f92c895-6b41-42d6-be6b-508ee44812fa",
            "4f92c895-6b41-42d6-be6b-508ee44812fb",
        ]

    @patch("aqueduct_dagster.sources.bernco_manual.backfill.load_source_config")
    def test_default_backfill_location_ids_is_empty_when_key_not_configured(self, mock_cfg):
        mock_cfg.return_value = {}
        assert default_backfill_location_ids() == []

    @patch("aqueduct_dagster.sources.bernco_manual.backfill.load_source_config")
    def test_default_backfill_location_ids_raises_on_missing_config(self, mock_cfg):
        mock_cfg.side_effect = FileNotFoundError("no .dlt/config.toml")
        with pytest.raises(FileNotFoundError, match="no .dlt/config.toml"):
            default_backfill_location_ids()

    @patch("aqueduct_dagster.sources.bernco_manual.backfill._fetch_locations")
    @patch("aqueduct_dagster.sources.bernco_manual.backfill.build_bernco_manual_client")
    @patch("aqueduct_dagster.sources.bernco_manual.backfill.load_source_config")
    def test_prepare_backfill_locations(self, mock_cfg, mock_build_client, mock_fetch_locations):
        mock_cfg.return_value = {"api_base_url": "https://api"}
        mock_build_client.return_value = _Dummy_Client
        mock_fetch_locations.return_value = (_LOCATIONS, None)
        client, locations, locations_by_id = prepare_backfill()
        assert client is _Dummy_Client
        assert locations == _LOCATIONS
        assert locations_by_id["4f92c895-6b41-42d6-be6b-508ee44812fa"]["name"] == "9-Mile Hill LF"
        mock_fetch_locations.assert_called_once_with(_Dummy_Client)

    @patch("aqueduct_dagster.sources.bernco_manual.backfill._fetch_locations")
    @patch("aqueduct_dagster.sources.bernco_manual.backfill.build_bernco_manual_client")
    @patch("aqueduct_dagster.sources.bernco_manual.backfill.load_source_config")
    def test_prepare_backfill_closes_client_if_fetch_locations_failed(
        self, mock_cfg, mock_build_client, mock_fetch_locations
    ):
        mock_cfg.return_value = {"api_base_url": "https://api"}
        client = MagicMock(spec=Client)
        mock_build_client.return_value = client
        mock_fetch_locations.side_effect = ReadError("boom")

        with pytest.raises(ReadError):
            prepare_backfill()

        client.close.assert_called_once()


# -- run_backfill_chunk --


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


CHUNK_START = datetime(2026, 1, 1, tzinfo=UTC)
CHUNK_END = datetime(2026, 2, 1, tzinfo=UTC)


class TestBerncoManualBackfillChunk:
    @patch("aqueduct_dagster.sources.bernco_manual.backfill.read_parquet_rows_for_load_id")
    @patch("aqueduct_dagster.sources.bernco_manual.backfill.run_backfill_ingest")
    def test_run_backfill_chunk_reads_by_exact_load_id_and_loads_bundles(
        self, mock_run_ingest, mock_read_rows
    ):
        mock_run_ingest.return_value = 1781192390.555875
        mock_read_rows.return_value = (
            [BERNCO_MANUAL_RESULTS],
            frozenset({"bad1.parquet", "bad2.parquet"}),
        )
        loader = _StubFrostLoader()
        result = run_backfill_chunk(
            client=_Dummy_Client,
            locations=_LOCATIONS,
            locations_by_id=_locations_by_id(_LOCATIONS),
            location_ids=["4f92c895-6b41-42d6-be6b-508ee44812fa"],
            chunk_start=CHUNK_START,
            chunk_end=CHUNK_END,
            loader=loader,  # type: ignore[arg-type]
            bucket="my-bucket",
            fs=MagicMock(),
            run_key="test-run",
        )
        ingest_kwargs = mock_run_ingest.call_args.kwargs
        assert ingest_kwargs["pipeline_name_prefix"] == BACKFILL_PIPELINE_NAME
        assert ingest_kwargs["dataset"] == GCS_DATASET
        assert ingest_kwargs["run_key"] == "test-run"
        args, kwargs = mock_read_rows.call_args
        assert args[0] == "my-bucket"
        assert args[1] == f"{GCS_DATASET}/{BACKFILL_TABLE_NAME}/**/*.parquet"
        assert args[2] == 1781192390.555875
        assert result.rows_ingested == 1
        assert result.bundles_loaded == 1
        assert result.observations_posted == 1
        assert result.observations_deleted == 1
        assert result.files_skipped_bad_name == frozenset({"bad1.parquet", "bad2.parquet"})
        assert len(loader.ensure_calls) == 1
        assert len(loader.load_window_calls) == 1
        ds_key, ds_id, records = loader.load_window_calls[0]
        assert ds_id == "ds-1"
        assert len(records) == 1

    @patch("aqueduct_dagster.sources.bernco_manual.backfill.read_parquet_rows_for_load_id")
    @patch("aqueduct_dagster.sources.bernco_manual.backfill.run_backfill_ingest")
    def test_run_backfill_chunk_with_no_rows_loads_nothing(self, mock_run_ingest, mock_read_rows):
        mock_run_ingest.return_value = 100.0
        mock_read_rows.return_value = (
            [],
            frozenset(),
        )
        loader = _StubFrostLoader()
        result = run_backfill_chunk(
            client=_Dummy_Client,
            locations=_LOCATIONS,
            locations_by_id=_locations_by_id(_LOCATIONS),
            location_ids=["4f92c895-6b41-42d6-be6b-508ee44812fa"],
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

    @patch("aqueduct_dagster.sources.bernco_manual.backfill.read_parquet_rows_for_load_id")
    @patch("aqueduct_dagster.sources.bernco_manual.backfill.run_backfill_ingest")
    def test_run_backfill_chunk_handles_empty_loads_ids_without_crashing(
        self, mock_run_ingest, mock_read_rows
    ):
        mock_run_ingest.return_value = None
        loader = _StubFrostLoader()
        result = run_backfill_chunk(
            client=_Dummy_Client,
            locations=_LOCATIONS,
            locations_by_id=_locations_by_id(_LOCATIONS),
            location_ids=[""],
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
        mock_read_rows.assert_not_called()
