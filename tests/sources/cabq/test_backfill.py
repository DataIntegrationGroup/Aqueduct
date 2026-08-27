from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from aqueduct_dagster.loader import LoadResult
from aqueduct_dagster.sources.cabq.backfill import (
    BACKFILL_PIPELINE_NAME,
    BACKFILL_TABLE_NAME,
    _locations_by_id,
    cabq_backfill_readings,
    default_backfill_location_ids,
    prepare_backfill,
    run_backfill_chunk,
)
from aqueduct_dagster.sources.cabq.transform import GCS_DATASET

_DUMMY_CLIENT = MagicMock(spec=httpx.Client)

_LOCATIONS = [
    {
        "sys_loc_code": "IW4",
        "loc_name": "LALF GROUNDWATER INJECTION WELL 4",
        "latitude": -106.599332407,
        "longitude": 35.170730266,
    }
]

_READINGS_DATA = [{"measurement_date": 1391079600000, "water_depth": 162.396}]

CABQ_RESULTS = {
    "reading_id": "IW4_1391079600000",
    "location_id": "IW4",
    "location_name": "LALF GROUNDWATER INJECTION WELL 4",
    "latitude": -106.599332407,
    "longitude": 35.170730266,
    "timestamp": 1391079600000,
    "value": 162.396,
}

# -- cabq_backfill_readings --------------


class TestCabqBackfillReadings:
    @patch("aqueduct_dagster.sources.cabq.backfill._fetch_readings_for_location")
    def test_passes_start_and_end_ts_through(self, mock_fetch):
        mock_fetch.return_value = (_READINGS_DATA, None)
        list(
            cabq_backfill_readings(
                client=_DUMMY_CLIENT,
                locations=[_LOCATIONS[0]],
                location_ids=["IW4"],
                start_ts=123,
                end_ts=456,
            )
        )
        _client, _loc_id, start_time, end_time = mock_fetch.call_args[0]
        assert start_time == 123
        assert end_time == 456

    @patch("aqueduct_dagster.sources.cabq.backfill._fetch_readings_for_location")
    def test_yields_flat_rows(self, mock_fetch):
        mock_fetch.return_value = (_READINGS_DATA, None)
        rows = list(
            cabq_backfill_readings(
                client=_DUMMY_CLIENT,
                locations=[_LOCATIONS[0]],
                location_ids=["IW4"],
                start_ts=123,
                end_ts=456,
            )
        )
        assert rows == [CABQ_RESULTS]

    @patch("aqueduct_dagster.sources.cabq.backfill._fetch_readings_for_location")
    def test_skips_location_on_404(self, mock_fetch):
        mock_fetch.return_value = (None, None)
        rows = list(
            cabq_backfill_readings(
                client=_DUMMY_CLIENT,
                locations=[_LOCATIONS[0]],
                location_ids=["IW4"],
                start_ts=123,
                end_ts=456,
            )
        )
        assert rows == []

    @patch("aqueduct_dagster.sources.cabq.backfill._fetch_readings_for_location")
    def test_raises_on_real_fetch_error(self, mock_fetch):
        mock_fetch.return_value = (None, "HTTP 500")
        with pytest.raises(Exception, match="HTTP 500"):
            list(
                cabq_backfill_readings(
                    client=_DUMMY_CLIENT,
                    locations=[_LOCATIONS[0]],
                    location_ids=["IW4"],
                    start_ts=123,
                    end_ts=456,
                )
            )

    @patch("aqueduct_dagster.sources.cabq.backfill._fetch_readings_for_location")
    def test_fetch_error_message_includes_the_chunk_window(self, mock_fetch):
        mock_fetch.return_value = (None, "HTTP 500")
        start_ts = 1767225600
        end_ts = 1769904000
        with pytest.raises(Exception, match=r"2026-01-01.*2026-02-01.*HTTP 500"):
            list(
                cabq_backfill_readings(
                    client=_DUMMY_CLIENT,
                    locations=[_LOCATIONS[0]],
                    location_ids=["IW4"],
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
            )


# -- _locaitons_by_id --------------


def test_locations_by_id_shape():
    result = _locations_by_id(_LOCATIONS)
    assert result["IW4"] == {
        "name": "LALF GROUNDWATER INJECTION WELL 4",
        "latitude": -106.599332407,
        "longitude": 35.170730266,
    }


# -- prepare_backfill --------------


class TestCabqBackfillPreparation:
    @patch("aqueduct_dagster.sources.cabq.backfill.load_source_config")
    def test_default_backfill_location_ids_reads_the_configured_allowlist(self, mock_cfg):
        mock_cfg.return_value = {"location_ids": ["IW4", "IW3"]}
        assert default_backfill_location_ids() == ["IW4", "IW3"]

    @patch("aqueduct_dagster.sources.cabq.backfill.load_source_config")
    def test_default_backfill_location_ids_is_empty_when_key_not_configured(self, mock_cfg):
        mock_cfg.return_value = {}
        assert default_backfill_location_ids() == []

    @patch("aqueduct_dagster.sources.cabq.backfill.load_source_config")
    def test_default_backfill_location_ids_raises_on_missing_config(self, mock_cfg):
        mock_cfg.side_effect = FileNotFoundError("no .dlt/config.toml")
        with pytest.raises(FileNotFoundError):
            default_backfill_location_ids()

    @patch("aqueduct_dagster.sources.cabq.backfill._fetch_locations")
    @patch("aqueduct_dagster.sources.cabq.backfill.build_cabq_client")
    @patch("aqueduct_dagster.sources.cabq.backfill.load_source_config")
    def test_prepare_backfill_locations(self, mock_cfg, mock_build_client, mock_fetch_locations):
        mock_cfg.return_value = {"api_base_url": "https://api"}
        mock_build_client.return_value = _DUMMY_CLIENT
        mock_fetch_locations.return_value = (_LOCATIONS, None)

        client, locations, locations_by_id = prepare_backfill()

        assert client is _DUMMY_CLIENT
        assert locations == _LOCATIONS
        assert locations_by_id["IW4"]["name"] == "LALF GROUNDWATER INJECTION WELL 4"
        mock_fetch_locations.assert_called_once_with(_DUMMY_CLIENT)

    @patch("aqueduct_dagster.sources.cabq.backfill._fetch_locations")
    @patch("aqueduct_dagster.sources.cabq.backfill.build_cabq_client")
    @patch("aqueduct_dagster.sources.cabq.backfill.load_source_config")
    def test_prepare_backfill_closes_client_if_fetch_locations_fails(
        self, mock_cfg, mock_build_client, mock_fetch_locations
    ):
        mock_cfg.return_value = {"api_base_url": "https://api"}
        client = MagicMock(spec=httpx.Client)
        mock_build_client.return_value = client
        mock_fetch_locations.side_effect = httpx.ReadError("boom")

        with pytest.raises(httpx.ReadError):
            prepare_backfill()

        client.close.assert_called_once()


# -- run_backfill_chunk ----------


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


class TestCabqBackfillChunk:
    @patch("aqueduct_dagster.sources.cabq.backfill.read_parquet_rows_for_load_id")
    @patch("aqueduct_dagster.sources.cabq.backfill.run_backfill_ingest")
    def test_run_backfill_chunk_reads_by_exact_load_id_and_loads_bundles(
        self, mock_run_ingest, mock_read_rows
    ):
        mock_run_ingest.return_value = 1781192390.555875
        mock_read_rows.return_value = [CABQ_RESULTS]

        loader = _StubFrostLoader()
        result = run_backfill_chunk(
            client=_DUMMY_CLIENT,
            locations=_LOCATIONS,
            locations_by_id=_locations_by_id(_LOCATIONS),
            location_ids=["IW4"],
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

    @patch("aqueduct_dagster.sources.cabq.backfill.read_parquet_rows_for_load_id")
    @patch("aqueduct_dagster.sources.cabq.backfill.run_backfill_ingest")
    def test_run_backfill_chunk_with_no_rows_loads_nothing(self, mock_run_ingest, mock_read_rows):
        mock_run_ingest.return_value = 100.0
        mock_read_rows.return_value = []

        loader = _StubFrostLoader()
        result = run_backfill_chunk(
            client=_DUMMY_CLIENT,
            locations=_LOCATIONS,
            locations_by_id=_locations_by_id(_LOCATIONS),
            location_ids=["IW4"],
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

    @patch("aqueduct_dagster.sources.cabq.backfill.read_parquet_rows_for_load_id")
    @patch("aqueduct_dagster.sources.cabq.backfill.run_backfill_ingest")
    def test_run_backfill_chunk_handles_empty_loads_ids_without_crashing(
        self, mock_run_ingest, mock_read_rows
    ):
        mock_run_ingest.return_value = None

        loader = _StubFrostLoader()
        result = run_backfill_chunk(
            client=_DUMMY_CLIENT,
            locations=_LOCATIONS,
            locations_by_id=_locations_by_id(_LOCATIONS),
            location_ids=[""],  # e.g. a mistyped/nonexistent location id
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
