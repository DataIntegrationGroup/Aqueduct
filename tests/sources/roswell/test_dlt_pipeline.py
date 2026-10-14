from unittest.mock import MagicMock, patch

from aqueduct_dagster.sources.roswell.dlt_pipeline import _fetch_readings, roswell_readings

# -- _fetch_readings --

FILE_NAMES = ["observations_04-07-25.csv"]

FILE_CONTENTS = [
    "depth_to_water,site_id,measuring _agency,,timestamp",
    "120.7 ,18 ,City of Roswell,2025-4-7T0:2:25",
    "120.7 ,18 ,City of Roswell,2025-4-7T0:17:36",
    "120.7 ,18 ,City of Roswell,2025-4-7T0:32:46",
    "120.7 ,18 ,City of Roswell,2025-4-7T0:47:56",
    "120.7 ,18 ,City of Roswell,2025-4-7T1:3:7",
]

PROCESSED_DATA = [
    {
        "depth_to_water": 120.7,
        "site_id": 18,
        "measuring_agency": "City of Roswell",
        "timestamp": "2025-4-7T0:2:25",
    },
    {
        "depth_to_water": 120.7,
        "site_id": 18,
        "measuring_agency": "City of Roswell",
        "timestamp": "2025-4-7T0:17:36",
    },
    {
        "depth_to_water": 120.7,
        "site_id": 18,
        "measuring_agency": "City of Roswell",
        "timestamp": "2025-4-7T0:32:46",
    },
    {
        "depth_to_water": 120.7,
        "site_id": 18,
        "measuring_agency": "City of Roswell",
        "timestamp": "2025-4-7T0:47:56",
    },
    {
        "depth_to_water": 120.7,
        "site_id": 18,
        "measuring_agency": "City of Roswell",
        "timestamp": "2025-4-7T1:3:7",
    },
]


def _mock_fs(files: list[str], file_contents: list[str]) -> MagicMock:
    fs = MagicMock()
    fs.ls.return_value = files

    def _open(path, *_a, **_k):
        cm = MagicMock()
        mock_file = MagicMock()
        mock_file.readlines.return_value = file_contents
        cm.__enter__ = MagicMock(return_value=mock_file)
        cm.__exit__ = MagicMock(return_value=False)
        return cm

    fs.open.side_effect = _open
    return fs


class TestFetchReadings:
    def test_returns_list_on_success(self):
        client = _mock_fs(FILE_NAMES, FILE_CONTENTS)
        result, err = _fetch_readings(client, "gs://bucket", 0)
        assert result == PROCESSED_DATA
        assert err is None

    def test_raises_on_file_not_found(self):
        client = _mock_fs(["observations_04-07-26.csv"], [])
        client.open.side_effect = FileNotFoundError("file not found")
        result, err = _fetch_readings(client, "gs://bucket", 0)
        assert result is None
        assert err == "file not found"

    def test_raises_on_no_files(self):
        client = _mock_fs([], [])
        result, err = _fetch_readings(client, "gs://bucket", 0)
        assert result is None
        assert err == "No files found at gs://bucket"


# -- roswell_readings --

ROSWELL_READINGS = [
    {
        "reading_id": "18_1743984145",
        "location_id": 18,
        "location_name": "Site-18",
        "latitude": 33.29592,
        "longitude": -104.58409,
        "timestamp": 1743984145,
        "value": 120.7,
    },
    {
        "reading_id": "18_1743985056",
        "location_id": 18,
        "location_name": "Site-18",
        "latitude": 33.29592,
        "longitude": -104.58409,
        "timestamp": 1743985056,
        "value": 120.7,
    },
    {
        "reading_id": "18_1743985966",
        "location_id": 18,
        "location_name": "Site-18",
        "latitude": 33.29592,
        "longitude": -104.58409,
        "timestamp": 1743985966,
        "value": 120.7,
    },
    {
        "reading_id": "18_1743986876",
        "location_id": 18,
        "location_name": "Site-18",
        "latitude": 33.29592,
        "longitude": -104.58409,
        "timestamp": 1743986876,
        "value": 120.7,
    },
    {
        "reading_id": "18_1743987787",
        "location_id": 18,
        "location_name": "Site-18",
        "latitude": 33.29592,
        "longitude": -104.58409,
        "timestamp": 1743987787,
        "value": 120.7,
    },
]


class TestRoswellReadings:
    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.roswell.dlt_pipeline._fetch_readings")
    def test_returns_roswell_readings(self, mock_fetch_readings, mock_state):
        mock_fetch_readings.return_value = (PROCESSED_DATA, None)
        results = list(
            roswell_readings(
                client=_mock_fs(FILE_NAMES, FILE_CONTENTS),
                api_base_url="gs://bucket",
                start_ts=1000,
            )
        )
        assert mock_fetch_readings.called
        assert mock_fetch_readings.call_args.args.__contains__("gs://bucket")
        assert mock_fetch_readings.call_args.args.__contains__(1000)
        assert results == ROSWELL_READINGS

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.roswell.dlt_pipeline._fetch_readings")
    def test_error_increments_errored_count(self, mock_fetch_readings, mock_state):
        mock_fetch_readings.return_value = (None, "File not found")
        stats: dict = {}
        list(
            roswell_readings(
                client=_mock_fs(FILE_NAMES, FILE_CONTENTS),
                api_base_url="gs://bucket",
                start_ts=1000,
                _stats=stats,
            )
        )
        assert stats["locations_errored"] == 1

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.roswell.dlt_pipeline._fetch_readings")
    def test_no_data_does_not_increment_errored_count(self, mock_fetch_readings, mock_state):
        mock_fetch_readings.return_value = (None, None)
        stats: dict = {}
        list(
            roswell_readings(
                client=_mock_fs(FILE_NAMES, FILE_CONTENTS),
                api_base_url="gs://bucket",
                start_ts=1000,
                _stats=stats,
            )
        )
        assert stats["locations_errored"] == 0
        assert stats["locations_no_data"] == 1
        assert stats["failed_location_ids"] == []

    @patch("dlt.current.resource_state", return_value={"location_cursors": {}})
    @patch("aqueduct_dagster.sources.roswell.dlt_pipeline._fetch_readings")
    def test_error_does_not_advance_cursor(self, mock_fetch_readings, mock_state):
        mock_fetch_readings.return_value = (None, "file not found")
        state: dict = {"location_cursors": {18: 1000}}
        list(
            roswell_readings(
                client=_mock_fs(FILE_NAMES, FILE_CONTENTS),
                api_base_url="gs://bucket",
                start_ts=1000,
            )
        )
        assert state["location_cursors"] == {18: 1000}  # unchanged
