from unittest.mock import MagicMock

from aqueduct_dagster.sources.roswell.dlt_pipeline import _fetch_readings

# -- fetch readings --

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
    returnable = ""
    for s in file_contents:
        returnable += s + "\n"

    def _open(path, *_a, **_k):
        cm = MagicMock()
        if path.split("/")[-1] not in files:
            cm.side_effect = FileNotFoundError
        else:
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
