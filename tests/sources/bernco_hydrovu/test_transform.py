"""
tests/sources/bernco_hydrovu/test_transform.py

Unit tests for the pure parts of the canonical_bundles_bernco_hydrovu transform.

Offline: nothing here touches GCS. The asset body itself is thin glue over
shared/gcs.py helpers that tests/shared/test_gcs.py already covers.
"""

from __future__ import annotations

from unittest.mock import patch

from aqueduct_dagster.sources.bernco_hydrovu.transform import (
    GCS_DATASET,
    WATERMARK_PATH,
    sentinel_floor,
)
from aqueduct_dagster.sources.hydrovu_transform_common import group_readings_by_location

_MODULE = "aqueduct_dagster.sources.bernco_hydrovu.transform"


def _config(initial_start_date: str = "2026-05-01") -> dict:
    return {"sources": {"bernco_hydrovu": {"initial_start_date": initial_start_date}}}


# ── sentinel_floor ────────────────────────────────────────────────────────────


class TestSentinelFloor:
    def test_resolves_initial_start_date_to_unix_seconds(self):
        with patch(f"{_MODULE}.load_config", return_value=_config("2026-05-01")):
            assert sentinel_floor() == 1777593600

    def test_tracks_config_rather_than_hardcoding_the_date(self):
        """The floor and the dlt cursor read the same key, so moving
        initial_start_date moves both together."""
        with patch(f"{_MODULE}.load_config", return_value=_config("2020-01-01")):
            assert sentinel_floor() == 1577836800

    def test_floor_is_above_the_known_sentinel_timestamps(self):
        """Epoch 0 (WhisperingPines-1002958) and 960 (E-55-POD 15-1173850TD)."""
        with patch(f"{_MODULE}.load_config", return_value=_config()):
            floor = sentinel_floor()
        assert floor > 960


# ── module constants the load step depends on ─────────────────────────────────


class TestWatermarkWiring:
    def test_watermark_lives_under_the_dataset(self):
        assert WATERMARK_PATH == f"{GCS_DATASET}/_bernco_hydrovu_transform_watermark.json"


# ── grouping / join ───────────────────────────────────────────────────────────

_LOCATIONS = {
    6255051791532032: {
        "name": "SierraVista-966932",
        "description": "",
        "latitude": 35.123,
        "longitude": -106.353,
    },
    4890597735137280: {
        "name": "E-94077-1193582VL (Anaya-1)",
        "description": "1193582",
        "latitude": 35.062,
        "longitude": -106.151,
    },
}


def _row(location_id=6255051791532032, timestamp=1782346800, value=70.97, parameter_id="4"):
    return {
        "reading_id": f"{location_id}_{parameter_id}_{timestamp}",
        "location_id": location_id,
        "timestamp": timestamp,
        "parameter_id": parameter_id,
        "unit_id": "35",
        "value": value,
    }


class TestGroupReadingsByLocation:
    def test_one_record_per_location(self):
        rows = [_row(location_id=6255051791532032), _row(location_id=4890597735137280)]
        assert len(group_readings_by_location(rows, _LOCATIONS)) == 2

    def test_readings_for_one_location_are_collected_into_one_record(self):
        rows = [_row(timestamp=1782346800), _row(timestamp=1782361200)]
        records = group_readings_by_location(rows, _LOCATIONS)
        assert len(records) == 1
        assert len(records[0]["readings"]) == 2

    def test_joins_location_metadata(self):
        records = group_readings_by_location([_row()], _LOCATIONS)
        rec = records[0]
        assert rec["location_name"] == "SierraVista-966932"
        assert rec["latitude"] == 35.123
        assert rec["longitude"] == -106.353

    def test_joins_well_number_description(self):
        records = group_readings_by_location([_row(location_id=4890597735137280)], _LOCATIONS)
        assert records[0]["location_description"] == "1193582"

    def test_unknown_location_still_produces_a_record(self):
        """A reading whose location is missing from the locations parquet must not
        kill the run."""
        records = group_readings_by_location([_row(location_id=999)], _LOCATIONS)
        assert len(records) == 1
        assert records[0]["location_name"] == ""
        assert records[0]["latitude"] is None

    def test_reading_shape_is_what_the_adapter_expects(self):
        records = group_readings_by_location([_row()], _LOCATIONS)
        reading = records[0]["readings"][0]
        assert set(reading) == {"parameter_id", "unit_id", "timestamp", "value"}

    def test_empty_rows_yields_no_records(self):
        assert group_readings_by_location([], _LOCATIONS) == []
