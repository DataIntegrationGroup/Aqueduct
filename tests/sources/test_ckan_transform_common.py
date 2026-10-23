"""
Unit tests for sources/ckan_transform_common.py, agency-agnostic: a synthetic
agency whose files already follow the WDI best practices, so it needs nothing
but a subclass. The EBWPC shim on top of this is covered in
tests/sources/ebwpc_ckan/.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from aqueduct_dagster.canonical.canonical_constants import (
    DTW_OBS_PROP,
    MANUAL_SENSOR,
    METRES_TO_FEET,
    TRANSDUCER_SENSOR,
    UNIT_FOOT,
)
from aqueduct_dagster.sources.ckan_transform_common import (
    DATA_SOURCE,
    WdiWaterLevelAdapter,
    clean_cell,
    group_by_site,
    latest_snapshots,
    read_ckan_table,
    transform_metadata,
)


class _WdiAgencyAdapter(WdiWaterLevelAdapter):
    """A WDI-compliant agency on a UTC-06:00 clock: configuration only."""

    AGENCY = "TESTAG"
    SOURCE_TZ = timezone(timedelta(hours=-6))


def _measurement(**overrides: object) -> dict:
    measurement = {
        "site_id": "W-1",
        "measurement_date": "2024-04-26",
        "measurement_time": "08:18:00",
        "depth_to_water": "54.5",
        "depth_to_water_unit": "ft",
        "measurement_type": "manual",
    }
    measurement.update(overrides)
    return measurement


def _record(measurements: list[dict] | None = None, **overrides: object) -> dict:
    record = {
        "site_id": "W-1",
        "site_name": "Well One",
        "longitude": -106.24,
        "latitude": 34.82,
        "elevation": None,
        "elevation_units": None,
        "lithology": None,
        "alternate_id": None,
        "location_source_specific": {},
        "thing_source_specific": {},
        "measurements": measurements if measurements is not None else [_measurement()],
    }
    record.update(overrides)
    return record


def _observations(*measurements: dict) -> list:
    return _WdiAgencyAdapter([]).to_observations(_record(list(measurements)))


# ── configuration ─────────────────────────────────────────────────────────────


class TestConfiguration:
    def test_agency_is_required(self):
        class _NoAgency(WdiWaterLevelAdapter):
            SOURCE_TZ = UTC

        with pytest.raises(ValueError, match="AGENCY"):
            _NoAgency([])

    def test_source_timezone_is_required(self):
        class _NoTimezone(WdiWaterLevelAdapter):
            AGENCY = "TESTAG"

        with pytest.raises(ValueError, match="SOURCE_TZ"):
            _NoTimezone([])


# ── to_thing ──────────────────────────────────────────────────────────────────


class TestToThing:
    def test_keys_follow_the_agency_and_site_id(self):
        thing = _WdiAgencyAdapter([]).to_thing(_record())
        assert thing.external_key == "testag-W-1"
        assert thing.location.external_key == "testag-W-1"

    def test_standard_properties(self):
        thing = _WdiAgencyAdapter([]).to_thing(_record())
        assert thing.name == "Water Well"
        assert thing.properties["agency"] == "TESTAG"
        assert thing.properties["source_id"] == "W-1"
        assert thing.location.name == "Well One"
        assert thing.location.properties["source_id"] == "W-1"
        assert "agency" not in thing.location.properties

    def test_site_id_names_the_location_when_there_is_no_site_name(self):
        thing = _WdiAgencyAdapter([]).to_thing(_record(site_name=None))
        assert thing.location.name == "W-1"

    def test_geometry_is_longitude_then_latitude(self):
        thing = _WdiAgencyAdapter([]).to_thing(_record())
        assert thing.location.geometry == {"type": "Point", "coordinates": [-106.24, 34.82]}

    def test_alternate_id_only_when_present(self):
        assert "alternate_id" not in _WdiAgencyAdapter([]).to_thing(_record()).properties
        alternate = [{"id": "RG-1234", "agency": "NMOSE"}]
        thing = _WdiAgencyAdapter([]).to_thing(_record(alternate_id=alternate))
        assert thing.properties["alternate_id"] == alternate
        assert thing.location.properties["alternate_id"] == alternate

    def test_elevation_in_feet_is_stored_in_metres(self):
        thing = _WdiAgencyAdapter([]).to_thing(_record(elevation=6712.0, elevation_units="ft"))
        elevation = thing.location.properties["source_specific"]["elevation"]
        assert elevation == {"value": pytest.approx(6712.0 / METRES_TO_FEET), "unit": "m"}

    def test_elevation_in_metres_is_kept(self):
        thing = _WdiAgencyAdapter([]).to_thing(_record(elevation=2045.8, elevation_units="m"))
        assert thing.location.properties["source_specific"]["elevation"] == {
            "value": 2045.8,
            "unit": "m",
        }

    def test_elevation_in_an_unknown_unit_is_omitted(self):
        thing = _WdiAgencyAdapter([]).to_thing(_record(elevation=10.0, elevation_units="cubits"))
        assert thing.location.properties["source_specific"]["elevation"] is None

    def test_lithology_and_agency_extras_land_in_source_specific(self):
        thing = _WdiAgencyAdapter([]).to_thing(
            _record(
                lithology="Alluvium",
                location_source_specific={"county": "Torrance"},
                thing_source_specific={"primary_use": "Monitoring"},
            )
        )
        assert thing.properties["source_specific"] == {
            "primary_use": "Monitoring",
            "lithology": "Alluvium",
        }
        assert thing.location.properties["source_specific"]["county"] == "Torrance"


# ── to_observations ───────────────────────────────────────────────────────────


class TestToObservations:
    def test_local_time_is_converted_to_utc(self):
        [obs] = _observations(_measurement(measurement_time="08:18:00"))
        assert obs.phenomenon_time == datetime(2024, 4, 26, 14, 18, tzinfo=UTC)
        assert obs.phenomenon_time.tzinfo == UTC

    def test_missing_time_is_local_midnight_and_flagged(self):
        [obs] = _observations(_measurement(measurement_time=None))
        assert obs.phenomenon_time == datetime(2024, 4, 26, 6, 0, tzinfo=UTC)
        assert obs.parameters["source_specific"] == {"time_missing": True}

    def test_a_timed_reading_has_no_source_specific_parameters(self):
        [obs] = _observations(_measurement())
        assert obs.parameters == {"data_source": DATA_SOURCE}

    def test_wdi_desired_fields_become_standard_parameters(self):
        [obs] = _observations(
            _measurement(measuring_agency="NMBGMR", water_level_status="static", notes="windy")
        )
        assert obs.parameters["measuring_agency"] == "NMBGMR"
        assert obs.parameters["water_level_status"] == "static"
        assert obs.parameters["source_specific"] == {"notes": "windy"}

    def test_feet_pass_through(self):
        [obs] = _observations(_measurement(depth_to_water="54.5", depth_to_water_unit="ft"))
        assert obs.result == 54.5

    def test_metres_are_converted_to_feet(self):
        [obs] = _observations(_measurement(depth_to_water=10.0, depth_to_water_unit="m"))
        assert obs.result == pytest.approx(10.0 * METRES_TO_FEET)

    def test_an_unknown_unit_is_refused(self):
        adapter = _WdiAgencyAdapter([])
        rec = _record([_measurement(depth_to_water_unit="psi")])
        assert adapter.to_observations(rec) == []
        assert adapter.dropped == {"unusable_depth_or_unit": 1}

    def test_an_unknown_measurement_type_is_dropped(self):
        adapter = _WdiAgencyAdapter([])
        rec = _record([_measurement(measurement_type="estimated")])
        assert adapter.to_observations(rec) == []
        assert adapter.dropped == {"unknown_measurement_type": 1}

    def test_an_unreadable_date_is_dropped(self):
        adapter = _WdiAgencyAdapter([])
        rec = _record([_measurement(measurement_date="4/26/2024")])
        assert adapter.to_observations(rec) == []
        assert adapter.dropped == {"unusable_timestamp": 1}

    def test_a_future_timestamp_is_dropped(self):
        """It would move the datastream's FROST watermark past every real reading."""
        adapter = _WdiAgencyAdapter([])
        rec = _record([_measurement(measurement_date="2085-06-01"), _measurement()])
        assert len(adapter.to_observations(rec)) == 1
        assert adapter.dropped == {"future_timestamp": 1}

    def test_exact_duplicates_are_dropped(self):
        adapter = _WdiAgencyAdapter([])
        rec = _record([_measurement(), _measurement(), _measurement(depth_to_water="55.0")])
        assert len(adapter.to_observations(rec)) == 2
        assert adapter.dropped == {"duplicate": 1}

    def test_each_type_gets_its_own_datastream_key(self):
        manual, transducer = _observations(
            _measurement(measurement_type="manual"), _measurement(measurement_type="Transducer")
        )
        assert manual.datastream_external_key == "testag-W-1-dtw-manual"
        assert transducer.datastream_external_key == "testag-W-1-dtw"


# ── _build_datastreams / run ──────────────────────────────────────────────────


class TestDatastreams:
    def test_only_types_with_observations_get_a_datastream(self):
        [bundle] = _WdiAgencyAdapter([_record()]).run()
        assert [ds.external_key for ds in bundle.datastreams] == ["testag-W-1-dtw-manual"]

    def test_transducer_and_manual_streams(self):
        rec = _record(
            [_measurement(measurement_type="transducer"), _measurement(measurement_type="manual")]
        )
        [bundle] = _WdiAgencyAdapter([rec]).run()
        transducer, manual = bundle.datastreams

        assert transducer.external_key == "testag-W-1-dtw"
        assert transducer.name == "Groundwater Levels"
        assert transducer.sensor == TRANSDUCER_SENSOR
        assert transducer.properties == {
            "topic": "Water Quantity",
            "source_specific": {"is_continuous": True},
        }

        assert manual.external_key == "testag-W-1-dtw-manual"
        assert manual.name == "Manual Groundwater Levels"
        assert manual.sensor == MANUAL_SENSOR
        assert manual.properties["source_specific"] == {"is_continuous": False}

        for ds in bundle.datastreams:
            assert ds.observed_property == DTW_OBS_PROP
            assert ds.unit_of_measurement == UNIT_FOOT
            assert len(bundle.observations[ds.external_key]) == 1

    def test_a_site_with_nothing_usable_gets_no_datastream(self):
        rec = _record([_measurement(depth_to_water_unit="psi")])
        [bundle] = _WdiAgencyAdapter([rec]).run()
        assert bundle.datastreams == []

    def test_a_bad_record_is_counted_and_the_rest_still_map(self):
        adapter = _WdiAgencyAdapter([_record(longitude=None), _record(site_id="W-2")])
        bundles = list(adapter.run())
        assert len(bundles) == 1
        assert adapter.failure_count == 1


# ── raw-zone helpers ──────────────────────────────────────────────────────────


class TestCleanCell:
    @pytest.mark.parametrize("junk", [None, "", "  ", "N/A", "n/a", "#REF!", "#VALUE!", "<Null>"])
    def test_junk_is_none(self, junk):
        assert clean_cell(junk) is None

    def test_values_are_stripped(self):
        assert clean_cell(" E-9673 ") == "E-9673"


class TestLatestSnapshots:
    def test_keeps_only_each_resources_newest_snapshot(self):
        rows = [
            {"resource_id": "a", "_dlt_load_id": "1700000000.1", "row_number": 1},
            {"resource_id": "a", "_dlt_load_id": "1700000000.1", "row_number": 2},
            {"resource_id": "b", "_dlt_load_id": "1700000000.1", "row_number": 1},
            {"resource_id": "a", "_dlt_load_id": "1800000000.2", "row_number": 1},
        ]
        kept, superseded = latest_snapshots(rows)
        assert kept == [rows[2], rows[3]]
        assert superseded == 2

    def test_no_rows(self):
        assert latest_snapshots([]) == ([], 0)


class TestGroupBySite:
    _LOCATIONS = {"w-1": {"site_id": "W-1"}, "w-2": {"site_id": "W-2"}}

    def test_one_record_per_site_carrying_its_location(self):
        records, unmatched = group_by_site(
            [_measurement(site_id="w-1"), _measurement(site_id="w-1"), _measurement(site_id="w-2")],
            self._LOCATIONS,
        )
        assert [(r["site_id"], len(r["measurements"])) for r in records] == [("W-1", 2), ("W-2", 1)]
        assert unmatched == {}

    def test_sites_without_a_location_are_dropped_and_counted(self, caplog):
        records, unmatched = group_by_site(
            [_measurement(site_id="w-9"), _measurement(site_id="w-9")], self._LOCATIONS
        )
        assert records == []
        assert unmatched == {"w-9": 2}
        assert "w-9 has no location" in caplog.text

    def test_no_measurements(self):
        assert group_by_site([], self._LOCATIONS) == ([], {})


class TestReadCkanTable:
    def test_raises_when_the_table_never_landed(self):
        fs = MagicMock()
        fs.glob.return_value = []
        with pytest.raises(FileNotFoundError, match="ckan_locations"):
            read_ckan_table("bucket", "raw_x", "ckan_locations", fs)
        fs.glob.assert_called_once_with("bucket/raw_x/ckan_locations/**/*.parquet")


class TestTransformMetadata:
    def test_defaults_describe_an_empty_run(self):
        metadata = transform_metadata(files_skipped_bad_name=0, since_load_id=1.5, max_load_id=None)
        assert metadata["rows_read"].value == 0
        assert metadata["bundles_produced"].value == 0
        assert metadata["sites_without_location"].value == {}
        assert metadata["watermark_before"].value == "1.5"
        assert metadata["watermark_after"].value == "None"
