"""
tests/sources/bernco_hydrovu/test_adapter.py

Unit tests for BerncoHydroVuAdapter.

No real API or GCS calls. Uses mock records matching the grouped record shape
produced by hydrovu_transform_common.group_readings_by_location().

Record shape (one per location):
  {
    "location_id":          int,
    "location_name":        str,
    "location_description": str,   # well/permit number, or "" if unset
    "latitude":             float,
    "longitude":            float,
    "readings": [
      {"parameter_id": str, "unit_id": str, "timestamp": int, "value": float},
    ]
  }

Parameter IDs (per /sispec/friendlynames, 2026-08-24):
  "4"  = Level: Depth to Water (metres -> converted to feet)
  "3"  = Depth (NOT depth to water). Same unitId "35", opposite direction
  "1"  = Temperature, "2" = Pressure, "9" = Actual Conductivity, "33" = Battery Level
"""

from datetime import UTC

from aqueduct_dagster.canonical.canonical_constants import METRES_TO_FEET
from aqueduct_dagster.sources.bernco_hydrovu.adapter import BerncoHydroVuAdapter

# ── Shared test data ──────────────────────────────────────────────────────────

SONDE_ID = 6255051791532032  # SierraVista-966932 - Aqua TROLL sonde, carries DTW
VULINK_ID = 4890597735137280  # E-94077-1193582VL - gateway diagnostics, no DTW
WHISPERING_PINES_ID = 5617246532927488  # returns a DTW reading stamped epoch 0

# 2026-05-01, BernCo's initial_start_date and so its sentinel floor.
FLOOR = 1777593600

# The three DTW readings SierraVista returns in the doc's captured response.
DTW_READINGS = [
    {"parameter_id": "4", "unit_id": "35", "timestamp": 1782346800, "value": 70.972789728},
    {"parameter_id": "4", "unit_id": "35", "timestamp": 1782361200, "value": 70.975276896},
    {"parameter_id": "4", "unit_id": "35", "timestamp": 1782375600, "value": 70.97276534400001},
]

PRESSURE_READING = {
    "parameter_id": "2",
    "unit_id": "17",
    "timestamp": 1782346800,
    "value": 18.035131,
}
TEMP_READING = {"parameter_id": "1", "unit_id": "1", "timestamp": 1782346800, "value": 14.099037}
CONDUCTIVITY_READING = {
    "parameter_id": "9",
    "unit_id": "65",
    "timestamp": 1782346800,
    "value": 1380.7738,
}
BATTERY_READING = {
    "parameter_id": "33",
    "unit_id": "241",
    "timestamp": 1782000000,
    "value": 74.40609741210938,
}
BARO_READING = {
    "parameter_id": "16",
    "unit_id": "17",
    "timestamp": 1782000000,
    "value": 11.57793941870145,
}
# Parameter 3 is the sensor's depth below the water surface, the opposite direction
# from parameter 4, and easy to conflate because both are unitId "35".
DEPTH_READING = {"parameter_id": "3", "unit_id": "35", "timestamp": 1782346800, "value": 2.5}

# Bad device clocks, not 1970 measurements. The values look entirely plausible.
SENTINEL_EPOCH_READING = {"parameter_id": "4", "unit_id": "35", "timestamp": 0, "value": 108.683}
SENTINEL_960_READING = {"parameter_id": "4", "unit_id": "35", "timestamp": 960, "value": 33.1}


def _record(
    location_id=SONDE_ID,
    location_name="SierraVista-966932",
    location_description="",
    latitude=35.123,
    longitude=-106.353,
    readings=None,
) -> dict:
    return {
        "location_id": location_id,
        "location_name": location_name,
        "location_description": location_description,
        "latitude": latitude,
        "longitude": longitude,
        "readings": readings if readings is not None else list(DTW_READINGS),
    }


def _vulink_record() -> dict:
    """The gateway-diagnostics profile: no parameterId="4" anywhere in it."""
    return _record(
        location_id=VULINK_ID,
        location_name="E-94077-1193582VL (Anaya-1)",
        location_description="1193582",
        latitude=35.062,
        longitude=-106.151,
        readings=[TEMP_READING, BATTERY_READING, BARO_READING],
    )


# ── to_thing ──────────────────────────────────────────────────────────────────


class TestToThing:
    def test_external_key_uses_integer_location_id(self):
        rec = _record(location_id=SONDE_ID)
        thing = BerncoHydroVuAdapter([rec]).to_thing(rec)
        assert thing.external_key == "bernco-6255051791532032"

    def test_external_key_uses_integer_location_id_when_description_populated(self):
        rec = _record(location_id=VULINK_ID, location_description="1193582")
        thing = BerncoHydroVuAdapter([rec]).to_thing(rec)
        assert thing.external_key == "bernco-4890597735137280"

    def test_location_external_key_matches_thing(self):
        rec = _record()
        thing = BerncoHydroVuAdapter([rec]).to_thing(rec)
        assert thing.location.external_key == thing.external_key

    def test_agency_in_properties(self):
        rec = _record()
        thing = BerncoHydroVuAdapter([rec]).to_thing(rec)
        assert thing.properties["agency"] == "BERNCO"

    def test_agency_not_in_location_properties(self):
        rec = _record()
        thing = BerncoHydroVuAdapter([rec]).to_thing(rec)
        assert "agency" not in thing.location.properties

    def test_source_id_is_string_on_thing(self):
        rec = _record(location_id=SONDE_ID)
        thing = BerncoHydroVuAdapter([rec]).to_thing(rec)
        assert thing.properties["source_id"] == "6255051791532032"

    def test_source_id_is_string_on_location(self):
        rec = _record(location_id=SONDE_ID)
        thing = BerncoHydroVuAdapter([rec]).to_thing(rec)
        assert thing.location.properties["source_id"] == "6255051791532032"

    def test_well_number_stored_in_source_specific_on_thing(self):
        rec = _record(location_description="1193582")
        thing = BerncoHydroVuAdapter([rec]).to_thing(rec)
        assert thing.properties["source_specific"]["hydrovu_description"] == "1193582"

    def test_well_number_stored_in_source_specific_on_location(self):
        rec = _record(location_description="1193582")
        thing = BerncoHydroVuAdapter([rec]).to_thing(rec)
        assert thing.location.properties["source_specific"]["hydrovu_description"] == "1193582"

    def test_empty_description_is_preserved_as_empty_string(self):
        """27 of BernCo's 53 locations have no description — that must not become None."""
        rec = _record(location_description="")
        thing = BerncoHydroVuAdapter([rec]).to_thing(rec)
        assert thing.properties["source_specific"]["hydrovu_description"] == ""

    def test_geometry_is_geojson_point(self):
        rec = _record(latitude=35.123, longitude=-106.353)
        thing = BerncoHydroVuAdapter([rec]).to_thing(rec)
        geom = thing.location.geometry
        assert geom["type"] == "Point"
        assert geom["coordinates"] == [-106.353, 35.123]  # [lon, lat] order

    def test_thing_name_is_water_well(self):
        rec = _record()
        thing = BerncoHydroVuAdapter([rec]).to_thing(rec)
        assert thing.name == "Water Well"

    def test_location_name_matches_hydrovu_name(self):
        rec = _record(location_name="SierraVista-966932")
        thing = BerncoHydroVuAdapter([rec]).to_thing(rec)
        assert thing.location.name == "SierraVista-966932"


# ── to_observations ───────────────────────────────────────────────────────────


class TestToObservations:
    def test_sonde_record_yields_one_observation_per_dtw_reading(self):
        rec = _record(readings=[*DTW_READINGS, PRESSURE_READING, TEMP_READING])
        obs = BerncoHydroVuAdapter([rec]).to_observations(rec)
        assert len(obs) == 3

    def test_skips_temperature(self):
        rec = _record(readings=[TEMP_READING])
        assert BerncoHydroVuAdapter([rec]).to_observations(rec) == []

    def test_skips_pressure(self):
        rec = _record(readings=[PRESSURE_READING])
        assert BerncoHydroVuAdapter([rec]).to_observations(rec) == []

    def test_skips_conductivity(self):
        rec = _record(readings=[CONDUCTIVITY_READING])
        assert BerncoHydroVuAdapter([rec]).to_observations(rec) == []

    def test_skips_battery_level(self):
        rec = _record(readings=[BATTERY_READING])
        assert BerncoHydroVuAdapter([rec]).to_observations(rec) == []

    def test_skips_parameter_3_depth(self):
        """Parameter 3 is the sensor's depth below the water surface, not depth to
        water. Same unitId "35", opposite direction. Treating it as DTW would
        silently invert the measurement."""
        rec = _record(readings=[DEPTH_READING])
        assert BerncoHydroVuAdapter([rec]).to_observations(rec) == []

    def test_vulink_diagnostics_record_yields_no_observations(self):
        rec = _vulink_record()
        assert BerncoHydroVuAdapter([rec]).to_observations(rec) == []

    def test_empty_readings_returns_empty(self):
        rec = _record(readings=[])
        assert BerncoHydroVuAdapter([rec]).to_observations(rec) == []

    def test_missing_readings_key_returns_empty(self):
        rec = _record()
        del rec["readings"]
        assert BerncoHydroVuAdapter([rec]).to_observations(rec) == []

    def test_converts_metres_to_feet(self):
        rec = _record(readings=[DTW_READINGS[0]])
        obs = BerncoHydroVuAdapter([rec]).to_observations(rec)
        assert abs(obs[0].result - 70.972789728 * METRES_TO_FEET) < 0.001

    def test_converted_result_is_larger_than_the_metre_value(self):
        """Guards against the factor being applied the wrong way round."""
        rec = _record(readings=[DTW_READINGS[0]])
        obs = BerncoHydroVuAdapter([rec]).to_observations(rec)
        assert obs[0].result > 70.972789728

    def test_phenomenon_time_is_utc(self):
        rec = _record(readings=[DTW_READINGS[0]])
        obs = BerncoHydroVuAdapter([rec]).to_observations(rec)
        assert obs[0].phenomenon_time.tzinfo == UTC

    def test_phenomenon_time_correct_value(self):
        rec = _record(readings=[DTW_READINGS[0]])
        obs = BerncoHydroVuAdapter([rec]).to_observations(rec)
        assert obs[0].phenomenon_time.timestamp() == 1782346800

    def test_result_quality_is_none(self):
        """HydroVu returns no QC flags."""
        rec = _record(readings=[DTW_READINGS[0]])
        obs = BerncoHydroVuAdapter([rec]).to_observations(rec)
        assert obs[0].result_quality is None

    def test_datastream_key_format(self):
        rec = _record(location_id=SONDE_ID)
        obs = BerncoHydroVuAdapter([rec]).to_observations(rec)
        assert obs[0].datastream_external_key == "bernco-6255051791532032-dtw"


# ── Sentinel timestamps ───────────────────────────────────────────────────────


class TestSentinelFloor:
    def test_drops_epoch_zero_reading(self):
        """WhisperingPines-1002958 reports DTW 108.683 m at timestamp 0."""
        rec = _record(
            location_id=WHISPERING_PINES_ID,
            readings=[SENTINEL_EPOCH_READING, *DTW_READINGS],
        )
        obs = BerncoHydroVuAdapter([rec], min_timestamp=FLOOR).to_observations(rec)
        assert len(obs) == 3

    def test_drops_near_epoch_reading(self):
        """E-55-POD 15-1173850TD's sentinel is 960, not 0. A `timestamp > 0`
        check would let this through."""
        rec = _record(readings=[SENTINEL_960_READING])
        obs = BerncoHydroVuAdapter([rec], min_timestamp=FLOOR).to_observations(rec)
        assert obs == []

    def test_keeps_reading_exactly_at_the_floor(self):
        at_floor = {"parameter_id": "4", "unit_id": "35", "timestamp": FLOOR, "value": 12.0}
        rec = _record(readings=[at_floor])
        obs = BerncoHydroVuAdapter([rec], min_timestamp=FLOOR).to_observations(rec)
        assert len(obs) == 1

    def test_keeps_reading_one_second_above_the_floor(self):
        above = {"parameter_id": "4", "unit_id": "35", "timestamp": FLOOR + 1, "value": 12.0}
        rec = _record(readings=[above])
        obs = BerncoHydroVuAdapter([rec], min_timestamp=FLOOR).to_observations(rec)
        assert len(obs) == 1

    def test_no_floor_by_default_keeps_sentinels(self):
        """min_timestamp=None disables the floor. PVACD relies on this setting."""
        rec = _record(readings=[SENTINEL_EPOCH_READING])
        obs = BerncoHydroVuAdapter([rec]).to_observations(rec)
        assert len(obs) == 1

    def test_dropped_sentinel_is_not_counted_as_an_adapter_failure(self):
        """A filtered reading is expected data, not a broken record."""
        rec = _record(readings=[SENTINEL_EPOCH_READING, *DTW_READINGS])
        adapter = BerncoHydroVuAdapter([rec], min_timestamp=FLOOR)
        list(adapter.run())
        assert adapter.failure_count == 0


# ── _build_datastreams ────────────────────────────────────────────────────────


class TestBuildDatastreams:
    def _make_thing(self):
        rec = _record()
        adapter = BerncoHydroVuAdapter([rec])
        return adapter.to_thing(rec), adapter

    def test_returns_exactly_one_datastream(self):
        thing, adapter = self._make_thing()
        assert len(adapter._build_datastreams(thing)) == 1

    def test_datastream_external_key_format(self):
        thing, adapter = self._make_thing()
        assert adapter._build_datastreams(thing)[0].external_key == "bernco-6255051791532032-dtw"

    def test_datastream_unit_symbol_is_ft(self):
        thing, adapter = self._make_thing()
        ds = adapter._build_datastreams(thing)[0]
        assert ds.unit_of_measurement["symbol"] == "ft"

    def test_datastream_unit_name_is_Foot(self):
        thing, adapter = self._make_thing()
        ds = adapter._build_datastreams(thing)[0]
        assert ds.unit_of_measurement["name"] == "Foot"

    def test_datastream_sensor_is_vulink(self):
        thing, adapter = self._make_thing()
        ds = adapter._build_datastreams(thing)[0]
        assert ds.sensor.external_key == "sensor-hydrovu-vulink"

    def test_datastream_observed_property_is_dtw(self):
        thing, adapter = self._make_thing()
        ds = adapter._build_datastreams(thing)[0]
        assert ds.observed_property.external_key == "observed-property-depth-to-water-bgs"

    def test_datastream_thing_reference(self):
        thing, adapter = self._make_thing()
        ds = adapter._build_datastreams(thing)[0]
        assert ds.thing is thing


# ── run (end-to-end) ──────────────────────────────────────────────────────────


class TestRun:
    def test_one_bundle_per_location(self):
        records = [_record(location_id=SONDE_ID), _vulink_record()]
        bundles = list(BerncoHydroVuAdapter(records).run())
        assert len(bundles) == 2

    def test_bundle_observations_keyed_by_datastream(self):
        rec = _record(location_id=SONDE_ID)
        bundles = list(BerncoHydroVuAdapter([rec]).run())
        assert "bernco-6255051791532032-dtw" in bundles[0].observations

    def test_bundle_carries_every_dtw_reading(self):
        rec = _record(location_id=SONDE_ID)
        bundles = list(BerncoHydroVuAdapter([rec]).run())
        assert len(bundles[0].observations["bernco-6255051791532032-dtw"]) == 3

    def test_vulink_location_still_yields_a_bundle_with_its_datastream(self):
        """No observations, but the Thing/Location/Datastream metadata is still
        worth upserting. The location exists even when it reports no water level."""
        bundles = list(BerncoHydroVuAdapter([_vulink_record()]).run())
        assert len(bundles) == 1
        assert bundles[0].datastreams[0].external_key == "bernco-4890597735137280-dtw"
        assert all(len(v) == 0 for v in bundles[0].observations.values())

    def test_empty_records_yields_no_bundles(self):
        assert list(BerncoHydroVuAdapter([]).run()) == []

    def test_fully_successful_batch_has_no_failures(self):
        records = [_record(location_id=SONDE_ID), _vulink_record()]
        adapter = BerncoHydroVuAdapter(records)
        list(adapter.run())
        assert adapter.failure_count == 0

    def test_bad_record_is_skipped_and_counted_as_failure(self):
        """Missing location_id raises KeyError inside to_thing(). Caught by
        BaseAdapter.run(), which must count it rather than only log it."""
        bad_record = _record()
        del bad_record["location_id"]

        adapter = BerncoHydroVuAdapter([bad_record])
        bundles = list(adapter.run())

        assert bundles == []
        assert adapter.failure_count == 1

    def test_mixed_batch_good_records_still_produce_bundles(self):
        bad_record = _record()
        del bad_record["location_id"]
        records = [_record(location_id=SONDE_ID), bad_record, _vulink_record()]

        adapter = BerncoHydroVuAdapter(records)
        bundles = list(adapter.run())

        assert len(bundles) == 2
        assert adapter.failure_count == 1
