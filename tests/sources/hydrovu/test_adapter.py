"""
tests/sources/hydrovu/test_adapter.py

Unit tests for HydroVuAdapter.
No real API calls — uses mock records matching the grouped record shape
produced by hydrovu.transform._group_by_location().

Record shape (one per location):
  {
    "location_id":          int,
    "location_name":        str,
    "location_description": str,   # well number or "" if unnamed
    "latitude":             float,
    "longitude":            float,
    "readings": [
      {"parameter_id": str, "unit_id": str, "timestamp": int, "value": float},
    ]
  }

Parameter IDs (confirmed June 2026):
  "4"  = Depth to Water (metres → convert to feet)
  "1"  = Temperature (skipped)
  "33" = Battery Level (skipped)
"""

from datetime import UTC

from aqueduct_dagster.sources.hydrovu.adapter import (
    METRES_TO_FEET,
    HydroVuAdapter,
)

# ── Shared test data ──────────────────────────────────────────────────────────

DTW_READING = {"parameter_id": "4", "unit_id": "35", "timestamp": 1780704000, "value": 10.0}
TEMP_READING = {"parameter_id": "1", "unit_id": "1", "timestamp": 1780704000, "value": 22.5}
BATTERY_READING = {"parameter_id": "33", "unit_id": "241", "timestamp": 1780704000, "value": 54.0}


def _record(
    location_id=4745648669458432,
    location_name="Bartlett Level Troll",
    location_description="827276",
    latitude=33.067,
    longitude=-104.371,
    readings=None,
) -> dict:
    return {
        "location_id": location_id,
        "location_name": location_name,
        "location_description": location_description,
        "latitude": latitude,
        "longitude": longitude,
        "readings": readings if readings is not None else [DTW_READING],
    }


# ── to_thing ──────────────────────────────────────────────────────────────────


class TestToThing:
    def test_external_key_uses_integer_location_id(self):
        rec = _record(location_id=4745648669458432, location_description="827276")
        thing = HydroVuAdapter([rec]).to_thing(rec)
        assert thing.external_key == "pvacd-4745648669458432"

    def test_external_key_uses_integer_location_id_when_description_empty(self):
        rec = _record(location_id=4586726273318912, location_description="")
        thing = HydroVuAdapter([rec]).to_thing(rec)
        assert thing.external_key == "pvacd-4586726273318912"

    def test_location_external_key_matches_thing(self):
        rec = _record()
        thing = HydroVuAdapter([rec]).to_thing(rec)
        assert thing.location.external_key == thing.external_key

    def test_agency_in_properties(self):
        rec = _record()
        thing = HydroVuAdapter([rec]).to_thing(rec)
        assert thing.properties["agency"] == "PVACD"

    def test_source_id_is_integer_location_id(self):
        rec = _record(location_id=4745648669458432, location_description="827276")
        thing = HydroVuAdapter([rec]).to_thing(rec)
        assert thing.properties["source_id"] == 4745648669458432

    def test_well_number_stored_in_hydrovu_description(self):
        rec = _record(location_id=4745648669458432, location_description="827276")
        thing = HydroVuAdapter([rec]).to_thing(rec)
        assert thing.properties["hydrovu.description"] == "827276"

    def test_geometry_is_geojson_point(self):
        rec = _record(latitude=33.067, longitude=-104.371)
        thing = HydroVuAdapter([rec]).to_thing(rec)
        geom = thing.location.geometry
        assert geom["type"] == "Point"
        assert geom["coordinates"] == [-104.371, 33.067]  # [lon, lat] order

    def test_thing_name_is_water_well(self):
        rec = _record(location_name="Bartlett Level Troll")
        thing = HydroVuAdapter([rec]).to_thing(rec)
        assert thing.name == "Water Well"

    def test_location_name_matches_hydrovu_name(self):
        rec = _record(location_name="Bartlett Level Troll")
        thing = HydroVuAdapter([rec]).to_thing(rec)
        assert thing.location.name == "Bartlett Level Troll"


# ── to_observations ───────────────────────────────────────────────────────────


class TestToObservations:
    def test_returns_only_dtw_readings(self):
        rec = _record(readings=[DTW_READING, TEMP_READING, BATTERY_READING])
        obs = HydroVuAdapter([rec]).to_observations(rec)
        assert len(obs) == 1

    def test_skips_temperature(self):
        rec = _record(readings=[TEMP_READING])
        assert HydroVuAdapter([rec]).to_observations(rec) == []

    def test_skips_battery(self):
        rec = _record(readings=[BATTERY_READING])
        assert HydroVuAdapter([rec]).to_observations(rec) == []

    def test_empty_readings_returns_empty(self):
        rec = _record(readings=[])
        assert HydroVuAdapter([rec]).to_observations(rec) == []

    def test_converts_metres_to_feet(self):
        rec = _record(readings=[{**DTW_READING, "value": 10.0}])
        obs = HydroVuAdapter([rec]).to_observations(rec)
        assert abs(obs[0].result - 10.0 * METRES_TO_FEET) < 0.001

    def test_phenomenon_time_is_utc(self):
        rec = _record(readings=[DTW_READING])
        obs = HydroVuAdapter([rec]).to_observations(rec)
        assert obs[0].phenomenon_time.tzinfo == UTC

    def test_phenomenon_time_correct_value(self):
        rec = _record(readings=[{**DTW_READING, "timestamp": 1780704000}])
        obs = HydroVuAdapter([rec]).to_observations(rec)
        assert obs[0].phenomenon_time.timestamp() == 1780704000

    def test_datastream_key_format(self):
        rec = _record(location_id=4745648669458432, location_description="827276")
        obs = HydroVuAdapter([rec]).to_observations(rec)
        assert obs[0].datastream_external_key == "pvacd-4745648669458432-dtw"

    def test_multiple_dtw_readings_all_returned(self):
        readings = [
            {**DTW_READING, "timestamp": 1780704000, "value": 10.0},
            {**DTW_READING, "timestamp": 1780707600, "value": 10.5},
        ]
        rec = _record(readings=readings)
        obs = HydroVuAdapter([rec]).to_observations(rec)
        assert len(obs) == 2


# ── _build_datastreams ────────────────────────────────────────────────────────


class TestBuildDatastreams:
    def _make_thing(self):
        rec = _record()
        adapter = HydroVuAdapter([rec])
        return adapter.to_thing(rec), adapter

    def test_returns_exactly_one_datastream(self):
        thing, adapter = self._make_thing()
        assert len(adapter._build_datastreams(thing)) == 1

    def test_datastream_external_key_format(self):
        thing, adapter = self._make_thing()
        assert adapter._build_datastreams(thing)[0].external_key == "pvacd-4745648669458432-dtw"

    def test_datastream_unit_symbol_is_ft(self):
        thing, adapter = self._make_thing()
        ds = adapter._build_datastreams(thing)[0]
        assert ds.unit_of_measurement["symbol"] == "ft"

    def test_datastream_unit_name_is_Foot(self):
        thing, adapter = self._make_thing()
        ds = adapter._build_datastreams(thing)[0]
        assert ds.unit_of_measurement["name"] == "Foot"

    def test_datastream_thing_reference(self):
        thing, adapter = self._make_thing()
        ds = adapter._build_datastreams(thing)[0]
        assert ds.thing is thing


# ── run (end-to-end) ──────────────────────────────────────────────────────────


class TestRun:
    def test_one_bundle_per_location(self):
        records = [
            _record(location_id=1, location_description="111"),
            _record(location_id=2, location_description="222"),
        ]
        bundles = list(HydroVuAdapter(records).run())
        assert len(bundles) == 2

    def test_bundle_observations_keyed_by_datastream(self):
        rec = _record(
            location_id=4745648669458432, location_description="827276", readings=[DTW_READING]
        )
        bundles = list(HydroVuAdapter([rec]).run())
        assert "pvacd-4745648669458432-dtw" in bundles[0].observations

    def test_non_dtw_readings_produce_empty_observations(self):
        rec = _record(readings=[TEMP_READING, BATTERY_READING])
        bundles = list(HydroVuAdapter([rec]).run())
        assert all(len(v) == 0 for v in bundles[0].observations.values())

    def test_empty_records_yields_no_bundles(self):
        assert list(HydroVuAdapter([]).run()) == []
