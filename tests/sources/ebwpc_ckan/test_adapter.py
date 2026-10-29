"""
Unit tests for EBWPC's shim (raw CKAN rows -> WDI shape) and EbwpcCkanAdapter.
Input rows are the raw samples from docs/sources/ebwpc_ckan.md, shaped exactly
as they land in parquet: ckan_common.snapshot_rows() plus dlt's column-name
normalisation (NMBG_ID -> nmbg_id, UTM_Zone13N_Easting -> utm_zone13_n_easting).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from dlt.common.normalizers.naming.snake_case import NamingConvention

from aqueduct_dagster.canonical.canonical_constants import (
    DTW_OBS_PROP,
    MANUAL_SENSOR,
    METRES_TO_FEET,
    TRANSDUCER_SENSOR,
)
from aqueduct_dagster.canonical.canonical_model import CanonicalBundle
from aqueduct_dagster.sources.ckan_common import resource_metadata_rows, snapshot_rows
from aqueduct_dagster.sources.ckan_transform_common import group_by_site
from aqueduct_dagster.sources.ebwpc_ckan.adapter import (
    EbwpcCkanAdapter,
    ebwpc_locations,
    ebwpc_measurements,
    parse_description,
    site_key,
    well_id,
)
from tests.sources.ebwpc_ckan.conftest import (
    ALL_RESOURCES,
    E_1639,
    E_8428,
    E_9673,
    FILES,
    LOCATIONS,
    _resource,
)

_NAMING = NamingConvention()

# The live locations file, complete (docs/sources/ebwpc_ckan.md). Note E-2043
# and the trailing space after E-9673.
_ALL_LOCATIONS_CSV = (
    b"NMBG_ID,UTM_Zone13N_Easting,UTM_Zone13N_Northing,Datum\r\n"
    b"E-2298,388505,3892477,NAD83\r\n"
    b"E-94077,395066,3880536,NAD83\r\n"
    b"Osita Ranch,428285,3889072,NAD83\r\n"
    b"Magnum Steel,401055,3875642,NAD83\r\n"
    b"E-00428,399315,3865705,NAD83\r\n"
    b"E-50-4,405656,3861382,NAD83\r\n"
    b"E-2043,398176,3855674,NAD83\r\n"
    b"E-1639-POD1,424100,3812580,NAD83\r\n"
    b"T-6363,404197,3802110,NAD83\r\n"
    b"E-10652,393754,3824033,NAD83\r\n"
    b"Smith-1,387742,3824322,NAD83\r\n"
    b"E-7545,381162,3834482,NAD83\r\n"
    b"E-8428,386426,3853605,NAD83\r\n"
    b"E-9673 ,381972,3879947,NAD83\r\n"
    b"E-9407-archived,432560,3837007,NAD83\r\n"
    b"Lujan-1-archived,393864,3824679,NAD83\r\n"
    b"Ruby Shaw WM-archived,382687,3822107,NAD83\r\n"
    b"E-50-1-archived,405241,3864604,NAD83\r\n"
    b"E-6385-archived,402585,3874082,NAD83\r\n"
    b"Greene-4-archived,403139,3792977,NAD83\r\n"
    b"Hagerman HQ-archived,426891,3889512,NAD83\r\n"
)
# An archived well with only below-top-of-casing readings.
_E_6385 = _resource("6385", "E-6385-Archived", "e-6385.csv")
_E_6385_CSV = (
    b"site_id,measurement_date,measurement_time,manual_depth_Below_TOC\r\n"
    b"E-6385-archived,2/18/2009,16:55,131.81\r\n"
    b"E-6385-archived,4/15/2009,16:06,136.16\r\n"
)
_HEADER = b"site_id,measurement_date,measurement_time,transducer_depth_bgs,manual_depth_bgs\r\n"


def _landed(resource: dict, body: bytes) -> list[dict]:
    """Rows of one file as they land in parquet."""
    return [
        {_NAMING.normalize_identifier(column): value for column, value in row.items()}
        for row in snapshot_rows(resource, body)
    ]


def _well(resource: dict, *lines: bytes) -> list[dict]:
    """A synthetic well file in the standard columns."""
    return _landed(resource, _HEADER + b"".join(line + b"\r\n" for line in lines))


_LOCATIONS = ebwpc_locations(
    _landed(LOCATIONS, _ALL_LOCATIONS_CSV), resource_metadata_rows(ALL_RESOURCES)
)


def _run(rows: list[dict]) -> tuple[dict[str, CanonicalBundle], EbwpcCkanAdapter, dict]:
    """The transform's path from raw rows: shim, join, adapter. Returns bundles
    keyed by Thing external_key, the adapter, and the shim's drop tally."""
    measurements, dropped = ebwpc_measurements(rows)
    records, _ = group_by_site(measurements, _LOCATIONS)
    adapter = EbwpcCkanAdapter(records)
    bundles = {bundle.datastreams[0].thing.external_key: bundle for bundle in adapter.run()}
    return bundles, adapter, dropped


def _observations(bundle: CanonicalBundle, suffix: str) -> list:
    [key] = [key for key in bundle.observations if key.endswith(f"-{suffix}")]
    return bundle.observations[key]


# ── well IDs and key format ───────────────────────────────────────────────────


class TestWellIds:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("E-1639-POD1", "E-1639-POD1"),
            ("E-50-1-archived", "E-50-1"),
            ("E-50-1-Archived", "E-50-1"),
            ("Hagerman HQ-archived", "Hagerman HQ"),
            ("Magnum Steel", "Magnum Steel"),
            ("E-9673 ", "E-9673"),
            ("E-2043", "E-2034"),
        ],
    )
    def test_well_id_trims_drops_archived_and_corrects_the_typo(self, raw, expected):
        assert well_id(raw) == expected

    @pytest.mark.parametrize(
        ("spellings", "key"),
        [
            (("E-1639-POD1", "E-1639-POD1", "e-1639-pod1"), "e-1639-pod1"),
            (("E-50-1-archived", "E-50-1-Archived", "e-50-1 archived"), "e-50-1"),
            (
                ("Hagerman HQ-archived", "Hagerman HQ-Archived", "Hagerman HQ-archived"),
                "hagerman hq",
            ),
            (("E-9673 ", "E-9673", "e-9673"), "e-9673"),
            (("E-2043", "E-2034", "e-2034"), "e-2034"),
        ],
    )
    def test_locations_resource_and_site_id_spellings_share_one_key(self, spellings, key):
        assert {site_key(spelling) for spelling in spellings} == {key}


class TestKeyFormat:
    def test_thing_location_and_datastream_keys(self):
        bundles, _, _ = _run(_landed(E_8428, FILES[E_8428["id"]]))
        bundle = bundles["ebwpc-E-8428"]
        assert bundle.datastreams[0].thing.location.external_key == "ebwpc-E-8428"
        assert [ds.external_key for ds in bundle.datastreams] == [
            "ebwpc-E-8428-dtw",
            "ebwpc-E-8428-dtw-manual",
        ]

    def test_archived_well_drops_the_suffix(self):
        rows = _well(
            _resource("501", "E-50-1-Archived", "e-50-1.csv"),
            b"e-50-1 archived,4/15/2009,10:00,,120.5",
        )
        bundles, _, _ = _run(rows)
        assert list(bundles) == ["ebwpc-E-50-1"]
        thing = bundles["ebwpc-E-50-1"].datastreams[0].thing
        assert thing.location.properties["source_specific"]["is_archived"] is True

    def test_the_e2043_typo_is_loaded_as_e2034(self):
        rows = _well(_resource("2034", "E-2034", "e-2034.csv"), b"e-2034,4/15/2009,10:00,,88.1")
        bundles, _, _ = _run(rows)
        assert list(bundles) == ["ebwpc-E-2034"]

    def test_the_trailing_space_is_trimmed(self):
        bundles, _, _ = _run(_landed(E_9673, FILES[E_9673["id"]]))
        assert list(bundles) == ["ebwpc-E-9673"]
        assert bundles["ebwpc-E-9673"].datastreams[0].thing.location.name == "E-9673"

    def test_a_name_with_a_space_keeps_it(self):
        rows = _well(
            _resource("ms", "Magnum Steel", "magnum-steel.csv"),
            b"Magnum Steel,4/15/2009,10:00,,301.2",
        )
        bundles, _, _ = _run(rows)
        assert [ds.external_key for ds in bundles["ebwpc-Magnum Steel"].datastreams] == [
            "ebwpc-Magnum Steel-dtw-manual"
        ]


# ── observation count ─────────────────────────────────────────────────────────


class TestObservationCount:
    def test_e8428_one_row_can_yield_both_readings(self):
        bundles, _, _ = _run(_landed(E_8428, FILES[E_8428["id"]]))
        bundle = bundles["ebwpc-E-8428"]
        assert len(_observations(bundle, "dtw")) == 2
        assert len(_observations(bundle, "dtw-manual")) == 4

    def test_e1639_pod1(self):
        bundles, _, _ = _run(_landed(E_1639, FILES[E_1639["id"]]))
        bundle = bundles["ebwpc-E-1639-POD1"]
        assert len(_observations(bundle, "dtw")) == 2
        assert len(_observations(bundle, "dtw-manual")) == 3

    def test_e9673_drops_the_readings_that_lost_their_date(self):
        bundles, _, dropped = _run(_landed(E_9673, FILES[E_9673["id"]]))
        bundle = bundles["ebwpc-E-9673"]
        assert [ds.external_key for ds in bundle.datastreams] == ["ebwpc-E-9673-dtw"]
        assert len(_observations(bundle, "dtw")) == 1
        assert dropped == {"missing_or_junk_date": 2}

    def test_one_bundle_per_well(self):
        rows = [
            *_landed(E_8428, FILES[E_8428["id"]]),
            *_landed(E_1639, FILES[E_1639["id"]]),
            *_landed(E_9673, FILES[E_9673["id"]]),
        ]
        bundles, adapter, _ = _run(rows)
        assert sorted(bundles) == ["ebwpc-E-1639-POD1", "ebwpc-E-8428", "ebwpc-E-9673"]
        assert adapter.failure_count == 0

    def test_exact_duplicate_readings_are_loaded_once(self):
        rows = _well(
            _resource("501", "E-50-1-Archived", "e-50-1.csv"),
            b"e-50-1 archived,4/15/2009,10:00,,120.5",
            b"e-50-1 archived,4/15/2009,10:00,,120.5",
            b"e-50-1 archived,6/4/2009,10:00,,121.0",
        )
        bundles, adapter, _ = _run(rows)
        assert len(_observations(bundles["ebwpc-E-50-1"], "dtw-manual")) == 2
        assert adapter.dropped == {"duplicate": 1}


# ── unit conversion ───────────────────────────────────────────────────────────


class TestUnitConversion:
    def test_utm_13n_is_converted_to_wgs84_longitude_latitude(self):
        e_8428 = _LOCATIONS["e-8428"]
        assert e_8428["longitude"] == pytest.approx(-106.241852, abs=1e-6)
        assert e_8428["latitude"] == pytest.approx(34.818378, abs=1e-6)

    def test_geometry_carries_the_converted_point(self):
        bundles, _, _ = _run(_landed(E_8428, FILES[E_8428["id"]]))
        location = bundles["ebwpc-E-8428"].datastreams[0].thing.location
        lon, lat = location.geometry["coordinates"]
        assert (lon, lat) == pytest.approx((-106.241852, 34.818378), abs=1e-6)

    def test_utm_values_are_kept_as_published(self):
        source_specific = _LOCATIONS["e-8428"]["location_source_specific"]
        assert source_specific == {
            "utm_easting": 386426.0,
            "utm_northing": 3853605.0,
            "utm_zone": "13N",
            "horizontal_datum": "NAD83",
            "is_archived": False,
        }

    def test_depths_are_feet_with_no_conversion(self):
        bundles, _, _ = _run(_landed(E_8428, FILES[E_8428["id"]]))
        bundle = bundles["ebwpc-E-8428"]
        assert [obs.result for obs in _observations(bundle, "dtw")] == [57.17145, 54.98248]
        assert {ds.unit_of_measurement["symbol"] for ds in bundle.datastreams} == {"ft"}

    def test_elevation_from_the_description_is_stored_in_metres(self):
        bundles, _, _ = _run(_landed(E_8428, FILES[E_8428["id"]]))
        thing = bundles["ebwpc-E-8428"].datastreams[0].thing
        assert thing.location.properties["source_specific"]["elevation"] == {
            "value": pytest.approx(6712 / METRES_TO_FEET),
            "unit": "m",
        }
        assert thing.properties["source_specific"] == {"lithology": "Madera Formation"}

    def test_a_well_without_a_described_elevation_has_none(self):
        bundles, _, _ = _run(_landed(E_1639, FILES[E_1639["id"]]))
        thing = bundles["ebwpc-E-1639-POD1"].datastreams[0].thing
        assert thing.location.properties["source_specific"]["elevation"] is None
        assert thing.properties["source_specific"] == {"lithology": None}


class TestParseDescription:
    def test_the_live_e8428_description(self):
        assert parse_description(E_8428["description"]) == (6712.0, "Madera Formation")

    def test_a_compound_formation_and_thousands_separator(self):
        description = "Elevation 6,250 (ft AMSL), Approximate Depth Unknown, Alluvium/Abo Formation"
        assert parse_description(description) == (6250.0, "Alluvium/Abo Formation")

    def test_elevation_without_a_formation(self):
        assert parse_description("Elevation 6100 (ft AMSL), Approximate Depth Unknown") == (
            6100.0,
            None,
        )

    def test_a_rough_place_only(self):
        assert parse_description("About 5 miles south of Moriarty, NM") == (None, None)


# ── UTC timestamps ────────────────────────────────────────────────────────────


class TestUtcTimestamps:
    def test_the_newest_reading_is_seven_hours_ahead_in_utc(self):
        """Old FROST stamped it 2024-04-26T08:18:00Z; the fixed UTC−07:00 clock makes it 15:18Z."""
        bundles, _, _ = _run(_landed(E_8428, FILES[E_8428["id"]]))
        newest = _observations(bundles["ebwpc-E-8428"], "dtw")[-1]
        assert newest.phenomenon_time == datetime(2024, 4, 26, 15, 18, tzinfo=UTC)
        assert newest.phenomenon_time.tzinfo == UTC

    def test_am_pm_times_and_two_digit_years(self):
        bundles, _, _ = _run(_landed(E_1639, FILES[E_1639["id"]]))
        times = [obs.phenomenon_time for obs in _observations(bundles["ebwpc-E-1639-POD1"], "dtw")]
        assert times == [
            datetime(2018, 4, 13, 16, 0, tzinfo=UTC),
            datetime(2018, 4, 13, 23, 0, tzinfo=UTC),
        ]

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            (b"4/13/18", "2018-04-13"),
            (b"6/1/68", "2068-06-01"),
            (b"6/1/85", "1985-06-01"),  # never 2085
            (b"6/1/1985", "1985-06-01"),  # an explicit century is kept
        ],
    )
    def test_two_digit_years_use_the_standard_century_pivot(self, raw, expected):
        [measurement] = ebwpc_measurements(_well(E_8428, b"e-8428," + raw + b",9:00,55.1,"))[0]
        assert measurement["measurement_date"] == expected

    def test_a_spring_forward_hour_is_not_shifted_or_rejected(self):
        """02:30 doesn't exist on 2024-03-10 in America/Denver; the loggers' fixed clock has it."""
        rows = _well(E_8428, b"e-8428,3/10/2024,2:30,55.1,")
        bundles, _, _ = _run(rows)
        [obs] = _observations(bundles["ebwpc-E-8428"], "dtw")
        assert obs.phenomenon_time == datetime(2024, 3, 10, 9, 30, tzinfo=UTC)

    def test_summer_is_also_seven_hours(self):
        rows = _well(E_8428, b"e-8428,7/1/2023,12:00,55.1,")
        bundles, _, _ = _run(rows)
        [obs] = _observations(bundles["ebwpc-E-8428"], "dtw")
        assert obs.phenomenon_time == datetime(2023, 7, 1, 19, 0, tzinfo=UTC)


# ── empty and missing data ────────────────────────────────────────────────────


class TestEmptyAndMissingData:
    def test_no_rows_no_bundles(self):
        bundles, adapter, dropped = _run([])
        assert bundles == {}
        assert dropped == {}
        assert adapter.failure_count == 0

    def test_missing_time_is_local_midnight_and_flagged(self):
        bundles, _, _ = _run(_landed(E_1639, FILES[E_1639["id"]]))
        first = _observations(bundles["ebwpc-E-1639-POD1"], "dtw-manual")[0]
        assert first.result == 67.65
        assert first.phenomenon_time == datetime(2018, 1, 18, 7, 0, tzinfo=UTC)
        assert first.parameters == {
            "data_source": "CKAN NM Water Data Catalog",
            "source_specific": {"time_missing": True},
        }

    def test_a_timed_reading_is_not_flagged(self):
        bundles, _, _ = _run(_landed(E_8428, FILES[E_8428["id"]]))
        newest = _observations(bundles["ebwpc-E-8428"], "dtw")[-1]
        assert newest.parameters == {"data_source": "CKAN NM Water Data Catalog"}

    @pytest.mark.parametrize("junk", [b"N/A", b"#REF!", b"#VALUE!", b""])
    def test_junk_depths_are_dropped(self, junk):
        measurements, dropped = ebwpc_measurements(
            _well(E_8428, b"e-8428,4/15/2009,0:00," + junk + b"," + junk)
        )
        assert measurements == []
        assert dropped == {"no_usable_depth": 1}

    def test_blank_trailing_rows_are_dropped(self):
        rows = _landed(E_9673, FILES[E_9673["id"]] + b",,,,,,,,\r\n,,,,,,,,\r\n")
        _, dropped = ebwpc_measurements(rows)
        assert dropped == {"missing_or_junk_date": 2, "no_usable_depth": 2}

    def test_excel_zero_date_is_dropped_as_unparseable(self):
        measurements, dropped = ebwpc_measurements(_well(E_8428, b"e-8428,1/0/1900,0:00,,58.0"))
        assert measurements == []
        assert dropped == {"unparseable_date": 1}

    def test_a_blank_date_is_told_apart_from_an_unparseable_one(self):
        _, dropped = ebwpc_measurements(
            _well(E_8428, b"e-8428,,0:00,,58.0", b"e-8428,13/45/2009,0:00,,58.0")
        )
        assert dropped == {"missing_or_junk_date": 1, "unparseable_date": 1}

    def test_a_row_without_site_id_is_dropped(self):
        measurements, dropped = ebwpc_measurements(_well(E_8428, b",4/15/2009,0:00,,58.0"))
        assert measurements == []
        assert dropped == {"no_site_id": 1}

    def test_a_well_without_a_location_is_dropped_and_counted(self):
        rows = _well(_resource("zz", "E-9999", "e-9999.csv"), b"e-9999,4/15/2009,0:00,,58.0")
        measurements, _ = ebwpc_measurements(rows)
        records, unmatched = group_by_site(measurements, _LOCATIONS)
        assert records == []
        assert unmatched == {"e-9999": 1}

    def test_a_location_row_without_coordinates_is_skipped(self, caplog):
        rows = _landed(
            LOCATIONS,
            b"NMBG_ID,UTM_Zone13N_Easting,UTM_Zone13N_Northing,Datum\r\n"
            b"E-8428,N/A,3853605,NAD83\r\n"
            b"E-7545,381162,3834482,NAD83\r\n",
        )
        assert list(ebwpc_locations(rows, [])) == ["e-7545"]
        assert "Skipping location row" in caplog.text

    def test_a_manual_only_well_gets_only_the_manual_datastream(self):
        bundles, _, _ = _run(_well(E_8428, b"e-8428,4/15/2009,0:00,,58.3"))
        assert [ds.external_key for ds in bundles["ebwpc-E-8428"].datastreams] == [
            "ebwpc-E-8428-dtw-manual"
        ]

    def test_every_location_in_the_live_file_is_kept(self):
        assert len(_LOCATIONS) == 21
        assert _LOCATIONS["hagerman hq"]["site_id"] == "Hagerman HQ"
        assert _LOCATIONS["hagerman hq"]["location_source_specific"]["is_archived"] is True


# ── parameter filtering ───────────────────────────────────────────────────────


class TestParameterFiltering:
    def test_below_top_of_casing_readings_are_skipped_and_counted(self):
        measurements, dropped = ebwpc_measurements(_landed(_E_6385, _E_6385_CSV))
        assert measurements == []
        assert dropped == {"toc_reading_skipped": 2}

    def test_unnamed_trailing_columns_are_ignored(self):
        # e-94077 has stray dates in unnamed column 6.
        rows = _landed(E_9673, FILES[E_9673["id"]])
        rows[0]["column_6"] = "7/24/2013"
        measurements, _ = ebwpc_measurements(rows)
        assert [m["depth_to_water"] for m in measurements] == [237.50416]

    def test_each_depth_column_maps_to_its_own_sensor_and_stream(self):
        bundles, _, _ = _run(_landed(E_8428, FILES[E_8428["id"]]))
        transducer, manual = bundles["ebwpc-E-8428"].datastreams

        assert transducer.sensor == TRANSDUCER_SENSOR
        assert transducer.name == "Groundwater Levels"
        assert transducer.properties["source_specific"] == {"is_continuous": True}

        assert manual.sensor == MANUAL_SENSOR
        assert manual.name == "Manual Groundwater Levels"
        assert manual.properties["source_specific"] == {"is_continuous": False}

        for ds in (transducer, manual):
            assert ds.observed_property == DTW_OBS_PROP
            assert ds.properties["topic"] == "Water Quantity"

    def test_a_site_id_that_disagrees_with_the_resource_name_warns_once(self, caplog):
        rows = _well(E_1639, b"e-1639,4/13/18,9:00:00 AM,68.0,", b"e-1639,4/14/18,9:00:00 AM,68.1,")
        measurements, _ = ebwpc_measurements(rows)
        assert {m["site_id"] for m in measurements} == {"e-1639"}
        assert caplog.text.count("doesn't match the resource name") == 1
