"""
WDI water-level transform layer, shared by every agency on the NM Water Data
Catalog (CKAN).

Agency-specific code stays in one small place. Each agency's shim (in its
sources/<name>/adapter.py) translates raw CKAN rows into the WDI best-practice
location + measurement shape, and WdiWaterLevelAdapter maps that shape to
CanonicalBundles. An agency whose files already follow the best practices
needs only a subclass setting AGENCY and SOURCE_TZ.
"""

from __future__ import annotations

import logging
import math
from collections import Counter
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, tzinfo

import gcsfs
from dagster import MetadataValue

from aqueduct_dagster.canonical.base_adapter import BaseAdapter
from aqueduct_dagster.canonical.canonical_constants import (
    DTW_OBS_PROP,
    MANUAL_SENSOR,
    METRES_TO_FEET,
    TRANSDUCER_SENSOR,
    UNIT_FOOT,
    OM_Measurement,
    gwl_datastream_meta,
    manual_gwl_datastream_meta,
)
from aqueduct_dagster.canonical.canonical_model import (
    CanonicalBundle,
    CanonicalDatastream,
    CanonicalLocation,
    CanonicalObservation,
    CanonicalSensor,
    CanonicalThing,
)
from aqueduct_dagster.shared.gcs import _read_parquet_files

logger = logging.getLogger(__name__)

#: Observation parameters.data_source: the platform the files come from.
DATA_SOURCE = "CKAN NM Water Data Catalog"
#: Datastream properties.topic, as on old FROST's CKAN datastreams.
TOPIC = "Water Quantity"

# What agencies publish in place of a value.
_JUNK_VALUES = frozenset({"", "n/a", "#ref!", "#value!", "<null>"})
_FEET = frozenset({"ft", "feet", "foot"})
_METRES = frozenset({"m", "meter", "meters", "metre", "metres"})
# WDI's desired measurement fields, which are the standard parameters keys too.
_WDI_PARAMETERS = (
    "measuring_agency",
    "measurement_method",
    "water_level_status",
    "measurement_point_height",
    "water_level_accuracy",
)


def clean_cell(value: object) -> str | None:
    """A raw cell stripped, or None when blank or junk (N/A, #REF!, #VALUE!, <Null>)."""
    if value is None:
        return None
    text = str(value).strip()
    return None if text.casefold() in _JUNK_VALUES else text


@dataclass(frozen=True)
class StreamSpec:
    """How one WDI measurement_type becomes a Datastream."""

    suffix: str
    sensor: CanonicalSensor
    meta: Callable[[str, str], dict]
    is_continuous: bool


#: WDI measurement_type -> Datastream. The continuous stream keeps the plain
#: dtw suffix the HydroVu sources use; the manual one is qualified, as on old FROST.
WDI_MEASUREMENT_TYPES: dict[str, StreamSpec] = {
    "transducer": StreamSpec("dtw", TRANSDUCER_SENSOR, gwl_datastream_meta, True),
    "manual": StreamSpec("dtw-manual", MANUAL_SENSOR, manual_gwl_datastream_meta, False),
}


class WdiWaterLevelAdapter(BaseAdapter):
    """Depth-to-water mapping for a CKAN agency's WDI-shaped records, one per
    located site. Subclasses supply AGENCY and SOURCE_TZ (the clock the
    agency's naive dates and times are on).

    Record shape (from group_by_site()):
      {"site_id": str, "site_name": str | None, "longitude": float, "latitude": float,
       "elevation": float | None, "elevation_units": str | None, "lithology": str | None,
       "alternate_id": [{"id": str, "agency": str}] | None,
       "location_source_specific": dict, "thing_source_specific": dict,
       "measurements": [{"measurement_date": "YYYY-MM-DD", "measurement_time": "HH:MM:SS" | None,
                         "depth_to_water": float | str, "depth_to_water_unit": str,
                         "measurement_type": "transducer" | "manual", ...}]}

    A missing measurement_time is 00:00 local, flagged time_missing. Measurements
    that can't be mapped, or are dated in the future, are dropped and tallied in
    `dropped` by reason; a Datastream is built only for a type the site has
    observations of."""

    #: Agency code seeding every external_key. Set by each agency's subclass.
    AGENCY: str = ""
    #: The agency's clock. Set by each agency's subclass.
    SOURCE_TZ: tzinfo | None = None
    MEASUREMENT_TYPES: dict[str, StreamSpec] = WDI_MEASUREMENT_TYPES

    def __init__(self, records: list[dict]) -> None:
        if not self.AGENCY:
            raise ValueError(f"{type(self).__name__} must set AGENCY")
        if self.SOURCE_TZ is None:
            raise ValueError(f"{type(self).__name__} must set SOURCE_TZ")
        super().__init__(agency=self.AGENCY)
        self._records = records
        self.dropped: Counter[str] = Counter()
        self._now = datetime.now(UTC)
        # Suffixes to_observations() produced per site. run() calls it before
        # _build_datastreams(), which builds only these.
        self._suffixes: dict[str, set[str]] = {}

    def extract(self) -> Iterator[dict]:
        yield from self._records

    def to_thing(self, record: dict) -> CanonicalThing:
        source_id = str(record["site_id"])
        external_key = self.make_location_key(source_id)
        standard: dict = {"source_id": source_id}
        if record.get("alternate_id"):
            standard["alternate_id"] = record["alternate_id"]

        location = CanonicalLocation(
            external_key=external_key,
            name=record.get("site_name") or source_id,
            description="Location of well where measurements are made",
            geometry={
                "type": "Point",
                "coordinates": [float(record["longitude"]), float(record["latitude"])],
            },
            properties={
                **standard,
                "source_specific": {
                    **record.get("location_source_specific", {}),
                    "elevation": _elevation_in_metres(
                        record.get("elevation"), record.get("elevation_units"), source_id
                    ),
                },
            },
        )

        return CanonicalThing(
            external_key=external_key,
            name="Water Well",
            description="Well drilled or set into subsurface for the purposes of pumping water or monitoring groundwater",
            location=location,
            properties={
                "agency": self.agency,
                **standard,
                "source_specific": {
                    **record.get("thing_source_specific", {}),
                    "lithology": record.get("lithology"),
                },
            },
        )

    def to_observations(self, record: dict) -> list[CanonicalObservation]:
        source_id = str(record["site_id"])
        observations: list[CanonicalObservation] = []
        suffixes: set[str] = set()
        seen: set[tuple[str, datetime, float]] = set()

        for measurement in record.get("measurements", []):
            kind = str(measurement.get("measurement_type") or "").strip().casefold()
            spec = self.MEASUREMENT_TYPES.get(kind)
            if spec is None:
                self._drop("unknown_measurement_type", source_id, measurement)
                continue
            result = _depth_in_feet(
                measurement.get("depth_to_water"), measurement.get("depth_to_water_unit")
            )
            if result is None:
                self._drop("unusable_depth_or_unit", source_id, measurement)
                continue
            phenomenon_time = self._utc(
                measurement.get("measurement_date"), measurement.get("measurement_time")
            )
            if phenomenon_time is None:
                self._drop("unusable_timestamp", source_id, measurement)
                continue
            if phenomenon_time > self._now:
                # A year typo or misread century. Loaded, it would move the
                # datastream's FROST watermark into the future and skip every
                # real reading after it.
                self._drop("future_timestamp", source_id, measurement)
                continue

            ds_key = self.make_datastream_key(source_id, spec.suffix)
            if (ds_key, phenomenon_time, result) in seen:
                self.dropped["duplicate"] += 1
                continue
            seen.add((ds_key, phenomenon_time, result))
            suffixes.add(spec.suffix)
            observations.append(
                CanonicalObservation(
                    phenomenon_time=phenomenon_time,
                    result=result,
                    datastream_external_key=ds_key,
                    parameters=_parameters(measurement),
                )
            )

        self._suffixes[source_id] = suffixes
        return observations

    def _build_datastreams(self, thing: CanonicalThing) -> list[CanonicalDatastream]:
        source_id = str(thing.properties["source_id"])
        suffixes = self._suffixes.get(source_id, set())
        datastreams = []
        for spec in self.MEASUREMENT_TYPES.values():
            if spec.suffix not in suffixes:
                continue
            meta = spec.meta(self.agency, thing.name)
            datastreams.append(
                CanonicalDatastream(
                    external_key=self.make_datastream_key(source_id, spec.suffix),
                    name=meta["name"],
                    description=meta["description"],
                    observation_type=OM_Measurement,
                    unit_of_measurement=UNIT_FOOT,
                    thing=thing,
                    sensor=spec.sensor,
                    observed_property=DTW_OBS_PROP,
                    properties={
                        "topic": TOPIC,
                        "source_specific": {"is_continuous": spec.is_continuous},
                    },
                )
            )
        return datastreams

    def _utc(self, measurement_date: object, measurement_time: object) -> datetime | None:
        try:
            day = date.fromisoformat(str(measurement_date))
            clock = time.fromisoformat(str(measurement_time)) if measurement_time else time(0)
        except ValueError:
            return None
        return datetime.combine(day, clock, tzinfo=self.SOURCE_TZ).astimezone(UTC)

    def _drop(self, reason: str, source_id: str, measurement: dict) -> None:
        self.dropped[reason] += 1
        logger.debug("Dropping measurement (%s): site=%s %r", reason, source_id, measurement)


def _depth_in_feet(value: object, unit: object) -> float | None:
    """The depth in feet, or None if it isn't a finite number in a known unit."""
    try:
        depth = float(str(value))
    except ValueError:
        return None
    if not math.isfinite(depth):
        return None
    unit_text = str(unit or "").strip().casefold()
    if unit_text in _FEET:
        return depth
    if unit_text in _METRES:
        return depth * METRES_TO_FEET
    return None


def _elevation_in_metres(value: object, unit: object, source_id: str) -> dict | None:
    if value is None:
        return None
    unit_text = str(unit or "").strip().casefold()
    if unit_text in _FEET:
        return {"value": float(str(value)) / METRES_TO_FEET, "unit": "m"}
    if unit_text in _METRES:
        return {"value": float(str(value)), "unit": "m"}
    logger.warning("Site %s: elevation unit %r not recognised — elevation omitted", source_id, unit)
    return None


def _parameters(measurement: dict) -> dict:
    parameters: dict = {
        key: measurement[key] for key in _WDI_PARAMETERS if measurement.get(key) is not None
    }
    parameters["data_source"] = DATA_SOURCE
    source_specific: dict = {}
    if not measurement.get("measurement_time"):
        source_specific["time_missing"] = True
    if measurement.get("notes"):
        source_specific["notes"] = measurement["notes"]
    if source_specific:
        parameters["source_specific"] = source_specific
    return parameters


def read_ckan_table(bucket: str, dataset: str, table: str, fs: gcsfs.GCSFileSystem) -> list[dict]:
    """Every row of a replace table (ckan_resources, ckan_locations), which
    always holds one current snapshot. Raises FileNotFoundError if it has
    never landed."""
    pattern = f"{bucket}/{dataset}/{table}/**/*.parquet"
    files = fs.glob(pattern)
    if not files:
        raise FileNotFoundError(
            f"No {table} parquet found at {pattern}. "
            f"Ensure {dataset}'s ingest asset has run at least once."
        )
    rows = _read_parquet_files(files, fs, None)
    logger.info("Read %d %s row(s) from GCS (%s)", len(rows), table, dataset)
    return rows


def latest_snapshots(rows: list[dict]) -> tuple[list[dict], int]:
    """Keeps only each resource's newest snapshot (by _dlt_load_id) of the
    ckan_readings rows past the watermark. There are several when a file
    changed twice between FROST loads; the older ones are superseded copies.
    Returns (rows, rows_superseded)."""
    newest: dict[str, float] = {}
    for row in rows:
        load_id = float(row["_dlt_load_id"])
        if load_id > newest.get(row["resource_id"], -math.inf):
            newest[row["resource_id"]] = load_id
    kept = [row for row in rows if float(row["_dlt_load_id"]) == newest[row["resource_id"]]]
    return kept, len(rows) - len(kept)


def group_by_site(
    measurements: list[dict], locations: Mapping[str, dict]
) -> tuple[list[dict], dict[str, int]]:
    """Joins WDI measurements to their site, one record per site, for
    WdiWaterLevelAdapter. `locations` is keyed by the same site_id the
    measurements carry (the shim normalizes both sides). Measurements of a site
    with no location are dropped and returned as {site_id: count}."""
    records: dict[str, dict] = {}
    unmatched: Counter[str] = Counter()
    for measurement in measurements:
        key = measurement["site_id"]
        location = locations.get(key)
        if location is None:
            unmatched[key] += 1
            continue
        if key not in records:
            records[key] = {**location, "measurements": []}
        records[key]["measurements"].append(measurement)
    for site, count in sorted(unmatched.items()):
        logger.warning("Site %s has no location — %d measurement(s) dropped", site, count)
    return list(records.values()), dict(unmatched)


def transform_metadata(
    *,
    files_skipped_bad_name: int,
    since_load_id: float | None,
    max_load_id: float | None,
    rows_read: int = 0,
    rows_superseded: int = 0,
    rows_dropped: Mapping[str, int] | None = None,
    measurements_mapped: int = 0,
    sites_without_location: Mapping[str, int] | None = None,
    observations_dropped: Mapping[str, int] | None = None,
    bundles: list[CanonicalBundle] | None = None,
    adapter_failures: int = 0,
) -> dict[str, MetadataValue]:
    """Shared shape for a CKAN transform asset's output metadata. The defaults
    describe a run with nothing new to read."""
    bundles = bundles or []
    return {
        "rows_read": MetadataValue.int(rows_read),
        "rows_superseded": MetadataValue.int(rows_superseded),
        "rows_dropped": MetadataValue.json(dict(rows_dropped or {})),
        "measurements_mapped": MetadataValue.int(measurements_mapped),
        "sites_without_location": MetadataValue.json(dict(sites_without_location or {})),
        "observations_dropped": MetadataValue.json(dict(observations_dropped or {})),
        "observations_produced": MetadataValue.int(
            sum(len(obs) for bundle in bundles for obs in bundle.observations.values())
        ),
        "bundles_produced": MetadataValue.int(len(bundles)),
        "adapter_failures": MetadataValue.int(adapter_failures),
        "files_skipped_bad_name": MetadataValue.int(files_skipped_bad_name),
        "watermark_before": MetadataValue.text(str(since_load_id)),
        "watermark_after": MetadataValue.text(str(max_load_id)),
    }
