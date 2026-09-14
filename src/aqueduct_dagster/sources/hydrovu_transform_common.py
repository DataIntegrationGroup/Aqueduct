"""
sources/hydrovu_transform_common.py

Vendor-level HydroVu transform layer, shared by every HydroVu tenant.

What lives here:
  HydroVuDtwAdapter           the DTW mapping. Subclasses set AGENCY
  read_locations_from_gcs()   reads a tenant's hydrovu_locations parquet
  group_readings_by_location() flat reading rows -> one record per location
  transform_metadata()        the shape of a transform asset's output metadata

What stays in each tenant's sources/<name>/: its GCS dataset, its watermark path,
its own transform result dataclass, and any hazard specific to that tenant (BernCo's
sentinel-timestamp floor is passed in as min_timestamp rather than assumed here).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime

import gcsfs
import pyarrow.parquet as pq
from dagster import MetadataValue

from aqueduct_dagster.canonical.base_adapter import BaseAdapter
from aqueduct_dagster.canonical.canonical_constants import (
    DTW_OBS_PROP,
    HYDROVU_SENSOR,
    METRES_TO_FEET,
    UNIT_FOOT,
    OM_Measurement,
    gwl_datastream_meta,
)
from aqueduct_dagster.canonical.canonical_model import (
    CanonicalDatastream,
    CanonicalLocation,
    CanonicalObservation,
    CanonicalThing,
)

logger = logging.getLogger(__name__)

#: "Level: Depth to Water"
#: unitId "35" and measure in opposite directions. See docs/sources/bernco_hydrovu.md.
DTW_PARAMETER_ID = "4"


class HydroVuDtwAdapter(BaseAdapter):
    """
    Depth-to-water mapping for a HydroVu tenant.

    Receives pre-grouped records (one per location) from a transform asset,
    converts metres to feet, and builds one DTW CanonicalBundle per location.
    Subclasses supply AGENCY.

    external_key convention:
      - Always str(location_id) ( )the HydroVu integer location ID, cast to str).
      - location_description (the well/permit number, e.g. "827276") is NOT part of
        the key. It is stored under properties.source_specific.hydrovu_description.
      - Location and Thing share f"{agency_lower}-{source_id}"; the datastream is
        that plus "-dtw".

    Record shape expected (one per location), as produced by
    group_readings_by_location():
      {
        "location_id":          int    HydroVu integer ID
        "location_name":        str    e.g. "SierraVista-966932"
        "location_description": str    well number, or "" if unset
        "latitude":             float
        "longitude":            float
        "readings": [
          {"parameter_id": "4", "unit_id": "35", "timestamp": int, "value": float},
          ...
        ]
      }

    min_timestamp: readings stamped before this Unix second are dropped as bad device
      clocks rather than history. None disables the floor entirely. A caller that
      fetches older history (a backfill) must pass its own floor.
    """

    #: Agency code seeding every external_key. Set by each tenant's subclass.
    AGENCY: str = ""

    def __init__(self, records: list[dict], min_timestamp: int | None = None) -> None:
        if not self.AGENCY:
            raise ValueError(f"{type(self).__name__} must set AGENCY")
        super().__init__(agency=self.AGENCY)
        self._records = records
        self.min_timestamp = min_timestamp

    def extract(self) -> Iterator[dict]:
        yield from self._records

    def to_thing(self, record: dict) -> CanonicalThing:
        source_id = str(record["location_id"])
        external_key = self.make_location_key(source_id)

        location = CanonicalLocation(
            external_key=external_key,
            name=record["location_name"],
            description="Location of well where measurements are made",
            geometry={
                "type": "Point",
                "coordinates": [record["longitude"], record["latitude"]],
            },
            properties={
                "source_id": source_id,
                "source_specific": {
                    "hydrovu_description": record["location_description"],
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
                "source_id": source_id,
                "source_specific": {
                    "hydrovu_description": record["location_description"],
                },
            },
        )

    def to_observations(self, record: dict) -> list[CanonicalObservation]:
        source_id = str(record["location_id"])
        ds_key = self.make_datastream_key(source_id, "dtw")

        observations = []
        for reading in record.get("readings", []):
            if reading["parameter_id"] != DTW_PARAMETER_ID:
                continue
            if self.min_timestamp is not None and reading["timestamp"] < self.min_timestamp:
                # A real-looking value on a 1970 timestamp. Loading it would drag every
                # min-date and time-series axis downstream with it.
                logger.warning(
                    "Dropping reading below the sanity floor: location=%s timestamp=%s "
                    "value=%s floor=%s",
                    source_id,
                    reading["timestamp"],
                    reading["value"],
                    self.min_timestamp,
                )
                continue
            value_ft = reading["value"] * METRES_TO_FEET
            observations.append(
                CanonicalObservation(
                    phenomenon_time=datetime.fromtimestamp(reading["timestamp"], tz=UTC),
                    result=value_ft,
                    datastream_external_key=ds_key,
                )
            )
        return observations

    def _build_datastreams(self, thing: CanonicalThing) -> list[CanonicalDatastream]:
        source_id = str(thing.properties["source_id"])
        ds_key = self.make_datastream_key(source_id, "dtw")
        meta = gwl_datastream_meta(self.agency, thing.name)

        return [
            CanonicalDatastream(
                external_key=ds_key,
                name=meta["name"],
                description=meta["description"],
                observation_type=OM_Measurement,
                unit_of_measurement=UNIT_FOOT,
                thing=thing,
                sensor=HYDROVU_SENSOR,
                observed_property=DTW_OBS_PROP,
            )
        ]


def read_locations_from_gcs(
    bucket_url: str, dataset: str, fs: gcsfs.GCSFileSystem
) -> dict[int, dict]:
    """
    Reads a tenant's hydrovu_locations parquet (write_disposition="replace", so it
    is always a single up-to-date file).
    Returns a dict keyed by location_id for an O(1) join with readings rows.
    """
    bucket = bucket_url.replace("gs://", "")
    pattern = f"{bucket}/{dataset}/hydrovu_locations/**/*.parquet"
    files = fs.glob(pattern)
    if not files:
        raise FileNotFoundError(
            f"No locations parquet found at {pattern}. "
            f"Ensure {dataset}'s ingest asset has run at least once."
        )

    locations: dict[int, dict] = {}
    for f in files:
        with fs.open(f) as fh:
            table = pq.read_table(fh)
            df = table.to_pydict()
            for i in range(len(df["id"])):
                locations[df["id"][i]] = {
                    "name": df["name"][i],
                    "description": df["description"][i],
                    "latitude": df["latitude"][i],
                    "longitude": df["longitude"][i],
                }

    logger.info("Read %d locations from GCS (%s)", len(locations), dataset)
    return locations


def group_readings_by_location(rows: list[dict], locations: dict[int, dict]) -> list[dict]:
    """
    Groups flat readings rows into one record per location, joining location
    metadata (name, description, lat, lon) from the locations reference dict.

    A location missing from `locations` still produces a record, with empty name and
    description and None coordinates. HydroVuDtwAdapter then fails that one record
    and BaseAdapter.run() counts it, rather than the whole run dying on a KeyError.
    """
    groups: dict[int, dict] = {}
    for row in rows:
        loc_id = row["location_id"]
        if loc_id not in groups:
            loc = locations.get(loc_id, {})
            groups[loc_id] = {
                "location_id": loc_id,
                "location_name": loc.get("name", ""),
                "location_description": loc.get("description", ""),
                "latitude": loc.get("latitude"),
                "longitude": loc.get("longitude"),
                "readings": [],
            }
        groups[loc_id]["readings"].append(
            {
                "parameter_id": row["parameter_id"],
                "unit_id": row["unit_id"],
                "timestamp": row["timestamp"],
                "value": row["value"],
            }
        )
    return list(groups.values())


def transform_metadata(
    *,
    dtw_rows_read: int,
    locations_grouped: int,
    bundles_produced: int,
    adapter_failures: int,
    since_load_id: float | None,
    max_load_id: float | None,
) -> dict[str, MetadataValue]:
    """Shared shape for a HydroVu transform asset's output metadata"""
    return {
        "dtw_rows_read": MetadataValue.int(dtw_rows_read),
        "locations_grouped": MetadataValue.int(locations_grouped),
        "bundles_produced": MetadataValue.int(bundles_produced),
        "adapter_failures": MetadataValue.int(adapter_failures),
        "watermark_before": MetadataValue.text(str(since_load_id)),
        "watermark_after": MetadataValue.text(str(max_load_id)),
    }
