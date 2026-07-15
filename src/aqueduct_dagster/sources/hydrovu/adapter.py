"""
sources/hydrovu/adapter.py

Transforms grouped HydroVu parquet rows into CanonicalBundles for FROST.

Called by hydrovu/transform.py which:
  1. Reads raw parquet from GCS
  2. Filters to DTW rows (parameter_id="4") before grouping
  3. Groups filtered rows by location_id into one record per location
  4. Passes records to HydroVuAdapter(records).run()

Record shape expected by this adapter (one per location):
  {
    "location_id":          int    — HydroVu integer ID
    "location_name":        str    — e.g. "Bartlett Level Troll"
    "location_description": str    — well number e.g. "827276", or "" if unnamed
    "latitude":             float
    "longitude":            float
    "readings": [
      {"parameter_id": "4", "unit_id": "35", "timestamp": int, "value": float},
      ...
    ]
  }

Mapping confirmed against old HydroVu STAO implementation and
/sispec/friendlynames endpoint (June 2026):
  parameterId "4"  = Level: Depth to Water
  unitId      "35" = metres → convert to feet (* 3.28084)
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime

from aqueduct_dagster.canonical.base_adapter import BaseAdapter
from aqueduct_dagster.canonical.canonical_constants import (
    DTW_OBS_PROP,
    HYDROVU_SENSOR,
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

AGENCY = "PVACD"

# Confirmed via HydroVu /sispec/friendlynames and old STAO implementation
DTW_PARAMETER_ID = "4"
METRES_TO_FEET = 3.28084


class HydroVuAdapter(BaseAdapter):
    """
    Adapter for PVACD HydroVu groundwater level data.

    Receives pre-grouped records (one per location, DTW readings only)
    from the transform asset. Converts metres to feet and builds CanonicalBundles.

    external_key convention:
      - Uses location_description (well number e.g. "827276") when present
      - Falls back to str(location_id) for unnamed locations (e.g. Transwestern)
      - Format: "pvacd-{source_id}"  e.g. "pvacd-827276", "pvacd-4586726273318912"
    """

    def __init__(self, records: list[dict]) -> None:
        super().__init__(agency=AGENCY)
        self._records = records

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
