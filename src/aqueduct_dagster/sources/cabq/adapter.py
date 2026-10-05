"""
Mapping-only adapter for CABQ data — raw records come from GCS (written by
dlt). Fetching and auth live entirely in sources/cabq/dlt_pipeline.py.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime

from aqueduct_dagster.canonical import (
    DTW_OBS_PROP,
    MANUAL_SENSOR,
    UNIT_FOOT,
    OM_Measurement,
    gwl_datastream_meta,
)
from aqueduct_dagster.canonical.base_adapter import BaseAdapter
from aqueduct_dagster.canonical.canonical_model import (
    CanonicalDatastream,
    CanonicalLocation,
    CanonicalObservation,
    CanonicalThing,
)

logger = logging.getLogger(__name__)
AGENCY = "CABQ"


class CabqAdapter(BaseAdapter):
    """Adapter for CABQ groundwater level data. Receives pre-grouped records
    (one per location) — cabq/transform.py owns GCS reading, this only maps.

    Record shape (one per location):
      {"location_id": str, "location_name": str, "latitude": float,
       "longitude": float, "readings": [{"timestamp": int, "value": float}, ...]}"""

    def __init__(self, records: list[dict]) -> None:
        super().__init__(agency=AGENCY)
        self._records = records

    def extract(self) -> Iterator[dict]:
        yield from self._records

    def to_thing(self, record: dict) -> CanonicalThing:
        source_id = str(record["location_id"])
        external_key = self.make_location_key(source_id)
        return CanonicalThing(
            external_key=external_key,
            name="Water Well",
            description="Well drilled or set into subsurface for the purposes of pumping water or monitoring "
            + "groundwater",
            location=CanonicalLocation(
                external_key=external_key,
                name=record["location_name"],
                description="Location of well where measurements were collected",
                geometry={
                    "type": "Point",
                    "coordinates": [record["longitude"], record["latitude"]],
                },
                properties={"source_id": source_id, "source_specific": {}},
            ),
            properties={
                "agency": self.agency,
                "source_id": source_id,
                "source_specific": {},
            },
        )

    def to_observations(self, record: dict) -> list[CanonicalObservation]:
        source_id = str(record["location_id"])
        ds_key = self.make_datastream_key(source_id, "dtw")
        observations = []
        for reading in record["readings"]:
            observations.append(
                CanonicalObservation(
                    phenomenon_time=datetime.fromtimestamp(reading["timestamp"], tz=UTC),
                    result=reading["value"],
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
                sensor=MANUAL_SENSOR,
                observed_property=DTW_OBS_PROP,
            )
        ]
