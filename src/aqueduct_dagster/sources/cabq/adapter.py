"""
adapters/cabq_adapter.py

Mapping-only adapter for CABQ data.
Raw records come from GCS (written by dlt).

Responsibilities:
  - to_thing()           map a raw location record → CanonicalThing + CanonicalLocation
  - to_observations()    map raw readings → list[CanonicalObservation]
  - _build_datastreams() build CanonicalDatastream for this Thing
  - extract()            reads from GCS — called by run()

Fetching and auth live entirely in pipeline/cabq_dlt_pipeline.py
"""

from __future__ import annotations

import logging
from collections.abc import Iterator

from aqueduct_dagster.canonical.base_adapter import BaseAdapter
from aqueduct_dagster.canonical.canonical_model import (
    CanonicalDatastream,
    CanonicalObservation,
    CanonicalThing,
)

logger = logging.getLogger(__name__)
AGENCY = "CABQ"


class CabqAdapter(BaseAdapter):
    """
    Adapter for CABQ groundwater level data.

    Receives pre-grouped records (one per location) from the transform asset.
    transform_cabq.py owns GCS reading — this adapter only does mapping.

    Record shape expected (one per location — to define when implementing):
      {
        "location_id":   str    — CABQ station identifier
        "location_name": str    — station name
        "latitude":      float
        "longitude":     float
        "readings": [
          {"timestamp": int, "value": float, ...},
          ...
        ]
      }
    """

    def __init__(self, records: list[dict]) -> None:
        super().__init__(agency=AGENCY)
        self._records = records

    def extract(self) -> Iterator[dict]:
        yield from self._records

    def to_thing(self, record: dict) -> CanonicalThing:
        # TODO: map location record → CanonicalThing + CanonicalLocation
        # Follow HydroVuAdapter.to_thing() pattern:
        #   source_id = str(record["location_id"])
        #   external_key = self.make_location_key(source_id)
        #   build CanonicalLocation with GeoJSON Point geometry
        #   build CanonicalThing with agency + source_id in properties
        raise NotImplementedError("CabqAdapter.to_thing is not implemented yet")

    def to_observations(self, record: dict) -> list[CanonicalObservation]:
        # TODO: map readings → list[CanonicalObservation]
        # Follow HydroVuAdapter.to_observations() pattern:
        #   ds_key = self.make_datastream_key(source_id, "dtw")
        #   for each reading: CanonicalObservation(phenomenon_time=UTC datetime, result=float)
        raise NotImplementedError("CabqAdapter.to_observations is not implemented yet")

    def _build_datastreams(self, thing: CanonicalThing) -> list[CanonicalDatastream]:
        # TODO: build CanonicalDatastream using canonical constants
        # Follow HydroVuAdapter._build_datastreams() pattern:
        #   ds_key = self.make_datastream_key(source_id, "dtw")
        #   meta = gwl_datastream_meta(self.agency, thing.name)
        #   use MANUAL_SENSOR (CABQ is manual measurement), DTW_OBS_PROP, UNIT_FOOT
        raise NotImplementedError("CabqAdapter._build_datastreams is not implemented yet")
