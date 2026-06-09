"""
adapters/hydrovu_adapter.py

Mapping-only adapter for HydroVu data.
Raw records come from GCS (written by dlt).

Responsibilities:
  - to_thing()           map a raw location record → CanonicalThing + CanonicalLocation
  - to_observations()    map raw readings → list[CanonicalObservation]
  - _build_datastreams() build CanonicalDatastream for this Thing
  - extract()            reads from GCS — called by run()
"""

from __future__ import annotations
import logging
from datetime import datetime
from typing import Iterator

from aqueduct_dagster.canonical.base_adapter import BaseAdapter
from aqueduct_dagster.canonical.canonical_model import (
    CanonicalDatastream, CanonicalLocation, CanonicalObservation, CanonicalThing,
)
from aqueduct_dagster.canonical.canonical_constants import (
    HYDROVU_SENSOR, DTW_OBS_PROP, OM_Measurement, UNIT_FOOT, gwl_datastream_meta,
)

logger = logging.getLogger(__name__)
AGENCY = "PVACD"


class HydroVuAdapter(BaseAdapter):

    def __init__(self) -> None:
        super().__init__(agency=AGENCY)

    def extract(self) -> Iterator[dict]:
        # TODO: read from GCS parquet and yield one record per location
        pass

    def to_thing(self, record: dict) -> CanonicalThing:
        # TODO: map location record → CanonicalThing + CanonicalLocation
        pass

    def to_observations(self, record: dict) -> list[CanonicalObservation]:
        # TODO: map readings → list[CanonicalObservation]
        pass

    def _build_datastreams(self, thing: CanonicalThing) -> list[CanonicalDatastream]:
        # TODO: build CanonicalDatastream using canonical constants
        pass