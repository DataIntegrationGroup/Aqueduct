"""
Mapping-only adapter for EBID (OneRain Contrail) — not implemented yet.
Deferred to a follow-up ticket; see docs/sources/ebid.md Open Questions.
"""

from __future__ import annotations

from collections.abc import Iterator

from aqueduct_dagster.canonical.base_adapter import BaseAdapter
from aqueduct_dagster.canonical.canonical_model import (
    CanonicalDatastream,
    CanonicalObservation,
    CanonicalThing,
)

AGENCY = "EBID"


class EbidAdapter(BaseAdapter):
    def __init__(self, records: list[dict]) -> None:
        super().__init__(agency=AGENCY)
        self._records = records

    def extract(self) -> Iterator[dict]:
        yield from self._records

    def to_thing(self, record: dict) -> CanonicalThing:
        raise NotImplementedError("EbidAdapter.to_thing is not implemented yet")

    def to_observations(self, record: dict) -> list[CanonicalObservation]:
        raise NotImplementedError("EbidAdapter.to_observations is not implemented yet")

    def _build_datastreams(self, thing: CanonicalThing) -> list[CanonicalDatastream]:
        raise NotImplementedError("EbidAdapter._build_datastreams is not implemented yet")
