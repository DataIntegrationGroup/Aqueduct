"""

⚠ REVIEW NEEDED — this is a reference/sample implementation.
  Needs to be reviewed and tested against frost_sta_client before real use.

What this file does:
  Takes a CanonicalDatastream + list of CanonicalObservations and writes
  them to a local FROST server via the SensorThings API.

  Two responsibilities:
    1. ensure_datastream() — idempotent upsert of all metadata in order:
       Location → Thing → Sensor → ObservedProperty → Datastream
       Keyed on external_key so re-runs never create duplicates.

    2. load_observations() — posts observations as chunked Data Array batches.
       Watermark filters out already-loaded records so FROST never gets duplicates
       (SensorThings API does not de-duplicate on its own).
"""

from __future__ import annotations

import abc
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Iterable, Iterator, Protocol, Sequence, runtime_checkable

from aqueduct_dagster.canonical.canonical_model import (
    CanonicalDatastream,
    CanonicalLocation,
    CanonicalSensor,
    CanonicalObservedProperty,
)

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 1000


# --------------------------------------------------------------------------- #
# ObservationRecord + LoadResult — loader-specific, not in canonical model
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class ObservationRecord:
    phenomenon_time: datetime
    result: float
    parameters: dict | None = None


@dataclass
class LoadResult:
    datastream_key: str
    considered: int = 0
    posted: int = 0
    skipped: int = 0
    new_watermark: datetime | None = None


# --------------------------------------------------------------------------- #
# Watermark store
# --------------------------------------------------------------------------- #

@runtime_checkable
class WatermarkStore(Protocol):
    def get(self, datastream_key: str) -> datetime | None: ...
    def set(self, datastream_key: str, watermark: datetime) -> None: ...


class InMemoryWatermarkStore:
    """Dev/test only — not durable across runs. Use FrostWatermarkStore in production."""

    def __init__(self) -> None:
        self._wm: dict[str, datetime] = {}

    def get(self, datastream_key: str) -> datetime | None:
        return self._wm.get(datastream_key)

    def set(self, datastream_key: str, watermark: datetime) -> None:
        self._wm[datastream_key] = watermark


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _chunked(items: Sequence, size: int) -> Iterator[Sequence]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _with_retry(fn: Callable, *, attempts: int = 5, base_delay: float = 0.5):
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt == attempts:
                break
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning("FROST call failed (%d/%d): %s; retry in %.1fs",
                           attempt, attempts, exc, delay)
            time.sleep(delay)
    assert last_exc is not None
    raise last_exc


# --------------------------------------------------------------------------- #
# FrostLoader — abstract base, client-agnostic
# --------------------------------------------------------------------------- #

class FrostLoader(abc.ABC):
    """
    Handles ordering, upsert, watermark filtering, chunking, and retry.
    Subclasses implement the _find_* / _create_* / _post_data_array hooks.
    """

    KEY_FIELD = "externalId"

    def __init__(self, watermarks: WatermarkStore, chunk_size: int = DEFAULT_CHUNK_SIZE) -> None:
        self.watermarks = watermarks
        self.chunk_size = chunk_size

    def ensure_datastream(self, spec: CanonicalDatastream) -> str:
        """Idempotent upsert of full metadata graph. Returns FROST Datastream id."""
        location_id = self._upsert(self._find_location, self._create_location, spec.thing.location)
        thing_id    = self._upsert(self._find_thing, self._create_thing, spec.thing, location_id=location_id)
        sensor_id   = self._upsert(self._find_sensor, self._create_sensor, spec.sensor)
        obsprop_id  = self._upsert(self._find_observed_property, self._create_observed_property, spec.observed_property)
        return self._upsert(self._find_datastream, self._create_datastream, spec,
                            thing_id=thing_id, sensor_id=sensor_id, observed_property_id=obsprop_id)

    def _upsert(self, find: Callable, create: Callable, spec, **links) -> str:
        existing = find(spec.external_key)
        if existing is not None:
            return existing
        return create(spec, **links)

    def load_observations(self, datastream_key: str, datastream_id: str,
                          records: Iterable[ObservationRecord]) -> LoadResult:
        """Filter by watermark, post chunked Data Array batches, advance watermark."""
        ordered = sorted(records, key=lambda r: r.phenomenon_time)
        result = LoadResult(datastream_key=datastream_key, considered=len(ordered))

        wm = self.watermarks.get(datastream_key)
        if wm is None:
            wm = self._max_phenomenon_time(datastream_id)
        if wm is not None:
            kept = [r for r in ordered if r.phenomenon_time > wm]
            result.skipped = len(ordered) - len(kept)
            ordered = kept

        if not ordered:
            result.new_watermark = wm
            return result

        for chunk in _chunked(ordered, self.chunk_size):
            _with_retry(lambda c=chunk: self._post_data_array(datastream_id, c))
            chunk_max = chunk[-1].phenomenon_time
            self.watermarks.set(datastream_key, chunk_max)
            result.posted += len(chunk)
            result.new_watermark = chunk_max

        logger.info("datastream %s: posted %d, skipped %d, watermark->%s",
                    datastream_key, result.posted, result.skipped, result.new_watermark)
        return result

    @abc.abstractmethod
    def _find_location(self, external_key: str) -> str | None: ...
    @abc.abstractmethod
    def _create_location(self, spec: CanonicalLocation) -> str: ...
    @abc.abstractmethod
    def _find_thing(self, external_key: str) -> str | None: ...
    @abc.abstractmethod
    def _create_thing(self, spec, *, location_id: str) -> str: ...
    @abc.abstractmethod
    def _find_sensor(self, external_key: str) -> str | None: ...
    @abc.abstractmethod
    def _create_sensor(self, spec: CanonicalSensor) -> str: ...
    @abc.abstractmethod
    def _find_observed_property(self, external_key: str) -> str | None: ...
    @abc.abstractmethod
    def _create_observed_property(self, spec: CanonicalObservedProperty) -> str: ...
    @abc.abstractmethod
    def _find_datastream(self, external_key: str) -> str | None: ...
    @abc.abstractmethod
    def _create_datastream(self, spec: CanonicalDatastream, *, thing_id: str,
                           sensor_id: str, observed_property_id: str) -> str: ...
    @abc.abstractmethod
    def _post_data_array(self, datastream_id: str, chunk: Sequence[ObservationRecord]) -> None: ...
    @abc.abstractmethod
    def _max_phenomenon_time(self, datastream_id: str) -> datetime | None: ...


# --------------------------------------------------------------------------- #
# FrostStaClientLoader — concrete implementation using frost_sta_client
# ⚠ TODOs below must be verified against frost_sta_client before real use
# --------------------------------------------------------------------------- #

class FrostStaClientLoader(FrostLoader):

    def __init__(self, service, watermarks: WatermarkStore,
                 chunk_size: int = DEFAULT_CHUNK_SIZE) -> None:
        super().__init__(watermarks, chunk_size)
        self.service = service

    def _find_id_by_key(self, entity_query, external_key: str) -> str | None:
        # TODO: verify filter syntax and escape external_key for safety
        flt = f"properties/{self.KEY_FIELD} eq '{external_key}'"
        for entity in entity_query.query().filter(flt).list():
            return str(entity.id)
        return None

    def _find_location(self, external_key: str) -> str | None:
        return self._find_id_by_key(self.service.locations(), external_key)

    def _create_location(self, spec: CanonicalLocation) -> str:
        import frost_sta_client as fsc
        # TODO: verify GeoJSON geometry format expected by frost_sta_client
        location = fsc.Location(
            name=spec.name, description=spec.description,
            location=spec.geometry, encoding_type=spec.encoding_type,
            properties={**spec.properties, self.KEY_FIELD: spec.external_key},
        )
        self.service.create(location)
        return str(location.id)

    def _find_thing(self, external_key: str) -> str | None:
        return self._find_id_by_key(self.service.things(), external_key)

    def _create_thing(self, spec, *, location_id: str) -> str:
        import frost_sta_client as fsc
        thing = fsc.Thing(
            name=spec.name, description=spec.description,
            properties={**spec.properties, self.KEY_FIELD: spec.external_key},
        )
        # TODO: link location by id-only ref — re-nesting full Location creates duplicates
        self.service.create(thing)
        return str(thing.id)

    def _find_sensor(self, external_key: str) -> str | None:
        return self._find_id_by_key(self.service.sensors(), external_key)

    def _create_sensor(self, spec: CanonicalSensor) -> str:
        import frost_sta_client as fsc
        sensor = fsc.Sensor(
            name=spec.name, description=spec.description,
            encoding_type=spec.encoding_type, metadata=spec.metadata,
            properties={**spec.properties, self.KEY_FIELD: spec.external_key},
        )
        self.service.create(sensor)
        return str(sensor.id)

    def _find_observed_property(self, external_key: str) -> str | None:
        return self._find_id_by_key(self.service.observed_properties(), external_key)

    def _create_observed_property(self, spec: CanonicalObservedProperty) -> str:
        import frost_sta_client as fsc
        op = fsc.ObservedProperty(
            name=spec.name, definition=spec.definition, description=spec.description,
            properties={**spec.properties, self.KEY_FIELD: spec.external_key},
        )
        self.service.create(op)
        return str(op.id)

    def _find_datastream(self, external_key: str) -> str | None:
        return self._find_id_by_key(self.service.datastreams(), external_key)

    def _create_datastream(self, spec: CanonicalDatastream, *, thing_id: str,
                           sensor_id: str, observed_property_id: str) -> str:
        import frost_sta_client as fsc
        # TODO: attach Thing/Sensor/ObservedProperty as id-only refs — re-nesting creates duplicates
        datastream = fsc.Datastream(
            name=spec.name, description=spec.description,
            observation_type=spec.observation_type,
            unit_of_measurement=spec.unit_of_measurement,
            properties={**spec.properties, self.KEY_FIELD: spec.external_key},
        )
        self.service.create(datastream)
        return str(datastream.id)

    def _post_data_array(self, datastream_id: str, chunk: Sequence[ObservationRecord]) -> None:
        import frost_sta_client as fsc
        dav = fsc.model.ext.data_array_value.DataArrayValue()
        dav.components = {dav.Property.PHENOMENON_TIME, dav.Property.RESULT}
        # TODO: set dav.datastream to id-only Datastream ref
        for rec in chunk:
            dav.add_observation(fsc.Observation(
                result=rec.result,
                phenomenon_time=rec.phenomenon_time.isoformat(),
            ))
        doc = fsc.model.ext.data_array_document.DataArrayDocument()
        doc.add_data_array_value(dav)
        self.service.observations().create(doc)

    def _max_phenomenon_time(self, datastream_id: str) -> datetime | None:
        # TODO: query FROST for newest observation to recover lost watermark
        # ds = self.service.datastreams().find(datastream_id)
        # obs = ds.get_observations().query().orderby("phenomenonTime desc").top(1).list()
        return None