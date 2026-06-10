"""
loader/frost_loader.py

Writes CanonicalBundles to a FROST SensorThings API server.

Two responsibilities:
  1. ensure_datastream() — idempotent upsert of full metadata graph:
       Location → Thing (linked to Location) → Sensor → ObservedProperty
       → Datastream (linked to all four)
     Each entity is looked up by properties/externalId before creation.
     Links are ID-only references — never re-nest full objects (creates duplicates).

  2. load_observations() — posts observations as chunked Data Array batches.
     Watermark filters already-loaded records. Watermark is advanced per chunk
     so a partial failure doesn't re-post on the next run.

frost_sta_client object model notes:
  - Thing.locations accepts a list of Location objects (wraps in EntityList)
  - Datastream accepts thing/sensor/observed_property as typed objects
  - An object constructed with only id= serializes as {"@iot.id": id} — safe ID-only ref
  - unit_of_measurement must be a UnitOfMeasurement instance, not a dict
  - DataArrayValue: set datastream then components (order matters), then add_observation
  - Observation.phenomenon_time is stored as-is (str or datetime) by the library
"""

import abc
import logging
import time
from datetime import datetime, timezone
from typing import Callable, Iterable, Iterator, Sequence

from aqueduct_dagster.canonical.canonical_model import (
    CanonicalDatastream,
    CanonicalLocation,
    CanonicalObservedProperty,
    CanonicalSensor,
)

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 1000


# --------------------------------------------------------------------------- #
# ObservationRecord + LoadResult
# --------------------------------------------------------------------------- #

class ObservationRecord:
    __slots__ = ("phenomenon_time", "result")

    def __init__(self, phenomenon_time: datetime, result: float) -> None:
        self.phenomenon_time = phenomenon_time
        self.result = result


class LoadResult:
    __slots__ = ("datastream_key", "considered", "posted", "skipped", "new_watermark")

    def __init__(self, datastream_key: str) -> None:
        self.datastream_key = datastream_key
        self.considered = 0
        self.posted = 0
        self.skipped = 0
        self.new_watermark: datetime | None = None


# --------------------------------------------------------------------------- #
# WatermarkStore protocol + in-memory implementation
# --------------------------------------------------------------------------- #

class WatermarkStore(abc.ABC):
    @abc.abstractmethod
    def get(self, datastream_key: str) -> datetime | None: ...
    @abc.abstractmethod
    def set(self, datastream_key: str, watermark: datetime) -> None: ...


class InMemoryWatermarkStore(WatermarkStore):
    """Dev/test only — not durable across runs."""

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
        yield items[i: i + size]


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
            logger.warning(
                "FROST call failed (%d/%d): %s — retry in %.1fs", attempt, attempts, exc, delay
            )
            time.sleep(delay)
    assert last_exc is not None
    raise last_exc


# --------------------------------------------------------------------------- #
# FrostLoader — abstract base
# --------------------------------------------------------------------------- #

class FrostLoader(abc.ABC):
    """
    Handles entity ordering, upsert, watermark filtering, chunking, and retry.
    Subclasses implement the _find_* / _create_* / _post_data_array hooks.
    """

    KEY_FIELD = "externalId"

    def __init__(self, watermarks: WatermarkStore, chunk_size: int = DEFAULT_CHUNK_SIZE) -> None:
        self.watermarks = watermarks
        self.chunk_size = chunk_size

    def ensure_datastream(self, spec: CanonicalDatastream) -> str:
        """Idempotent upsert of the full metadata graph. Returns FROST Datastream id."""
        location_id = self._upsert(self._find_location, self._create_location, spec.thing.location)
        thing_id = self._upsert(
            self._find_thing, self._create_thing, spec.thing, location_id=location_id
        )
        sensor_id = self._upsert(self._find_sensor, self._create_sensor, spec.sensor)
        obsprop_id = self._upsert(
            self._find_observed_property, self._create_observed_property, spec.observed_property
        )
        return self._upsert(
            self._find_datastream, self._create_datastream, spec,
            thing_id=thing_id, sensor_id=sensor_id, observed_property_id=obsprop_id,
        )

    def _upsert(self, find: Callable, create: Callable, spec, **links) -> str:
        existing = find(spec.external_key)
        if existing is not None:
            return existing
        return create(spec, **links)

    def load_observations(
        self,
        datastream_key: str,
        datastream_id: str,
        records: Iterable[ObservationRecord],
    ) -> LoadResult:
        """Filter by watermark, post in chunks, advance watermark per chunk."""
        ordered = sorted(records, key=lambda r: r.phenomenon_time)
        result = LoadResult(datastream_key=datastream_key)
        result.considered = len(ordered)

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

        logger.info(
            "datastream %s: posted %d, skipped %d, watermark→%s",
            datastream_key, result.posted, result.skipped, result.new_watermark,
        )
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
    def _create_datastream(
        self, spec: CanonicalDatastream, *,
        thing_id: str, sensor_id: str, observed_property_id: str,
    ) -> str: ...
    @abc.abstractmethod
    def _post_data_array(self, datastream_id: str, chunk: Sequence[ObservationRecord]) -> None: ...
    @abc.abstractmethod
    def _max_phenomenon_time(self, datastream_id: str) -> datetime | None: ...


# --------------------------------------------------------------------------- #
# FrostStaClientLoader — concrete implementation using frost_sta_client
# --------------------------------------------------------------------------- #

class FrostStaClientLoader(FrostLoader):
    """
    Concrete FrostLoader backed by frost_sta_client.

    ID-only linking pattern (avoids duplicates):
      - _create_thing: sets thing.locations = [fsc.Location(id=location_id)]
      - _create_datastream: passes thing/sensor/observed_property as id-only objects
      - _post_data_array: sets dav.datastream = fsc.Datastream(id=datastream_id)

    unit_of_measurement from the canonical model is a plain dict — converted to
    UnitOfMeasurement instance before passing to fsc.Datastream.
    """

    def __init__(self, service, watermarks: WatermarkStore, chunk_size: int = DEFAULT_CHUNK_SIZE) -> None:
        super().__init__(watermarks, chunk_size)
        self.service = service

    # ── find helpers ────────────────────────────────────────────────────────

    def _find_id_by_key(self, entity_dao, external_key: str) -> str | None:
        flt = f"properties/{self.KEY_FIELD} eq '{external_key}'"
        for entity in entity_dao.query().filter(flt).list():
            return str(entity.id)
        return None

    def _find_location(self, external_key: str) -> str | None:
        return self._find_id_by_key(self.service.locations(), external_key)

    def _find_thing(self, external_key: str) -> str | None:
        return self._find_id_by_key(self.service.things(), external_key)

    def _find_sensor(self, external_key: str) -> str | None:
        return self._find_id_by_key(self.service.sensors(), external_key)

    def _find_observed_property(self, external_key: str) -> str | None:
        return self._find_id_by_key(self.service.observed_properties(), external_key)

    def _find_datastream(self, external_key: str) -> str | None:
        return self._find_id_by_key(self.service.datastreams(), external_key)

    # ── create helpers ───────────────────────────────────────────────────────

    def _create_location(self, spec: CanonicalLocation) -> str:
        import frost_sta_client as fsc
        location = fsc.Location(
            name=spec.name,
            description=spec.description,
            encoding_type=spec.encoding_type,
            location=spec.geometry,
            properties={**spec.properties, self.KEY_FIELD: spec.external_key},
        )
        self.service.create(location)
        logger.info("Created Location id=%s key=%s", location.id, spec.external_key)
        return str(location.id)

    def _create_thing(self, spec, *, location_id: str) -> str:
        import frost_sta_client as fsc
        thing = fsc.Thing(
            name=spec.name,
            description=spec.description,
            properties={**spec.properties, self.KEY_FIELD: spec.external_key},
            locations=[fsc.Location(id=int(location_id))],  # ID-only — avoids duplicate Location
        )
        self.service.create(thing)
        logger.info("Created Thing id=%s key=%s", thing.id, spec.external_key)
        return str(thing.id)

    def _create_sensor(self, spec: CanonicalSensor) -> str:
        import frost_sta_client as fsc
        sensor = fsc.Sensor(
            name=spec.name,
            description=spec.description,
            encoding_type=spec.encoding_type,
            metadata=spec.metadata,
            properties={**spec.properties, self.KEY_FIELD: spec.external_key},
        )
        self.service.create(sensor)
        logger.info("Created Sensor id=%s key=%s", sensor.id, spec.external_key)
        return str(sensor.id)

    def _create_observed_property(self, spec: CanonicalObservedProperty) -> str:
        import frost_sta_client as fsc
        op = fsc.ObservedProperty(
            name=spec.name,
            definition=spec.definition,
            description=spec.description,
            properties={**spec.properties, self.KEY_FIELD: spec.external_key},
        )
        self.service.create(op)
        logger.info("Created ObservedProperty id=%s key=%s", op.id, spec.external_key)
        return str(op.id)

    def _create_datastream(
        self,
        spec: CanonicalDatastream,
        *,
        thing_id: str,
        sensor_id: str,
        observed_property_id: str,
    ) -> str:
        import frost_sta_client as fsc
        from frost_sta_client.model.ext.unitofmeasurement import UnitOfMeasurement as UoM

        uom = UoM(
            name=spec.unit_of_measurement["name"],
            symbol=spec.unit_of_measurement["symbol"],
            definition=spec.unit_of_measurement["definition"],
        )
        ds = fsc.Datastream(
            name=spec.name,
            description=spec.description,
            observation_type=spec.observation_type,
            unit_of_measurement=uom,
            properties={**spec.properties, self.KEY_FIELD: spec.external_key},
            # ID-only refs — avoids re-creating Thing/Sensor/ObservedProperty
            thing=fsc.Thing(id=int(thing_id)),
            sensor=fsc.Sensor(id=int(sensor_id)),
            observed_property=fsc.ObservedProperty(id=int(observed_property_id)),
        )
        self.service.create(ds)
        logger.info("Created Datastream id=%s key=%s", ds.id, spec.external_key)
        return str(ds.id)

    # ── observation batch posting ────────────────────────────────────────────

    def _post_data_array(self, datastream_id: str, chunk: Sequence[ObservationRecord]) -> None:
        import frost_sta_client as fsc
        from frost_sta_client.model.ext.data_array_value import DataArrayValue
        from frost_sta_client.model.ext.data_array_document import DataArrayDocument

        dav = DataArrayValue()
        # datastream must be set before components (DataArrayValue.__getstate__ uses dav.datastream.id)
        dav.datastream = fsc.Datastream(id=int(datastream_id))
        dav.components = {
            DataArrayValue.Property.PHENOMENON_TIME,
            DataArrayValue.Property.RESULT,
        }
        for rec in chunk:
            dav.add_observation(fsc.Observation(
                phenomenon_time=rec.phenomenon_time,
                result=rec.result,
            ))
        doc = DataArrayDocument()
        doc.add_data_array_value(dav)
        self.service.observations().create(doc)

    # ── watermark recovery ───────────────────────────────────────────────────

    def _max_phenomenon_time(self, datastream_id: str) -> datetime | None:
        """Query FROST for the most recent observation on this datastream."""
        import frost_sta_client as fsc
        try:
            ds = fsc.Datastream(id=int(datastream_id))
            ds.service = self.service
            obs_list = (
                ds.get_observations()
                .query()
                .orderby("phenomenonTime")   # DESC by default in frost_sta_client
                .top(1)
                .list()
            )
            for ob in obs_list:
                if ob.phenomenon_time is None:
                    continue
                pt = ob.phenomenon_time
                if isinstance(pt, datetime):
                    return pt.replace(tzinfo=timezone.utc) if pt.tzinfo is None else pt
                # frost_sta_client stores datetime as ISO string after __setstate__
                return datetime.fromisoformat(pt.replace("Z", "+00:00"))
        except Exception as exc:
            logger.warning("Could not recover watermark for datastream %s: %s", datastream_id, exc)
        return None
