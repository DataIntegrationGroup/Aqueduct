"""
SensorThings canonical data model — the contract between source adapters and frost_loader.py.

Rules every adapter must follow:
  - external_key must be stable and globally unique across runs (used for upsert)
  - All timestamps must be UTC
  - Elevation always in metres
  - Use constants from canonical_constants.py for units, sensors, observed properties
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class CanonicalLocation:
    """Where the Thing is (lat/lon). geometry is GeoJSON Point; elevation lives in
    properties.source_specific, not here. external_key e.g. 'pvacd-4745648669458432'."""

    external_key: str
    name: str
    description: str
    geometry: dict  # GeoJSON Point
    encoding_type: str = "application/geo+json"
    properties: dict = field(default_factory=dict)


@dataclass(frozen=True)
class CanonicalThing:
    """The monitored object (a well). properties must include 'agency';
    external_key e.g. 'pvacd-4745648669458432'."""

    external_key: str
    name: str
    description: str
    location: CanonicalLocation
    properties: dict = field(default_factory=dict)


@dataclass(frozen=True)
class CanonicalSensor:
    """The instrument/method used. Use canonical_constants.py constants
    (e.g. MANUAL_SENSOR, HYDROVU_SENSOR) — don't create new ones in adapters."""

    external_key: str
    name: str
    description: str
    encoding_type: str
    metadata: str
    properties: dict = field(default_factory=dict)


@dataclass(frozen=True)
class CanonicalObservedProperty:
    """What's measured (e.g. depth to water). definition is an ontology URI (ODM2, QUDT).
    Use canonical_constants.py constants — don't create new ones in adapters."""

    external_key: str
    name: str
    definition: str
    description: str
    properties: dict = field(default_factory=dict)


@dataclass(frozen=True)
class CanonicalDatastream:
    """A time series (one Thing + Sensor + ObservedProperty); a well can have several.
    external_key encodes thing+property, e.g. 'cabq-COA-0001-dtw'."""

    external_key: str
    name: str
    description: str
    observation_type: str
    unit_of_measurement: dict
    thing: CanonicalThing
    sensor: CanonicalSensor
    observed_property: CanonicalObservedProperty
    properties: dict = field(default_factory=dict)


@dataclass(frozen=True)
class CanonicalObservation:
    """A single value + timestamp. phenomenon_time is UTC; result is in the
    Datastream's unit; parameters/result_quality are optional."""

    phenomenon_time: datetime
    result: float
    datastream_external_key: str
    parameters: dict | None = None
    result_quality: str | None = None


@dataclass
class CanonicalBundle:
    """Everything an adapter emits for one location; frost_loader.py processes
    one bundle at a time. observations is keyed by datastream external_key."""

    datastreams: list[CanonicalDatastream]
    observations: dict[str, list[CanonicalObservation]]
