"""
canonical_model.py
Defines the SensorThings canonical data model for Aqueduct.

These dataclasses are the contract between source adapters and the FROST loader.
- Adapters produce these shapes
- frost_loader.py consumes these shapes
- Neither knows about the other's internals

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
    """Where the Thing is. In water data: lat/lon of a well or gauge station.

    geometry: GeoJSON Point — {'type': 'Point', 'coordinates': [lon, lat, elev_metres]}
    external_key: e.g. 'pvacd-NM-28258' or 'cabq-COA-0001'
    """
    external_key: str
    name: str
    description: str
    geometry: dict                          # GeoJSON Point
    encoding_type: str = "application/geo+json"
    properties: dict = field(default_factory=dict)


@dataclass(frozen=True)
class CanonicalThing:
    """The physical object being monitored. In water data: a groundwater well.

    properties must always include {'agency': '<AGENCY_CODE>'}
    external_key: e.g. 'pvacd-NM-28258' or 'cabq-COA-0001'
    """
    external_key: str
    name: str
    description: str
    location: CanonicalLocation
    properties: dict = field(default_factory=dict)


@dataclass(frozen=True)
class CanonicalSensor:
    """The instrument or method used to take the measurement.

    Use constants from canonical_constants.py — do not create new sensors inside adapters.
    If the source doesn't specify the instrument, use MANUAL_SENSOR or CONTINUOUS_LOGGER.
    """
    external_key: str
    name: str
    description: str
    encoding_type: str
    metadata: str
    properties: dict = field(default_factory=dict)


@dataclass(frozen=True)
class CanonicalObservedProperty:
    """What is being measured. In water data: depth to water, water elevation, etc.

    definition: URI from an established ontology (ODM2, QUDT).
    Use constants from canonical_constants.py — do not create new properties inside adapters.
    """
    external_key: str
    name: str
    definition: str
    description: str
    properties: dict = field(default_factory=dict)


@dataclass(frozen=True)
class CanonicalDatastream:
    """A time series: one ObservedProperty, one Sensor, one Thing.

    A single well can have multiple Datastreams (e.g. depth to water AND water elevation).
    external_key must encode thing + property: e.g. 'cabq-COA-0001-dtw'
    unit_of_measurement and observation_type: use constants from canonical_constants.py
    """
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
    """A single measurement: value + timestamp.

    phenomenon_time: always UTC — convert from source timezone if needed.
    result: numeric value in the unit defined by the Datastream.
    parameters: optional per-observation metadata (e.g. measurement_method, dry_indicator).
    result_quality: optional QC flag — leave None if source doesn't provide it.
    """
    phenomenon_time: datetime
    result: float
    datastream_external_key: str
    parameters: dict | None = None
    result_quality: str | None = None


@dataclass
class CanonicalBundle:
    """Everything an adapter emits for one source location (one well, one gauge).

    The FROST loader processes one bundle at a time.
    observations is keyed by datastream external_key.
    """
    datastreams: list[CanonicalDatastream]
    observations: dict[str, list[CanonicalObservation]]
