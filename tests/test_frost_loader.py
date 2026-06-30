"""
tests/test_frost_loader.py

Unit tests for FrostLoader.ensure_datastream retry behavior.
No live FROST server required — all FROST calls are provided by a test double.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from unittest.mock import patch

import pytest

from aqueduct_dagster.canonical.canonical_model import (
    CanonicalDatastream,
    CanonicalLocation,
    CanonicalObservedProperty,
    CanonicalSensor,
    CanonicalThing,
)
from aqueduct_dagster.loader.frost_loader import FrostLoader, ObservationRecord
from aqueduct_dagster.loader.watermark_store import InMemoryWatermarkStore

# ── test double ─────────────────────────────────────────────────────────────


class _StubLoader(FrostLoader):
    """
    Minimal concrete FrostLoader for unit testing ensure_datastream retry.

    side_effects: dict mapping entity key to a list of responses. Each item is
    either None (not found), a str id (found), or an Exception (raise on that call).
    When the list is exhausted, the default behavior is used (None for find, str id for create).
    """

    _FIND_DEFAULTS: dict[str, str | None] = {
        "find_location": None,
        "find_thing": None,
        "find_sensor": None,
        "find_obsprop": None,
        "find_ds": None,
    }
    _CREATE_DEFAULTS: dict[str, str] = {
        "create_location": "loc-1",
        "create_thing": "thing-1",
        "create_sensor": "sensor-1",
        "create_obsprop": "obsprop-1",
        "create_ds": "ds-1",
    }

    def __init__(self, side_effects: dict[str, list] | None = None) -> None:
        super().__init__(InMemoryWatermarkStore())
        self._side_effects: dict[str, list] = {k: list(v) for k, v in (side_effects or {}).items()}
        self.call_counts: dict[str, int] = {}

    def _pop(self, key: str) -> str | None:
        self.call_counts[key] = self.call_counts.get(key, 0) + 1
        effects = self._side_effects.get(key, [])
        if effects:
            r = effects.pop(0)
            if isinstance(r, Exception):
                raise r
            return r
        return self._FIND_DEFAULTS.get(key) or self._CREATE_DEFAULTS.get(key)

    def _find_location(self, key: str) -> str | None:
        return self._pop("find_location")

    def _create_location(self, spec: CanonicalLocation) -> str:
        return self._pop("create_location")  # type: ignore[return-value]

    def _find_thing(self, key: str) -> str | None:
        return self._pop("find_thing")

    def _create_thing(self, spec: CanonicalThing, *, location_id: str) -> str:
        return self._pop("create_thing")  # type: ignore[return-value]

    def _find_sensor(self, key: str) -> str | None:
        return self._pop("find_sensor")

    def _create_sensor(self, spec: CanonicalSensor) -> str:
        return self._pop("create_sensor")  # type: ignore[return-value]

    def _find_observed_property(self, key: str) -> str | None:
        return self._pop("find_obsprop")

    def _create_observed_property(self, spec: CanonicalObservedProperty) -> str:
        return self._pop("create_obsprop")  # type: ignore[return-value]

    def _find_datastream(self, key: str) -> str | None:
        return self._pop("find_ds")

    def _create_datastream(
        self,
        spec: CanonicalDatastream,
        *,
        thing_id: str,
        sensor_id: str,
        observed_property_id: str,
    ) -> str:
        return self._pop("create_ds")  # type: ignore[return-value]

    def _post_data_array(self, datastream_id: str, chunk: Sequence[ObservationRecord]) -> None:
        pass

    def _max_phenomenon_time(self, datastream_id: str) -> datetime | None:
        return None


# ── fixtures ────────────────────────────────────────────────────────────────


def _make_spec() -> CanonicalDatastream:
    loc = CanonicalLocation(
        external_key="test-loc-1",
        name="Test Location",
        description="desc",
        geometry={"type": "Point", "coordinates": [-106.0, 35.0]},
    )
    thing = CanonicalThing(
        external_key="test-thing-1", name="Test Well", description="desc", location=loc
    )
    sensor = CanonicalSensor(
        external_key="test-sensor-1",
        name="Test Sensor",
        description="desc",
        encoding_type="application/pdf",
        metadata="http://example.com",
    )
    op = CanonicalObservedProperty(
        external_key="test-op-1",
        name="Depth to Water",
        definition="http://example.com/dtw",
        description="desc",
    )
    return CanonicalDatastream(
        external_key="test-ds-1",
        name="Test DS",
        description="desc",
        observation_type="http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        unit_of_measurement={"name": "ft", "symbol": "ft", "definition": "http://example.com/ft"},
        thing=thing,
        sensor=sensor,
        observed_property=op,
    )


# ── tests ────────────────────────────────────────────────────────────────────


@patch("aqueduct_dagster.loader.frost_loader.time.sleep")
def test_ensure_datastream_retries_transient_failure(mock_sleep):
    """A transient exception on the first find call is retried and succeeds."""
    loader = _StubLoader(side_effects={"find_location": [OSError("transient")]})
    ds_id = loader.ensure_datastream(_make_spec())
    assert ds_id == "ds-1"
    assert loader.call_counts["find_location"] == 2  # failed once, retried once
    mock_sleep.assert_called_once()  # one backoff delay


@patch("aqueduct_dagster.loader.frost_loader.time.sleep")
def test_ensure_datastream_raises_after_all_retries_exhausted(mock_sleep):
    """After all retry attempts fail, ensure_datastream propagates the exception."""
    loader = _StubLoader(side_effects={"find_location": [OSError("persistent")] * 5})
    with pytest.raises(OSError, match="persistent"):
        loader.ensure_datastream(_make_spec())
    assert loader.call_counts["find_location"] == 5  # exhausted all attempts


@patch("aqueduct_dagster.loader.frost_loader.time.sleep")
def test_ensure_datastream_succeeds_when_entity_already_exists(mock_sleep):
    """When find returns an existing id, create is never called."""
    loader = _StubLoader(side_effects={"find_location": ["existing-loc-id"]})
    loader.ensure_datastream(_make_spec())
    assert loader.call_counts.get("create_location", 0) == 0
    mock_sleep.assert_not_called()
