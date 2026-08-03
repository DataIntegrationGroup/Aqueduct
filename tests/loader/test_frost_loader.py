"""
tests/loader/test_frost_loader.py

Unit tests for FrostLoader.ensure_datastream retry behavior.
No live FROST server required — all FROST calls are provided by a test double.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from aqueduct_dagster.canonical.canonical_model import (
    CanonicalDatastream,
    CanonicalLocation,
    CanonicalObservedProperty,
    CanonicalSensor,
    CanonicalThing,
)
from aqueduct_dagster.loader.frost_loader import (
    FrostLoader,
    FrostStaClientLoader,
    ObservationRecord,
)
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

    def __init__(
        self,
        side_effects: dict[str, list] | None = None,
        deleted_count: int = 0,
    ) -> None:
        super().__init__(InMemoryWatermarkStore())
        self._side_effects: dict[str, list] = {k: list(v) for k, v in (side_effects or {}).items()}
        self.call_counts: dict[str, int] = {}
        self._deleted_count = deleted_count
        self.delete_windows: list[tuple[str, datetime, datetime]] = []
        self.posted_chunks: list[list[ObservationRecord]] = []

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
        self.posted_chunks.append(list(chunk))

    def _max_phenomenon_time(self, datastream_id: str) -> datetime | None:
        return None

    def _delete_observations_in_window(
        self, datastream_id: str, window_start: datetime, window_end: datetime
    ) -> int:
        self.delete_windows.append((datastream_id, window_start, window_end))
        return self._deleted_count


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


# ── _post_data_array response body checks ────────────────────────────────────


class _ObsStub:
    """Minimal Observation-like object with a self_link attribute."""

    def __init__(self, self_link: str) -> None:
        self.self_link = self_link


def _make_fsc_loader_with_post_result(post_results: list) -> FrostStaClientLoader:
    """Build a FrostStaClientLoader whose observations().create() returns post_results."""
    from unittest.mock import MagicMock

    service = MagicMock()
    service.observations.return_value.create.return_value = post_results
    return FrostStaClientLoader(service, InMemoryWatermarkStore())


def test_post_data_array_raises_on_partial_frost_rejection():
    """RuntimeError raised when FROST returns error strings for some observations."""
    results = [
        _ObsStub("http://frost/v1.1/Observations(1)"),
        _ObsStub("error: violates uniqueness constraint"),
        _ObsStub("http://frost/v1.1/Observations(3)"),
    ]
    loader = _make_fsc_loader_with_post_result(results)
    with pytest.raises(RuntimeError, match="FROST rejected 1/3"):
        loader._post_data_array("42", [])


def test_post_data_array_raises_on_full_frost_rejection():
    """RuntimeError raised when all observations are rejected by FROST."""
    results = [_ObsStub("error: bad request")] * 5
    loader = _make_fsc_loader_with_post_result(results)
    with pytest.raises(RuntimeError, match="FROST rejected 5/5"):
        loader._post_data_array("42", [])


def test_post_data_array_does_not_raise_on_full_success():
    """No exception raised when all observations are accepted (all URLs in response)."""
    results = [
        _ObsStub("http://frost/v1.1/Observations(1)"),
        _ObsStub("http://frost/v1.1/Observations(2)"),
    ]
    loader = _make_fsc_loader_with_post_result(results)
    loader._post_data_array("42", [])  # must not raise


# ── load_window (delete-then-repost) ─────────────────────────────────────────

WINDOW_START = datetime(2026, 1, 1, tzinfo=UTC)
WINDOW_END = datetime(2026, 2, 1, tzinfo=UTC)


def _rec(day: int, value: float = 1.0) -> ObservationRecord:
    return ObservationRecord(phenomenon_time=datetime(2026, 1, day, tzinfo=UTC), result=value)


def test_load_window_deletes_before_posting():
    loader = _StubLoader(deleted_count=3)
    records = [_rec(10), _rec(5)]
    result = loader.load_window("ds-key", "42", records, WINDOW_START, WINDOW_END)

    assert loader.delete_windows == [("42", WINDOW_START, WINDOW_END)]
    assert result.considered == 2
    assert result.posted == 2
    assert result.deleted == 3
    # posted in phenomenon_time order, not input order
    assert [r.phenomenon_time.day for r in loader.posted_chunks[0]] == [5, 10]


def test_load_window_with_no_records_still_deletes_the_window():
    loader = _StubLoader(deleted_count=5)
    result = loader.load_window("ds-key", "42", [], WINDOW_START, WINDOW_END)

    assert loader.delete_windows == [("42", WINDOW_START, WINDOW_END)]
    assert result.posted == 0
    assert loader.posted_chunks == []


def test_load_window_advances_watermark_when_window_is_ahead():
    loader = _StubLoader()
    records = [_rec(5), _rec(20)]
    result = loader.load_window("ds-key", "42", records, WINDOW_START, WINDOW_END)

    assert result.new_watermark == datetime(2026, 1, 20, tzinfo=UTC)
    assert loader.watermarks.get("ds-key") == datetime(2026, 1, 20, tzinfo=UTC)


def test_load_window_does_not_rewind_watermark_for_historical_correction():
    """Correcting old history (e.g. a vendor correction) must never move the
    watermark backward — a later, unrelated normal load already advanced it."""
    loader = _StubLoader()
    loader.watermarks.set("ds-key", datetime(2026, 6, 1, tzinfo=UTC))
    records = [_rec(5), _rec(20)]  # all within January, well behind June

    result = loader.load_window("ds-key", "42", records, WINDOW_START, WINDOW_END)

    assert result.posted == 2  # still reposted
    assert result.new_watermark == datetime(2026, 6, 1, tzinfo=UTC)  # unchanged
    assert loader.watermarks.get("ds-key") == datetime(2026, 6, 1, tzinfo=UTC)


def test_load_window_empty_records_preserves_existing_watermark():
    loader = _StubLoader()
    loader.watermarks.set("ds-key", datetime(2026, 6, 1, tzinfo=UTC))

    result = loader.load_window("ds-key", "42", [], WINDOW_START, WINDOW_END)

    assert result.new_watermark == datetime(2026, 6, 1, tzinfo=UTC)


# ── load_window: window-range validation (defense in depth) ──────────────────


def test_load_window_raises_on_out_of_window_record_before_any_side_effect():
    """
    A record outside [window_start, window_end) must be rejected BEFORE any
    delete or post happens — zero side effects on failure, so the window is
    left completely untouched rather than deleted-and-then-wrongly-repostable.
    """
    loader = _StubLoader(deleted_count=99)  # would prove delete ran, if it did
    out_of_window = ObservationRecord(phenomenon_time=datetime(2026, 2, 5, tzinfo=UTC), result=1.0)
    records = [_rec(5), out_of_window]

    with pytest.raises(ValueError, match="outside the requested window"):
        loader.load_window("ds-key", "42", records, WINDOW_START, WINDOW_END)

    assert loader.delete_windows == []
    assert loader.posted_chunks == []


def test_load_window_boundary_start_is_inclusive():
    loader = _StubLoader(deleted_count=0)
    at_start = ObservationRecord(phenomenon_time=WINDOW_START, result=1.0)

    result = loader.load_window("ds-key", "42", [at_start], WINDOW_START, WINDOW_END)

    assert result.posted == 1


def test_load_window_boundary_end_is_exclusive():
    loader = _StubLoader(deleted_count=0)
    at_end = ObservationRecord(phenomenon_time=WINDOW_END, result=1.0)

    with pytest.raises(ValueError, match="outside the requested window"):
        loader.load_window("ds-key", "42", [at_end], WINDOW_START, WINDOW_END)


# ── FrostStaClientLoader._delete_observations_in_window ──────────────────────


def test_delete_observations_in_window_builds_correct_filter_and_deletes_each_match():
    import frost_sta_client as fsc

    obs_1 = MagicMock()
    obs_2 = MagicMock()
    service = MagicMock()

    loader = FrostStaClientLoader(service, InMemoryWatermarkStore())

    with patch.object(fsc, "Datastream") as mock_ds_cls:
        mock_ds = MagicMock()
        mock_ds_cls.return_value = mock_ds
        mock_query = mock_ds.get_observations.return_value.query.return_value
        mock_query.filter.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_query.list.return_value = [obs_1, obs_2]

        deleted = loader._delete_observations_in_window(
            "42",
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 2, 1, tzinfo=UTC),
        )

    assert deleted == 2
    assert service.delete.call_count == 2
    service.delete.assert_any_call(obs_1)
    service.delete.assert_any_call(obs_2)
    filter_arg = mock_query.filter.call_args[0][0]
    assert "phenomenonTime ge 2026-01-01T00:00:00Z" in filter_arg
    assert "phenomenonTime lt 2026-02-01T00:00:00Z" in filter_arg


def test_delete_observations_in_window_returns_zero_when_nothing_matches():
    import frost_sta_client as fsc

    service = MagicMock()
    loader = FrostStaClientLoader(service, InMemoryWatermarkStore())

    with patch.object(fsc, "Datastream") as mock_ds_cls:
        mock_ds = MagicMock()
        mock_ds_cls.return_value = mock_ds
        mock_query = mock_ds.get_observations.return_value.query.return_value
        mock_query.filter.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_query.list.return_value = []

        deleted = loader._delete_observations_in_window(
            "42",
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 2, 1, tzinfo=UTC),
        )

    assert deleted == 0
    service.delete.assert_not_called()


def test_delete_observations_in_window_materializes_all_pages_before_deleting():
    """
    Regression test: EntityList.__next__ fetches later pages lazily via
    @iot.nextLink, and FROST builds that link as $top=N&$skip=N (skip/offset
    based). Deleting while still iterating would shrink the underlying result
    set mid-pagination, shifting the skip offset for every later page and
    silently skipping matches for any window spanning more than one page.
    This verifies every entity is consumed from the query result before the
    first delete() call happens, regardless of how many "pages" it spans.
    """
    import frost_sta_client as fsc

    call_order: list[tuple[str, int]] = []

    class _LazyPaginatedResult:
        """Simulates EntityList: iterating triggers lazy per-item fetch."""

        def __init__(self, items: list) -> None:
            self._items = items

        def __iter__(self):
            def gen():
                for item in self._items:
                    call_order.append(("yield", item))
                    yield item

            return gen()

    obs_1, obs_2, obs_3 = 1, 2, 3  # plain ints stand in for observation entities
    service = MagicMock()
    service.delete.side_effect = lambda ob: call_order.append(("delete", ob))

    loader = FrostStaClientLoader(service, InMemoryWatermarkStore())

    with patch.object(fsc, "Datastream") as mock_ds_cls:
        mock_ds = MagicMock()
        mock_ds_cls.return_value = mock_ds
        mock_query = mock_ds.get_observations.return_value.query.return_value
        mock_query.filter.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_query.list.return_value = _LazyPaginatedResult([obs_1, obs_2, obs_3])

        deleted = loader._delete_observations_in_window(
            "42",
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 2, 1, tzinfo=UTC),
        )

    assert deleted == 3
    # All three yields must happen before the first delete — i.e. the full
    # result set is materialized before any entity is deleted.
    first_delete_idx = next(i for i, (kind, _) in enumerate(call_order) if kind == "delete")
    assert [kind for kind, _ in call_order[:first_delete_idx]] == ["yield", "yield", "yield"]
