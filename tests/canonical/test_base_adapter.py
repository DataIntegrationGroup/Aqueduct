"""
tests/canonical/test_base_adapter.py

Unit tests for BaseAdapter.run()'s failure capture and the shared
log_if_adapter_failed() helper — the one piece every source adapter gets
for free (see canonical/base_adapter.py).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from aqueduct_dagster.canonical.base_adapter import BaseAdapter, log_if_adapter_failed
from aqueduct_dagster.canonical.canonical_model import CanonicalObservation, CanonicalThing


class _DummyAdapter(BaseAdapter):
    """Minimal concrete BaseAdapter: raises for any record with bad=True."""

    def __init__(self, records: list[dict]) -> None:
        super().__init__(agency="test")
        self._records = records

    def extract(self):
        yield from self._records

    def to_thing(self, record: dict) -> CanonicalThing:
        if record.get("bad"):
            raise ValueError("boom")
        return CanonicalThing(
            external_key="thing-1",
            name="Thing",
            description="",
            location=None,  # type: ignore[arg-type]
        )

    def to_observations(self, record: dict) -> list[CanonicalObservation]:
        return []

    def _build_datastreams(self, thing: CanonicalThing) -> list:
        return []


class TestRunFailureCapture:
    def test_fully_successful_batch_has_no_failures(self):
        adapter = _DummyAdapter([{"bad": False}, {"bad": False}])
        list(adapter.run())
        assert adapter.failure_count == 0

    def test_bad_record_is_counted_and_logged_with_detail(self, caplog):
        bad = {"bad": True}
        adapter = _DummyAdapter([bad])

        with caplog.at_level("ERROR", logger="aqueduct_dagster.canonical.base_adapter"):
            bundles = list(adapter.run())

        assert bundles == []
        assert adapter.failure_count == 1
        assert "boom" in caplog.text
        assert repr(bad) in caplog.text

    def test_mixed_batch_good_records_still_yield_bundles(self):
        adapter = _DummyAdapter([{"bad": False}, {"bad": True}, {"bad": False}])
        bundles = list(adapter.run())
        assert len(bundles) == 2
        assert adapter.failure_count == 1


class TestLogIfAdapterFailed:
    def test_no_warning_when_nothing_failed(self):
        adapter = _DummyAdapter([{"bad": False}])
        list(adapter.run())
        log = MagicMock()

        log_if_adapter_failed(adapter, log)

        log.warning.assert_not_called()

    def test_warns_with_count_when_something_failed(self):
        adapter = _DummyAdapter([{"bad": True}, {"bad": True}])
        list(adapter.run())
        log = MagicMock()

        log_if_adapter_failed(adapter, log)

        log.warning.assert_called_once()
        fmt, *args = log.warning.call_args[0]
        assert (
            fmt % tuple(args)
        ) == "2 record(s) failed to adapt and were skipped — see adapter_failures metadata"

    def test_context_prefix_is_included_when_given(self):
        adapter = _DummyAdapter([{"bad": True}])
        list(adapter.run())
        log = MagicMock()

        log_if_adapter_failed(adapter, log, context="chunk [2026-01-01, 2026-02-01)")

        fmt, *args = log.warning.call_args[0]
        assert (fmt % tuple(args)).startswith("chunk [2026-01-01, 2026-02-01): 1 record(s)")

    def test_accepts_either_stdlib_logger_or_dagster_log(self):
        """log only needs .warning() — a plain logging.Logger works too."""
        import logging

        adapter = _DummyAdapter([{"bad": True}])
        list(adapter.run())

        log_if_adapter_failed(adapter, logging.getLogger("test"))  # must not raise
