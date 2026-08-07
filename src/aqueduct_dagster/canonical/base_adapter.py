"""
base_adapter.py
Abstract base class for all Aqueduct source adapters.

Every source adapter inherits from BaseAdapter and implements three methods:
  - extract()          pull raw records from the source
  - to_thing()         map one record to a CanonicalThing (+ its Location)
  - to_observations()  map one record to a list of CanonicalObservations

The Dagster pipeline calls adapter.run() — the same call regardless of source.
"""

from __future__ import annotations

import abc
import logging
from collections.abc import Iterator
from typing import Any

from aqueduct_dagster.canonical.canonical_constants import make_datastream_key, make_location_key
from aqueduct_dagster.canonical.canonical_model import (
    CanonicalBundle,
    CanonicalObservation,
    CanonicalThing,
)

logger = logging.getLogger(__name__)


class BaseAdapter(abc.ABC):
    def __init__(self, agency: str) -> None:
        # agency code used to build external_keys — must be consistent across runs
        self.agency = agency.upper()
        # Counts records run() failed to adapt — surfaced by callers as
        # run/output metadata. Full per-failure detail (record + error) is
        # already in the logger.error() call below; no need to also retain
        # it in memory here.
        self.failure_count = 0

    # ── Three methods every adapter must implement ────────────────────────────

    @abc.abstractmethod
    def extract(self) -> Iterator[dict]:
        """Pull raw records from the source (GCS).
        Yield one raw record at a time. Do not transform here."""
        ...

    @abc.abstractmethod
    def to_thing(self, record: dict) -> CanonicalThing:
        """Map one raw record to a CanonicalThing (with its Location inside).
        Called once per station — not once per observation.
        properties must include {'agency': self.agency}."""
        ...

    @abc.abstractmethod
    def to_observations(self, record: dict) -> list[CanonicalObservation]:
        """Map one raw record to a list of CanonicalObservations.
        phenomenon_time must be UTC. result must be float."""
        ...

    @abc.abstractmethod
    def _build_datastreams(self, thing: CanonicalThing) -> list:
        """Build CanonicalDatastreams for this Thing using canonical constants."""
        ...

    # ── run() — called by the pipeline, do not override ──────────────────────

    def run(self) -> Iterator[CanonicalBundle]:
        """Orchestrates extract → transform → yield bundle.
        Bad records are logged and skipped — one failure won't stop the run."""
        for record in self.extract():
            try:
                thing = self.to_thing(record)
                observations = self.to_observations(record)

                obs_by_ds: dict[str, list[CanonicalObservation]] = {}
                for obs in observations:
                    obs_by_ds.setdefault(obs.datastream_external_key, []).append(obs)

                yield CanonicalBundle(
                    datastreams=self._build_datastreams(thing),
                    observations=obs_by_ds,
                )
            except Exception as exc:
                logger.error("adapter=%s error=%s record=%r", self.__class__.__name__, exc, record)
                self.failure_count += 1

    # ── Helpers available to all adapters ─────────────────────────────────────

    def make_location_key(self, source_id: str) -> str:
        return make_location_key(self.agency, source_id)

    def make_datastream_key(self, source_id: str, suffix: str) -> str:
        return make_datastream_key(self.agency, source_id, suffix)


def log_if_adapter_failed(adapter: BaseAdapter, log: Any, context: str = "") -> None:
    """
    Warns once via `log` (stdlib logger or Dagster context.log — either works,
    only .warning() is required) if `adapter.run()` recorded any failures.
    Call after `run()` is exhausted. `context`, if given, is prefixed to the
    message (e.g. a chunk window description) — shared by every source's
    transform/backfill code so the message and count logic can't drift apart.
    """
    if not adapter.failure_count:
        return
    prefix = f"{context}: " if context else ""
    log.warning(
        "%s%d record(s) failed to adapt and were skipped — see adapter_failures metadata",
        prefix,
        adapter.failure_count,
    )
