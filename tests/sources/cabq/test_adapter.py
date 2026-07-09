"""
tests/sources/cabq/test_adapter.py

Unit tests for CabqAdapter.
No real API calls — safe to run without credentials.

TODO: fill in once real CABQ API response shape is known.
"""

from aqueduct_dagster.sources.cabq.adapter import CabqAdapter  # noqa: F401


def test_to_thing_produces_correct_key():
    # TODO: add mock record and assert external_key follows "cabq-<source_id>" pattern
    pass


def test_to_observations_returns_canonical_obs():
    # TODO: add mock readings and assert CanonicalObservation fields
    pass


def test_build_datastreams_returns_one_stream():
    # TODO: assert datastream external_key follows "cabq-<source_id>-dtw" pattern
    pass
