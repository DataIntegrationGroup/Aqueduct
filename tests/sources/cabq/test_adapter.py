"""
tests/sources/cabq/test_adapter.py

Unit tests for CabqAdapter.
No real API calls — safe to run without credentials.
"""

from datetime import UTC

from aqueduct_dagster.sources.cabq.adapter import CabqAdapter  # noqa: F401

_record = {
    "location_id": "IW4",
    "location_name": "LALF GROUNDWATER INJECTION WELL 4",
    "latitude": -106.599332407,
    "longitude": 35.170730266,
    "readings": [{"timestamp": 1391079600, "value": 4927.15}],
}


def test_to_thing_produces_correct_key():
    thing = CabqAdapter([_record]).to_thing(_record)
    assert thing.external_key == "cabq-IW4"


def test_to_observations_returns_canonical_obs():
    observations = CabqAdapter([_record]).to_observations(_record)
    assert len(observations) == 1
    assert observations[0].datastream_external_key == "cabq-IW4-dtw"
    assert observations[0].phenomenon_time.timestamp() == 1391079600
    assert observations[0].phenomenon_time.tzinfo == UTC
    assert observations[0].result == 4927.15


def test_build_datastreams_returns_one_stream():
    adapter = CabqAdapter([_record])
    datastream = adapter._build_datastreams(adapter.to_thing(_record))
    assert len(datastream) == 1
    assert datastream[0].external_key == "cabq-IW4-dtw"
