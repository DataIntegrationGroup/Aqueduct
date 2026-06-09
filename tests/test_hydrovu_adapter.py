"""
tests/test_hydrovu_adapter.py
⚠ REVIEW NEEDED — tests are stubs only.
  Fill in once real HydroVu API response shape is confirmed and
  adapter methods are implemented.
Unit tests for HydroVuAdapter using a mocked HTTP response.
No real API calls — safe to run without credentials.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from aqueduct_dagster.adapters.hydrovu_adapter import HydroVuAdapter


MOCK_LOCATIONS = [
    {"id": "123", "name": "Zumwalt Well", "gps": {"latitude": 36.1, "longitude": -106.2, "elevation": 5400.0}},
]

# TODO: update this shape once real HydroVu readings response is confirmed
MOCK_READINGS = [
    {
        "timestamp": "2024-06-01T00:00:00Z",
        "parameters": [
            {"parameterId": "TODO_depth_param_id", "parameterName": "Depth to Water", "value": 45.3, "unitSymbol": "ft"},
        ],
    }
]


@pytest.fixture
def adapter():
    return HydroVuAdapter()


def test_to_thing_produces_correct_key(adapter):
    record = {"location": MOCK_LOCATIONS[0], "readings": MOCK_READINGS}
    thing = adapter.to_thing(record)
    assert thing.external_key == "pvacd-123"
    assert thing.properties["agency"] == "PVACD"


def test_to_observations_skips_unknown_params(adapter):
    record = {
        "location": MOCK_LOCATIONS[0],
        "readings": [
            {"timestamp": "2024-06-01T00:00:00Z", "parameters": [
                {"parameterId": "unknown_param", "value": 99.9},
            ]}
        ],
    }
    # Should return empty list since param ID doesn't match
    obs = adapter.to_observations(record)
    assert obs == []


def test_to_observations_returns_canonical_obs(adapter):
    # Once real param ID is confirmed, update DEPTH_TO_WATER_PARAM_ID and this test
    record = {"location": MOCK_LOCATIONS[0], "readings": MOCK_READINGS}
    obs = adapter.to_observations(record)
    # For now just assert it doesn't crash
    # TODO: assert len(obs) == 1 after DEPTH_TO_WATER_PARAM_ID is set


def test_build_datastreams_returns_one_stream(adapter):
    record = {"location": MOCK_LOCATIONS[0], "readings": MOCK_READINGS}
    thing = adapter.to_thing(record)
    datastreams = adapter._build_datastreams(thing)
    assert len(datastreams) == 1
    assert datastreams[0].external_key == "pvacd-123-dtw"
