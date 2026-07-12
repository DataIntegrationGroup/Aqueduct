"""
tests/defs/assets/test_load.py

Unit tests for the FROST request timeout injection.
No live FROST server or Dagster runtime required.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from aqueduct_dagster.defs.assets.load import FROST_REQUEST_TIMEOUT, _apply_frost_timeout


def test_timeout_is_injected_into_execute():
    service = MagicMock()
    orig_execute = service.execute
    _apply_frost_timeout(service)

    service.execute("GET", "http://frost/v1.1/Locations")

    orig_execute.assert_called_once_with(
        "GET", "http://frost/v1.1/Locations", timeout=FROST_REQUEST_TIMEOUT
    )


def test_explicit_timeout_is_not_overridden():
    service = MagicMock()
    orig_execute = service.execute
    _apply_frost_timeout(service)

    service.execute("GET", "http://frost/v1.1/Locations", timeout=5)

    orig_execute.assert_called_once_with("GET", "http://frost/v1.1/Locations", timeout=5)


def test_other_kwargs_are_passed_through():
    service = MagicMock()
    orig_execute = service.execute
    _apply_frost_timeout(service)

    service.execute("POST", "http://frost/v1.1/Observations", json={"result": 1.5})

    orig_execute.assert_called_once_with(
        "POST",
        "http://frost/v1.1/Observations",
        json={"result": 1.5},
        timeout=FROST_REQUEST_TIMEOUT,
    )


def test_custom_timeout_value_is_respected():
    service = MagicMock()
    orig_execute = service.execute
    _apply_frost_timeout(service, timeout=10)

    service.execute("GET", "http://frost/v1.1/Things")

    orig_execute.assert_called_once_with("GET", "http://frost/v1.1/Things", timeout=10)
