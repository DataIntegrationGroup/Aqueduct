"""
defs/assets/load.py

Two terminal assets — one per source — both backed by a shared private helper.

  frost_load_hydrovu  upstream: canonical_bundles_hydrovu
  frost_load_cabq     upstream: canonical_bundles_cabq

Splitting into two assets keeps each source pipeline fully independent:
hydrovu_pipeline and cabq_pipeline can run on different schedules without
either blocking the other.

All real FROST loading logic lives in _frost_load() — adding a new source
is just a new two-line asset that calls it.

No source-specific logic here — the canonical model is the contract.
"""

from __future__ import annotations

import logging

from dagster import AssetExecutionContext, asset

from aqueduct_dagster.canonical.canonical_model import CanonicalBundle

logger = logging.getLogger(__name__)


def _frost_load(context: AssetExecutionContext, bundles: list[CanonicalBundle]) -> None:
    """
    Loads a list of CanonicalBundles into FROST via the SensorThings API.
    Called by each source-specific asset — shared logic, no source coupling.

    For each bundle:
      1. ensure_datastream() — idempotent upsert of Thing/Location/Sensor/etc.
      2. load_observations() — filtered by watermark, posted as Data Array chunks
      3. Watermark advanced per successful chunk
    """
    # TODO: build FROST service client from FROST_SERVICE_ROOT_URL env var
    # TODO: build FrostWatermarkStore(context)
    # TODO: build FrostStaClientLoader(service, watermarks)
    # TODO: for each bundle, for each datastream:
    #         ds_id = loader.ensure_datastream(datastream)
    #         loader.load_observations(datastream.external_key, ds_id, observations)
    pass


@asset(
    name="frost_load_hydrovu",
    group_name="hydrovu",
    description="Loads HydroVu CanonicalBundles into the local FROST server.",
    compute_kind="frost",
)
def frost_load_hydrovu(
    context: AssetExecutionContext,
    canonical_bundles_hydrovu: list[CanonicalBundle],
) -> None:
    _frost_load(context, canonical_bundles_hydrovu)


@asset(
    name="frost_load_cabq",
    group_name="cabq",
    description="Loads CABQ CanonicalBundles into the local FROST server.",
    compute_kind="frost",
)
def frost_load_cabq(
    context: AssetExecutionContext,
    canonical_bundles_cabq: list[CanonicalBundle],
) -> None:
    _frost_load(context, canonical_bundles_cabq)
