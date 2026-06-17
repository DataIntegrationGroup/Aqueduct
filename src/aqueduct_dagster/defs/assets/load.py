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

import logging
import os

from dagster import AssetExecutionContext, MetadataValue, asset

from aqueduct_dagster.canonical.canonical_model import CanonicalBundle, CanonicalObservation
from aqueduct_dagster.defs.assets.transform_hydrovu import HydroVuTransformResult, commit_watermark
from aqueduct_dagster.loader.frost_loader import FrostStaClientLoader, ObservationRecord
from aqueduct_dagster.loader.watermark_store import FrostWatermarkStore

logger = logging.getLogger(__name__)


def _frost_load(context: AssetExecutionContext, bundles: list[CanonicalBundle]) -> None:
    """
    Loads a list of CanonicalBundles into FROST via the SensorThings API.
    Called by each source-specific asset — shared logic, no source coupling.

    For each bundle:
      1. ensure_datastream() — idempotent upsert of Thing/Location/Sensor/etc.
      2. load_observations() — filtered by watermark, posted as Data Array chunks
      3. Watermark advanced per chunk — partial failures resume cleanly
    """
    import frost_sta_client as fsc

    frost_url = os.environ.get("FROST_SERVICE_ROOT_URL", "http://localhost:8081/FROST-Server/v1.1")
    # FROST_SERVICE_ROOT_URL is the server's own root (no version suffix in docker-compose).
    # frost_sta_client constructs entity URLs by appending directly to this base,
    # so it must include /v1.1 — append it if not already present.
    if not frost_url.rstrip("/").endswith("/v1.1"):
        frost_url = frost_url.rstrip("/") + "/v1.1"
    service = fsc.SensorThingsService(frost_url)
    watermarks = FrostWatermarkStore(context)
    loader = FrostStaClientLoader(service, watermarks)

    total_posted = 0
    total_skipped = 0

    for bundle in bundles:
        for datastream in bundle.datastreams:
            ds_id = loader.ensure_datastream(datastream)

            raw_obs: list[CanonicalObservation] = bundle.observations.get(
                datastream.external_key, []
            )
            records = [
                ObservationRecord(phenomenon_time=o.phenomenon_time, result=o.result)
                for o in raw_obs
            ]

            result = loader.load_observations(datastream.external_key, ds_id, records)
            total_posted += result.posted
            total_skipped += result.skipped

            context.log.info(
                "Datastream %s (FROST id=%s): posted=%d skipped=%d watermark=%s",
                datastream.external_key, ds_id,
                result.posted, result.skipped, result.new_watermark,
            )

    context.log.info(
        "FROST load complete: %d bundle(s), %d posted, %d skipped",
        len(bundles), total_posted, total_skipped,
    )
    context.add_output_metadata({
        "bundles_loaded": MetadataValue.int(len(bundles)),
        "observations_posted": MetadataValue.int(total_posted),
        "observations_skipped": MetadataValue.int(total_skipped),
    })


@asset(
    name="frost_load_hydrovu",
    group_name="hydrovu",
    description="Loads HydroVu CanonicalBundles into the local FROST server.",
    compute_kind="frost",
)
def frost_load_hydrovu(
    context: AssetExecutionContext,
    canonical_bundles_hydrovu: HydroVuTransformResult,
) -> None:
    _frost_load(context, canonical_bundles_hydrovu.bundles)
    if canonical_bundles_hydrovu.max_load_id is not None:
        commit_watermark(canonical_bundles_hydrovu.max_load_id)
        context.log.info(
            "Transform watermark committed after FROST success: max_load_id=%s",
            canonical_bundles_hydrovu.max_load_id,
        )


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
