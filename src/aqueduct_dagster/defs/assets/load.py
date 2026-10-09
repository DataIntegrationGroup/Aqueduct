"""
Terminal load assets — one per source — backed by a shared private helper.

Generated per entry in shared/source_registry.py's SOURCE_REGISTRY (also used
by defs/definitions.py); adding a source needs no new code here.
commit_watermark is called inside the factory so it can never be forgotten.

No source-specific logic here — the canonical model is the contract.
"""

import logging
from typing import Any

from dagster import AssetExecutionContext, AssetIn, MetadataValue, OpExecutionContext, asset

from aqueduct_dagster.canonical.canonical_model import CanonicalBundle
from aqueduct_dagster.loader.frost_auth import attach_id_token_auth, service_root_url
from aqueduct_dagster.loader.frost_loader import FrostStaClientLoader, observation_records_for
from aqueduct_dagster.loader.watermark_store import FrostWatermarkStore
from aqueduct_dagster.shared.gcs import (
    _gcs_bucket_url,
    _gcs_filesystem,
    commit_watermark,
    transform_watermark_path,
)
from aqueduct_dagster.shared.source_registry import SOURCE_REGISTRY

logger = logging.getLogger(__name__)

FROST_REQUEST_TIMEOUT = 30  # seconds per request; retries handled by _with_retry in frost_loader


def _apply_frost_timeout(service: Any, timeout: int = FROST_REQUEST_TIMEOUT) -> None:
    """Inject a per-request timeout into every SensorThingsService.execute() call."""
    _orig = service.execute

    def _execute_with_timeout(method: str, url: str, **kwargs: Any) -> Any:
        kwargs.setdefault("timeout", timeout)
        return _orig(method, url, **kwargs)

    service.execute = _execute_with_timeout


def build_frost_loader(
    context: AssetExecutionContext | OpExecutionContext, dataset: str
) -> FrostStaClientLoader:
    """Builds a FrostStaClientLoader against the configured FROST server (see
    service_root_url()) — shared by every frost_load_* asset and the backfill
    job, so both authenticate through the exact same code path."""
    import frost_sta_client as fsc

    frost_url = service_root_url()
    service = fsc.SensorThingsService(frost_url)
    # No-op against a local docker FROST; attaches a Cloud Run ID token otherwise.
    attach_id_token_auth(service, frost_url)
    _apply_frost_timeout(service)
    bucket = _gcs_bucket_url().replace("gs://", "")
    watermarks = FrostWatermarkStore(context, _gcs_filesystem(), bucket, dataset=dataset)
    return FrostStaClientLoader(service, watermarks)


def _frost_load(
    context: AssetExecutionContext, bundles: list[CanonicalBundle], *, dataset: str
) -> None:
    """Loads CanonicalBundles into FROST: idempotent upsert of Thing/Location/Sensor,
    then observations posted filtered by watermark, advanced per chunk so partial
    failures resume cleanly."""
    loader = build_frost_loader(context, dataset)

    total_posted = 0
    total_skipped = 0

    for bundle in bundles:
        for datastream in bundle.datastreams:
            ds_id = loader.ensure_datastream(datastream)
            records = observation_records_for(bundle, datastream)
            result = loader.load_observations(datastream.external_key, ds_id, records)
            total_posted += result.posted
            total_skipped += result.skipped

            context.log.info(
                "Datastream %s (FROST id=%s): posted=%d skipped=%d watermark=%s",
                datastream.external_key,
                ds_id,
                result.posted,
                result.skipped,
                result.new_watermark,
            )

    context.log.info(
        "FROST load complete: %d bundle(s), %d posted, %d skipped",
        len(bundles),
        total_posted,
        total_skipped,
    )
    context.add_output_metadata(
        {
            "bundles_loaded": MetadataValue.int(len(bundles)),
            "observations_posted": MetadataValue.int(total_posted),
            "observations_skipped": MetadataValue.int(total_skipped),
        }
    )


def _make_frost_load_asset(name: str, dataset: str) -> Any:
    """Generates a frost_load_{name} asset: runs _frost_load() then commits the
    transform watermark on success. transform_result must have .bundles and
    .max_load_id — checked for every registry entry by tests/defs/test_definitions.py."""
    watermark_path = transform_watermark_path(dataset, name)

    @asset(
        name=f"frost_load_{name}",
        group_name=name,
        description=f"Loads {name.upper()} CanonicalBundles into the configured FROST server.",
        compute_kind="frost",
        ins={"transform_result": AssetIn(f"canonical_bundles_{name}")},
    )
    def _asset(context: AssetExecutionContext, transform_result: Any) -> None:
        _frost_load(context, transform_result.bundles, dataset=dataset)
        if transform_result.max_load_id is not None:
            commit_watermark(watermark_path, transform_result.max_load_id)
            context.log.info(
                "Transform watermark committed after FROST success: max_load_id=%s",
                transform_result.max_load_id,
            )

    return _asset


# Generate one load asset per source — discovered automatically by load_assets_from_package_module.
# Bound into the module namespace via globals() so adding a source to SOURCE_REGISTRY is the only
# change needed here — no hardcoded assignment line to remember.
for _cfg in SOURCE_REGISTRY:
    globals()[f"frost_load_{_cfg['name']}"] = _make_frost_load_asset(
        name=_cfg["name"], dataset=_cfg["dataset"]
    )
