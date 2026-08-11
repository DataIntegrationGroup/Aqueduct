#!/usr/bin/env python
"""
deploy/31_verify_dagster_auth.py

Verifies that a Dagster+ service account key actually works, by running the exact
code path Dagster+ Serverless runs — locally, without deploying anything.

    export GCP_SERVICE_ACCOUNT_KEY_B64=<blob from ./deploy/30_dagster_gcp_auth.sh --emit-key>
    export FROST_SERVICE_ROOT_URL=https://<frost-run-url>/FROST-Server
    unset GOOGLE_APPLICATION_CREDENTIALS
    uv run python deploy/31_verify_dagster_auth.py

Why this exists rather than a handful of `gcloud --impersonate-service-account`
commands: impersonation proves the IAM *bindings* are right while saying nothing
about the *key*. A revoked, truncated, or superseded key passes every impersonation
check and still fails in Dagster+. Only exercising ensure_adc() with the real blob
tests what Serverless does.

Each layer is checked independently and the script keeps going after a failure, so
one run tells you everything that is broken rather than only the first thing.
Read-only apart from a single probe object in the bucket, which is deleted again.

Exits 0 if every layer passes, 1 otherwise.
"""

from __future__ import annotations

import os
import sys
import traceback
import uuid

# Import through the package so this tests the same modules the assets import.
from aqueduct_dagster.loader.frost_auth import (
    ENV_FROST_URL,
    attach_id_token_auth,
    service_root_url,
)
from aqueduct_dagster.shared.config import load_config
from aqueduct_dagster.shared.gcp_auth import ENV_ADC_PATH, ENV_KEY_B64, ensure_adc
from aqueduct_dagster.shared.gcs import _gcs_bucket_url, _gcs_filesystem

PROBE_NAME = f"_adc_probe_{uuid.uuid4().hex[:8]}.txt"


class Check:
    """Collects pass/fail per layer so one run reports every problem."""

    def __init__(self) -> None:
        self.failures: list[str] = []

    def run(self, label: str, fn) -> object | None:
        print(f"\n== {label} ==")
        try:
            result = fn()
        except Exception as exc:  # noqa: BLE001 — this is a diagnostic tool
            self.failures.append(label)
            print(f"   FAIL: {type(exc).__name__}: {exc}")
            if os.environ.get("VERBOSE"):
                traceback.print_exc()
            return None
        print("   OK")
        return result


def check_adc() -> str:
    """ensure_adc() must leave GOOGLE_APPLICATION_CREDENTIALS on a real file."""
    if not os.environ.get(ENV_KEY_B64):
        raise RuntimeError(
            f"{ENV_KEY_B64} is not set. This script is meant to verify the key that "
            f"goes into Dagster+; export it first (./deploy/30_dagster_gcp_auth.sh --emit-key)."
        )
    ensure_adc()
    path = os.environ.get(ENV_ADC_PATH)
    if not path or not os.path.isfile(path):
        raise RuntimeError(f"{ENV_ADC_PATH} was not set to a readable file (got {path!r}).")
    print(f"   credentials materialized at {path}")
    return path


def check_gcs() -> str:
    """Write, read back, and delete one small object — the dlt destination path."""
    bucket = _gcs_bucket_url().replace("gs://", "")
    fs = _gcs_filesystem()
    probe = f"{bucket}/{PROBE_NAME}"
    payload = b"aqueduct adc probe"

    with fs.open(probe, "wb") as fh:
        fh.write(payload)
    try:
        with fs.open(probe, "rb") as fh:
            if fh.read() != payload:
                raise RuntimeError(f"probe object {probe} read back with unexpected contents")
    finally:
        fs.rm(probe)

    print(f"   wrote, read, and deleted gs://{probe}")
    return bucket


def check_secret_manager() -> str:
    """Access the HydroVu secret — the credentials ingest itself needs."""
    from google.cloud import secretmanager

    config = load_config()
    project_number = config["destination"]["filesystem"]["gcp_project_number"]
    secret_id = config["sources"]["hydrovu"]["gcp_secret"]

    client = secretmanager.SecretManagerServiceClient()
    name = client.secret_version_path(project_number, secret_id, "latest")
    payload = client.access_secret_version(name=name).payload.data

    if not payload:
        raise RuntimeError(f"secret {secret_id} resolved but is empty")
    # Never print the value — only that it exists and is plausibly shaped.
    print(f"   secret '{secret_id}' accessible ({len(payload)} bytes)")
    return secret_id


def check_frost() -> str:
    """Mint an ID token and GET the service document."""
    import frost_sta_client as fsc

    url = service_root_url()
    if not os.environ.get(ENV_FROST_URL):
        print(f"   note: {ENV_FROST_URL} unset — using the .dlt/config.toml default")

    service = fsc.SensorThingsService(url)
    authenticated = attach_id_token_auth(service, url)
    if not authenticated:
        raise RuntimeError(
            f"{url} is a local address, so no token was attached and this check "
            f"proves nothing about production. Set {ENV_FROST_URL} to the deployed "
            f"FROST URL to verify the Cloud Run path."
        )

    # execute() raises on a non-2xx itself, so a 403 from a missing run.invoker
    # binding surfaces here as an HTTPError rather than a silent empty result.
    response = service.execute("GET", url, timeout=30)
    entities = [e.get("name") for e in response.json().get("value", [])]
    if "Things" not in entities:
        raise RuntimeError(f"unexpected service document at {url}: {entities}")

    print(f"   {url} returned {response.status_code} with {len(entities)} entity sets")
    return url


def main() -> int:
    print("Verifying the Dagster+ service account key against real GCP.")
    print("This is the same path Dagster+ Serverless runs.")

    check = Check()
    adc_ok = check.run("1. ADC bootstrap from the env var", check_adc)

    if adc_ok is None:
        print("\nADC failed — the remaining layers would all fail for the same reason.")
        print("FAILED: 1. ADC bootstrap from the env var")
        return 1

    check.run("2. GCS  — write / read / delete a probe object", check_gcs)
    check.run("3. Secret Manager — access the HydroVu secret", check_secret_manager)
    check.run("4. FROST — ID token against Cloud Run", check_frost)

    print()
    if check.failures:
        print(f"FAILED: {', '.join(check.failures)}")
        print("Re-run with VERBOSE=1 for tracebacks.")
        return 1

    print("All layers passed. This key is ready to paste into Dagster+.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
