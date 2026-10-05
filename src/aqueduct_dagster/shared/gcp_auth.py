"""
Bootstraps Application Default Credentials (ADC) from an environment variable.

Every GCP client here asks for ADC via google.auth.default(), which works
locally via `gcloud auth application-default login` but has nothing to
discover on Dagster+ Serverless (no metadata server, no mountable file).
Dagster's answer is a base64-encoded service account key env var, decoded
once here at the ADC layer, so the bucket, Secret Manager, and dlt clients
all pick it up without each needing their own bootstrap.

GOOGLE_APPLICATION_CREDENTIALS must be a *path*, not the credentials themselves.
"""

from __future__ import annotations

import atexit
import base64
import binascii
import json
import logging
import os
import tempfile
from typing import Any

logger = logging.getLogger(__name__)

#: Environment variable holding the base64-encoded service account key JSON.
#: Set in Dagster+ under Deployment → Environment variables, scoped to both the
#: full deployment and branch deployments. Never committed.
ENV_KEY_B64 = "GCP_SERVICE_ACCOUNT_KEY_B64"

#: The variable Google's auth libraries read. A filesystem path, never a payload.
ENV_ADC_PATH = "GOOGLE_APPLICATION_CREDENTIALS"

#: Fields a usable key must carry, so a truncated/wrong-type key fails here
#: with a clear message instead of google-auth's unhelpful generic error.
#: project_id isn't required by google-auth, but every real key has one and
#: the success log reports it.
_REQUIRED_FIELDS = ("client_email", "private_key", "project_id", "token_uri")

# Set once the current process has a usable ADC path, so the three call sites can
# each call ensure_adc() freely without racing to write duplicate key files.
_bootstrapped = False


class AdcBootstrapError(RuntimeError):
    """Raised when ENV_KEY_B64 is set but does not contain a usable key."""


def _decode_key(raw: str) -> tuple[bytes, dict[str, Any]]:
    """Decodes and validates the base64 key payload; returns (decoded bytes for
    disk, parsed dict for validation/logging). Never includes the payload in an
    error message — these surface in Dagster run logs, not a secret store."""
    try:
        # validate=False so whitespace and newlines introduced by copy-paste or by
        # `base64` line-wrapping are tolerated rather than rejected.
        decoded = base64.b64decode(raw, validate=False)
    except (binascii.Error, ValueError) as exc:
        raise AdcBootstrapError(
            f"{ENV_KEY_B64} is not valid base64. Re-encode the service account "
            f"key JSON with: base64 < key.json"
        ) from exc

    try:
        key = json.loads(decoded)
    except json.JSONDecodeError as exc:
        raise AdcBootstrapError(
            f"{ENV_KEY_B64} decoded successfully but is not JSON. It must be the "
            f"full service account key file, base64-encoded."
        ) from exc

    if not isinstance(key, dict):
        raise AdcBootstrapError(
            f"{ENV_KEY_B64} decoded to {type(key).__name__}, expected a JSON object."
        )

    if key.get("type") != "service_account":
        raise AdcBootstrapError(
            f"{ENV_KEY_B64} is not a service account key "
            f"(expected type='service_account', got type={key.get('type')!r})."
        )

    missing = [f for f in _REQUIRED_FIELDS if not key.get(f)]
    if missing:
        raise AdcBootstrapError(
            f"{ENV_KEY_B64} is missing required field(s): {', '.join(missing)}. "
            f"The key file may be truncated."
        )

    return decoded, key


def _write_key_file(decoded: bytes) -> str:
    """Writes the key to a 0600 private temp file (outside the repo, so a stray
    `git add` can't catch it) and returns its path. mkstemp + explicit chmod
    means it's never briefly world-readable via the ambient umask."""
    fd, path = tempfile.mkstemp(prefix="aqueduct-adc-", suffix=".json")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(decoded)
    except BaseException:
        os.unlink(path)
        raise

    os.chmod(path, 0o600)

    # Best-effort cleanup. Dagster+ tears the container down after a run anyway,
    # but a long-lived `dagster dev` process should not leave keys behind on exit.
    atexit.register(_remove_quietly, path)
    return path


def _remove_quietly(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


def ensure_adc() -> None:
    """Makes ADC available to this process if it isn't already — idempotent,
    safe to call anywhere, before constructing any GCP client. Existing ADC
    (local gcloud login, or GOOGLE_APPLICATION_CREDENTIALS already set) is left
    untouched; otherwise decodes ENV_KEY_B64 into a temp file.

    Raises AdcBootstrapError if ENV_KEY_B64 is set but unusable."""
    global _bootstrapped

    if _bootstrapped:
        return

    existing = os.environ.get(ENV_ADC_PATH)
    if existing and os.path.isfile(existing):
        logger.debug("%s already points at %s — leaving ADC as-is.", ENV_ADC_PATH, existing)
        _bootstrapped = True
        return

    raw = os.environ.get(ENV_KEY_B64)
    if not raw:
        logger.debug(
            "%s not set and no %s file present — relying on ambient ADC "
            "(gcloud application-default login, or a metadata server).",
            ENV_KEY_B64,
            ENV_ADC_PATH,
        )
        return

    decoded, key = _decode_key(raw)
    path = _write_key_file(decoded)
    os.environ[ENV_ADC_PATH] = path
    _bootstrapped = True

    # Identity only — never the key, and never the full JSON.
    logger.info(
        "ADC bootstrapped from %s: service account %s (project %s).",
        ENV_KEY_B64,
        key["client_email"],
        key["project_id"],
    )
