"""
shared/gcp_auth.py

Bootstraps Application Default Credentials (ADC) from an environment variable.

Why this exists: every GCP client in this repo asks for ADC and nothing else.
These resolve through `google.auth.default()`.

Locally that works because of `gcloud auth application-default login`. Dagster+
Serverless, however, has no metadata server and no way to mount a credentials
file, so ADC has nothing to discover and all three fail. Dagster's documented
answer for Serverless is a base64-encoded service account key in an environment
variable.  once, at the ADC layer rather than per client.
One bootstrap therefore satisfies the bucket, Secret Manager, and dlt together.

`GOOGLE_APPLICATION_CREDENTIALS` must be a *path*, not the credentials themselves.
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

#: Fields a usable service account key must carry. Checked so a truncated or
#: wrong-type key fails here with a clear message.
#:
#: client_email, token_uri, and private_key are exactly what
#: google.oauth2.service_account.Credentials.from_service_account_info() requires —
#: omit any one and it raises "Service account info was not in the expected format",
#: naming no environment variable and giving no hint where the key came from.
#: project_id is not required by google-auth, but every gcloud-issued key carries it
#: and the success log reports it, so a key without one is malformed enough to reject.
_REQUIRED_FIELDS = ("client_email", "private_key", "project_id", "token_uri")

# Set once the current process has a usable ADC path, so the three call sites can
# each call ensure_adc() freely without racing to write duplicate key files.
_bootstrapped = False


class AdcBootstrapError(RuntimeError):
    """Raised when ENV_KEY_B64 is set but does not contain a usable key."""


def _decode_key(raw: str) -> tuple[bytes, dict[str, Any]]:
    """
    Decodes and validates the base64 key payload.

    Returns the decoded bytes alongside the parsed dict: the bytes are what gets
    written to disk, while the dict is only used for validation and for logging
    the identity.

    Deliberately never includes the payload (or any fragment of it) in an error
    message — these errors surface in Dagster run logs, which are not a secret
    store. The variable name is enough to act on.
    """
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
    """
    Writes the key to a private temp file and returns its path.

    The file is exactly 0600, always. mkstemp opens with O_CREAT|O_EXCL and mode
    0600, so the key is never briefly world-readable; the explicit chmod below pins
    the mode rather than leaving it to the ambient umask. Lives in the system temp
    dir, outside the repo, so it cannot be picked up by a stray `git add`.
    """
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
    """
    Makes ADC available to this process, if it isn't already. Safe to call anywhere.

    Idempotent and cheap after the first call. Call it immediately before
    constructing any GCP client; see the three call sites named in this module's
    docstring.

    Resolution order:
      1. Already bootstrapped in this process → nothing to do.
      2. GOOGLE_APPLICATION_CREDENTIALS already points at a real file → leave it
         alone. This is what keeps `gcloud auth application-default login` and any
         externally-mounted key working untouched.
      3. ENV_KEY_B64 unset → nothing to do. Ambient ADC (local gcloud login, or a
         metadata server) is expected to supply credentials, and letting
         google.auth raise its own error is more useful than pre-empting it here.
      4. Otherwise decode ENV_KEY_B64, write it to a private temp file, and point
         GOOGLE_APPLICATION_CREDENTIALS at that file.

    Raises AdcBootstrapError if ENV_KEY_B64 is set but unusable.
    """
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
