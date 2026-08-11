"""
tests/shared/test_gcp_auth.py

Unit tests for the ADC bootstrap in shared/gcp_auth.py.
Entirely offline — no GCP client is ever constructed, only environment and
filesystem state is asserted.

The security-relevant test here is test_error_never_leaks_payload: these errors
land in Dagster run logs, which are not a secret store, so a malformed key must
never be echoed back.
"""

from __future__ import annotations

import base64
import json
import os
import stat
from pathlib import Path
from typing import Any

import pytest

from aqueduct_dagster.shared import gcp_auth
from aqueduct_dagster.shared.gcp_auth import (
    ENV_ADC_PATH,
    ENV_KEY_B64,
    AdcBootstrapError,
    ensure_adc,
)


def _key(**overrides: Any) -> dict[str, Any]:
    key = {
        "type": "service_account",
        "project_id": "waterdatainitiative-271000",
        "client_email": "aqueduct-dlt-writer@waterdatainitiative-271000.iam.gserviceaccount.com",
        "private_key": "-----BEGIN PRIVATE KEY-----\nFAKEKEYMATERIAL\n-----END PRIVATE KEY-----\n",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    key.update(overrides)
    return key


def _b64(payload: Any) -> str:
    raw = payload if isinstance(payload, str) else json.dumps(payload)
    return base64.b64encode(raw.encode()).decode()


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """
    ensure_adc() caches success in a module-level flag so the three call sites can
    call it freely. Reset it between tests, and clear both env vars so a developer's
    real ADC setup can't influence the result.
    """
    monkeypatch.setattr(gcp_auth, "_bootstrapped", False)
    monkeypatch.delenv(ENV_KEY_B64, raising=False)
    monkeypatch.delenv(ENV_ADC_PATH, raising=False)


class TestNoOpCases:
    def test_no_op_when_key_env_unset(self):
        """Local dev and the test suite must be unaffected — ambient ADC is left to work."""
        ensure_adc()
        assert ENV_ADC_PATH not in os.environ

    def test_does_not_clobber_existing_credentials(self, monkeypatch, tmp_path):
        """An already-working ADC path wins, so `gcloud auth application-default login` survives."""
        existing = tmp_path / "adc.json"
        existing.write_text("{}")
        monkeypatch.setenv(ENV_ADC_PATH, str(existing))
        monkeypatch.setenv(ENV_KEY_B64, _b64(_key()))

        ensure_adc()

        assert os.environ[ENV_ADC_PATH] == str(existing)
        assert existing.read_text() == "{}"

    def test_replaces_stale_credentials_path(self, monkeypatch, tmp_path):
        """A path pointing at a file that no longer exists is not usable ADC — override it."""
        monkeypatch.setenv(ENV_ADC_PATH, str(tmp_path / "does-not-exist.json"))
        monkeypatch.setenv(ENV_KEY_B64, _b64(_key()))

        ensure_adc()

        written = os.environ[ENV_ADC_PATH]
        assert written != str(tmp_path / "does-not-exist.json")
        assert json.loads(Path(written).read_text())["type"] == "service_account"


class TestSuccessfulBootstrap:
    def test_writes_key_and_points_env_at_it(self, monkeypatch):
        key = _key()
        monkeypatch.setenv(ENV_KEY_B64, _b64(key))

        ensure_adc()

        path = os.environ[ENV_ADC_PATH]
        assert os.path.isfile(path)
        assert json.loads(Path(path).read_text()) == key

    def test_key_file_is_private(self, monkeypatch):
        """0600 — the key must never be briefly readable by other users on the host."""
        monkeypatch.setenv(ENV_KEY_B64, _b64(_key()))

        ensure_adc()

        mode = stat.S_IMODE(os.stat(os.environ[ENV_ADC_PATH]).st_mode)
        assert mode == 0o600, f"expected 0600, got {oct(mode)}"

    def test_tolerates_wrapped_base64(self, monkeypatch):
        """`base64` without -w0 wraps at 76 columns; copy-paste adds newlines too."""
        blob = _b64(_key())
        wrapped = "\n".join(blob[i : i + 76] for i in range(0, len(blob), 76))
        monkeypatch.setenv(ENV_KEY_B64, wrapped)

        ensure_adc()

        assert json.loads(Path(os.environ[ENV_ADC_PATH]).read_text()) == _key()

    def test_idempotent_across_calls(self, monkeypatch):
        """All three call sites invoke this; it must not write a new key file each time."""
        monkeypatch.setenv(ENV_KEY_B64, _b64(_key()))

        ensure_adc()
        first = os.environ[ENV_ADC_PATH]
        ensure_adc()
        ensure_adc()

        assert os.environ[ENV_ADC_PATH] == first


class TestRejectsBadInput:
    def test_rejects_invalid_base64(self, monkeypatch):
        monkeypatch.setenv(ENV_KEY_B64, "not valid base64 !!!")
        with pytest.raises(AdcBootstrapError, match="not valid base64"):
            ensure_adc()

    def test_rejects_non_json(self, monkeypatch):
        monkeypatch.setenv(ENV_KEY_B64, _b64("this is plain text, not json"))
        with pytest.raises(AdcBootstrapError, match="is not JSON"):
            ensure_adc()

    def test_rejects_json_that_is_not_an_object(self, monkeypatch):
        monkeypatch.setenv(ENV_KEY_B64, _b64("[1, 2, 3]"))
        with pytest.raises(AdcBootstrapError, match="expected a JSON object"):
            ensure_adc()

    def test_rejects_wrong_credential_type(self, monkeypatch):
        """An authorized_user key (what `gcloud auth` writes) is not a service account key."""
        monkeypatch.setenv(ENV_KEY_B64, _b64(_key(type="authorized_user")))
        with pytest.raises(AdcBootstrapError, match="not a service account key"):
            ensure_adc()

    def test_rejects_truncated_key(self, monkeypatch):
        truncated = _key()
        del truncated["private_key"]
        monkeypatch.setenv(ENV_KEY_B64, _b64(truncated))
        with pytest.raises(AdcBootstrapError, match="missing required field"):
            ensure_adc()

    @pytest.mark.parametrize("field", ["client_email", "private_key", "project_id", "token_uri"])
    def test_rejects_key_missing_any_google_auth_required_field(self, monkeypatch, field):
        """
        google-auth needs client_email, token_uri, and private_key; omitting any one
        makes it raise "Service account info was not in the expected format" from deep
        inside a client library, naming no environment variable. Catching it here is
        the entire point of validating, so every field is covered — token_uri
        especially, since it is easy to leave out of a hand-assembled key.
        """
        partial = _key()
        del partial[field]
        monkeypatch.setenv(ENV_KEY_B64, _b64(partial))
        with pytest.raises(AdcBootstrapError, match=f"missing required field.*{field}"):
            ensure_adc()

    def test_leaves_env_unset_on_failure(self, monkeypatch):
        monkeypatch.setenv(ENV_KEY_B64, _b64("garbage"))
        with pytest.raises(AdcBootstrapError):
            ensure_adc()
        assert ENV_ADC_PATH not in os.environ

    def test_error_never_leaks_payload(self, monkeypatch):
        """Errors surface in Dagster run logs — no key material may appear in them."""
        sentinel = "SUPER-SECRET-KEY-MATERIAL"
        blob = _b64(_key(type="authorized_user", private_key=sentinel))
        monkeypatch.setenv(ENV_KEY_B64, blob)

        with pytest.raises(AdcBootstrapError) as exc_info:
            ensure_adc()

        message = str(exc_info.value)
        assert sentinel not in message
        assert blob not in message
        assert ENV_KEY_B64 in message, "the message should still name the variable to fix"

    def test_success_log_omits_key_material(self, monkeypatch, caplog):
        sentinel = "SUPER-SECRET-KEY-MATERIAL"
        monkeypatch.setenv(ENV_KEY_B64, _b64(_key(private_key=sentinel)))

        with caplog.at_level("DEBUG", logger="aqueduct_dagster.shared.gcp_auth"):
            ensure_adc()

        assert sentinel not in caplog.text
        # Identity is useful and safe — it's how you confirm which SA a run used.
        assert "aqueduct-dlt-writer@" in caplog.text
