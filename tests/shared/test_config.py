"""
tests/shared/test_config.py

Unit tests for the cwd-independent config resolution in shared/config.py.

The behavior that matters: resolution must not depend on the working directory,
because Dagster+ Serverless runs from a PEX whose cwd is not the repo root. Every
test writes its own .dlt/config.toml into tmp_path and patches _PACKAGE_DIR, so the
real repo checkout can never make a test pass by accident.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from aqueduct_dagster.shared import config as config_mod
from aqueduct_dagster.shared.config import (
    ENV_DLT_PROJECT_DIR,
    config_path,
    load_config,
    settings_dir,
)

_SAMPLE = """
[destination.filesystem]
gcp_project_number = "95715287188"
bucket_url = "gs://test-bucket"
"""


def _make_settings(root: Path, body: str = _SAMPLE) -> Path:
    """Creates <root>/.dlt/config.toml and returns root."""
    d = root / ".dlt"
    d.mkdir(parents=True, exist_ok=True)
    (d / "config.toml").write_text(body)
    return root


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    """
    Clear the override and point _PACKAGE_DIR at an empty dir, so nothing resolves
    unless a test sets it up explicitly.
    """
    monkeypatch.delenv(ENV_DLT_PROJECT_DIR, raising=False)
    empty = tmp_path / "empty-package"
    empty.mkdir()
    monkeypatch.setattr(config_mod, "_PACKAGE_DIR", empty)


class TestResolutionOrder:
    def test_env_override_wins(self, monkeypatch, tmp_path):
        override = _make_settings(tmp_path / "override")
        _make_settings(tmp_path / "cwd")
        monkeypatch.chdir(tmp_path / "cwd")
        monkeypatch.setenv(ENV_DLT_PROJECT_DIR, str(override))

        assert settings_dir() == override

    def test_falls_back_to_packaged_copy(self, monkeypatch, tmp_path):
        """The Dagster+ Serverless case: the wheel's copy is found without help from cwd."""
        packaged = _make_settings(tmp_path / "site-packages" / "aqueduct_dagster")
        monkeypatch.setattr(config_mod, "_PACKAGE_DIR", packaged)
        # cwd deliberately has no .dlt/ at all.
        monkeypatch.chdir(tmp_path)

        assert settings_dir() == packaged

    def test_packaged_copy_beats_cwd(self, monkeypatch, tmp_path):
        packaged = _make_settings(tmp_path / "packaged")
        _make_settings(tmp_path / "cwd")
        monkeypatch.setattr(config_mod, "_PACKAGE_DIR", packaged)
        monkeypatch.chdir(tmp_path / "cwd")

        assert settings_dir() == packaged

    def test_falls_back_to_cwd(self, monkeypatch, tmp_path):
        """The editable-install / `dagster dev` case — today's local behavior, preserved."""
        cwd = _make_settings(tmp_path / "repo-root")
        monkeypatch.chdir(cwd)

        assert settings_dir() == cwd


class TestDltProjectDirExport:
    def test_exports_env_for_dlt(self, monkeypatch, tmp_path):
        """
        dlt resolves the [sources.pvacd_hydrovu] block through its own provider chain, so
        it has to be pointed at the same file we resolved.
        """
        root = _make_settings(tmp_path / "repo")
        monkeypatch.chdir(root)

        settings_dir()

        assert os.environ[ENV_DLT_PROJECT_DIR] == str(root)

    def test_does_not_override_an_explicit_setting(self, monkeypatch, tmp_path):
        override = _make_settings(tmp_path / "explicit")
        monkeypatch.setenv(ENV_DLT_PROJECT_DIR, str(override))

        settings_dir()

        assert os.environ[ENV_DLT_PROJECT_DIR] == str(override)


class TestFailure:
    def test_raises_listing_every_path_tried(self, monkeypatch, tmp_path):
        """A wrong answer here shows up much later inside dlt, so the error must be actionable."""
        monkeypatch.chdir(tmp_path)

        with pytest.raises(FileNotFoundError) as exc_info:
            settings_dir()

        message = str(exc_info.value)
        assert str(tmp_path) in message
        assert ENV_DLT_PROJECT_DIR in message


class TestLoading:
    def test_load_config_parses_the_resolved_file(self, monkeypatch, tmp_path):
        root = _make_settings(tmp_path / "repo")
        monkeypatch.chdir(root)

        assert load_config()["destination"]["filesystem"]["bucket_url"] == "gs://test-bucket"

    def test_config_path_points_at_the_file(self, monkeypatch, tmp_path):
        root = _make_settings(tmp_path / "repo")
        monkeypatch.chdir(root)

        assert config_path() == root / ".dlt" / "config.toml"
        assert config_path().is_file()

    def test_missing_key_raises_keyerror_naming_it(self, monkeypatch, tmp_path):
        """load_config() returns the document whole so callers get a precise KeyError."""
        root = _make_settings(tmp_path / "repo", body="[destination.filesystem]\n")
        monkeypatch.chdir(root)

        with pytest.raises(KeyError, match="bucket_url"):
            _ = load_config()["destination"]["filesystem"]["bucket_url"]
