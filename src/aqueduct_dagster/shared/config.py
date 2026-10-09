"""
Locates and loads `.dlt/config.toml` without depending on the working directory.

Why: reading it as `cwd/.dlt/config.toml` breaks on Dagster+ Serverless — a
PEX whose cwd isn't the repo root, and whose wheel (packaging only
src/aqueduct_dagster) never shipped the root-level file.

Fix: pyproject.toml force-includes the file into the wheel at
aqueduct_dagster/.dlt/config.toml (kept as `.dlt` since that's what dlt itself
looks for under DLT_PROJECT_DIR). settings_dir() finds it relative to this
module, not cwd, and exports DLT_PROJECT_DIR so dlt agrees with us.

One config file per environment, no merging; local editable installs still
resolve to the repo-root copy.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import toml

logger = logging.getLogger(__name__)

#: dlt's override for its run dir. The settings folder is `.dlt` beneath it.
ENV_DLT_PROJECT_DIR = "DLT_PROJECT_DIR"

_SETTINGS_DIRNAME = ".dlt"
_CONFIG_FILENAME = "config.toml"

# src/aqueduct_dagster/ — where the wheel's force-included copy lands.
_PACKAGE_DIR = Path(__file__).resolve().parent.parent


def _candidate_dirs() -> list[Path]:
    """Directories that may contain a `.dlt/` settings folder, priority order:
    1. DLT_PROJECT_DIR — explicit operator override, always wins.
    2. Package directory — the wheel/PEX copy (Dagster+ Serverless).
    3. Working directory — editable-install / `dagster dev`, preserving
       today's local behavior."""
    dirs = []
    override = os.environ.get(ENV_DLT_PROJECT_DIR)
    if override:
        dirs.append(Path(override))
    dirs.append(_PACKAGE_DIR)
    dirs.append(Path.cwd())
    return dirs


def settings_dir() -> Path:
    """Returns the dir containing `.dlt/config.toml`, exporting DLT_PROJECT_DIR
    to match if unset — dlt resolves its own `dlt.config.value` defaults
    through its own provider chain, so it must be pointed at the same file.

    Raises FileNotFoundError naming every path tried, rather than surfacing
    later as a confusing missing-config error inside dlt."""
    tried = []
    for d in _candidate_dirs():
        candidate = d / _SETTINGS_DIRNAME / _CONFIG_FILENAME
        tried.append(str(candidate))
        if candidate.is_file():
            if not os.environ.get(ENV_DLT_PROJECT_DIR):
                os.environ[ENV_DLT_PROJECT_DIR] = str(d)
                logger.debug("%s set to %s", ENV_DLT_PROJECT_DIR, d)
            return d

    raise FileNotFoundError(
        f"Could not locate {_SETTINGS_DIRNAME}/{_CONFIG_FILENAME}. Tried: "
        + "; ".join(tried)
        + f". Set {ENV_DLT_PROJECT_DIR} to the directory containing {_SETTINGS_DIRNAME}/."
    )


def config_path() -> Path:
    """Full path to the resolved `.dlt/config.toml`."""
    return settings_dir() / _SETTINGS_DIRNAME / _CONFIG_FILENAME


def load_config() -> dict[str, Any]:
    """Parses `.dlt/config.toml` and returns it whole — callers index in
    themselves, so a missing key raises a KeyError that names it, clearer
    than a wrapper's generic message."""
    return toml.load(config_path())
