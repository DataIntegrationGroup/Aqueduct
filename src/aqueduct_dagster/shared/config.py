"""
shared/config.py

Locates and loads `.dlt/config.toml` without depending on the working directory.

Why this exists: three call sites used to read the file as
`os.path.join(os.getcwd(), ".dlt", "config.toml")`. That holds for `dagster dev`
launched from the repo root and breaks everywhere else, including likely
Dagster+ Serverless (where the deployed artifact is a PEX built from the wheel and
the working directory is not the repo root). Since `pyproject.toml` packages only
`src/aqueduct_dagster`, the root-level `.dlt/config.toml` was not shipped at all,
so those reads would raise FileNotFoundError before any GCP call was even attempted.

The fix has two halves:
  * `pyproject.toml` force-includes `.dlt/config.toml` into the wheel at
    `aqueduct_dagster/.dlt/config.toml`, so it travels with the code. The `.dlt`
    directory name is preserved deliberately (see below).
  * `settings_dir()` finds that copy relative to this module rather than to the cwd,
    and exports DLT_PROJECT_DIR so dlt's own config resolution agrees with ours.
    dlt treats DLT_PROJECT_DIR as its run dir and looks for a `.dlt` settings
    folder beneath it, which is why the packaged path keeps that name.

There is exactly one config file in play in any given environment; this module does
not merge candidates. Local editable installs resolve to the repo-root copy, so
local behavior is unchanged.
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
    """
    Directories that may contain a `.dlt/` settings folder, highest priority first.

    1. DLT_PROJECT_DIR — an explicit operator override always wins.
    2. The package directory — the wheel/PEX copy. This is the Dagster+ Serverless case.
    3. The working directory — the editable-install / `dagster dev` case, and the
       path that preserves today's local behavior.
    """
    dirs = []
    override = os.environ.get(ENV_DLT_PROJECT_DIR)
    if override:
        dirs.append(Path(override))
    dirs.append(_PACKAGE_DIR)
    dirs.append(Path.cwd())
    return dirs


def settings_dir() -> Path:
    """
    Returns the directory containing `.dlt/config.toml`, and exports DLT_PROJECT_DIR
    to match when it isn't already set.

    Exporting the variable is the point of doing this eagerly: dlt resolves
    `dlt.config.value` defaults (the whole `[sources.hydrovu]` block) through its own
    provider chain, not through this module, so it has to be pointed at the same file.

    Raises FileNotFoundError naming every path tried, since a wrong answer here
    surfaces later as a confusing missing-config error inside dlt.
    """
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
    """
    Parses `.dlt/config.toml` and returns it whole.

    Callers index into the result themselves so a missing key raises a KeyError that
    names it, which is clearer than a wrapper's generic message.
    """
    return toml.load(config_path())
