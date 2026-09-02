"""
tests/defs/test_definitions.py

Guards the two places where SOURCE_REGISTRY's strings have to agree with something
written independently somewhere else. Both failure modes are silent — nothing raises,
the run just does the wrong thing — so they are worth a test rather than a comment.

1. Asset/job/schedule generation. defs/definitions.py and defs/assets/load.py build
   names by f-string from the registry `name`, while sources/<name>/ hard-codes the
   same names in its @asset decorators. A mismatch produces a job whose selection
   names assets that do not exist; it only surfaces when someone launches a run.

2. The dataset string. Each source writes it three times — SOURCE_REGISTRY, the
   source's build_pipeline(), and its transform module's GCS_DATASET — with nothing
   cross-checking them. If the registry and transform disagree, the load asset commits
   the transform watermark to a path the transform never reads, and every run
   reprocesses from zero. shared/gcs.py:transform_watermark_path() unifies the
   filename but not the dataset it sits in.

Offline: no GCS, FROST, or dlt destination is touched — build_source_pipeline is
patched out so build_pipeline() can be inspected without credentials.
"""

from __future__ import annotations

import importlib
from unittest.mock import patch

import pytest

from aqueduct_dagster.defs.definitions import defs
from aqueduct_dagster.shared.gcs import transform_watermark_path
from aqueduct_dagster.shared.source_registry import SOURCE_REGISTRY

_NAMES = [cfg["name"] for cfg in SOURCE_REGISTRY]


def _asset_keys() -> set[str]:
    graph = defs.resolve_asset_graph()
    return {key.to_user_string() for key in graph.get_all_asset_keys()}


@pytest.mark.parametrize("name", _NAMES)
def test_registry_entry_has_its_three_assets(name):
    """raw_{name}_readings → canonical_bundles_{name} → frost_load_{name} all resolve."""
    expected = {
        f"raw_{name}_readings",
        f"canonical_bundles_{name}",
        f"frost_load_{name}",
    }
    assert expected <= _asset_keys()


@pytest.mark.parametrize("name", _NAMES)
def test_registry_entry_has_its_job_and_schedule(name):
    assert f"{name}_pipeline" in {job.name for job in defs.resolve_all_job_defs()}
    assert f"{name}_schedule" in {schedule.name for schedule in defs.schedules}


@pytest.mark.parametrize("cfg", SOURCE_REGISTRY, ids=_NAMES)
def test_transform_module_agrees_with_registry_dataset(cfg):
    """
    The transform's GCS_DATASET and WATERMARK_PATH must match what defs/assets/load.py
    derives from the registry — the read side and the write side of the same file.
    """
    transform = importlib.import_module(f"aqueduct_dagster.sources.{cfg['name']}.transform")

    assert transform.GCS_DATASET == cfg["dataset"]
    assert transform.WATERMARK_PATH == transform_watermark_path(cfg["dataset"], cfg["name"])


@pytest.mark.parametrize("cfg", SOURCE_REGISTRY, ids=_NAMES)
def test_dlt_pipeline_writes_to_the_registry_dataset(cfg):
    """
    build_pipeline() passes dataset_name positionally to build_source_pipeline(); it is
    the third independent copy of the dataset string and the one that decides where
    parquet actually lands.
    """
    module = f"aqueduct_dagster.sources.{cfg['name']}.dlt_pipeline"
    dlt_pipeline = importlib.import_module(module)

    with patch(f"{module}.build_source_pipeline") as mock_build:
        dlt_pipeline.build_pipeline()

    _pipeline_name, dataset_name = mock_build.call_args.args
    assert dataset_name == cfg["dataset"]
