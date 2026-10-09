"""
Unit tests for three places where SOURCE_REGISTRY must agree with something
written independently elsewhere. All three fail silently — nothing raises,
the run just does the wrong thing — so each is worth a test, not just a
comment.

1. Asset/job/schedule names: defs/definitions.py and defs/assets/load.py
   f-string them from the registry `name`; sources/<name>/ hardcodes the same
   names in @asset decorators. A mismatch builds a job selecting assets that
   don't exist, surfacing only when someone launches a run.

2. The dataset string, written three times (SOURCE_REGISTRY, build_pipeline(),
   and the transform's GCS_DATASET) with nothing cross-checking them. If
   registry and transform disagree, the load asset commits the watermark to a
   path the transform never reads, and every run reprocesses from zero.

3. The transform result type: defs/assets/load.py reads .bundles and
   .max_load_id off an Any-typed result. Each source defines its own
   dataclass, so a renamed/missing field type-checks fine and only raises
   AttributeError at the end of a live run.

Offline: build_source_pipeline is patched out, so no GCS/FROST/dlt destination
is touched.
"""

from __future__ import annotations

import dataclasses
import importlib
from typing import get_type_hints
from unittest.mock import patch

import pytest

from aqueduct_dagster.canonical.canonical_model import CanonicalBundle
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
    """GCS_DATASET/WATERMARK_PATH must match what defs/assets/load.py derives
    from the registry — read side vs write side."""
    transform = importlib.import_module(f"aqueduct_dagster.sources.{cfg['name']}.transform")

    assert transform.GCS_DATASET == cfg["dataset"]
    assert transform.WATERMARK_PATH == transform_watermark_path(cfg["dataset"], cfg["name"])


@pytest.mark.parametrize("cfg", SOURCE_REGISTRY, ids=_NAMES)
def test_dlt_pipeline_writes_to_the_registry_dataset(cfg):
    """The third independent copy of the dataset string, and the one that
    decides where parquet actually lands — passed positionally to
    build_source_pipeline()."""
    module = f"aqueduct_dagster.sources.{cfg['name']}.dlt_pipeline"
    dlt_pipeline = importlib.import_module(module)

    with patch(f"{module}.build_source_pipeline") as mock_build:
        dlt_pipeline.build_pipeline()

    _pipeline_name, dataset_name = mock_build.call_args.args
    assert dataset_name == cfg["dataset"]


@pytest.mark.parametrize("name", _NAMES)
def test_transform_result_satisfies_load_contract(name):
    """bundles and max_load_id — the two fields frost_load_{name} reads
    (loaded into FROST; committed as the transform watermark)."""
    transform_fn = defs.resolve_assets_def(f"canonical_bundles_{name}").op.compute_fn.decorated_fn
    result_type = get_type_hints(transform_fn)["return"]

    assert dataclasses.is_dataclass(result_type)
    fields = get_type_hints(result_type)
    assert fields["bundles"] == list[CanonicalBundle]
    assert fields["max_load_id"] == float | None
