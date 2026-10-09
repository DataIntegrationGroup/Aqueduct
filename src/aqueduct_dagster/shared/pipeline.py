"""
Shared dlt pipeline factory for all sources. Each source's build_pipeline()
calls this with explicit pipeline_name and dataset_name so both values are
visible on one line — copy-paste can't silently leave a wrong dataset_name
buried inside a function body.
"""

import dlt
from dlt.destinations import filesystem

from aqueduct_dagster.shared.config import settings_dir
from aqueduct_dagster.shared.gcp_auth import ensure_adc
from aqueduct_dagster.shared.gcs import _gcs_bucket_url


def build_source_pipeline(pipeline_name: str, dataset_name: str) -> dlt.Pipeline:
    """Returns a dlt pipeline writing parquet to the filesystem (GCS) destination.

    Bucket comes from _gcs_bucket_url() (GCS_BUCKET_URL env var, or config.toml's
    bucket_url). Both args are required so a new source can't omit either by
    accident; always call pipeline.run(..., loader_file_format="parquet").

    settings_dir() and ensure_adc() must run before dlt resolves anything —
    the former exports DLT_PROJECT_DIR so dlt finds config.toml when cwd isn't
    the repo root, the latter supplies GCS credentials. Both are idempotent."""
    settings_dir()
    ensure_adc()
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=filesystem(bucket_url=_gcs_bucket_url()),
        dataset_name=dataset_name,
    )
