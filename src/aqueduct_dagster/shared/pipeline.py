"""
shared/pipeline.py

Shared dlt pipeline factory for all sources.
Each source's build_pipeline() calls this with explicit pipeline_name and
dataset_name so both values are visible on one line — copy-paste can't
silently leave a wrong dataset_name buried inside a function body.
"""

import dlt
from dlt.destinations import filesystem

from aqueduct_dagster.shared.config import settings_dir
from aqueduct_dagster.shared.gcp_auth import ensure_adc
from aqueduct_dagster.shared.gcs import _gcs_bucket_url


def build_source_pipeline(pipeline_name: str, dataset_name: str) -> dlt.Pipeline:
    """
    Returns a dlt pipeline writing parquet to the filesystem (GCS) destination.

    Bucket is resolved by _gcs_bucket_url(): the GCS_BUCKET_URL env var if set,
    otherwise [destination.filesystem] bucket_url in config.toml

    Both args are required so a new source module can't omit either by accident.
    Always call pipeline.run(..., loader_file_format="parquet") at the call site.

    The two setup calls have to happen before dlt resolves anything: settings_dir()
    exports DLT_PROJECT_DIR so dlt finds config.toml when the cwd isn't the repo
    root, and ensure_adc() supplies the credentials its GCS destination will need.
    Both are idempotent.
    """
    settings_dir()
    ensure_adc()
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=filesystem(bucket_url=_gcs_bucket_url()),
        dataset_name=dataset_name,
    )
