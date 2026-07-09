"""
pipeline/_pipeline.py

Shared dlt pipeline factory for all sources.
Each source's build_pipeline() calls this with explicit pipeline_name and
dataset_name so both values are visible on one line — copy-paste can't
silently leave a wrong dataset_name buried inside a function body.
"""

import dlt


def build_source_pipeline(pipeline_name: str, dataset_name: str) -> dlt.Pipeline:
    """
    Returns a dlt pipeline writing parquet to the filesystem (GCS) destination.
    Bucket is read from config.toml [destination.filesystem] bucket_url.

    Both args are required so a new source module can't omit either by accident.
    Always call pipeline.run(..., loader_file_format="parquet") at the call site.
    """
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="filesystem",
        dataset_name=dataset_name,
    )
