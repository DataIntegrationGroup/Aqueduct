"""
defs/assets/_gcs.py

Shared GCS helpers for all source transform and load assets.
Source-agnostic — no knowledge of HydroVu, CABQ, or any specific dataset.
"""

import json
import logging
import os

import gcsfs
import toml

logger = logging.getLogger(__name__)


def _gcs_bucket_url() -> str:
    config_path = os.path.join(os.getcwd(), ".dlt", "config.toml")
    return toml.load(config_path)["destination"]["filesystem"]["bucket_url"]


def _gcs_filesystem(project: str = "") -> gcsfs.GCSFileSystem:
    if project:
        return gcsfs.GCSFileSystem(project=project, token="google_default")
    return gcsfs.GCSFileSystem(token="google_default")


def read_transform_watermark(
    fs: gcsfs.GCSFileSystem, bucket: str, watermark_path: str
) -> float | None:
    """Returns the last processed load_id, or None if no watermark exists yet."""
    wm_path = f"{bucket}/{watermark_path}"
    try:
        with fs.open(wm_path) as f:
            return json.load(f).get("last_load_id")
    except FileNotFoundError:
        return None


def write_transform_watermark(
    fs: gcsfs.GCSFileSystem, bucket: str, watermark_path: str, load_id: float
) -> None:
    wm_path = f"{bucket}/{watermark_path}"
    with fs.open(wm_path, "w") as f:
        json.dump({"last_load_id": load_id}, f)
    logger.info("Transform watermark updated: last_load_id=%s", load_id)


def commit_watermark(watermark_path: str, max_load_id: float) -> None:
    """Write the transform watermark. Called by the load step after FROST confirms success."""
    bucket_url = _gcs_bucket_url()
    fs = _gcs_filesystem()
    write_transform_watermark(fs, bucket_url.replace("gs://", ""), watermark_path, max_load_id)
