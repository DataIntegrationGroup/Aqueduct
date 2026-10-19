"""
NM Water Data Catalog (CKAN) client, shared by every agency that publishes
there. Agency-agnostic: each agency's folder supplies its dataset UUID,
locations resource name, dlt resources and dlt state.

The per-resource cursor is a file fingerprint, not a timestamp: CKAN serves
whole files with no server-side time filter, and agencies correct old values
in place. An unchanged file is not downloaded. A changed file lands again in
full as a new snapshot. Every cell lands as a string, so the raw zone reproduces
exactly what the agency published (the adapter handles parsing).

Every request must carry the x-cf-bypass header, or Cloudflare answers 403.
Design and evidence: docs/sources/ckan_water_data_catalog.md.
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
from collections.abc import Iterator
from typing import Any

import httpx
from google.cloud import secretmanager

from aqueduct_dagster.shared.config import load_config
from aqueduct_dagster.shared.gcp_auth import ensure_adc
from aqueduct_dagster.shared.http import (
    DEFAULT_MAX_RETRIES as _MAX_RETRIES,
)
from aqueduct_dagster.shared.http import (
    TRANSIENT_HTTP_ERRORS as _TRANSIENT_ERRORS,
)
from aqueduct_dagster.shared.http import (
    build_unauthenticated_client,
    retry_transient,
)

logger = logging.getLogger(__name__)

CF_BYPASS_HEADER = "x-cf-bypass"
# package_show is ~390 KB (mostly spatial_full GeoJSON); the largest EBWPC file is ~2.5 MB.
_CKAN_TIMEOUT = httpx.Timeout(connect=30.0, read=120.0, write=30.0, pool=30.0)
_FINGERPRINT_KEYS = ("last_modified", "size", "url")


class CkanApiError(RuntimeError):
    """CKAN answered 2xx but with success: false."""


def resolve_cf_bypass(cf_bypass: str, gcp_secret: str) -> str:
    """Returns the x-cf-bypass header value, fetched from Secret Manager unless
    already supplied (tests/local overrides). The payload is the bare value."""
    if cf_bypass:
        return cf_bypass

    project_number = load_config()["destination"]["filesystem"]["gcp_project_number"]

    # Secret Manager authenticates via ADC, so the bootstrap must run first. Idempotent.
    ensure_adc()
    sm_client = secretmanager.SecretManagerServiceClient()
    name = sm_client.secret_version_path(project_number, gcp_secret, "latest")
    response = sm_client.access_secret_version(name=name)
    value: str = response.payload.data.decode("UTF-8").strip()
    return value


def build_ckan_client(api_base_url: str, cf_bypass: str) -> httpx.Client:
    """Unauthenticated client plus the Cloudflare bypass header. Redirects are
    not followed (httpx default), so the header can't follow one off-site."""
    client = build_unauthenticated_client(api_base_url, timeout=_CKAN_TIMEOUT)
    client.headers[CF_BYPASS_HEADER] = cf_bypass
    return client


def package_show(client: httpx.Client, dataset_id: str) -> dict[str, Any]:
    """Returns the dataset with its full resource list. Raises httpx.HTTPError
    (a Cloudflare 403 included) or CkanApiError."""
    resp = retry_transient(
        lambda: client.get("/api/3/action/package_show", params={"id": dataset_id}),
        on_retry=lambda exc, attempt, delay: logger.warning(
            "package_show %s: transient error (%s) on attempt %d — retrying in %.0fs",
            dataset_id,
            exc,
            attempt,
            delay,
        ),
    )
    resp.raise_for_status()
    body = resp.json()
    if not body.get("success"):
        raise CkanApiError(f"package_show {dataset_id} returned success=false: {body.get('error')}")
    result: dict[str, Any] = body["result"]
    logger.info("package_show %s: %d resources", dataset_id, len(result.get("resources", [])))
    return result


def normalise_format(fmt: str | None) -> str:
    """CKAN's format is free text (sometimes CSV, sometimes .csv)."""
    return (fmt or "").strip().lstrip(".").lower()


def resource_label(resource: dict) -> str:
    """The resource's name, falling back to its id (name is optional in CKAN)."""
    return str(resource.get("name") or resource["id"])


def split_resources(
    resources: list[dict], locations_resource_name: str | None
) -> tuple[dict | None, list[dict], dict[str, str]]:
    """Returns (locations resource or None, measurement resources, skipped
    {name: reason}). Selected by name and normalized format.
    Non-CSV resources are skipped."""
    wanted = (locations_resource_name or "").strip().casefold()
    locations: dict | None = None
    measurements: list[dict] = []
    skipped: dict[str, str] = {}
    for resource in resources:
        label = resource_label(resource)
        if normalise_format(resource.get("format")) != "csv":
            skipped[label] = f"format {resource.get('format') or '(none)'}: only CSV is ingested"
            continue
        if wanted and label.strip().casefold() == wanted:
            locations = resource
        else:
            measurements.append(resource)
    return locations, measurements, skipped


def download_resource(client: httpx.Client, resource: dict) -> tuple[bytes | None, str | None]:
    """Downloads resource["url"], which is always taken fresh from package_show.
    Refuses any URL not on the catalog's own scheme and host.
    Returns (body, None) on success or (None, reason) on any failure."""
    label = resource_label(resource)
    url = resource.get("url") or ""
    try:
        target = httpx.URL(url)
    except httpx.InvalidURL as exc:
        return None, f"invalid URL {url!r}: {exc}"
    base = client.base_url
    if (target.scheme, target.host) != (base.scheme, base.host):
        logger.warning(
            "Resource %s: URL %s is not on the catalog host — not downloaded", label, url
        )
        return None, f"URL host {target.host!r} is not the catalog host {base.host!r}"

    try:
        resp = retry_transient(
            lambda: client.get(target, headers={"Accept": "*/*"}),
            on_retry=lambda exc, attempt, delay: logger.warning(
                "Resource %s: transient error (%s) on attempt %d — retrying in %.0fs",
                label,
                exc,
                attempt,
                delay,
            ),
        )
    except _TRANSIENT_ERRORS as exc:
        logger.warning(
            "Resource %s: transient error after %d attempts — skipping", label, _MAX_RETRIES
        )
        return None, f"transient network error after {_MAX_RETRIES} attempts: {exc}"

    if not resp.is_success:
        logger.warning("Resource %s: HTTP %s — skipping", label, resp.status_code)
        return None, f"HTTP {resp.status_code}"
    return resp.content, None


def parse_csv(body: bytes, label: str = "CSV") -> list[dict[str, str]]:
    """Every cell as a string, keyed by its header. A blank or missing header
    becomes column_<n> (1-based position), so EBWPC's empty trailing header
    cells and the stray values beyond them land instead of colliding on one
    "" key. Empty lines are skipped."""
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError:
        logger.warning("%s is not valid UTF-8 — decoding as latin-1", label)
        text = body.decode("latin-1")

    reader = csv.reader(io.StringIO(text, newline=""))
    header = [name.strip() for name in next(reader, [])]
    rows: list[dict[str, str]] = []
    for cells in reader:
        if not cells:
            continue
        rows.append(
            {
                (header[i] if i < len(header) and header[i] else f"column_{i + 1}"): value
                for i, value in enumerate(cells)
            }
        )
    return rows


def snapshot_rows(resource: dict, body: bytes) -> list[dict[str, Any]]:
    """Parsed rows of one file, each with provenance columns naming the exact
    resource version (content_sha256) and its row_number (1-based, header
    excluded). Raises csv.Error on a file the csv module can't read."""
    provenance = {
        "resource_id": resource["id"],
        "resource_name": resource_label(resource),
        "resource_last_modified": resource.get("last_modified"),
        "source_url": resource.get("url"),
        "content_sha256": hashlib.sha256(body).hexdigest(),
    }
    return [
        {**cells, **provenance, "row_number": row_number}
        for row_number, cells in enumerate(parse_csv(body, resource_label(resource)), start=1)
    ]


def iter_resource_snapshots(
    client: httpx.Client,
    resources: list[dict],
    fingerprints: dict[str, dict],
    stats: dict,
) -> Iterator[dict]:
    """Yields every row of each resource whose file changed since the last run.

    fingerprints: caller's {resource_id: {last_modified, size, url, sha256,
      name}} from dlt state, mutated in place.
    stats: populated at generator end: rows_yielded, resources_selected/
      landed/unchanged/same_content/errored, failed_resources,
      resources_added (no fingerprint yet), resources_removed.

    reading_id is resource_id + content hash + row_number, unique across
    snapshots, since a changed file lands again in full.

    One failing resource never stops the others. Errors are counted and logged."""
    landed = 0
    unchanged = 0
    same_content = 0
    errored = 0
    rows_yielded = 0
    failed: list[str] = []
    added: list[str] = []

    current_ids = {resource["id"] for resource in resources}
    removed_ids = [rid for rid in fingerprints if rid not in current_ids]
    removed = [str(fingerprints[rid].get("name") or rid) for rid in removed_ids]
    for rid in removed_ids:
        del fingerprints[rid]
    if removed:
        logger.warning(
            "Resource(s) no longer in the dataset: %s — check whether a replacement was added",
            removed,
        )

    for resource in resources:
        rid = resource["id"]
        label = resource_label(resource)
        fingerprint = {key: resource.get(key) for key in _FINGERPRINT_KEYS}
        previous = fingerprints.get(rid)

        if previous is None:
            added.append(label)
        elif all(previous.get(key) == value for key, value in fingerprint.items()):
            unchanged += 1
            continue

        body, err = download_resource(client, resource)
        if err is not None or body is None:
            logger.warning(
                "Resource %s failed: %s — fingerprint not advanced, will retry next run", label, err
            )
            errored += 1
            failed.append(label)
            continue

        sha256 = hashlib.sha256(body).hexdigest()
        if previous is not None and previous.get("sha256") == sha256:
            logger.info("Resource %s: metadata changed but content is identical — skipped", label)
            fingerprints[rid] = {**fingerprint, "sha256": sha256, "name": label}
            same_content += 1
            continue

        try:
            rows = snapshot_rows(resource, body)
        except csv.Error as exc:
            logger.warning("Resource %s: unreadable CSV (%s) — will retry next run", label, exc)
            errored += 1
            failed.append(label)
            continue

        for row in rows:
            row["reading_id"] = f"{rid}_{sha256[:16]}_{row['row_number']}"
            yield row
        rows_yielded += len(rows)
        landed += 1
        logger.info("Resource %s: landed %d rows (sha256 %s)", label, len(rows), sha256[:16])

        # Advance only after every row is yielded — an abandoned generator leaves it unwritten.
        fingerprints[rid] = {**fingerprint, "sha256": sha256, "name": label}

    logger.info(
        "CKAN snapshot extract complete: %d landed, %d unchanged, %d same content, "
        "%d errored, %d rows yielded",
        landed,
        unchanged,
        same_content,
        errored,
        rows_yielded,
    )
    # Populated at generator end only — if dlt abandons the generator, the
    # asset falls back to stats.get(..., 0) defaults.
    stats["rows_yielded"] = rows_yielded
    stats["resources_selected"] = len(resources)
    stats["resources_landed"] = landed
    stats["resources_unchanged"] = unchanged
    stats["resources_same_content"] = same_content
    stats["resources_errored"] = errored
    stats["failed_resources"] = failed
    stats["resources_added"] = added
    stats["resources_removed"] = removed
