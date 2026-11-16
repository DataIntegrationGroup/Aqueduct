"""
dlt source for EBID's OneRain Contrail telemetry API — plain HTTP, XML
responses, system_key carried as a query param, written to GCS.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from xml.etree import ElementTree

import dlt
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
from aqueduct_dagster.shared.http import build_unauthenticated_client, retry_transient
from aqueduct_dagster.shared.pipeline import build_source_pipeline

logger = logging.getLogger(__name__)

_TIMEOUT = httpx.Timeout(connect=30.0, read=60.0, write=30.0, pool=30.0)
_DATA_API_PATH = "/OneRain/DataAPI"  # exact-match, case-sensitive servlet path
_DTW_SENSOR_CLASS = "102"  # see docs/sources/ebid.md — not in the vendor's standard table
_WINDOW_SECONDS = 10 * 24 * 60 * 60  # 10-day windows, matching the retired Airbyte connector
_MAX_ROWS_PER_RESPONSE = 5000  # OneRain truncates silently past this
_NOT_A_WELL = "no DTW sensor (sensor_class=102, 'Well' in location) found"


class EbidApiError(RuntimeError):
    """OneRain answered HTTP 200 with an <error> element."""


def resolve_ebid_system_key(system_key: str, gcp_secret: str) -> str:
    """Fetches system_key from Secret Manager unless already supplied."""
    if system_key:
        return system_key

    project_number = load_config()["destination"]["filesystem"]["gcp_project_number"]
    ensure_adc()
    sm_client = secretmanager.SecretManagerServiceClient()
    name = sm_client.secret_version_path(project_number, gcp_secret, "latest")
    response = sm_client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8").strip()


def build_ebid_client(api_base_url: str, system_key: str) -> httpx.Client:
    """Unauthenticated client with system_key as a default query param.
    api_base_url is the host root; _DATA_API_PATH is joined per call so
    base_url's trailing slash can't turn it into a 404."""
    client = build_unauthenticated_client(api_base_url, timeout=_TIMEOUT)
    client.params = {"system_key": system_key}
    return client


def _parse_onerain_xml(content: bytes) -> list[dict[str, str]]:
    """Parses <onerain><response><general><row>...</row></general></response>
    </onerain> into one dict per row. OneRain answers HTTP 200 even on
    error, so <error> is the only failure signal."""
    root = ElementTree.fromstring(content)
    error = root.find("error")
    if error is not None:
        raise EbidApiError(error.text or "unknown OneRain error")

    general = root.find("response/general")
    if general is None:
        raise EbidApiError("malformed OneRain response: no <general> element")

    rows = []
    for row in general.findall("row"):
        rows.append({child.tag: (child.text or "") for child in row})
    return rows


def _get(client: httpx.Client, params: dict) -> tuple[list[dict[str, str]] | None, str | None]:
    """GETs the OneRain servlet with the given method/params, retried on
    transient network errors. Returns (rows, None) on success, (None, reason)
    otherwise — a bad system_key or parameter is not retryable."""
    try:
        resp = retry_transient(
            lambda: client.get(_DATA_API_PATH, params=params),
            on_retry=lambda exc, attempt, delay: logger.warning(
                "OneRain %s: transient error (%s) on attempt %d — retrying in %.0fs",
                params.get("method"),
                exc,
                attempt,
                delay,
            ),
        )
    except _TRANSIENT_ERRORS as exc:
        return None, f"transient network error after {_MAX_RETRIES} attempts: {exc}"

    # OneRain's own errors come back as HTTP 200 (see _parse_onerain_xml); a
    # real non-2xx means an infra problem in front of it — degrade, don't raise.
    if resp.status_code >= 400:
        logger.warning("OneRain %s: HTTP %s", params.get("method"), resp.status_code)
        return None, f"HTTP {resp.status_code}"

    try:
        return _parse_onerain_xml(resp.content), None
    except (EbidApiError, ElementTree.ParseError) as exc:
        return None, str(exc)


def _fetch_sites(client: httpx.Client) -> tuple[list[dict[str, str]] | None, str | None]:
    """GetSiteMetaData, no or_site_id filter — the key's own Output System
    scoping already restricts this to the sites EBID granted access to."""
    return _get(client, {"method": "GetSiteMetaData"})


def _fetch_dtw_sensor(
    client: httpx.Client, or_site_id: str
) -> tuple[dict[str, str] | None, str | None]:
    """GetSensorMetaData for one site, filtered to the DTW sensor. Discovered
    per site rather than assumed as or_sensor_id=4 — see docs/sources/ebid.md."""
    sensors, err = _get(client, {"method": "GetSensorMetaData", "or_site_id": or_site_id})
    if err is not None:
        return None, err
    for sensor in sensors or []:
        if sensor.get("sensor_class") == _DTW_SENSOR_CLASS and "Well" in sensor.get("location", ""):
            return sensor, None
    return None, _NOT_A_WELL


def _fetch_readings(
    client: httpx.Client,
    or_site_id: str,
    or_sensor_id: str,
    start_time: int,
    end_time: int | None = None,
) -> tuple[list[dict[str, str]] | None, str | None]:
    """GetSensorData for one (site, sensor). No timezone= param, so Contrail
    defaults to UTC. Caller windows start_time/end_time to stay under the
    5000-row cap."""
    params = {
        "method": "GetSensorData",
        "or_site_id": or_site_id,
        "or_sensor_id": or_sensor_id,
        "data_start": datetime.fromtimestamp(start_time, tz=UTC).strftime("%Y-%m-%d %H:%M:%S"),
    }
    if end_time is not None:
        params["data_end"] = datetime.fromtimestamp(end_time, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
    return _get(client, params)


def _iter_site_readings(
    client: httpx.Client,
    start_ts: int,
    sites: list[dict[str, str]],
    cursors: dict[str, int],
    stats: dict,
) -> Iterator[dict]:
    """Yields one flat record per (site, reading) across every site in
    `sites`. A failed site is logged and skipped, never blocking the rest."""
    now_ts = int(datetime.now(tz=UTC).timestamp())
    fetched = errored = skipped = 0
    failed_ids: list[str] = []
    rows_yielded = 0

    for site in sites:
        or_site_id = site["or_site_id"]
        sensor, err = _fetch_dtw_sensor(client, or_site_id)
        if sensor is None:
            if err == _NOT_A_WELL:
                # Not a real failure — some sites in the key's own site list
                # (e.g. river gauges) were never DTW wells. Keeping this out
                # of errored/failed_ids so a real failure isn't lost in the noise.
                skipped += 1
            else:
                logger.warning("Site %s: %s — skipping", or_site_id, err)
                errored += 1
                failed_ids.append(or_site_id)
            continue
        or_sensor_id = sensor["or_sensor_id"]

        window_start = max(cursors.get(or_site_id, 0), start_ts)
        site_failed = False
        while window_start < now_ts:
            window_end = min(window_start + _WINDOW_SECONDS, now_ts)
            rows, err = _fetch_readings(client, or_site_id, or_sensor_id, window_start, window_end)
            if err is not None:
                logger.warning(
                    "Site %s: %s — cursor not advanced past %s, will retry next run",
                    or_site_id,
                    err,
                    window_start,
                )
                errored += 1
                failed_ids.append(or_site_id)
                site_failed = True
                break

            truncated = len(rows or []) >= _MAX_ROWS_PER_RESPONSE
            if truncated:
                logger.warning(
                    "Site %s: window %s-%s hit the %d-row cap — re-fetching the"
                    " remainder in a narrower window",
                    or_site_id,
                    window_start,
                    window_end,
                    _MAX_ROWS_PER_RESPONSE,
                )

            # Sorted only on truncation, where resuming from the max
            # timestamp seen depends on it — otherwise window_end already
            # bounds the next start regardless of row order.
            rows_to_process = (
                sorted(rows or [], key=lambda r: r["data_time"]) if truncated else (rows or [])
            )
            last_ts = window_start
            for row in rows_to_process:
                ts = int(
                    datetime.strptime(row["data_time"], "%Y-%m-%d %H:%M:%S")
                    .replace(tzinfo=UTC)
                    .timestamp()
                )
                last_ts = max(last_ts, ts)
                rows_yielded += 1
                yield {
                    "reading_id": f"{or_site_id}_{or_sensor_id}_{ts}",
                    "or_site_id": or_site_id,
                    "or_sensor_id": or_sensor_id,
                    "timestamp": ts,
                    "value": float(row["data_value"]),
                    "raw_value": float(row["raw_value"]) if row.get("raw_value") else None,
                    "receive_time": row.get("receive_time") or None,
                    "data_quality": row.get("data_quality") or None,
                }

            # Advance past window_end normally; +1 past a row landing exactly
            # on it avoids a dupe if data_end is inclusive. On truncation,
            # resume right after the last row fetched instead of window_end,
            # so the untruncated remainder gets re-fetched narrower, not lost.
            next_start = last_ts + 1 if truncated else max(window_end, last_ts + 1)
            cursors[or_site_id] = next_start
            window_start = next_start

        if not site_failed:
            fetched += 1

    logger.info(
        "ebid_readings extract complete: %d fetched, %d skipped (not a well), %d errored, %d rows yielded",
        fetched,
        skipped,
        errored,
        rows_yielded,
    )
    stats["rows_yielded"] = rows_yielded
    stats["sites_fetched"] = fetched
    stats["sites_skipped"] = skipped
    stats["sites_errored"] = errored
    stats["failed_site_ids"] = failed_ids


@dlt.source(name="ebid")
def ebid_source(
    system_key: str = "",
    gcp_secret: str = dlt.config.value,
    api_base_url: str = dlt.config.value,
    initial_start_date: str = dlt.config.value,
    _stats: dict | None = None,
) -> Any:
    """GetSiteMetaData is fetched once here (shared by both resources);
    everything else is per-site and lives in ebid_readings."""
    start_ts = int(
        datetime.strptime(initial_start_date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
    )
    key = resolve_ebid_system_key(system_key, gcp_secret)
    client = build_ebid_client(api_base_url, key)
    try:
        sites, err = _fetch_sites(client)
        if sites is None:
            raise EbidApiError(f"GetSiteMetaData failed: {err}")
    except Exception:
        client.close()
        raise
    return (
        ebid_sites(sites=sites),
        ebid_readings(
            client=client,
            start_ts=start_ts,
            sites=sites,
            _stats=_stats if _stats is not None else {},
        ),
    )


@dlt.resource(name="ebid_sites", write_disposition="replace")
def ebid_sites(sites: list[dict[str, str]]) -> Iterator[dict]:
    """Full replace every run, so a site added/removed in Contrail shows up immediately."""
    for site in sites:
        yield {
            "or_site_id": site["or_site_id"],
            "site_id": site["site_id"],
            "location": site["location"],
            "latitude": float(site["latitude_dec"]),
            "longitude": float(site["longitude_dec"]),
        }


@dlt.resource(name="ebid_readings", write_disposition="append", primary_key="reading_id")
def ebid_readings(
    client: httpx.Client,
    start_ts: int,
    sites: list[dict[str, str]],
    _stats: dict | None = None,
) -> Iterator[dict]:
    """Per-site cursor in dlt.current.resource_state(). Closes the shared
    client in a finally block — the only resource doing I/O with it."""
    try:
        cursors: dict[str, int] = dlt.current.resource_state().setdefault("site_cursors", {})
        yield from _iter_site_readings(
            client, start_ts, sites, cursors, _stats if _stats is not None else {}
        )
    finally:
        client.close()


def build_pipeline() -> dlt.Pipeline:
    return build_source_pipeline("ebid", "raw_ebid")


def run_pipeline() -> None:
    pipeline = build_pipeline()
    load_info = pipeline.run(ebid_source(), loader_file_format="parquet")
    logger.info("Load complete: %s", load_info)
