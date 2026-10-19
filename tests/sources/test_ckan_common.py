"""
Unit tests for sources/ckan_common.py
Offline: httpx.MockTransport answers by URL path, Secret Manager is patched.
Fixtures are a synthetic catalog; the EBWPC-shaped ones live in tests/sources/ebwpc_ckan/.
"""

from __future__ import annotations

import csv
import hashlib
from unittest.mock import MagicMock, patch

import httpx
import pytest

from aqueduct_dagster.sources.ckan_common import (
    CF_BYPASS_HEADER,
    CkanApiError,
    build_ckan_client,
    download_resource,
    iter_resource_snapshots,
    normalise_format,
    package_show,
    parse_csv,
    resolve_cf_bypass,
    snapshot_rows,
    split_resources,
)
from tests.conftest import client_with_routes

_BASE = "https://catalog.example.org"
_DATASET_ID = "11111111-2222-3333-4444-555555555555"


def _resource(rid: str, name: str, filename: str, **overrides: object) -> dict:
    resource = {
        "id": rid,
        "name": name,
        "format": "CSV",
        "last_modified": "2025-03-12T18:11:29.830723",
        "size": 100,
        "url": f"{_BASE}/dataset/{_DATASET_ID}/resource/{rid}/download/{filename}",
    }
    resource.update(overrides)
    return resource


_WELL_A = _resource("aaaa", "Well-A", "well-a.csv")
_WELL_B = _resource("bbbb", "Well-B", "well-b.csv")
_WELL_A_CSV = (
    b"site_id,measurement_date,depth\r\nwell-a,4/15/2009,58.3\r\nwell-a,6/4/2009,57.22\r\n"
)
_WELL_B_CSV = b"site_id,measurement_date,depth\r\nwell-b,1/18/18,N/A\r\n"


def _path(resource: dict) -> str:
    return httpx.URL(resource["url"]).path


def _catalog(routes: dict[str, httpx.Response | Exception] | None = None):
    """Client whose download routes serve both wells, overridable per path."""
    default: dict[str, httpx.Response | Exception] = {
        _path(_WELL_A): httpx.Response(200, content=_WELL_A_CSV),
        _path(_WELL_B): httpx.Response(200, content=_WELL_B_CSV),
    }
    default.update(routes or {})
    return client_with_routes(default, base_url=_BASE)


def _drive(
    resources: list[dict],
    fingerprints: dict,
    routes: dict[str, httpx.Response | Exception] | None = None,
) -> tuple[list[dict], dict, list[httpx.Request]]:
    client, calls = _catalog(routes)
    stats: dict = {}
    with patch("time.sleep"):
        rows = list(iter_resource_snapshots(client, resources, fingerprints, stats))
    return rows, stats, calls


# ── resolve_cf_bypass / build_ckan_client ─────────────────────────────────────


class TestResolveCfBypass:
    def test_returns_immediately_when_value_already_given(self):
        with patch("aqueduct_dagster.sources.ckan_common.secretmanager") as mock_sm:
            assert resolve_cf_bypass("given", "ignored-secret") == "given"
        mock_sm.SecretManagerServiceClient.assert_not_called()

    @patch("aqueduct_dagster.sources.ckan_common.secretmanager.SecretManagerServiceClient")
    @patch("aqueduct_dagster.sources.ckan_common.ensure_adc")
    @patch("aqueduct_dagster.sources.ckan_common.load_config")
    def test_reads_the_bare_value_from_secret_manager(
        self, mock_load_config, mock_ensure_adc, mock_sm_cls
    ):
        mock_load_config.return_value = {
            "destination": {"filesystem": {"gcp_project_number": "12345"}}
        }
        mock_sm = mock_sm_cls.return_value
        mock_sm.access_secret_version.return_value = MagicMock(
            payload=MagicMock(data=b"bypass-value\n")
        )

        assert resolve_cf_bypass("", "ckan_cf_bypass") == "bypass-value"
        mock_sm.secret_version_path.assert_called_once_with("12345", "ckan_cf_bypass", "latest")
        mock_ensure_adc.assert_called_once()


class TestBuildCkanClient:
    def test_sends_the_cloudflare_bypass_header(self):
        client = build_ckan_client(_BASE, "bypass-value")
        assert client.headers[CF_BYPASS_HEADER] == "bypass-value"
        assert client.headers["Accept"] == "application/json"
        assert str(client.base_url).rstrip("/") == _BASE


# ── package_show ──────────────────────────────────────────────────────────────


class TestPackageShow:
    def test_returns_the_result_and_hits_the_action_api_by_uuid(self):
        body = {"success": True, "result": {"id": _DATASET_ID, "resources": [_WELL_A]}}
        client, calls = client_with_routes(
            {"/api/3/action/package_show": httpx.Response(200, json=body)}, base_url=_BASE
        )
        assert package_show(client, _DATASET_ID)["resources"] == [_WELL_A]
        assert calls[0].url.params["id"] == _DATASET_ID

    def test_raises_on_a_cloudflare_403(self):
        client, _ = client_with_routes(
            {"/api/3/action/package_show": httpx.Response(403)}, base_url=_BASE
        )
        with pytest.raises(httpx.HTTPStatusError):
            package_show(client, _DATASET_ID)

    def test_raises_ckan_api_error_on_success_false(self):
        body = {"success": False, "error": {"message": "Not found"}}
        client, _ = client_with_routes(
            {"/api/3/action/package_show": httpx.Response(200, json=body)}, base_url=_BASE
        )
        with pytest.raises(CkanApiError, match="Not found"):
            package_show(client, _DATASET_ID)

    def test_retries_a_transient_error(self):
        body = {"success": True, "result": {"id": _DATASET_ID, "resources": []}}
        responses = iter([httpx.ReadTimeout("slow"), httpx.Response(200, json=body)])

        def handler(request: httpx.Request) -> httpx.Response:
            item = next(responses)
            if isinstance(item, Exception):
                raise item
            return item

        client = httpx.Client(base_url=_BASE, transport=httpx.MockTransport(handler))
        with patch("time.sleep"):
            assert package_show(client, _DATASET_ID)["id"] == _DATASET_ID


# ── resource selection ────────────────────────────────────────────────────────


class TestSplitResources:
    @pytest.mark.parametrize("fmt", ["CSV", ".csv", " csv ", "Csv"])
    def test_normalises_free_text_formats(self, fmt):
        assert normalise_format(fmt) == "csv"

    def test_missing_format_is_not_csv(self):
        assert normalise_format(None) == ""

    def test_picks_out_the_locations_resource_by_name(self):
        locations = _resource("llll", "Well Locations", "locations.csv")
        found, measurements, skipped = split_resources(
            [_WELL_A, locations, _WELL_B], "well locations"
        )
        assert found is locations
        assert measurements == [_WELL_A, _WELL_B]
        assert skipped == {}

    def test_skips_non_csv_resources_with_a_reason(self):
        xlsx = _resource("xxxx", "Archive", "archive.xlsx", format="XLSX")
        found, measurements, skipped = split_resources([_WELL_A, xlsx], "Well Locations")
        assert found is None
        assert measurements == [_WELL_A]
        assert "XLSX" in skipped["Archive"]


# ── download_resource ─────────────────────────────────────────────────────────


class TestDownloadResource:
    def test_returns_the_exact_bytes_and_asks_for_any_content_type(self):
        client, calls = _catalog()
        body, err = download_resource(client, _WELL_A)
        assert (body, err) == (_WELL_A_CSV, None)
        assert calls[0].headers["Accept"] == "*/*"

    def test_returns_a_reason_on_a_server_error(self):
        client, _ = _catalog({_path(_WELL_A): httpx.Response(500)})
        assert download_resource(client, _WELL_A) == (None, "HTTP 500")

    def test_returns_a_reason_after_exhausting_transient_retries(self):
        client, calls = _catalog({_path(_WELL_A): httpx.ConnectError("down")})
        with patch("time.sleep"):
            body, err = download_resource(client, _WELL_A)
        assert body is None
        assert err is not None and "transient" in err
        assert len(calls) == 3

    @pytest.mark.parametrize(
        "url",
        [
            "https://elsewhere.example.com/well-a.csv",
            "http://catalog.example.org/dataset/x/download/well-a.csv",
        ],
    )
    def test_never_sends_the_bypass_header_off_the_catalog(self, url):
        # A CKAN link resource can point anywhere; the client carries a secret.
        client, calls = _catalog()
        body, err = download_resource(client, {**_WELL_A, "url": url})
        assert body is None
        assert err is not None
        assert calls == []


# ── parse_csv / snapshot_rows ─────────────────────────────────────────────────


class TestParseCsv:
    def test_every_cell_is_a_string_and_crlf_is_handled(self):
        rows = parse_csv(_WELL_A_CSV)
        assert rows == [
            {"site_id": "well-a", "measurement_date": "4/15/2009", "depth": "58.3"},
            {"site_id": "well-a", "measurement_date": "6/4/2009", "depth": "57.22"},
        ]

    def test_strips_a_utf8_bom_from_the_first_header(self):
        rows = parse_csv(b"\xef\xbb\xbfsite_id,depth\r\nx,1\r\n")
        assert rows == [{"site_id": "x", "depth": "1"}]

    def test_blank_trailing_headers_become_positional_columns(self):
        # EBWPC: "...,manual_depth_bgs,,,," — DictReader would fold these into one "" key.
        rows = parse_csv(b"site_id,depth,,\r\nx,1,stray,\r\n")
        assert rows == [{"site_id": "x", "depth": "1", "column_3": "stray", "column_4": ""}]

    def test_cells_beyond_the_header_are_kept(self):
        rows = parse_csv(b"site_id,depth\r\nx,1,6/5/2009\r\n")
        assert rows == [{"site_id": "x", "depth": "1", "column_3": "6/5/2009"}]

    def test_short_rows_and_empty_lines(self):
        rows = parse_csv(b"site_id,depth\r\nx\r\n\r\ny,2\r\n")
        assert rows == [{"site_id": "x"}, {"site_id": "y", "depth": "2"}]

    def test_falls_back_to_latin1(self):
        rows = parse_csv(b"site_id,notes\r\nx,caf\xe9\r\n")
        assert rows == [{"site_id": "x", "notes": "café"}]

    def test_header_only_file_has_no_rows(self):
        assert parse_csv(b"site_id,depth\r\n") == []
        assert parse_csv(b"") == []


class TestSnapshotRows:
    def test_adds_provenance_columns(self):
        rows = snapshot_rows(_WELL_B, _WELL_B_CSV)
        assert rows == [
            {
                "site_id": "well-b",
                "measurement_date": "1/18/18",
                "depth": "N/A",
                "resource_id": "bbbb",
                "resource_name": "Well-B",
                "resource_last_modified": "2025-03-12T18:11:29.830723",
                "source_url": _WELL_B["url"],
                "content_sha256": hashlib.sha256(_WELL_B_CSV).hexdigest(),
                "row_number": 1,
            }
        ]


# ── iter_resource_snapshots — the per-resource cursor ─────────────────────────


class TestFirstRun:
    def test_lands_every_row_of_every_resource(self):
        rows, stats, _ = _drive([_WELL_A, _WELL_B], {})
        assert [row["site_id"] for row in rows] == ["well-a", "well-a", "well-b"]
        assert stats["resources_landed"] == 2
        assert stats["rows_yielded"] == 3
        assert stats["resources_added"] == ["Well-A", "Well-B"]

    def test_reading_id_names_resource_snapshot_and_row(self):
        rows, _, _ = _drive([_WELL_A], {})
        sha = hashlib.sha256(_WELL_A_CSV).hexdigest()
        assert [row["reading_id"] for row in rows] == [f"aaaa_{sha[:16]}_1", f"aaaa_{sha[:16]}_2"]

    def test_stores_a_fingerprint_per_resource(self):
        fingerprints: dict = {}
        _drive([_WELL_A], fingerprints)
        assert fingerprints == {
            "aaaa": {
                "last_modified": _WELL_A["last_modified"],
                "size": _WELL_A["size"],
                "url": _WELL_A["url"],
                "sha256": hashlib.sha256(_WELL_A_CSV).hexdigest(),
                "name": "Well-A",
            }
        }


class TestSecondRun:
    def test_unchanged_metadata_downloads_nothing_and_lands_nothing(self):
        fingerprints: dict = {}
        _drive([_WELL_A, _WELL_B], fingerprints)

        rows, stats, calls = _drive([_WELL_A, _WELL_B], fingerprints)
        assert rows == []
        assert calls == []
        assert stats["resources_unchanged"] == 2
        assert stats["resources_added"] == []

    def test_reupload_of_identical_bytes_lands_nothing_but_refreshes_the_fingerprint(self):
        fingerprints: dict = {}
        _drive([_WELL_A], fingerprints)
        reuploaded = {**_WELL_A, "last_modified": "2026-01-01T00:00:00"}

        rows, stats, calls = _drive([reuploaded], fingerprints)
        assert rows == []
        assert len(calls) == 1
        assert stats["resources_same_content"] == 1
        assert fingerprints["aaaa"]["last_modified"] == "2026-01-01T00:00:00"

        # ...so the run after that doesn't download it again.
        _, _, calls = _drive([reuploaded], fingerprints)
        assert calls == []

    def test_changed_file_lands_again_in_full_as_a_new_snapshot(self):
        fingerprints: dict = {}
        first, _, _ = _drive([_WELL_A], fingerprints)
        updated_csv = _WELL_A_CSV + b"well-a,4/26/2024,56.08\r\n"
        updated = {**_WELL_A, "last_modified": "2026-01-01T00:00:00", "size": len(updated_csv)}

        rows, stats, _ = _drive(
            [updated], fingerprints, {_path(_WELL_A): httpx.Response(200, content=updated_csv)}
        )
        assert len(rows) == 3
        assert stats["resources_landed"] == 1
        # A new snapshot never reuses the previous snapshot's reading_ids.
        assert not {row["reading_id"] for row in rows} & {row["reading_id"] for row in first}


class TestFailureIsolation:
    def test_one_failed_resource_does_not_block_the_others(self):
        # The failing resource goes first: the one after it must still land.
        rows, stats, _ = _drive([_WELL_A, _WELL_B], {}, {_path(_WELL_A): httpx.Response(500)})
        assert [row["site_id"] for row in rows] == ["well-b"]
        assert stats["resources_errored"] == 1
        assert stats["resources_landed"] == 1
        assert stats["failed_resources"] == ["Well-A"]

    def test_failed_resource_keeps_its_old_fingerprint_and_retries_next_run(self):
        fingerprints: dict = {}
        _drive([_WELL_A], fingerprints)
        before = dict(fingerprints["aaaa"])
        updated = {**_WELL_A, "last_modified": "2026-01-01T00:00:00"}

        _drive([updated], fingerprints, {_path(_WELL_A): httpx.Response(503)})
        assert fingerprints["aaaa"] == before

        _, _, calls = _drive([updated], fingerprints)
        assert [call.url.path for call in calls] == [_path(_WELL_A)]

    def test_unreadable_csv_counts_as_an_error(self):
        with patch(
            "aqueduct_dagster.sources.ckan_common.snapshot_rows", side_effect=csv.Error("bad")
        ):
            rows, stats, _ = _drive([_WELL_A], {})
        assert rows == []
        assert stats["resources_errored"] == 1


class TestCatalogChanges:
    def test_removed_resource_is_reported_once_and_dropped_from_state(self):
        fingerprints: dict = {}
        _drive([_WELL_A, _WELL_B], fingerprints)

        _, stats, _ = _drive([_WELL_A], fingerprints)
        assert stats["resources_removed"] == ["Well-B"]
        assert "bbbb" not in fingerprints

        _, stats, _ = _drive([_WELL_A], fingerprints)
        assert stats["resources_removed"] == []

    def test_new_resource_is_reported_as_added_and_landed(self):
        fingerprints: dict = {}
        _drive([_WELL_A], fingerprints)

        rows, stats, calls = _drive([_WELL_A, _WELL_B], fingerprints)
        assert stats["resources_added"] == ["Well-B"]
        assert [row["site_id"] for row in rows] == ["well-b"]
        assert [call.url.path for call in calls] == [_path(_WELL_B)]
