"""
EBWPC fixtures and a local stand-in for GCS, shared by this folder's tests.

Catalog fixtures are trimmed from the live samples in docs/sources/ebwpc_ckan.md
(three well files, the XLSX archive and the locations file), served by
httpx.MockTransport. local_bucket points the real build_pipeline() at a
file:// bucket under tmp_path, so dlt runs for real without GCS, ADC,
~/.dlt or the network.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from aqueduct_dagster.sources.ckan_common import CF_BYPASS_HEADER
from aqueduct_dagster.sources.ebwpc_ckan import dlt_pipeline as ebwpc_dlt_pipeline
from tests.conftest import client_with_routes

CATALOG = "https://catalog.newmexicowaterdata.org"
DATASET_ID = "719844ad-46bf-46cf-a781-a111f114fe38"
BYPASS_VALUE = "test-bypass-value"
PACKAGE_SHOW_PATH = "/api/3/action/package_show"


def _resource(
    rid: str,
    name: str,
    filename: str,
    fmt: str = "CSV",
    size: int = 100,
    description: str = "",
) -> dict:
    return {
        "id": rid,
        "name": name,
        "description": description,
        "format": fmt,
        "last_modified": "2025-03-12T18:11:29.830723",
        "size": size,
        "url": f"{CATALOG}/dataset/{DATASET_ID}/resource/{rid}/download/{filename}",
        "package_id": DATASET_ID,
        "url_type": "upload",
    }


# Verbatim from the live package_show sample: one of the four wells whose
# description carries elevation and formation.
E_8428_DESCRIPTION = (
    "About 5 miles north of Tajique, NM\r\n"
    "Torrance County, New Mexico\r\n"
    "UTM Easting 386426, Northing 3853603, Zone 13, NAD83\r\n"
    "Elevation 6712 (ft AMSL), Approximate Depth Unknown,  Madera Formation\r\n"
)
E_8428 = _resource(
    "0c306f4d-1474-4d00-9d77-45b73513abfd",
    "E-8428",
    "e-8428.csv",
    size=2530586,
    description=E_8428_DESCRIPTION,
)
E_1639 = _resource("1e9a0c55-9a43-4e57-8f4b-2f0cbb1f3a10", "E-1639-POD1", "e-1639-pod1.csv", ".csv")
E_9673 = _resource("5d7c2b1a-3c55-4f0e-9d0a-7b8e6f2a4c11", "E-9673", "e-9673.csv")
E_9407_ARCHIVED = _resource(
    "be49076b-375e-4570-90e5-bcc9dc97ae04",
    "E-9407-Archived",
    "e-9407_hydrographdata_data.xlsx",
    "XLSX",
    description="About 6 miles uproad east of Lucy, NM",
)
LOCATIONS = _resource(
    "2f186d78-02d0-4d89-bb4f-540acb398774",
    "EBWPC Well Locations",
    "ebwpc_locations.csv",
    size=763,
    description="This table contains the locations of EBWPC wells in UTM Zone13 coordinates. ",
)
WELLS = [E_8428, E_1639, E_9673]
ALL_RESOURCES = [E_8428, E_1639, E_9407_ARCHIVED, E_9673, LOCATIONS]

FILES: dict[str, bytes] = {
    # Manual-only rows, both readings on one row, and the newest reading in the dataset.
    E_8428["id"]: (
        b"site_id,measurement_date,measurement_time,transducer_depth_bgs,manual_depth_bgs\r\n"
        b"e-8428,4/15/2009,0:00,,58.3\r\n"
        b"e-8428,6/4/2009,0:00,,57.22\r\n"
        b"e-8428,6/5/2009,10:00,57.17145,57.22\r\n"
        b"e-8428,4/26/2024,8:18,54.98248,56.08\r\n"
    ),
    # Two-digit years, AM/PM times, N/A time and depth.
    E_1639["id"]: (
        b"site_id,measurement_date,measurement_time,transducer_depth_bgs,manual_depth_bgs\r\n"
        b"e-1639-pod1,1/18/18,N/A,N/A,67.65\r\n"
        b"e-1639-pod1,4/13/18,9:00:00 AM,68.00145004,\r\n"
        b"e-1639-pod1,4/13/18,4:00:00 PM,67.91670368,67.63\r\n"
        b"e-1639-pod1,4/25/2024,3:12:00 PM,,68.61\r\n"
    ),
    # Empty trailing header cells and #VALUE! dates.
    E_9673["id"]: (
        b"site_id,measurement_date,measurement_time,transducer_depth_bgs,manual_depth_bgs,,,,\r\n"
        b"e-9673,7/24/2013,6:00,237.50416,,,,,\r\n"
        b"e-9673,#VALUE!,#VALUE!,N/A,237.03,,,,\r\n"
        b"e-9673,#VALUE!,#VALUE!,N/A,230.39,,,,\r\n"
    ),
    LOCATIONS["id"]: (
        b"NMBG_ID,UTM_Zone13N_Easting,UTM_Zone13N_Northing,Datum\r\n"
        b"E-8428,386426,3853605,NAD83\r\n"
        b"E-1639-POD1,424100,3812580,NAD83\r\n"
        b"E-9673 ,381972,3879947,NAD83\r\n"
    ),
}
WELL_ROWS = 11  # data rows across the three well files above


def path_of(resource: dict) -> str:
    return httpx.URL(resource["url"]).path


def package_show_response(resources: list[dict]) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "success": True,
            "result": {"id": DATASET_ID, "name": "ebwpc-gw-monitoring", "resources": resources},
        },
    )


class FakeCatalog:
    """The catalog behind ebwpc_ckan_source. Each source call builds one client,
    so each "run" gets its own recorded call list. Mutate `routes` to change
    an answer between runs."""

    def __init__(self) -> None:
        self.routes: dict[str, httpx.Response | Exception] = {
            PACKAGE_SHOW_PATH: package_show_response(ALL_RESOURCES),
            **{
                path_of(resource): httpx.Response(200, content=FILES[resource["id"]])
                for resource in [*WELLS, LOCATIONS]
            },
        }
        self.clients: list[httpx.Client] = []
        self.runs: list[list[httpx.Request]] = []
        self.api_base_urls: list[str] = []
        self.gcp_secrets: list[str] = []

    def build_client(self, api_base_url: str, cf_bypass: str) -> httpx.Client:
        client, calls = client_with_routes(
            self.routes, base_url=api_base_url, headers={CF_BYPASS_HEADER: cf_bypass}
        )
        self.api_base_urls.append(api_base_url)
        self.clients.append(client)
        self.runs.append(calls)
        return client

    def resolve_cf_bypass(self, cf_bypass: str, gcp_secret: str) -> str:
        self.gcp_secrets.append(gcp_secret)
        return cf_bypass or BYPASS_VALUE

    def paths(self, run: int = -1) -> list[str]:
        """URL paths requested during one run (default: the latest)."""
        return [request.url.path for request in self.runs[run]]

    def well_paths(self, run: int = -1) -> list[str]:
        wells = {path_of(resource) for resource in WELLS}
        return [path for path in self.paths(run) if path in wells]


@pytest.fixture
def catalog(monkeypatch: pytest.MonkeyPatch) -> FakeCatalog:
    fake = FakeCatalog()
    monkeypatch.setattr(ebwpc_dlt_pipeline, "build_ckan_client", fake.build_client)
    monkeypatch.setattr(ebwpc_dlt_pipeline, "resolve_cf_bypass", fake.resolve_cf_bypass)
    return fake


@pytest.fixture
def local_bucket(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A file:// bucket for build_source_pipeline(), with dlt's working dir
    (pipeline state) under tmp_path/dlt and telemetry off."""
    bucket = tmp_path / "bucket"
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    monkeypatch.setenv("RUNTIME__DLTHUB_TELEMETRY", "false")
    monkeypatch.setattr("aqueduct_dagster.shared.pipeline._gcs_bucket_url", lambda: bucket.as_uri())
    monkeypatch.setattr("aqueduct_dagster.shared.pipeline.ensure_adc", lambda: None)
    return bucket


def parquet_files(bucket: Path, table: str) -> list[Path]:
    return sorted((bucket / "raw_ebwpc_ckan" / table).glob("**/*.parquet"))
