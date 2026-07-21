# Aqueduct

[![CI](https://github.com/DataIntegrationGroup/Aqueduct/actions/workflows/ci.yml/badge.svg)](https://github.com/DataIntegrationGroup/Aqueduct/actions/workflows/ci.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Checked with mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)

Dagster + dlt + GCS + FROST SensorThings

**Contributing:** see [CONTRIBUTING.md](CONTRIBUTING.md) for branching, PRs, and releases.

Two independent source pipelines, each running on its own schedule:

```
HydroVu API  → dlt → GCS (parquet) → HydroVuAdapter → CanonicalBundle → frost_load_hydrovu → FROST
CABQ API     → dlt → GCS (parquet) → CabqAdapter    → CanonicalBundle → frost_load_cabq    → FROST
```

Orchestrated by Dagster. Each pipeline has three assets:

| Asset | HydroVu | CABQ |
|-------|---------|------|
| Ingest (dlt → GCS) | `raw_hydrovu_readings` | `raw_cabq_readings` |
| Transform (GCS → CanonicalBundles) | `canonical_bundles_hydrovu` | `canonical_bundles_cabq` |
| Load (CanonicalBundles → FROST) | `frost_load_hydrovu` | `frost_load_cabq` |



## Project structure

Organized as a vertical slice per source: everything specific to one agency's
pipeline (fetch → transform → adapt) lives together under `sources/<name>/`,
so onboarding a new source means adding one folder, not touching four
unrelated directories.

```
Aqueduct/
├── docker-compose.yml              # FROST + PostGIS
├── pyproject.toml                  # dependencies and build config
├── uv.lock                         # pinned dependency versions
├── .gitignore
├── .dlt/
│   └── config.toml                 # dlt non-secret config (bucket URL, API URLs, start dates)
├── docs/
│   ├── PIPELINE.md                 # end-to-end pipeline walkthrough (HydroVu reference)
│   ├── STORAGE_CONVENTIONS.md      # GCS bucket/path naming conventions
│   └── sources/
│       ├── _mapping_template.md    # blank template for onboarding a new source
│       └── pvacd_hydrovu.md        # HydroVu field-by-field canonical mapping reference
├── src/aqueduct_dagster/
│   ├── canonical/                  # shared data model — the contract every adapter maps into
│   │   ├── CANONICAL_MODEL.md      # entities, properties schema, and file roles
│   │   ├── canonical_model.py      # dataclasses: CanonicalBundle, Thing, Location, Datastream, etc.
│   │   ├── canonical_constants.py  # shared units, sensors, observed properties, key helpers
│   │   └── base_adapter.py         # abstract BaseAdapter — all source adapters inherit from this
│   ├── shared/                     # cross-cutting infra used by every source — no domain logic
│   │   ├── gcs.py                  # GCS filesystem access, parquet reads, watermark read/write
│   │   ├── pipeline.py             # build_source_pipeline() — shared dlt pipeline factory
│   │   ├── http.py                 # retry_transient(), TokenManager, BearerAuth, build_authenticated_client()
│   │   └── source_registry.py      # SOURCE_REGISTRY — single per-source config for definitions.py + load.py
│   ├── sources/                    # one folder per agency source (vertical slice)
│   │   ├── hydrovu/
│   │   │   ├── adapter.py          # HydroVu → CanonicalBundle mapping
│   │   │   ├── dlt_pipeline.py     # dlt source + resource + pipeline factory
│   │   │   ├── ingest.py           # Dagster asset: raw_hydrovu_readings
│   │   │   └── transform.py        # Dagster asset: canonical_bundles_hydrovu
│   │   └── cabq/                   # same shape as hydrovu/ — currently a stub
│   │       ├── adapter.py
│   │       ├── dlt_pipeline.py
│   │       ├── ingest.py
│   │       └── transform.py
│   ├── defs/
│   │   ├── assets/
│   │   │   └── load.py             # Dagster assets: frost_load_hydrovu, frost_load_cabq (shared factory)
│   │   ├── definitions.py          # Dagster entry point — jobs, schedules, asset registry
│   │   └── dagster_logging.py      # forward_python_logs_to_dagster() — stdlib logging → Dagster run logs
│   └── loader/
│       ├── frost_loader.py         # FrostLoader (abstract) + FrostStaClientLoader (concrete)
│       └── watermark_store.py      # FrostWatermarkStore — per-run dedup via Dagster context
└── tests/                          # mirrors src/aqueduct_dagster/'s layout above
    ├── conftest.py                 # cross-file test helpers (e.g. httpx.MockTransport/BearerAuth builders)
    ├── sources/{hydrovu,cabq}/
    ├── shared/
    ├── defs/assets/
    └── loader/
```

---

## Getting started

### Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Python | 3.13+ | [python.org](https://www.python.org/downloads/) or `pyenv install 3.13` |
| uv | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Docker + Docker Compose | 24+ | [docs.docker.com](https://docs.docker.com/get-docker/) |
| GCP service account | — | with Storage Object Admin on the GCS bucket |

---

### 1. Clone the branch

```bash
git clone https://github.com/DataIntegrationGroup/Aqueduct.git
cd Aqueduct
```

---

### 2. Install dependencies

```bash
uv sync
```

This reads `pyproject.toml` and installs all dependencies into a local `.venv` — no `requirements.txt` needed, `uv` manages everything.

---

### 3. Setup Authentication

**Local Development**

The Google Cloud Storage libraries will automatically detect local credentials that can be created by running the following command in your terminal. You will only need to run this command once to create the credential file.

```bash
gcloud auth application-default login
```

---

### 4. Run the test suite

```bash
uv run pytest
```

Tests are unit tests only — no GCS, FROST, or HydroVu API required. All tests should pass before you proceed.

---

## Linting, typing, and tests

Formatting (`ruff format`), linting (`ruff`), and type checking (`mypy src`) run in
pre-commit hooks and in [GitHub Actions](.github/workflows/ci.yml) on PRs to `main`,
alongside the pytest suite.

```bash
uv sync --group dev
uv run pre-commit install          # one-time: enable the git hook
uv run pre-commit run --all-files  # run all hooks manually
uv run pytest --cov=src/aqueduct_dagster
```

---

### 5. Start the local FROST server

```bash
docker compose up -d
```

This starts two containers:
- `web` — FROST-Server on port 8081 (`http://localhost:8081/FROST-Server/v1.1`)
- `database` — PostGIS (PostgreSQL 16) on port 5432

Verify it's up:

```bash
curl http://localhost:8081/FROST-Server/v1.1
```

---

### 6. Run Dagster

```bash
uv run dagster dev
```

Open the Dagster UI at `http://localhost:3000`.

To run the full HydroVu pipeline end-to-end:
1. Click **Assets** in the left nav
2. Select all three `hydrovu` group assets (`raw_hydrovu_readings`, `canonical_bundles_hydrovu`, `frost_load_hydrovu`)
3. Click **Materialize selected**

On first run, dlt fetches from `initial_start_date` in `.dlt/config.toml` (currently `2026-05-01`). Subsequent runs are incremental.

---

### 7. Verify data in FROST

After a successful pipeline run, query the local FROST server:

```bash
# All Things with Locations + Datastreams
curl -s "http://localhost:8081/FROST-Server/v1.1/Things?\$expand=Locations,Datastreams(\$expand=ObservedProperty,Sensor)" \
  | python3 -m json.tool

# Observation count
curl -s "http://localhost:8081/FROST-Server/v1.1/Observations?\$count=true&\$top=1" \
  | python3 -m json.tool
```

---

## Architecture notes

**Canonical model as the contract**
Adapters produce `CanonicalBundle` objects. The FROST loader consumes them. Neither knows about the other's internals — the canonical model is the only shared interface.

**Incremental loading**
dlt tracks a cursor (`timestamp` field) per source. On first run it fetches from `initial_start_date`. On subsequent runs it fetches only records newer than the last cursor value. Cursor state is persisted to GCS alongside the parquet files.

**Watermark deduplication**
`FrostWatermarkStore` tracks the last observation timestamp successfully loaded into FROST per datastream. Each run skips any observation at or before the watermark — FROST has no built-in deduplication.

**Independent pipelines**
`hydrovu_pipeline` and `cabq_pipeline` are completely independent Dagster jobs. Each has its own schedule and its own terminal load asset (`frost_load_hydrovu` / `frost_load_cabq`). Running one never triggers or blocks the other.

---
