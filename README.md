# Aqueduct-POC-Bravo

Aqueduct POC B — Dagster + dlt + GCS + FROST SensorThings

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

```
aqueduct-dagster-poc-v2/
├── docker-compose.yml              # FROST + PostGIS
├── .env.example                    # env var template — copy to .env
├── .dlt/
│   ├── config.toml                 # dlt non-secret config (bucket URL, API URLs, start dates)
│   └── secrets.toml.example        # dlt secrets template — copy to secrets.toml
├── src/aqueduct_dagster/
│   ├── canonical/                  # shared data model — adapters and loader both import from here
│   │   ├── CANONICAL_MODEL.md      # explains the canonical model, entities, and file roles
│   │   ├── canonical_model.py      # dataclasses: CanonicalBundle, Thing, Location, Datastream, etc.
│   │   ├── canonical_constants.py  # shared units, sensors, observed properties, key helpers
│   │   └── base_adapter.py         # abstract BaseAdapter — all source adapters inherit from this
│   ├── adapters/
│   │   ├── hydrovu_adapter.py      # HydroVu → CanonicalBundle mapping
│   │   └── cabq_adapter.py         # CABQ → CanonicalBundle mapping
│   ├── pipeline/
│   │   ├── hydrovu_dlt_pipeline.py # dlt source + resource + pipeline factory for HydroVu
│   │   └── cabq_dlt_pipeline.py    # dlt source + resource + pipeline factory for CABQ
│   ├── defs/
│   │   ├── assets/
│   │   │   ├── ingest_hydrovu.py   # Dagster asset: raw_hydrovu_readings
│   │   │   ├── ingest_cabq.py      # Dagster asset: raw_cabq_readings
│   │   │   ├── transform_hydrovu.py# Dagster asset: canonical_bundles_hydrovu
│   │   │   ├── transform_cabq.py   # Dagster asset: canonical_bundles_cabq
│   │   │   └── load.py             # Dagster assets: frost_load_hydrovu, frost_load_cabq
│   │   └── definitions.py          # Dagster entry point — jobs, schedules, asset registry
│   └── loader/
│       ├── frost_loader.py         # FrostLoader (abstract) + FrostStaClientLoader (concrete)
│       └── watermark_store.py      # FrostWatermarkStore — per-run dedup via Dagster context
└── tests/
    ├── test_hydrovu_adapter.py
    └── test_cabq_adapter.py
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
| HydroVu API credentials | — | client ID + secret from HydroVu |

---

### 1. Clone the branch

```bash
git clone https://github.com/your-org/aqueduct-poc-bravo.git
cd aqueduct-poc-bravo
git checkout scaffold-dagster-dlt/ST2DAT-78
```

---

### 2. Install dependencies

```bash
uv sync
```

This reads `pyproject.toml` and installs all dependencies into a local `.venv` — no `requirements.txt` needed, `uv` manages everything.

---

### 3. Configure secrets and environment

**Step A — copy and fill in `.env`:**

```bash
cp .env.example .env
```

Edit `.env` and set:
- `POSTGRES_PASSWORD` — any local password (e.g. `changeme`)
- `HYDROVU_CLIENT_ID` / `HYDROVU_CLIENT_SECRET` — from HydroVu
- `GOOGLE_APPLICATION_CREDENTIALS` — path to the `aqueduct-dlt-writer` service account JSON key file (ask Likitha for the key). Set `GCS_BUCKET_NAME` to your own test bucket — **do not use the shared `aqueduct-poc-bravo-pvacd` bucket for testing**, create your own and grant `aqueduct-dlt-writer` Storage Object Admin on it.

**Step B — copy and fill in `.dlt/secrets.toml`:**

```bash
cp .dlt/secrets.toml.example .dlt/secrets.toml
```

Edit `.dlt/secrets.toml` and fill in your HydroVu `client_id` / `client_secret`. For GCS, set `project_id` and `client_email` to match the `aqueduct-dlt-writer` service account and paste the `private_key` from the JSON key file.

> **Never commit `.env` or `.dlt/secrets.toml`** — both are in `.gitignore`.

---

### 4. Run the test suite

```bash
uv run pytest
```

Tests are unit tests only — no GCS, FROST, or HydroVu API required. All 27 tests should pass before you proceed.

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
