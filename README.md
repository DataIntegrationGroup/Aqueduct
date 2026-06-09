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
