# AGENTS.md

Guidance for AI coding agents (Cursor, Claude Code, Copilot, etc.) working in
this repo. Read this before making changes. Human contributors: see
[CONTRIBUTING.md](CONTRIBUTING.md) and [README.md](README.md).

## What this project is

Aqueduct is the New Mexico Bureau of Geology data pipeline. It ingests
environmental sensor data from external agency APIs, normalizes it through a
shared canonical model, and loads it into a FROST SensorThings API server.

Stack: **Dagster** (orchestration) + **dlt** (ingestion) + **GCS** (raw parquet
storage) + **FROST/PostGIS** (SensorThings backend). Python 3.13, managed with
**uv**.

Each source is an independent pipeline with three stages:

```
API → dlt → GCS (parquet) → Adapter → CanonicalBundle → FROST loader → FROST
```

| Stage | Asset (HydroVu) | Asset (CABQ) |
|---|---|---|
| Ingest (dlt → GCS) | `raw_hydrovu_readings` | `raw_cabq_readings` |
| Transform (GCS → CanonicalBundles) | `canonical_bundles_hydrovu` | `canonical_bundles_cabq` |
| Load (CanonicalBundles → FROST) | `frost_load_hydrovu` | `frost_load_cabq` |

HydroVu is live; CABQ is scaffolded (`cabq_*` raise `NotImplementedError`). Use
HydroVu as the reference implementation when wiring up a new source.

## The one rule that explains the design

**The canonical model is the only contract between stages.** Adapters produce
`CanonicalBundle`s; the FROST loader consumes them. Neither imports the other.
Source-specific logic lives only in `sources/<name>/` — everything downstream
of the adapter is source-agnostic. Preserve this boundary. Do not make the
loader aware of HydroVu, or an adapter aware of FROST.

## Repo map

```
src/aqueduct_dagster/
├── canonical/      # shared data model — the contract. Start here to understand the domain.
│   └── CANONICAL_MODEL.md   # read this for entity definitions and the properties schema
├── shared/         # cross-cutting infra used by every source — no domain logic
│   ├── gcs.py             # GCS filesystem access, parquet reads, watermark read/write
│   ├── pipeline.py        # build_source_pipeline() — shared dlt pipeline factory
│   ├── http.py            # retry_transient(), TokenManager, BearerAuth, build_authenticated_client()
│   └── source_registry.py # SOURCE_REGISTRY — single per-source config, read by definitions.py and load.py
├── sources/        # one folder per agency source (vertical slice) — see hydrovu/ as the reference
│   └── <name>/
│       ├── adapter.py       # raw rows → CanonicalBundle (source-specific)
│       ├── dlt_pipeline.py  # dlt source/resource/pipeline factory
│       ├── ingest.py        # Dagster asset: raw_<name>_readings
│       └── transform.py     # Dagster asset: canonical_bundles_<name>
├── defs/
│   ├── assets/
│   │   └── load.py         # Dagster assets: frost_load_<name>, generated per source from one factory
│   ├── definitions.py      # Dagster entry point: jobs, schedules, asset registry (also config-driven)
│   └── dagster_logging.py  # forward_python_logs_to_dagster() — stdlib logging → Dagster run logs
└── loader/         # frost_loader.py (FROST upserts) + watermark_store.py (dedup)
tests/              # mirrors src/aqueduct_dagster/'s layout above — unit tests only, no live GCS/FROST/API
├── conftest.py     # cross-file test helpers (e.g. httpx.MockTransport/BearerAuth builders)
```

## Environment & commands

```bash
uv sync --group dev                  # install everything into .venv
uv run pre-commit install            # one-time: enable git hooks
uv run pre-commit run --all-files    # ruff format + ruff + mypy on everything
uv run pytest                        # run the unit suite
uv run pytest --cov=src/aqueduct_dagster
docker compose up -d                 # local FROST (:8081) + PostGIS (:5432)
uv run dagster dev                   # Dagster UI at :3000
```

Add dependencies with `uv add <pkg>` so `pyproject.toml` and `uv.lock` stay in
sync. Never hand-edit `uv.lock`. There is no `requirements.txt`.

## Coding conventions

- **Python 3.13**, `from __future__ import annotations` at the top of modules.
- **Fully type-hinted.** `mypy src` runs in CI and must pass — no new `# type: ignore`
  without a reason in the comment.
- **ruff** does both formatting and linting; don't fight it, don't disable rules
  inline unless there's a documented reason (existing `# noqa: B008` on dlt
  incremental defaults is the idiomatic exception).
- `snake_case` for functions, modules, variables; `PascalCase` for classes.
- **Module docstrings** start with the file's own path and a short description of
  its role (see any existing module). Keep that style — it's how this repo
  documents intent. Module docstrings explain *why*; keep them current when behavior changes.
- Match GCS folder names to dlt resource names exactly (see
  [STORAGE_CONVENTIONS.md](./docs/STORAGE_CONVENTIONS.md)).

## Data-engineering guardrails

These are the things that bite in data pipelines specifically. Treat them as hard rules.

- **Secrets never touch git.** `.env` and `.dlt/secrets.toml` are gitignored;
  keep it that way. Real auth uses ADC / Secret Manager. If you find a secret in
  tracked code, stop and flag it.
- **Don't develop against the production bucket.** `gs://aqueduct-production`
  is shared. For any local run, point `GCS_BUCKET_URL` at your own test bucket.
- **Idempotency is mandatory.** Re-running any asset must be safe. dlt uses
  `primary_key` + incremental cursors; FROST loads are filtered by a watermark
  (`watermark_store.py`) and committed per chunk so partial failures resume
  cleanly. Don't add logic that double-counts or that can't be safely retried.
- **Incremental by default.** Respect the dlt cursor and the transform watermark.
  Don't trigger full reloads casually — backfills are expensive (Dagster+ credits
  and API quota). Any large backfill is a reviewed, deliberate action, not a
  default.
- **Don't hand-name or move files in GCS.** dlt owns the object layout; the
  `load_id` in each filename is the transform watermark. Change layout in
  `.dlt/config.toml`, never by relocating objects. Never edit `_dlt_*` control
  files.
- **Schema changes are additive.** New fields should be added without breaking
  existing readers; coordinate anything touching the canonical model in
  `canonical/`.
- **Tests stay offline.** The suite is unit-only — no live GCS/FROST/API calls.
  Mock external I/O; never reach the network from a test.

## Git & release workflow

- Branch from the **Jira ticket** using the branch name Jira provides
  (e.g. `ST2DAT-100-add-release-please`).
- PR title is a **Conventional Commit** (`feat:`, `fix:`, `chore:`, …) — it's
  linted in CI and drives releases.
- CI (`ruff format --check`, `ruff`, `mypy src`, `pytest`) and one approval are
  required before squash-merge.
- **release-please owns versioning.** Do **not** hand-edit `CHANGELOG.md`, bump
  the version in `pyproject.toml`, or touch `.release-please-manifest.json`.
  Conventional Commit history generates all of that automatically.

## Do not (without explicit human approval)

- Commit or print secrets, keys, or service-account JSON.
- Run a production backfill or write to the shared bucket.
- Hand-edit `CHANGELOG.md`, the version, or release-please manifests.
- Delete or modify `_dlt_*` files or any GCS object directly.
- Disable a lint/type/test check to get CI green.
- Cross the canonical-model boundary (source logic leaking into loader, or vice versa).
- Add a dependency without going through `uv`.

## Definition of done

`uv run pre-commit run --all-files` is clean, `uv run pytest` passes, new
behavior has unit tests, docstrings reflect what the code now does, and — if you
changed where data lands — [STORAGE_CONVENTIONS.md](./docs/STORAGE_CONVENTIONS.md) is
updated.
