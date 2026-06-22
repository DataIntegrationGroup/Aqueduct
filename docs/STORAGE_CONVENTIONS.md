# Storage Naming Conventions

How we name GCS buckets and the folders inside them for Aqueduct data.

This is a **living document** — keep it in sync with the code as the pipeline
grows. When you add a source, a zone, or a partitioning scheme, update the
[Current layout](#current-layout) section and add a line to the
[Changelog](#changelog) at the bottom.

- **Status:** raw zone only, date-partitioned, 2 agencies (PVACD via HydroVu live; CABQ scaffolded)
- **Last updated:** 2026-06-22

---

## TL;DR cheat sheet

| Thing | Rule | Example |
|---|---|---|
| Bucket | lowercase, hyphen-delimited, `aqueduct-<env>` | `aqueduct-production` |
| Zone prefix | `raw_` today; reserve `staging_` / `curated_` for later | `raw_` |
| Dataset (top folder) | `raw_<agency>`, lowercase `snake_case` | `raw_pvacd` |
| Table (sub-folder) | `<source>_<entity>`, lowercase `snake_case` | `hydrovu_readings` |
| Partition path | date-partitioned, Hive-style `key=value/` | `year=2024/month=06/day=18/` |
| Data file | **dlt-managed — never hand-name** | `1781192390.555875.0.parquet` |
| Control / sidecar file | leading underscore, not a data table | `_hydrovu_transform_watermark.json` |

Three guiding rules that cover almost everything:

1. **Lowercase + hyphens for buckets, lowercase + `snake_case` for everything inside.**
2. **One folder = one logical thing.** A dataset is one agency; a table folder holds one source's entity; nothing else lives in it.
3. **Don't invent file names.** dlt owns the file names inside table folders. The only files you name by hand are underscore-prefixed control files.

---

## Current layout

The standard layout — date-partitioned, built by dlt from `.dlt/config.toml`
and the pipeline factories:

```
gs://aqueduct-production/                # the raw-zone bucket (one per environment)
├── raw_pvacd/                           # agency PVACD — can hold several source tables
│   ├── hydrovu_locations/               # HydroVu source: reference table (write_disposition="replace")
│   │   └── year=2024/month=06/day=18/
│   │       └── <load_id>.<file_id>.parquet
│   ├── hydrovu_readings/                # HydroVu source: fact table (append, incremental)
│   │   └── year=2024/month=06/day=18/
│   │       └── <load_id>.<file_id>.parquet   # e.g. 1781192390.555875.0.parquet
│   ├── metermanager_readings/           # ← example: a 2nd PVACD source (not built yet)
│   │   └── year=2024/month=06/day=18/
│   │       └── <load_id>.<file_id>.parquet
│   ├── _hydrovu_transform_watermark.json    # app sidecar: highest load_id transformed
│   └── _dlt_*                           # dlt control tables (state, loads, version)
└── raw_cabq/                            # agency CABQ (scaffolded)
    └── cabq_readings/
        └── year=2024/month=06/day=18/
            └── <load_id>.<file_id>.parquet
```

dlt builds these paths from two settings:

- `bucket_url = "gs://aqueduct-production"` and the date-partitioned
  `layout` (see [Date partitioning](#date-partitioning)) in `.dlt/config.toml`
- `dataset_name=` in each `build_pipeline()` (`raw_pvacd`, `raw_cabq`), which dlt
  prepends as the top-level folder.

So every object lands at:
`gs://<bucket>/<dataset_name>/<table_name>/year=<y>/month=<m>/day=<d>/<load_id>.<file_id>.<ext>`.

---

## The rules in detail

### Buckets

Follow the safe subset of GCS bucket rules — it's also what's most portable
across tools:

- Lowercase letters, numbers, and hyphens only. (GCS also permits underscores
  and dots, but avoid them; dots turn a bucket into a "domain-named" bucket with
  extra verification rules.)
- Start and end with a letter or number; 3–63 characters; **globally unique
  across all of GCS**.
- Pattern: **`aqueduct-<env>`** — e.g. `aqueduct-production`. Use a separate
  `aqueduct-dev` / `aqueduct-stage` bucket for non-prod work rather than a test
  prefix inside production.
  - `env` — deployment context: `production`, `stage`, `dev`.
  - Agency scope is **not** in the bucket name — it lives in the dataset prefix
    (`raw_pvacd`, `raw_cabq`), so one production bucket holds every agency's data.

One bucket per environment keeps IAM and lifecycle rules simple. Don't split a
single logical dataset across multiple buckets.

### Zones

A zone says *how processed* the data is. We use the medallion idea, kept minimal:

| Zone | Prefix | Status |
|---|---|---|
| Raw (as-ingested, untransformed) | `raw_` | **in use** |
| Staging (cleaned / conformed) | `staging_` | reserved — add when needed |
| Curated (analysis-ready) | `curated_` | reserved — add when needed |

Today GCS holds **only the raw zone** — transform reads raw parquet, builds
`CanonicalBundle`s in memory, and loads straight to FROST. There is no staging
or curated zone on disk yet. Don't create one until there's a real consumer for
it; when you do, reuse the same dataset/table rules below with the new prefix.

### Datasets (top-level folders)

A dataset = one **agency** — the organization that owns the data (PVACD, CABQ).
Name it `raw_<agency>`: `raw_pvacd`, `raw_cabq`, …

An agency can expose data through **more than one source system** (HydroVu,
MeterManager, a CKAN portal, …). All of an agency's sources live under that one
agency dataset; the source name lives in the *table* prefix, not the dataset. So
PVACD's HydroVu and (future) MeterManager feeds both sit under `raw_pvacd`:

```
raw_pvacd/hydrovu_readings/
raw_pvacd/metermanager_readings/
```

Each Dagster pipeline sets a `dataset_name`. When an agency gains a second
source, that source's pipeline sets the **same** `dataset_name` as the agency
(`raw_pvacd`) and just uses a distinct table name — multiple pipelines can share
one agency dataset as long as their table names don't collide.

### Tables (sub-folders)

A table = one **entity from one source system** within the agency. Name it
`<source>_<entity>`, where `<source>` is the platform the data comes through:

- `hydrovu_locations` — HydroVu reference data (a place, a sensor, a site)
- `hydrovu_readings` — HydroVu fact/measurement stream
- `metermanager_readings` — a second PVACD source's stream (illustrative)
- `cabq_readings` — CABQ's stream

When an agency exposes its data directly rather than via a named third-party
system, the source prefix is just the agency name (as with `cabq_`).

Match the table name to the dlt `@dlt.resource(name=...)` exactly, so the GCS
folder and the dlt resource never drift apart.

### Data files

**Never name these by hand.** dlt writes them using the configured `layout`:
`{load_id}.{file_id}.{ext}` (e.g. `1781192390.555875.0.parquet`). The `load_id`
is the float Unix timestamp dlt stamps on every run — the transform step uses it
as its incremental watermark, so the names are load-bearing. Renaming or
reformatting them will break incremental reads.

The physical path under each table folder is **date-partitioned** — see
[Date partitioning](#date-partitioning). Change layout only in
`.dlt/config.toml`, never by moving files around.

### Control / sidecar files

Anything that isn't a data table gets a **leading underscore** so it's visually
and lexically separated from real data:

- `_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version` — dlt's own bookkeeping.
  Treat as read-only; never edit or delete.
- `_hydrovu_transform_watermark.json` — our sidecar tracking the highest
  `load_id` already transformed.

New app-managed state files follow the same pattern: `_<purpose>.json`, written
into the dataset folder they belong to.

### Date partitioning

Data files are written under a **Hive-style date hierarchy** (`key=value/`
folders — the convention every query engine and lifecycle tool understands), not
flat in a single prefix.

**Why this is the standard, not optional.** A flat layout writes one parquet file
per run directly under the table prefix — about 365 files per table after a year
of daily runs, all in one folder. That prevents GCS lifecycle policies,
date-range browsing, and compaction, and the prefix gets slower to list as it
grows. Date folders fix all of these.

Set this in `.dlt/config.toml` (dlt prepends `dataset_name` automatically):

```toml
[destination.filesystem]
layout = "{table_name}/year={year}/month={month}/day={day}/{load_id}.{file_id}.{ext}"
```

Which produces paths like:

```
raw_pvacd/hydrovu_readings/year=2024/month=06/day=18/1781192390.555875.0.parquet
```

instead of the old flat form:

```
raw_pvacd/hydrovu_readings/1781192390.555875.0.parquet
```

**Watermark is unaffected.** `transform_hydrovu.py` derives its incremental
watermark from the `load_id` embedded in the *filename*, not from the path —
adding `year=`/`month=`/`day=` folders does not change which files it picks up,
so no transform code change is needed.

---

## Decisions & known gaps

- **Date-partition `layout` must be live in config.** The date-partitioned
  `layout` (above) needs to be set in `.dlt/config.toml`. Until it is, dlt writes
  flat files under the table prefix. Applying it is a one-line, transform-safe
  change (the watermark reads `load_id` from the filename, not the path). Any
  existing flat files can be left in place or back-filled into date folders as a
  separate task.
- **No staging/curated zone yet** — intentional. Revisit only when a consumer
  needs pre-FROST data on disk.

---

## Adding data — checklist

**New source under an existing agency** (e.g. MeterManager for PVACD):

1. Name each dlt resource `<source>_<entity>`; the GCS table folder inherits it.
2. Set the pipeline's `dataset_name` to the **existing** agency dataset
   (`raw_<agency>`). Pipelines can share it — just don't reuse a table name.
3. Leave `bucket_url` and `layout` alone — they're shared.
4. Any new state file → `_<purpose>.json` inside the agency dataset folder.
5. Update [Current layout](#current-layout) and add a [Changelog](#changelog) line.

**New agency** (a brand-new data provider):

1. Pick the dataset name `raw_<agency>` and set `dataset_name="raw_<agency>"` in
   that pipeline's `build_pipeline()`.
2. Then follow steps 1 and 3–5 above for its first source.

---

## Changelog

| Date | Change |
|---|---|
| 2026-06-22 | Initial version. Covers the `aqueduct-production` raw-zone bucket, agency datasets (`raw_pvacd`, `raw_cabq`), `<source>_<entity>` tables, date-partitioned (`year=/month=/day=`) dlt layout, and the sidecar-file convention. |
