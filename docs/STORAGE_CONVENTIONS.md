# Storage Naming Conventions

How we name GCS buckets and the folders inside them for Aqueduct data.

This is a **living document** — keep it in sync with the code as the pipeline
grows. When you add a source, a zone, or a partitioning scheme, update the
[Current layout](#current-layout) section and add a line to the
[Changelog](#changelog) at the bottom.

- **Status:** raw zone only, date-partitioned, 3 source keys (PVACD via HydroVu and CABQ live; BernCo via HydroVu ingest-only)
- **Last updated:** 2026-09-02

---

## TL;DR cheat sheet

| Thing | Rule | Example |
|---|---|---|
| Bucket | lowercase, hyphen-delimited, `nmwdi-aqueduct-<env>` | `nmwdi-aqueduct-production` |
| Zone prefix | `raw_` today; reserve `staging_` / `curated_` for later | `raw_` |
| Dataset (top folder) | `raw_<source_key>`, lowercase `snake_case` | `raw_pvacd_hydrovu` |
| Table (sub-folder) | `<source>_<entity>`, lowercase `snake_case` | `hydrovu_readings` |
| Partition path | date-partitioned, Hive-style `key=value/` | `year=2024/month=06/day=18/` |
| Data file | **dlt-managed — never hand-name** | `1781192390.555875.0.parquet` |
| Control / sidecar file | leading underscore, not a data table | `_pvacd_hydrovu_transform_watermark.json` |

Three guiding rules that cover almost everything:

1. **Lowercase + hyphens for buckets, lowercase + `snake_case` for everything inside.**
2. **One folder = one logical thing.** A dataset is one agency's feed from one source system; a table folder holds one entity from it; nothing else lives in it.
3. **Don't invent file names.** dlt owns the file names inside table folders. The only files you name by hand are underscore-prefixed control files.

---

## Current layout

The standard layout — date-partitioned, built by dlt from `.dlt/config.toml`
and the pipeline factories:

```
gs://nmwdi-aqueduct-production/          # the raw-zone bucket (one per environment)
├── raw_pvacd_hydrovu/                   # PVACD's HydroVu feed — source key `pvacd_hydrovu`
│   ├── hydrovu_locations/               # HydroVu source: reference table (write_disposition="replace")
│   │   └── year=2024/month=06/day=18/
│   │       └── <load_id>.<file_id>.parquet
│   ├── hydrovu_readings/                # HydroVu source: fact table (append, incremental)
│   │   └── year=2024/month=06/day=18/
│   │       └── <load_id>.<file_id>.parquet   # e.g. 1781192390.555875.0.parquet
│   ├── hydrovu_backfill_readings/       # HydroVu Mode A backfill: separate table, own dlt
│   │   └── year=2024/month=06/day=18/   #   pipeline_name — never read by the normal scheduled
│   │       └── <load_id>.<file_id>.parquet   #   transform, so it can't interfere with production
│   ├── _pvacd_hydrovu_transform_watermark.json    # app sidecar: highest load_id transformed
│   ├── _backfill_checkpoints/           # one file per backfill run_key — completed chunks
│   │   └── pvacd_hydrovu-jan2026-repair.json  #   e.g. {"completed_chunks": ["<start>_<end>", ...]}
│   └── _dlt_*                           # dlt control tables (state, loads, version)
├── raw_pvacd_hydrovu_backfill/          # NOT a real dlt dataset — just the isolated FROST
│   └── _frost_watermarks.json           #   watermark file backfill jobs read/write, kept fully
│                                        #   separate from raw_pvacd_hydrovu/_frost_watermarks.json
├── raw_bernco_hydrovu/                  # BernCo's HydroVu feed. Same table names as PVACD's —
│   ├── hydrovu_locations/               #   the dataset is what separates them. Ingest only so
│   │   └── year=2024/month=06/day=18/   #   far: no transform, so no watermark sidecar yet.
│   │       └── <load_id>.<file_id>.parquet
│   ├── hydrovu_readings/
│   │   └── year=2024/month=06/day=18/
│   │       └── <load_id>.<file_id>.parquet
│   └── _dlt_*                           # dlt control tables — separate state from PVACD's,
│                                        #   so each tenant's cursors advance independently
├── raw_pvacd_metermanager/              # ← example: a 2nd PVACD source system (not built yet)
│   └── metermanager_readings/
│       └── year=2024/month=06/day=18/
│           └── <load_id>.<file_id>.parquet
└── raw_cabq/                            # CABQ, which exposes its data directly (scaffolded)
    └── cabq_readings/
        └── year=2024/month=06/day=18/
            └── <load_id>.<file_id>.parquet
```

dlt builds these paths from two settings:

- `bucket_url` and the date-partitioned `layout` (see
  [Date partitioning](#date-partitioning)) in `.dlt/config.toml`. 
  `bucket_url` can be overridden by setting a `GCS_BUCKET_URL` env var
- `dataset_name=` in each `build_pipeline()` (`raw_pvacd_hydrovu`, `raw_bernco_hydrovu`,
  `raw_cabq`), which dlt prepends as the top-level folder.

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
- Pattern: **`nmwdi-aqueduct-<env>`** — e.g. `nmwdi-aqueduct-production`. Use a
  separate `nmwdi-aqueduct-dev` / `nmwdi-aqueduct-stage` bucket for non-prod work
  rather than a test prefix inside production.
  - `nmwdi-` — organization prefix. This exists because of the global-uniqueness
    rule above, not as house style: the unprefixed `aqueduct-production` is already
    held by someone outside this organization, so it can never be created here.
    Project-scoped names are not enough for buckets — prefix new ones from the
    start rather than discovering the collision at provisioning time.
  - `env` — deployment context: `production`, `stage`, `dev`.
  - Agency scope is **not** in the bucket name — it lives in the dataset prefix
    (`raw_pvacd_hydrovu`, `raw_cabq`), so one production bucket holds every agency's data.

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

A dataset = one **source key** — one agency's feed from one source system. Name it
`raw_<source_key>`, using the same key as the `sources/<name>/` folder, the
`[sources.<name>]` config block, and the `SOURCE_REGISTRY` entry:
`raw_pvacd_hydrovu`, `raw_bernco_hydrovu`, `raw_cabq`, …

The source key itself is `<agency>_<source system>` when an agency's data arrives
through a named third-party platform, and just `<agency>` when the agency exposes its
data directly (hence `cabq`, not `cabq_cabq`).

**Why the key and not the agency.** Two things are true at once, and only a
source-keyed dataset covers both:

- One agency, several source systems — PVACD via HydroVu and (future) MeterManager.
- One source system, several agencies — HydroVu serves both PVACD and BernCo, on
  separate tenants with separate credentials.

Keying on the agency alone handles the first and collides on the second: both
tenants' HydroVu feeds would want `hydrovu_readings` inside the same dataset. Keying
on the source pair handles both, and gives each feed its own `_dlt_*` state, its own
`_frost_watermarks.json`, and its own transform watermark with nothing shared:

```
raw_pvacd_hydrovu/hydrovu_readings/       # PVACD's HydroVu tenant
raw_bernco_hydrovu/hydrovu_readings/      # BernCo's — same table name, different dataset
raw_pvacd_metermanager/metermanager_readings/
```

Each Dagster pipeline sets its own `dataset_name`, and no two pipelines share one.
That is the rule that makes a new tenant purely additive: nothing existing has to
move to make room for it.

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
- `_pvacd_hydrovu_transform_watermark.json` — our sidecar tracking the highest
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
layout = "{table_name}/year={YYYY}/month={MM}/day={DD}/{load_id}.{file_id}.{ext}"
```

Which produces paths like:

```
raw_pvacd_hydrovu/hydrovu_readings/year=2024/month=06/day=18/1781192390.555875.0.parquet
```

instead of the old flat form:

```
raw_pvacd_hydrovu/hydrovu_readings/1781192390.555875.0.parquet
```

**Watermark is unaffected.** `sources/pvacd_hydrovu/transform.py` derives its incremental
watermark from the `load_id` embedded in the *filename*, not from the path —
adding `year=`/`month=`/`day=` folders does not change which files it picks up,
so no transform code change is needed.

---

## Decisions & known gaps

- **Date-partition `layout` is live.** The date-partitioned `layout` is set in
  `.dlt/config.toml`. The transform layer uses a recursive `**/*.parquet` glob so
  it finds files in any depth of date subfolders. Pre-migration flat files are
  covered by the existing watermark and will not be reprocessed.
- **No staging/curated zone yet** — intentional. Revisit only when a consumer
  needs pre-FROST data on disk.

---

## Adding data — checklist

Every new feed is the same checklist, whether it's a new agency, a new source system
for an existing agency, or a second tenant on a source system already in use:

1. Pick the source key — `<agency>_<source system>`, or just `<agency>` if the agency
   exposes its data directly. It names the `sources/<name>/` folder, the
   `[sources.<name>]` config block, and the `SOURCE_REGISTRY` entry.
2. Set `dataset_name="raw_<source_key>"` in that pipeline's `build_pipeline()`, and
   the matching `dataset` in its `SOURCE_REGISTRY` entry. Never reuse another feed's
   dataset.
3. Name each dlt resource `<source>_<entity>`; the GCS table folder inherits it. Two
   tenants on one platform use the **same** table names — their datasets already
   separate them.
4. Leave `bucket_url` and `layout` alone — they're shared.
5. Any new state file → `_<purpose>.json` inside that feed's dataset folder.
6. Update [Current layout](#current-layout) and add a [Changelog](#changelog) line.

---

## Changelog

| Date | Change |
|---|---|
| 2026-06-22 | Initial version. Covers the `aqueduct-production` raw-zone bucket, agency datasets (`raw_pvacd`, `raw_cabq`), `<source>_<entity>` tables, date-partitioned (`year=/month=/day=`) dlt layout, and the sidecar-file convention. |
| 2026-06-24 | Applied date-partitioned layout to `.dlt/config.toml` (dlt tokens `{YYYY}/{MM}/{DD}`). Updated transform globs to `**/*.parquet` for recursive subdirectory traversal. Corrected layout token format throughout (was `{year}/{month}/{day}`). |
| 2026-07-20 | Added `hydrovu_backfill_readings` (Mode A backfill refetch, ST2DAT-202) — a separate table under `raw_pvacd`, not `hydrovu_readings`, so the normal scheduled transform never reads it. Added the `_backfill_checkpoints/{run_key}.json` control-file convention for per-chunk resume. |
| 2026-07-27 | Isolated the backfill job's FROST watermark from production's — added `raw_pvacd_backfill/_frost_watermarks.json`, a distinct file from `raw_pvacd/_frost_watermarks.json`, so a backfill run can no longer race with or silently advance the daily scheduled pipeline's own watermark. |
| 2026-08-11 | Adopted the `nmwdi-` bucket prefix: the pattern is now `nmwdi-aqueduct-<env>` and the production bucket is `gs://nmwdi-aqueduct-production`. The unprefixed `aqueduct-production` referenced in earlier entries was never created — the name is held by another organization, and GCS bucket names are globally unique. `bucket_url` in `.dlt/config.toml` still points at `gs://aqueduct-poc-bravo-pvacd`; moving it is a separate ticket. |
| 2026-08-13 | Production moved onto `gs://nmwdi-aqueduct-production` via `GCS_BUCKET_URL` on the Dagster+ full deployment; the committed `bucket_url` stays on `gs://aqueduct-poc-bravo-pvacd` so local runs cannot default to production. Production started **empty** — no data was copied — so dlt cursors restarted from `initial_start_date` and raw parquet from before this date exists only in the POC bucket. |
| 2026-08-28 | Datasets are now keyed on the **source key**, `raw_<source_key>`, not on the agency (ST2DAT-241). `raw_pvacd` became `raw_pvacd_hydrovu`, and BernCo's HydroVu tenant will land at `raw_bernco_hydrovu`. The old agency rule could not express two agencies on one source system: both tenants' HydroVu feeds would have wanted `hydrovu_readings` in one dataset. Table names are unchanged — `hydrovu_readings` and `hydrovu_locations` stay as they are under both tenants. Nothing was copied: `raw_pvacd/` is left in place, orphaned, and the renamed dataset starts empty, so PVACD's dlt cursors restarted from `initial_start_date`. |
| 2026-09-02 | `raw_bernco_hydrovu` exists for real (ST2DAT-130): BernCo's HydroVu tenant now lands `hydrovu_locations` and `hydrovu_readings` through its own dlt pipeline, with the same table names as PVACD and its own `_dlt_*` state. Ingest only — there is no transform yet, so no `_bernco_hydrovu_transform_watermark.json` and no `_frost_watermarks.json` under it. The `location_ids` allowlist in `.dlt/config.toml` is deliberately incomplete until the full DTW well list is pulled from a live `/locations/list`. |
