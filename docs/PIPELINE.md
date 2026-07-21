# Pipeline: End-to-End (PVACD HydroVu reference)

How data moves from a source API into FROST. HydroVu is the **reference
implementation** 
see the [checklist](#adding-a-new-source-checklist) at the bottom.


---

## TL;DR

```
HydroVu API → dlt → GCS (parquet) → Adapter → CanonicalBundle → FROST loader → FROST
```

| Stage | Dagster asset | Code |
|---|---|---|
| 1. Ingest | `raw_hydrovu_readings` | [sources/hydrovu/ingest.py](../src/aqueduct_dagster/sources/hydrovu/ingest.py), [dlt_pipeline.py](../src/aqueduct_dagster/sources/hydrovu/dlt_pipeline.py) |
| 2. Transform | `canonical_bundles_hydrovu` | [sources/hydrovu/transform.py](../src/aqueduct_dagster/sources/hydrovu/transform.py), [adapter.py](../src/aqueduct_dagster/sources/hydrovu/adapter.py) |
| 3. Load | `frost_load_hydrovu` | [defs/assets/load.py](../src/aqueduct_dagster/defs/assets/load.py), [loader/frost_loader.py](../src/aqueduct_dagster/loader/frost_loader.py) |

One job (`hydrovu_pipeline`), one daily schedule (`hydrovu_schedule`, cron
`0 6 * * *`), no Dagster partitioning — each run just processes everything new
since the last watermark. Both the job and schedule are generated from a
single config entry, not hand-written (see [Wiring](#wiring)).

---

## Stage 1 — Ingest: `raw_hydrovu_readings`

No upstream deps — this is the entry point.

[`ingest.py`](../src/aqueduct_dagster/sources/hydrovu/ingest.py) wraps a dlt
pipeline run in `forward_python_logs_to_dagster` (so dlt's stdlib logging
shows up in the Dagster UI) and calls `pipeline.run(hydrovu_source(...),
loader_file_format="parquet")`. It raises `dagster.Failure` only if *every*
location errored; otherwise it returns a `MaterializeResult` with metadata
(`rows_yielded`, `locations_fetched/skipped/no_data/errored`,
`failed_location_ids`).

[`dlt_pipeline.py`](../src/aqueduct_dagster/sources/hydrovu/dlt_pipeline.py)
defines the actual dlt source:

- `hydrovu_source()` — fetches OAuth creds from GCP Secret Manager (secret
  `hydrovu_pvacd`, see [Config](#config)), builds one shared `httpx.Client` via
  `build_authenticated_client()` + `BearerAuth` (shared infra, see
  [shared/http.py](../src/aqueduct_dagster/shared/http.py)), fetches the
  location list once, and returns two dlt resources:
  - **`hydrovu_locations`** — full refresh (`write_disposition="replace"`).
    Reference data: `{id, name, description, latitude, longitude}` per
    location.
  - **`hydrovu_readings`** — incremental (`write_disposition="append"`,
    `primary_key="reading_id"`). A **per-location cursor** lives in
    `dlt.current.resource_state()["location_cursors"]`, keyed by
    `str(location_id)`, and only advances after that location's fetch
    succeeds — a failed location retries from its own last-good cursor next
    run without blocking the others. Locations outside the `location_ids`
    allowlist (config) are skipped up front.
- Both resources page through the API using the `X-ISI-Start-Page` /
  `X-ISI-Next-Page` cursor headers, with `retry_transient` handling
  429/5xx/transient failures (429 respects `Retry-After`, falling back to a
  60s backoff, capped at 3 retries).
- Output lands as parquet in GCS (`raw_pvacd` dataset — see
  [STORAGE_CONVENTIONS.md](STORAGE_CONVENTIONS.md)).

## Stage 2 — Transform: `canonical_bundles_hydrovu`

Deps: `raw_hydrovu_readings`.

[`transform.py`](../src/aqueduct_dagster/sources/hydrovu/transform.py):

1. Reads the **transform watermark** (the highest dlt `load_id` already
   processed, stored as a GCS sidecar file — see
   [STORAGE_CONVENTIONS.md](STORAGE_CONVENTIONS.md#control--sidecar-files)).
2. Uses the shared `read_new_parquet_rows()` helper
   ([shared/gcs.py](../src/aqueduct_dagster/shared/gcs.py)) to read only
   parquet rows written *since* that watermark, filtered at read time to
   `parameter_id == "4"` (Depth to Water).
3. If there are no new rows, returns an empty result immediately — the
   locations file isn't even read.
4. Otherwise reads the `hydrovu_locations` parquet into a
   `{location_id: {...}}` dict, and `_group_by_location()` joins readings +
   location metadata into one record per location — the exact shape
   `HydroVuAdapter` expects: `{location_id, location_name,
   location_description, latitude, longitude, readings: [...]}`.
5. Instantiates `HydroVuAdapter(records)` and calls `list(adapter.run())`.
6. Returns `HydroVuTransformResult(bundles, max_load_id)`. **The watermark is
   not committed here** — it only advances after Stage 3 confirms FROST
   accepted the data (see [Idempotency](#idempotency--watermarks)).

[`adapter.py`](../src/aqueduct_dagster/sources/hydrovu/adapter.py)
(`HydroVuAdapter`, subclass of the shared `BaseAdapter`) converts one grouped
record into canonical entities:

- `to_thing()` — builds `CanonicalLocation` + `CanonicalThing`
  (`agency="PVACD"`, `source_id=str(location_id)`, well number stored in
  `properties.source_specific.hydrovu_description`).
- `to_observations()` — filters to `parameter_id="4"`, converts metres → feet
  (`× 3.28084`), one `CanonicalObservation` per reading.
- `_build_datastreams()` — one `CanonicalDatastream` per thing (DTW only),
  using shared constants `HYDROVU_SENSOR`, `DTW_OBS_PROP`, `UNIT_FOOT` from
  [canonical/canonical_constants.py](../src/aqueduct_dagster/canonical/canonical_constants.py).

`BaseAdapter.run()`
([canonical/base_adapter.py](../src/aqueduct_dagster/canonical/base_adapter.py),
shared by every source) drives all three per record: `to_thing()` →
`to_observations()` → bucket observations by `datastream_external_key` →
`_build_datastreams(thing)` → yield one `CanonicalBundle`. A single bad
record is caught, logged, and skipped — it doesn't fail the whole run.

For the full field-by-field mapping (source field → canonical field, with
real API evidence), see
[docs/sources/pvacd_hydrovu.md](sources/pvacd_hydrovu.md). For the shape of
the canonical model itself, see
[canonical/CANONICAL_MODEL.md](../src/aqueduct_dagster/canonical/CANONICAL_MODEL.md).

## Stage 3 — Load: `frost_load_hydrovu`

Deps: `canonical_bundles_hydrovu` (via `AssetIn`).

This stage has **no HydroVu-specific code** — it's generated once, generically,
for every source in `SOURCE_REGISTRY` (see [Wiring](#wiring)).
[`defs/assets/load.py`](../src/aqueduct_dagster/defs/assets/load.py) builds a
`FrostStaClientLoader`
([loader/frost_loader.py](../src/aqueduct_dagster/loader/frost_loader.py))
and, for every bundle/datastream:

1. `ensure_datastream()` — idempotently upserts the metadata graph in a fixed
   order: Location → Thing → Sensor → ObservedProperty → Datastream. Each step
   looks up by `externalId` first and only creates if missing, so re-running
   never duplicates entities. Links between entities are always ID-only
   references (e.g. `fsc.Location(id=...)`), never nested objects.
2. `load_observations()` — sorts records by `phenomenon_time`, filters out
   anything at or before the cached watermark (recovered from FROST's own
   `MAX(phenomenonTime)` if nothing's cached yet), and posts the remainder in
   chunks of 1000 via FROST's DataArray endpoint. **The watermark advances
   after every chunk**, so a mid-run failure only re-posts the last unfinished
   chunk on retry, not the whole backlog.
3. Only if this succeeds does the asset call `commit_watermark()` to advance
   the Stage 2 transform watermark — so a FROST failure never lets Stage 2
   skip past unprocessed data on the next run.

---

## Wiring

[`shared/source_registry.py`](../src/aqueduct_dagster/shared/source_registry.py)
is the single source of truth per source:

```python
SOURCE_REGISTRY: list[SourceConfig] = [
    {"name": "hydrovu", "dataset": "raw_pvacd", "cron": "0 6 * * *"},
    {"name": "cabq", "dataset": "raw_cabq", "cron": "0 8 * * *"},
]
```

[`defs/definitions.py`](../src/aqueduct_dagster/defs/definitions.py) loops
over this list to generate, per entry: a job (`{name}_pipeline`, selecting
`raw_{name}_readings` → `canonical_bundles_{name}` → `frost_load_{name}`) and
a schedule (`{name}_schedule`, using `cron`). Assets themselves are
auto-discovered via `load_assets_from_package_module` — nothing is
hand-registered per source. Adding a new source pipeline to the job/schedule
system means adding **one entry** to `SOURCE_REGISTRY`, nothing else.

## Idempotency & watermarks

Every stage is safe to re-run:

| Stage | Mechanism |
|---|---|
| Ingest | dlt `primary_key="reading_id"` (dedup on write) + per-location incremental cursor in dlt state |
| Transform | GCS sidecar watermark file, keyed on the highest dlt `load_id` processed — only committed after Load succeeds |
| Load | `WatermarkStore` per datastream (`FrostWatermarkStore`, GCS-backed JSON), advanced per chunk; recoverable from FROST's own `MAX(phenomenonTime)` if the store has nothing cached; entity upserts are find-or-create by `externalId` |

## Config

All HydroVu-specific settings live in `.dlt/config.toml` under
`[sources.hydrovu]`: `api_base_url`, `token_url`, `gcp_secret` (Secret Manager
secret name — real credentials never touch git), `initial_start_date`, and an
explicit `location_ids` allowlist. FROST's target URL is under
`[destination.frost]`. GCS bucket/layout is under `[destination.filesystem]`
— see [STORAGE_CONVENTIONS.md](STORAGE_CONVENTIONS.md) for the layout
convention itself.

## Tests

Mirrors `src/` layout, unit-only (no live GCS/FROST/API calls — see
[AGENTS.md](../AGENTS.md)):

- `tests/sources/hydrovu/test_adapter.py` — `HydroVuAdapter` against mock
  grouped records.
- `tests/sources/hydrovu/test_dlt_pipeline.py` — pagination, auth-retry,
  404/429/5xx handling, cursor behavior, via `httpx.MockTransport`.
- `tests/shared/test_http.py`, `tests/shared/test_gcs.py` — shared infra
  (`TokenManager`/`BearerAuth`/`retry_transient`, `read_new_parquet_rows`).
- `tests/loader/test_frost_loader.py`, `tests/loader/test_watermark_store.py`
  — FROST upsert/retry behavior and watermark persistence, against test
  doubles.

## Adding a new source checklist

1. Write `docs/sources/<name>.md` from
   [`_mapping_template.md`](sources/_mapping_template.md), filled against a
   real sample response
2. Create `sources/<name>/` with `adapter.py`, `dlt_pipeline.py`,
   `ingest.py`, `transform.py` — mirror HydroVu's structure.
3. Add one entry to `SOURCE_REGISTRY` — this alone generates the job,
   schedule, and `frost_load_<name>` asset.
4. Follow [STORAGE_CONVENTIONS.md](STORAGE_CONVENTIONS.md) for the GCS
   dataset/table names.
5. Do not touch `loader/` or `canonical/` unless the canonical model itself
   is missing a field — those stay source-agnostic (see
   [AGENTS.md](../AGENTS.md#the-one-rule-that-explains-the-design)).

---

## Changelog

| Date | Change |
|---|---|
| 2026-07-20 | Initial version, based on the live PVACD HydroVu pipeline. |
