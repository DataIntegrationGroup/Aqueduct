# Source Research: NM Water Data Catalog (CKAN)

**Platform:** CKAN, hosted by datHere (the catalog footer reads "Powered by datHere")
**Base URL:** `https://catalog.newmexicowaterdata.org` (CNAME to `newmexico.opendataportal.us`)
**Agencies investigated:** EBWPC and OSE Roswell (District 2). The same approach applies to any other
agency that publishes water levels to the catalog; City of Santa Fe and City of Deming are wanted later.
City of Roswell is out of scope: its data is in a GCS bucket, not on CKAN.
**Response format:** CKAN Action API JSON for metadata, CSV (legacy XLSX) resource files for data
**Source timezone:** not stated by any source, and not covered by the WDI best practices, so it is
decided per source by checking timestamps for missing or repeated hours on DST dates. An undecided
timezone blocks that source's production backfill. EBWPC's loggers run on a fixed UTC−07:00 (see
[EBWPC](#ebwpc-estancia-basin-water-planning-committee)).
**Update frequency:** irregular, and publication lags collection. EBWPC collects semi-annually and OSE
District 2 annually each winter. As of Jan 2026, EBWPC's newest reading was 2024-04-26 and its files were
last re-uploaded 2025-03-12.

---

## Summary

- **Recommendation:** use the CKAN Action API to find resources and detect changes, then download the
  raw resource file for the data. It is a hybrid of the two options the ticket asked about.
  - One `package_show` call per dataset per run lists every resource with its `last_modified` / `size`.
  - Only resources whose fingerprint changed are downloaded (from `resource.url`) and landed in full.
- **Not the DataStore API:** its inferred types corrupt values, it truncates at 100 rows unless paged,
  and it only exists when the catalog's push job succeeded. The legacy loader already abandoned it.
- **Not hardcoded spreadsheet URLs:** the filename in the URL changes on re-upload, and new wells arrive
  as new resources that a fixed list never sees. See [Options Compared](#options-compared).
- **One shared adapter.** Each agency's shim coerces its files into the **WDI best-practice location +
  measurement shape** (see [WDI Best Practices](#wdi-best-practices-and-what-they-mean-for-ingest)), and
  one shared adapter maps that shape to `CanonicalBundle`. A future source that already follows the best
  practices needs configuration only.
- **"Less robust" is fine here and is designed in:** a weekly schedule, no pagination, cursor or
  rate-limit machinery, a changed file simply lands again in full, and the first run *is* the backfill.

**Access.** Cloudflare returns `403` to every scripted client unless the request carries the
`x-cf-bypass` header with the value datHere issued. With it, the API and file downloads all work. Keep
the value in GCP Secret Manager, never in code or docs, and confirm it works from Dagster+. See
[Access](#access-cloudflare).

---

## Automated Ingest

### Platform and Endpoints

A standard CKAN instance with FileStore uploads, the DataStore extension (populated by the catalog's
pusher, most likely datHere's DataPusher+) and ckanext-dcat. All read endpoints are unauthenticated, but
every request must carry the `x-cf-bypass` header (value from Secret Manager) to get past Cloudflare
(see [Access](#access-cloudflare)). File downloads are served directly, with no redirect.

| Endpoint | Use | Gotchas |
|---|---|---|
| `GET /api/3/action/package_show?id={uuid}` | **Discovery and change detection.** One call returns every resource with `id`, `name`, `url`, `format`, `last_modified`, `size`, `hash`, `datastore_active`. | Configure the dataset UUID, not the renamable slug. UUIDs survived the catalog upgrade around 2026-03-19. Don't trust `hash`: it matches the downloaded file's MD5 for only 9 of EBWPC's 22 resources (the eight created in August 2024 and one XLSX), so hash the body yourself. `format` is free text (`CSV` and `.csv` both appear), so normalise it. |
| `GET /api/3/action/package_search?fq=organization:{org}&rows=1000` | Optional: detect new datasets from an organization. | Not needed for a fixed dataset list. |
| `GET {resource.url}`, i.e. `/dataset/{package_id}/resource/{resource_id}/download/{filename}` | **The data.** Exactly the bytes the agency uploaded. | `{filename}` changes when the agency re-uploads under a new name or format. Always take the URL from `package_show`, never from config. |
| `GET /api/3/action/datastore_search?resource_id={id}&limit={n}&offset={n}` | Typed JSON rows, if the resource was pushed. | **Default `limit` is 100**, so you must paginate. Types are inferred at push time. Exists only if `datastore_active`. |
| `GET /datastore/dump/{resource_id}?format=csv` | Full table in one response. | Same inferred types and `datastore_active` dependency. Archived dumps from 2021 to 2023 confirm it was enabled. |

### Options Compared

| | **A. Action API + file download** (recommended) | **B. DataStore API** | **C. Hardcoded spreadsheet URLs** |
|---|---|---|---|
| **Fidelity** | Exact agency bytes. | Types inferred at push time, so mixed `N/A`/number columns get coerced. | Exact bytes. |
| **Finds new wells and resources** | Yes: `package_show` lists them. | Only with `package_show`. | **No**. |
| **Survives re-uploads and renames** | Yes: the URL is read fresh every run. | Yes (keyed by resource ID). | **No**. EBWPC's XLSX->CSV switch changed every URL. |
| **Change detection, fit for rare updates** | `last_modified` / `size` per resource, so a weekly run is nearly always a no-op. | Same metadata, but a changed resource must be re-paged. | None: re-downloads unchanged data forever. |
| **Depends on the pusher job** | No. | **Yes.** No table when the push failed or the file is multi-sheet XLSX. | No. |
| **Pagination / truncation risk** | None. | **Yes.** DIE's OSE connector never paginates and gets 100 of ~1,003 rows. | None. |
| **XLSX** | Needs `openpyxl`, or skip. | Handled server-side, when the push worked. | Needs `openpyxl`. |
| **Effort** | Low: one metadata call, one GET per changed file, stdlib `csv`. | Medium: pagination and type un-coercion. | Lowest, but brittle. |

**Why A over B in one line:** for a clean WDI-style CSV the file and the DataStore hold the same rows, and
the file has none of B's failure modes. The legacy loader
[moved off the DataStore](https://github.com/NMWDI/CloudFunctions/blob/main/stao/ckan_stao.py) in a
2025-03-19 commit titled "use resource_show instead of datastore/dump".

### Recommended Design

#### Flow

```
package_show(dataset_id)                       one API call per dataset
  └─ for each resource matching the agency's selector (name / normalised format):
       fingerprint = (resource.id, last_modified, size, url)
       unchanged vs dlt resource state?  -> skip                (the normal case)
       changed   -> GET resource.url      -> sha256(body)
                   body hash unchanged?  -> update state, skip  (re-upload of the same file)
                   parse CSV             -> yield every row as strings + provenance columns
  └─ log added / removed / changed / skipped resources as asset metadata
```

#### Raw layer (dlt -> GCS)

- Land **every cell as a string**, plus provenance columns: `resource_id`, `resource_name`,
  `resource_last_modified`, `source_url`, `row_number`. Parsing belongs in the adapter, so the raw layer
  always reproduces what the agency published.
- Use `write_disposition="append"`, gated by the fingerprint. A changed resource lands a **complete new
  snapshot** under a new `load_id`, which the transform watermark picks up as usual
  (`read_new_parquet_rows_for_asset` in `defs/dagster_logging.py`).
  - **Don't use `replace`.** On the filesystem destination it truncates the whole table, wiping
    unchanged resources that share it.
  - Snapshots are small (EBWPC's whole dataset is ~35 MB), so keeping every version is cheap and gives
    an audit trail.
- Keep fingerprints in `dlt.current.resource_state()`, as `cabq` and `bernco_manual` do for their cursors.
- Select resources per agency by name and normalised format, never by a hardcoded ID list. Agencies don't
  always update in place: OSE published revised files as new resources alongside the old ones (see
  [OSE Roswell](#ose-roswell-district-2)). Surface added and removed resources in asset metadata so a
  person notices.

#### Transform (raw -> WDI shape -> canonical)

Two layers, so agency-specific mess stays in one small place.

**1. Agency shim**, the only per-agency code. It coerces raw rows into the WDI location + measurement
columns ([field mapping](#field-mapping-wdi---canonical)): lower-case and strip headers, drop empty trailing
columns, parse the agency's date/time formats to ISO, unpivot split depth columns (`transducer_depth_bgs`,
`manual_depth_bgs`) into `depth_to_water` + `measurement_type`, null junk values (`N/A`, `#VALUE!`,
`#REF!`, `<Null>`, blank), drop exact-duplicate rows, swap reversed lat/lon, convert UTM to lat/lon.

**2. Shared WDI adapter.** Maps WDI rows to `CanonicalBundle` and enforces the best-practice rules: drop
measurements whose `site_id` has no location with x/y, require `depth_to_water_unit` (convert to feet,
refuse unknown units), and pick the Manual vs continuous datastream from `measurement_type`.

**FROST dedup caveat.** Re-emitting a whole file is safe, because the loader's per-datastream
`phenomenonTime` watermark (`loader/watermark_store.py`) skips anything at or before the last loaded
observation. The flip side: **a corrected or backfilled value older than the watermark never reaches
FROST**. EBWPC says its transducer data is drift-corrected, so such corrections are plausible. This is
accepted for now. The fix belongs in the loader (e.g. a per-source "reload datastream when the snapshot
changed" mode) and is future work after the migration.

#### Schedule and failure handling

- **Weekly cron**, e.g. `"0 10 * * 1"`, in the `SourceConfig` entry (`shared/source_registry.py`
  already takes a per-source `cron` string). Daily would also cost one metadata call, but weekly matches
  how often the data moves.
- **No pagination, rate-limit or `Retry-After` handling.** Wrap each GET in `retry_transient()`
  (`shared/http.py`).
- **Per-resource failures** are logged and skipped, leaving the fingerprint unchanged so the resource
  retries next run. Fail the asset only if *every* selected resource failed, as `cabq/ingest.py` does.
- **No backfill job.** The first run downloads every file, and that is the backfill. The 17 archived
  EBWPC CSVs alone hold ~762k rows, which the FROST loader already chunks.

#### Module layout (follows the HydroVu tenant pattern)

| Module | Owns |
|---|---|
| `sources/ckan_common.py` | httpx client (`build_unauthenticated_client` plus the `x-cf-bypass` header from Secret Manager, overriding `Accept` for file downloads), `package_show`, fingerprinting, download, CSV -> string rows, provenance columns |
| `sources/wdi_transform_common.py` | The WDI location + measurement -> `CanonicalBundle` adapter and its validation rules |
| `sources/<agency>_ckan/` | Dataset UUID, resource selector, agency shim, `raw_<key>` dataset, dlt resource names. One `SourceConfig` entry each. |

**Source keys and agency codes:** `ebwpc_ckan` / `EBWPC` and `nmose_ckan` / `NMOSE`, following the
`<agency>_<source system>` rule in `STORAGE_CONVENTIONS.md`. Codes are upper-case and per agency, like
`PVACD`, `CABQ` and `BERNCO`, so OSE's district and basin go in `source_specific`. Old FROST used
`OSE-Roswell`.

**Dependencies and canonical touch points:**
- CSV needs only the stdlib `csv` module. EBWPC's one XLSX resource (`E-9407-Archived`) is skipped for
  now and logged in metadata, and EBWPC will be asked to re-upload it as CSV, so `openpyxl` isn't needed.
- EBWPC's locations are UTM-only, so the shim needs `pyproj`, which isn't in `uv.lock` today.
- **Sensors:** `canonical_constants.py` has only `MANUAL_SENSOR` and `HYDROVU_SENSOR`. EBWPC needs a
  pressure transducer (old FROST called it `Transducer`), which is in the Sensor list in
  `_mapping_template.md`. Coordinate the change in `canonical/`.
- **Observation parameters:** WDI's desired measurement fields match the standard `parameters` keys in
  `_mapping_template.md` one-to-one, so no new keys are needed.

**Tests** stay offline per the repo rule. Use `package_show` payloads and short sanitised CSV fixtures
behind `httpx.MockTransport` (`tests/conftest.py` helpers). Cover fingerprint skip, body-hash skip, a
per-resource failure, and every date format and junk value listed in the survey.

---

## Access (Cloudflare)

Without the `x-cf-bypass` header, Cloudflare answers every scripted request on every path with `403`
and a managed challenge (`cf-mitigated: challenge`) that a Dagster+ run can't pass. It is a datHere
platform setting, and datHere runs the catalog for NMBGMR/WDI under an NMT
[sole-source contract](https://www.nmt.edu/finance/purchasing/Dathere_Sole%20Source%20Justification.pdf),
so changes to the exception go through WDI's owner of the datHere relationship (to be documented), who
should hear before the value changes or is rotated.

- **Header:** `x-cf-bypass`, with the value datHere issued. Store it in GCP Secret Manager, like the
  HydroVu credentials, and have `ckan_common`'s client add it. Never put it in code, config or docs.
- **Only the header matters.** `User-Agent` and `Accept` make no difference: browser-like headers without
  it are still challenged, and curl's default User-Agent with it gets `200`.
- **Covers what the pipeline needs.** `status_show`, `package_show`, `datastore_search` and resource
  downloads all return `200` with the header, through `build_unauthenticated_client` as well as curl.
- **Downloads are complete.** All 22 EBWPC downloads match their `size`, and the 17 that the Internet
  Archive also holds are byte-identical to its copies.
- **Not yet confirmed from Dagster+.** It has only been tested from a developer machine. If datHere also
  scoped the exception to IP addresses, Dagster+ would still be blocked. Checking this is follow-up
  ticket 1.

---

## Reference

The WDI shape the pipeline maps to, what each source looks like today, and the legacy code.

EBWPC and OSE have both been checked against the live catalog. The other facts below come from three
sources, each dated where quoted: Internet Archive snapshots (the EBWPC dataset page, its RDF metadata and
17 of its files from 2026-01-06; catalog pages up to 2026-04-19), the legacy NMWDI loader code, and the old
FROST server `st2.newmexicowaterdata.org`.

### WDI Best Practices and What They Mean for Ingest

*Source:* the NM Water Data Initiative's "Best Practices for Sharing Water Level Data with the New Mexico
Water Data Initiative". It describes itself as "a living document" reflecting "best practices, not
enforceable standards". Contact: `newmexicowaterdata@nmt.edu`. It ships two templates,
`NMWDI_Water_level_Locations.csv` and `NMWDI_Water_Level_Measurements.csv`.

| Best-practice rule | What it means for the pipeline |
|---|---|
| Two files, a **location** file and a **measurement** file, joined on `site_id` | The shared adapter joins on `site_id`. A shim for a source that mixes the two (OSE) splits them. |
| Manual data: **one measurement file for all wells.** High-frequency data: **one file per location**, still with a `site_id` column | Expect 1 to N measurement resources per dataset. Never derive `site_id` from the resource name when the column exists. |
| CSV, one table per file. Multi-sheet Excel is "not readable by the Data Catalog's data API" | CSV is the main path. This confirms the DataStore is the catalog's intended "data API". XLSX is legacy only. |
| **Updates:** add new data to the existing CSV, "uploaded as the same resource" | The resource ID is the change key and every update is a full snapshot, which is what the fingerprint + append design assumes. |
| **File names** follow `Agency_Region_ProjectName_YYYYMM_DataType_Version` | Filenames and download URLs are *expected* to change, so never configure them. |
| Field names lowercase with underscores, all in row 1 | A compliant file needs no header surgery. The shim still lower-cases defensively. |
| Depth to water in **feet below ground surface**, units in a separate `*_unit` column | Map `depth_to_water` to `DTW_OBS_PROP` and validate `depth_to_water_unit`. |
| Manual and continuous readings in **one** `depth_to_water` field, told apart by `measurement_type` | `measurement_type` selects the datastream and sensor (Manual vs a continuous sensor such as a transducer). |
| Date `YYYY-MM-DD` and time `HH:MM:SS` (24-hour) in separate columns | Parse the two columns. **No timezone field and no guidance on one**, so the timezone is decided per source. |
| Locations without x,y "should not be included" | The adapter drops measurements for sites with no located location, and counts them in metadata. |

#### Field mapping: WDI -> canonical

**Location file**

| WDI field | Req. | Canonical target |
|---|---|---|
| `site_id` | required | Location/Thing `properties.source_id` (str) |
| `y_coord`, `x_coord` | required | Location GeoJSON `coordinates[1]`, `[0]`, after conversion to WGS84 lat/lon |
| `coordinate_system`, `horizontal_datum` | required | Drive the coordinate conversion. Originals kept in `properties.source_specific`. |
| `elevation`, `elevation_units` | required | Location `properties.source_specific.elevation`, in feet |
| `site_name` | desired | Location `name` if present, else `site_id` |
| `alternate_site_id` + `alternate_site_id_organization`, `ose_pod_id`, `ose_tag_id` | desired | `properties.alternate_id` = `[{id, agency}]`, OSE IDs with agency `NMOSE` |
| `well_depth`, `screen_top`, `screen_bottom` (+ units) | desired | Thing `properties.source_specific.well_depth` = `{value, unit: "ft"}` and `.screens` = `[{top, bottom}]` |
| Every other desired field (`vertical_datum`, `county`, `hole_depth`, `casing_diameter`, `well_completion_date`, `primary_use`, `primary_aquifer`, `lithology`, `location_source`, `location_accuracy*`, `elevation_source`, `elevation_accuracy`) | desired | `properties.source_specific` as-is (`primary_use` is from WDI's fixed value list) |

**Measurement file**

| WDI field | Req. | Canonical target |
|---|---|---|
| `site_id` | required | Join key to the location |
| `measurement_date` + `measurement_time` | required | Observation `phenomenonTime`, converted to UTC from the source's timezone |
| `depth_to_water`, `depth_to_water_unit` | required | Observation `result`, in feet |
| `measurement_type` | desired | Choice of Datastream and Sensor (manual vs continuous) |
| `measuring_agency`, `measurement_method`, `water_level_status`, `measurement_point_height`, `water_level_accuracy` | desired | Observation `parameters` standard keys, same names |
| `notes` | desired | Observation `parameters.source_specific.notes` |

### Per-Source Survey

Both agencies predate the best-practice document. **Neither is fully compliant**, which is why each gets
a shim.

#### EBWPC (Estancia Basin Water Planning Committee)

| | |
|---|---|
| **Dataset** | `ebwpc-gw-monitoring` / `719844ad-46bf-46cf-a781-a111f114fe38` ("EBWPC Monitoring Well Measurements"), org `estancia-basin-water-planning-committee` |
| **Collection** | Transducer and hand-measured water levels, measured semi-annually, by Sandia National Labs (2007 to 2009), HydroResolutions (2009 to 2021-02) and John Shomaker & Associates (2021-12 to present) |
| **QC note** | Transducer data checked for drift and corrected, and matched every 6 to 12 months to hand measurements |
| **Compliance** | **Partial.** Per-well files and a separate locations resource are the right idea, but the column names, split depth columns, date and time formats, missing unit columns and UTM-only locations all deviate. |

**Resources.** One resource per well, named by well ID (`E-8428`, `Smith-1`, `E-1639-POD1`). Retired
wells are suffixed `-Archived`: strip the suffix to get the `source_id`, record `is_archived` in
`source_specific`, and load them wherever a location exists.
- **Oct 2024 snapshot:** eight XLSX well resources.
- **Jan 2026 snapshot:** 22 resources, none modified after 2025-03-12. The original eight keep their
  resource IDs but are now CSV with new download URLs. 20 of the well resources are CSV. `E-9407-Archived`
  is still XLSX. `EBWPC Well Locations` is a CSV (763 bytes, 2024-12-04). `format` reads `CSV` on most
  and `.csv` on two.
- **Locations:** `EBWPC Well Locations` has `NMBG_ID`, `UTM_Zone13N_Easting`, `UTM_Zone13N_Northing` and
  `Datum` (`NAD83` on every row). It covers all 21 wells, the 14 active ones and the 7 archived ones.
- **Size:** ~35 MB in total. The 17 archived CSVs hold ~29 MB / ~762k rows. The largest, `e-2034.csv`,
  has 123k rows. The newest reading in any of them is 2024-04-26.

**Measurement columns.** Each file has `site_id,measurement_date,measurement_time`, then one or two of
these depth columns:

| Depth column | Meaning | Seen in |
|---|---|---|
| `transducer_depth_bgs` | Continuous, below ground surface | Most active wells |
| `manual_depth_bgs` | Manual, below ground surface | Most active wells, and `magnum-steel`, `e-50-1` |
| `manual_depth_BTOC` | Manual, **below top of casing** | `hagerman`, `rubyshaw` |
| `manual_depth_Below_TOC` | Manual, **below top of casing** | `e-6385` |

TOC readings are not BGS. Without a measuring-point height they cannot become `DTW_OBS_PROP` values, so
they are skipped for now and EBWPC will be asked for stick-up heights. The legacy loader skipped them
too, because the `KeyError` on `manual_depth_bgs` bailed out of those files.

**Data-quality issues**, from profiling the 17 archived CSVs (Jan 2026):
- **Dates:** `M/D/YY` in some files, `M/D/YYYY` in others. `e-1639-pod1` mixes both.
- **Times:** `h:mm:ss AM/PM` in some files, `H:MM` 24-hour in others, plus `N/A`. In 4 `e-9673` rows
  both date and time are `#VALUE!`, and 7 manual readings in `e-8428` are dated `1/0/1900` (Excel's
  zero date).
- **Junk depth values:** `N/A`, from 1 row in some files up to 168 (`smith-1`). The legacy loader also
  filtered `#REF!`.
- **Trailing empty columns** in the header (`...,manual_depth_bgs,,,,,`), and blank trailing rows (51 in
  `e-2034`, 49 in `e-94077`).
- **Exact-duplicate rows:** 533 in `e-2034`, none of which has a depth value, and 1 duplicated reading
  in `e-50-1` (whose 4 rows are 3 distinct readings).
- **Stray values in unnamed trailing columns:** `e-94077` has stray values, mostly a repeated date, in
  column 6 on 21 rows, all of them manual readings.
- **Inconsistent `site_id`:** `e-1639-pod1` (resource `E-1639-POD1`), `Hagerman HQ-archived`,
  `e-50-1 archived` (space-separated), `E-6385-archived`, `Magnum Steel`.
- **IDs that don't match, not missing locations.** Every well the legacy loader marked "no location" has
  one. `E-2034` is listed as `E-2043` (transposed digits), `E-9673` has a trailing space, and the loader
  skipped rows ending in `archived` (`E-50-1`, `Greene-4`, `Lujan-1`). Normalise IDs and alias `E-2043`
  to `E-2034`; see `ebwpc_ckan.md`.
- **Timezone: fixed UTC−07:00.** No timezone is stated, but the transducer logs
  ignore daylight saving: they have readings at 02:xx on every spring-forward date and no repeated hour
  in the fall. The clock is fixed, most likely MST. The legacy loader stamped these times as UTC, so
  old-FROST EBWPC times are 7 hours off.

**Old FROST footprint (`st2`):** 14 Locations and 28 Datastreams, each well having one
`Manual Groundwater Levels` (Sensor `Manual`) and one `Groundwater Levels` (Sensor `Transducer`).
The latest observation is 2024-04-26. Agency code: `EBWPC`.

**Sample** (`e-1639-pod1.csv`, Jan 2026 snapshot):

```csv
site_id,measurement_date,measurement_time,transducer_depth_bgs,manual_depth_bgs
e-1639-pod1,1/18/18,N/A,N/A,67.65
e-1639-pod1,4/13/18,9:00:00 AM,68.00145004,
e-1639-pod1,4/13/18,4:00:00 PM,67.91670368,67.63
e-1639-pod1,4/25/2024,3:12:00 PM,,68.61
```

#### OSE Roswell (District 2)

| | |
|---|---|
| **Dataset** | `pecos_region_manual_groundwater_levels` / `e2cdb9aa-dc39-45bc-afaf-0c7849452065` ("Pecos Region Groundwater Levels"), org `nm-ose-isc` |
| **Description** | "Manual, discrete groundwater level measurements from a regional well network around the lower Pecos Valley ... collected annually in winter by Office of State Engineer, District 2 Office. Data here begin in 2011." |
| **Resources to ingest** | `Roswell_waterlevels` (`roswell_wl_r2.csv`), `Hondo_waterlevels` (`hondo_wl_r2.csv`), `Ft_Summner_waterlevels` (`ft_sumner_wl_r2.csv`), all uploaded 2025-03-20 |
| **Compliance** | **No.** Location and measurement are in one table, headers are capitalized, there is no unit column, and revisions were added as new resources. |

**Revisions arrive as new resources.** The live dataset has 16 resources:
- **The legacy loaders' three CSVs** (`roswell_wl.csv` `75b89cfc-...`, `hondo_wl.csv` `ce18fbb9-...`,
  `ft_sumner.csv` `3fa1cd2c-...`, from 2021–2022) are still there.
- **The revised `*_r2.csv` files** were uploaded next to them in 2025 instead of replacing them, against
  the WDI "same resource" rule.
- **Everything else:** GeoJSON and zipped shapefiles (including a 2025 `ose_roswell_wl_201110_202003_v2.geojson`),
  an HTML page, and an "OSE Roswell SensorThings API" link that points at old FROST.

Select the three `*_waterlevels` CSVs by name, and surface any new resource so a person can decide
whether it supersedes them. The 2021 snapshot's tabular resource `5f64d411-...` no longer exists.

**Columns** (the live `_r2` files): `Site_ID, Date, Time, Location, DD_lat, DD_lon, DMS_lat, DMS_lon,
DTWGS, Basin, Comment, UTM_East, UTM_North` for Roswell. Hondo and Ft Sumner lack `Basin` and `Comment`.

| Column | Notes |
|---|---|
| `Site_ID` | USGS-style 15-digit site number with a space, e.g. `"323405 104242601"`. Use as `source_id`, and as a candidate `alternate_id` with agency USGS. |
| `Date`, `Time` | `Date` is ISO in the `_r2` files. **There is no real time anywhere:** every `_r2` `Time` is `12:00:00`, and the legacy file's is `1899/12/30` (Excel's epoch). Treat readings as date-only. 32 Roswell rows have a `Comment` saying "Date and time approximate." |
| `Location` | PLSS township/range string, e.g. `20S.26E.17.34331`. Goes in `source_specific`. |
| `DD_lat`, `DD_lon` | Decimal degrees. The legacy loader **swaps them when `lat < 0`**, implying some uploads had them reversed. None are reversed in the `_r2` files, but keep the guard. |
| `DTWGS` | Depth to water below ground surface, in feet. |
| `Basin` | `Roswell` (Roswell file only). Derive it from the resource for Hondo and Ft Sumner. |

**Coverage.** 222 sites, matching old FROST:
- **Roswell:** 196 sites, 2011-01-14 to 2020-03-06.
- **Hondo:** 14 sites, 2004 to 2015.
- **Ft Sumner:** 12 sites, 2011 to 2020-02-24.

Nothing newer than early 2020 has been published. Each site appears once per measurement, so the shim
derives the location file from the distinct `Site_ID` rows.

**Data-quality issues** (live `_r2` files):
- **Undatable rows:** 4 Roswell rows have an empty `Date`.
- **A year typo:** one row is dated `2316-03-02` (site `330509 104 360901`, whose ID also has a stray
  space).
- **Repeated values:** 21 consecutive readings across 19 sites repeat the previous year's depth exactly.
  For example, `323405 104242601` reads 11.1 ft in 2018, 2019 and 2020, where USGS has different values.
  Possibly carried forward; flag rather than drop.

**Old FROST footprint (`st2`):** 222 Locations and 222 `Manual Groundwater Levels` Datastreams
(Sensor `Manual`), 2011 to about 2020-02. Agency code: `OSE-Roswell`, with `properties.basin`.

**Sample** (`roswell_wl_r2.csv`, live; CRLF line endings):

```csv
Site_ID,Date,Time,Location,DD_lat,DD_lon,DMS_lat,DMS_lon,DTWGS,Basin,Comment,UTM_East,UTM_North
323405 104242601,2019-01-22,12:00:00,20S.26E.17.34331,32.566778,-104.408167,32 34 00.4,104 24 29.4,11.1,Roswell,,555556,3603420
323405 104242601,2020-02-17,12:00:00,20S.26E.17.34331,32.566778,-104.408167,32 34 00.4,104 24 29.4,11.1,Roswell,"Date and time approximate. Season range from Jan 21 to March 4, 2020.",555556,3603420
324752 104243201,,12:00:00,17S.26E.32.11223,32.798056,-104.408611,32 47 53,104 24 31,73.52,Roswell,,555371,3629050
```

### Prior Art

| Code | What it does | Lesson |
|---|---|---|
| [NMWDI/CloudFunctions `stao/ckan_stao.py`](https://github.com/NMWDI/CloudFunctions/blob/main/stao/ckan_stao.py) | `package_show`, filter resources by name, `httpx.get(resource.url)`, `csv.DictReader`. | The recommended pattern already worked in production, after the 2025-03-19 move off the DataStore. No change detection or provenance. |
| [`stao/ebwpc/entities.py`](https://github.com/NMWDI/CloudFunctions/blob/main/stao/ebwpc/entities.py) | EBWPC locations from `EBWPC Well Locations` (UTM->lat/lon), manual + transducer datastreams per well, four hardcoded `strptime` formats. | Its comments list every known per-well quirk. Don't repeat its UTC stamping of local times. |
| [`stao/ose_roswell_basin/entities.py`](https://github.com/NMWDI/CloudFunctions/blob/main/stao/ose_roswell_basin/entities.py) | Three resource IDs, dedupes sites, swaps lat/lon when `lat < 0`. | Keep the lat/lon guard. Its IDs point at the pre-2025 files, not the `_r2` revisions. |
| [DataIntegrationEngine `backend/connectors/ckan/source.py`](https://github.com/DataIntegrationGroup/DataIntegrationEngine/blob/main/backend/connectors/ckan/source.py) | OSE Roswell via `datastore_search?resource_id=...` with no `limit`/`offset`. | Silent truncation at CKAN's 100-row default. |

None of the legacy agency loaders is wired into `main.py`. All were run by hand from `__main__`,
which fits how rarely the data moves.

---

## Proposed Follow-Up Tickets

1. **Access (exception granted):** store the `x-cf-bypass` value in GCP Secret Manager, confirm it
   works from a Dagster+ run, and ask datHere whether the exception is also scoped by IP address or
   path. See [Access](#access-cloudflare).
1. **`ckan_common` + `wdi_transform_common`:** shared discover/fingerprint/parse, with the bypass header
   on the client, and the WDI -> canonical adapter. Offline unit tests against a synthetic WDI-template fixture. No agency yet.
1. **Feedback to WDI** on the best-practices document: add a timezone field, and ask providers to update
   resources in place instead of adding new ones (OSE added `_r2` copies next to its old files).
