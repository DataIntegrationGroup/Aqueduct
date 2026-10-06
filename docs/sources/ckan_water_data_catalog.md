# Source Research: NM Water Data Catalog (CKAN)

**Platform:** CKAN, hosted by datHere (the catalog footer reads "Powered by datHere")
**Base URL:** `https://catalog.newmexicowaterdata.org` (CNAME to `newmexico.opendataportal.us`)
**Agencies investigated:** EBWPC, OSE Roswell (District 2), City of Roswell. The same approach applies to any
other agency that publishes water levels to the catalog.
**Response format:** CKAN Action API JSON for metadata, CSV (legacy XLSX) resource files for data
**Source timezone:** not stated by any source, and not covered by the WDI best practices. See
[Open Questions](#open-questions).
**Update frequency:** irregular, and publication lags collection. EBWPC collects semi-annually and OSE
District 2 annually each winter. As of Jan 2026, EBWPC's newest reading was 2024-04-26 and its files were
last re-uploaded 2025-03-12.

---

## Summary

**Decision path**

1. **Ask datHere for an exception.** The catalog currently challenges every non-browser client, so no
   pipeline can reach it. The request goes through WDI. See
   [The Cloudflare Challenge](#the-cloudflare-challenge).
2. **Granted -> [Plan A](#plan-a-automated-ingest-with-a-dathere-exception):** a weekly automated
   pipeline over the CKAN Action API.
3. **Refused, or no answer within an agreed time box -> [Plan B](#plan-b-without-an-exception):** a
   person pulls the files with a browser into GCS, and the same pipeline reads them from there.
4. **Either way, start building now.** Only the fetch step differs between the two plans.

**Plan A in brief**

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

**The blocker in brief.** Cloudflare returns `403` to every scripted client on every path, including the
API and file downloads, while browsers get through unchallenged. Only datHere can change it. If the cause
is Bot Fight Mode, a WAF skip rule won't work, so the request covers both cases.

---

## Plan A: Automated Ingest (with a datHere Exception)

This is the target design. It assumes datHere exempts our requests from the Cloudflare challenge (see
[The Cloudflare Challenge](#the-cloudflare-challenge)). Everything after the fetch step is also what
[Plan B](#plan-b-without-an-exception) uses.

### Platform and Endpoints

A standard CKAN instance with FileStore uploads, the DataStore extension (populated by the catalog's
pusher, most likely datHere's DataPusher+) and ckanext-dcat. All read endpoints are unauthenticated
apart from the Cloudflare issue.

| Endpoint | Use | Gotchas |
|---|---|---|
| `GET /api/3/action/package_show?id={uuid}` | **Discovery and change detection.** One call returns every resource with `id`, `name`, `url`, `format`, `last_modified`, `size`, `hash`, `datastore_active`. | Configure the dataset UUID, not the renamable slug. UUIDs survived the catalog upgrade around 2026-03-19. `hash` (32 hex characters) is populated on all of EBWPC's resources, so it can join the fingerprint. `format` is free text (`CSV` and `.csv` both appear), so normalise it. |
| `GET /api/3/action/package_search?fq=organization:{org}&rows=1000` | Optional: detect new datasets from an organization. | Not needed for a fixed dataset list. |
| `GET {resource.url}`, i.e. `/dataset/{package_id}/resource/{resource_id}/download/{filename}` | **The data.** Exactly the bytes the agency uploaded. | `{filename}` changes when the agency re-uploads under a new name or format. Always take the URL from `package_show`, never from config. |
| `GET /api/3/action/datastore_search?resource_id={id}&limit={n}&offset={n}` | Typed JSON rows, if the resource was pushed. | **Default `limit` is 100**, so you must paginate. Types are inferred at push time. Exists only if `datastore_active`. |
| `GET /datastore/dump/{resource_id}?format=csv` | Full table in one response. | Same inferred types and `datastore_active` dependency. Archived dumps from 2021 to 2023 confirm it was enabled. |

### Options Compared

| | **A. Action API + file download** (recommended) | **B. DataStore API** | **C. Hardcoded spreadsheet URLs** |
|---|---|---|---|
| **Fidelity** | Exact agency bytes. | Types inferred at push time. OSE's `Time` comes back as `1899-12-30T00:00:00` (Excel epoch), and mixed `N/A`/number columns get coerced. | Exact bytes. |
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
- Select resources per agency by name and normalised format, never by a hardcoded ID list. IDs are
  stable only between re-uploads (OSE's changed when its dataset was rebuilt; see
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
FROST**. EBWPC says its transducer data is drift-corrected, so such corrections are plausible. See
[Open Questions](#open-questions).

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
| `sources/ckan_common.py` | httpx client (`build_unauthenticated_client`, overriding `Accept` for file downloads), `package_show`, fingerprinting, download, CSV -> string rows, provenance columns. Keep "get the `package_show` JSON and the file bytes" behind one function, so [Plan B](#plan-b-without-an-exception) can feed the same pipeline from GCS. |
| `sources/wdi_transform_common.py` | The WDI location + measurement -> `CanonicalBundle` adapter and its validation rules |
| `sources/<agency>_ckan/` | Dataset UUID, resource selector, agency shim, `raw_<key>` dataset, dlt resource names. One `SourceConfig` entry each. |

**Proposed source keys:** `ebwpc_ckan`, `ose_roswell_ckan`, `city_of_roswell_ckan`, following the
`<agency>_<source system>` rule in `STORAGE_CONVENTIONS.md`. TBD; see [Open Questions](#open-questions).

**Dependencies and canonical touch points:**
- CSV needs only the stdlib `csv` module. EBWPC has one XLSX resource left (`E-9407-Archived`). Add
  `openpyxl` (with `uv add`) only if that well is wanted (Open Question 3).
- EBWPC's locations are UTM-only, so the shim needs `pyproj`. Neither `pyproj` nor `openpyxl` is in
  `uv.lock` today.
- **Sensors:** `canonical_constants.py` has only `MANUAL_SENSOR` and `HYDROVU_SENSOR`. EBWPC needs a
  pressure transducer and City of Roswell a bubbler (old FROST called them `Transducer` and `Bubbler`).
  Both names are in the Sensor list in `_mapping_template.md`. Coordinate the change in `canonical/`.
- **Observation parameters:** WDI's desired measurement fields match the standard `parameters` keys in
  `_mapping_template.md` one-to-one, so no new keys are needed.

**Tests** stay offline per the repo rule. Use `package_show` payloads and short sanitised CSV fixtures
behind `httpx.MockTransport` (`tests/conftest.py` helpers). Cover fingerprint skip, body-hash skip, a
per-resource failure, and every date format and junk value listed in the survey.

---

## The Cloudflare Challenge

**Blocked for scripts, open to browsers.** Every scripted request returns `HTTP/2 403` with
`cf-mitigated: challenge` and Cloudflare's "Just a moment..." page. The page source sets
`cType: 'managed'`, i.e. a Cloudflare **managed challenge**, which a Dagster+ run can't pass. A normal
browser gets the JSON with no challenge at all (see below).

| Path | Result |
|---|---|
| `/api/3/action/status_show`, `package_show?id=ebwpc-gw-monitoring`, `datastore_search?resource_id=75b89cfc-...` | 403 challenge |
| `/dataset/719844ad-.../resource/0c5ca378-.../download/e-1639-pod1.csv` | 403 challenge |
| `/`, `/robots.txt` | 403 challenge |
| `/cdn-cgi/trace` (answered by Cloudflare itself) | 200 |

Tried with curl using a browser User-Agent, a `python-requests` User-Agent and `Accept: application/json`,
and from a separate cloud-hosted fetcher. Python `httpx` (the pipeline's client) and stdlib `urllib` get
the same 403. `http://` redirects to `https://` and is then challenged. The legacy NMWDI loaders and DIE's
CKAN connector (see [Prior Art](#prior-art)) are presumably broken the same way.

**Browser check.** In a fresh private window, on the same machine and IP where curl gets 403,
`package_show?id=ebwpc-gw-monitoring` returned the full JSON on the first request. There was one
navigation, no challenge page or reload, and no Cloudflare cookies. So Cloudflare challenges only clients
that look automated, and the trigger is something about the client itself (its connection fingerprint or
the headers it sends), not its IP or User-Agent.

### What we know about the rule

- **It is a datHere platform setting, not a New Mexico one.** Every hostname found in datHere's
  `opendataportal.us` Cloudflare zone is challenged on every path: `newmexico.` (the catalog's CNAME
  target), `catalog.` and `sandbox.opendataportal.us`.
- **Only datHere can change it.** DNS for `newmexicowaterdata.org` is hosted at GoDaddy, not Cloudflare,
  so NMBGMR has no Cloudflare settings of its own.
- **Cloudflare isn't new, but the challenge is.** The hostname's certificate history shows Cloudflare in
  front since spring 2024, and the Internet Archive got normal `200` pages through it until its last
  capture on 2026-04-19. So the challenge was switched on some time after that.
- **It targets non-browser clients, and two kinds of setting fit.** An unchallenged browser rules out
  "I'm Under Attack" mode and any rule that challenges every visitor. Browser-User-Agent curl being
  challenged rules out a User-Agent rule. Two explanations are left, and only datHere can tell them apart
  from the Ray ID:
  - **A Cloudflare bot product:** Bot Fight Mode (any plan), Super Bot Fight Mode (Pro and up) or
    Enterprise Bot Management. The browser got no `__cf_bm` cookie, which Cloudflare
    [says](https://developers.cloudflare.com/fundamentals/reference/policies-compliances/cloudflare-cookies/)
    it places on sites protected by Bot Management or Bot Fight Mode. That weakens this explanation but
    doesn't rule it out.
  - **A WAF custom rule** keyed on something browsers send and scripts don't, such as browser-only
    headers.
- **Which one it is decides the fix.** "You cannot bypass or skip Bot Fight Mode using WAF custom rules or
  Page Rules" ([Cloudflare docs](https://developers.cloudflare.com/bots/get-started/bot-fight-mode/)). If
  that is the cause, datHere must turn it off for the zone or move to Super Bot Fight Mode, which supports
  skip rules. Everything else can be exempted with a skip rule.

### What to ask for

datHere runs the catalog for NMBGMR/WDI under an NMT
[sole-source contract](https://www.nmt.edu/finance/purchasing/Dathere_Sole%20Source%20Justification.pdf),
so the request goes through WDI to datHere. Agree a time box (e.g. two weeks) after which
[Plan B](#plan-b-without-an-exception) starts by default.

1. Send the `cf-ray` value from a blocked request made the same day (e.g. `a4481169bdccf051-SJC`).
   datHere can look it up in Cloudflare's Security Events to see which feature issued the challenge.
2. Ask for an exception scoped to host `catalog.newmexicowaterdata.org` and to:
   - `/api/3/action/*` (read actions only: `package_show`, `package_search`, `datastore_search`,
     `status_show`)
   - `/dataset/*/resource/*/download/*`
3. Match it on a request header that only we send. A CKAN API token in the `Authorization` header is the
   natural choice, since CKAN accepts one even for public reads. Egress-IP allowlisting is the
   alternative, but a Dagster+ deployment may not have a stable egress IP range. Store the token in GCP
   Secret Manager like the HydroVu credentials.
4. If a Cloudflare change isn't possible, ask for one of the
   [middle-ground options](#middle-ground-options-dathere-could-offer) instead.

---

## Plan B: Without an Exception

Use this if datHere declines, or doesn't act within the time box. Only the fetch step changes.
Fingerprinting, the raw layer, the shims, the WDI adapter and the FROST load are the same as
[Plan A](#plan-a-automated-ingest-with-a-dathere-exception), and the [Reference](#reference-both-plans)
material applies unchanged.

### Default: manual browser pull into GCS

Browsers aren't challenged, so a person can fetch exactly what the pipeline would have fetched.

1. **Save the `package_show` JSON** for each dataset from the browser, e.g.
   `.../api/3/action/package_show?id=719844ad-46bf-46cf-a781-a111f114fe38`. It lists every resource with
   its `last_modified`, `size` and URL, so discovery and change detection still work.
2. **Download each resource file** from the dataset page (EBWPC has 22 resources, OSE 3). Save the file
   itself, not the link the browser ends up on: the catalog may redirect downloads to a signed S3 link
   that expires.
3. **Upload both to a GCS landing prefix**, e.g. `landing/<source_key>/<YYYY-MM-DD>/`. This is an input
   folder, separate from the dlt-owned `raw_*` datasets.
4. **The ingest reads the newest landing folder** instead of calling the catalog, then carries on as in
   Plan A.

**Costs.** A person has to notice updates and repeat the pull. Checking each dataset page's "Last Updated"
date now and then is enough, given how rarely the data moves. New wells appear only when someone pulls.
It is a stopgap, not the long-term answer.

**Do one pull now regardless.** It gives real, current EBWPC and OSE files to build and test the shims
and the adapter while the exception is pending.

### Middle-ground options datHere could offer

Both stay automatic and change only the fetch step:

- **Read access to the catalog's file storage.** Uploads live in an S3 bucket (`newmexico-s3-bucket`,
  seen in an archived redirect) and are served through short-lived signed links that CKAN generates, so
  there is no public path today. Change detection would use object timestamps instead of `package_show`.
- **A scheduled export** of the datasets we need to a GCS bucket we own.

### Other routes, by source

| Source | Route | Caveat |
|---|---|---|
| All three | **Old FROST server `st2.newmexicowaterdata.org`.** NMBGMR's own server, not behind Cloudflare, and answering `200`. Holds EBWPC (to 2024-04-26), OSE-Roswell (to about 2020-02) and CityOfRoswell (to 2025-03-15). | Frozen since the legacy loaders stopped, and EBWPC times are wrongly stamped as UTC. Fine for a one-time history backfill or cross-checks, not an ongoing feed. |
| OSE Roswell | **USGS Water Data API.** OSE's readings are in USGS under the same 15-digit site IDs, with measuring agency `NM001` ("New Mexico State Engineers Office"). | Spot check of one site (`323405104242601`): USGS readings run to 2022-01-06 and differ from the catalog's (USGS 2020-01-16 at 11.53 ft vs the catalog's 2020-02-17 at 11.1 ft, marked "Date and time approximate"). A related dataset, not a copy, so compare before relying on it. |
| City of Roswell | **The `roswellbubbler` GCS bucket** the legacy loader read. | It was never a CKAN source. If NMBGMR owns the bucket, this is the simplest route of all. |
| Any | **Providers send files directly** (EBWPC's contractor, OSE District 2) into the same landing prefix. | Same manual cost as the browser pull. |

### Not recommended

Making our client pass as a browser (driving a headless Chrome, reusing a browser's cookies or headers,
or using a library that copies Chrome's connection fingerprint), or calling the server behind Cloudflare
directly. These deliberately get around datHere's protection on a catalog NMBGMR pays datHere to run.
They also break without warning whenever datHere tunes its settings, and could get Dagster+ traffic
blocked outright.

---

## Reference (Both Plans)

The WDI shape both plans map to, what each source looks like today, and the legacy code.

EBWPC has been checked against its live `package_show`
and locations file, saved from a browser. Confirm the rest once access works or after the first browser
pull. Because of the Cloudflare block, additional catalog facts below come from three
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
| Manual and continuous readings in **one** `depth_to_water` field, told apart by `measurement_type` | `measurement_type` selects the datastream and sensor (Manual vs Transducer/Bubbler). |
| Date `YYYY-MM-DD` and time `HH:MM:SS` (24-hour) in separate columns | Parse the two columns. **No timezone field and no guidance on one.** See Open Questions. |
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
| `alternate_site_id` + `alternate_site_id_organization`, `ose_pod_id`, `ose_tag_id` | desired | `properties.alternate_id` = `[{id, agency}]`, OSE IDs with agency `NMOSE` (settle in the mapping ticket) |
| `well_depth`, `screen_top`, `screen_bottom` (+ units) | desired | Thing `properties.source_specific.well_depth` = `{value, unit: "ft"}` and `.screens` = `[{top, bottom}]` |
| Every other desired field (`vertical_datum`, `county`, `hole_depth`, `casing_diameter`, `well_completion_date`, `primary_use`, `primary_aquifer`, `lithology`, `location_source`, `location_accuracy*`, `elevation_source`, `elevation_accuracy`) | desired | `properties.source_specific` as-is (`primary_use` is from WDI's fixed value list) |

**Measurement file**

| WDI field | Req. | Canonical target |
|---|---|---|
| `site_id` | required | Join key to the location |
| `measurement_date` + `measurement_time` | required | Observation `phenomenonTime`, converted to UTC (timezone TBD) |
| `depth_to_water`, `depth_to_water_unit` | required | Observation `result`, in feet |
| `measurement_type` | desired | Choice of Datastream and Sensor (manual vs continuous) |
| `measuring_agency`, `measurement_method`, `water_level_status`, `measurement_point_height`, `water_level_accuracy` | desired | Observation `parameters` standard keys, same names |
| `notes` | desired | Observation `parameters.source_specific.notes` |

### Per-Source Survey

All three agencies predate the best-practice document. **None is fully compliant**, which is why each
gets a shim.

#### EBWPC (Estancia Basin Water Planning Committee)

| | |
|---|---|
| **Dataset** | `ebwpc-gw-monitoring` / `719844ad-46bf-46cf-a781-a111f114fe38` ("EBWPC Monitoring Well Measurements"), org `estancia-basin-water-planning-committee` |
| **Collection** | Transducer and hand-measured water levels, measured semi-annually, by Sandia National Labs (2007 to 2009), HydroResolutions (2009 to 2021-02) and John Shomaker & Associates (2021-12 to present) |
| **QC note** | Transducer data checked for drift and corrected, and matched every 6 to 12 months to hand measurements |
| **Compliance** | **Partial.** Per-well files and a separate locations resource are the right idea, but the column names, split depth columns, date and time formats, missing unit columns and UTM-only locations all deviate. |

**Resources.** One resource per well, named by well ID (`E-8428`, `Smith-1`, `E-1639-POD1`). Retired
wells are suffixed `-Archived`.
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

TOC readings are not BGS. Without a measuring-point height they cannot become `DTW_OBS_PROP` values.
The legacy loader skipped them because the `KeyError` on `manual_depth_bgs` bailed out of those files.

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
| **Resources (legacy loaders)** | Roswell `75b89cfc-f28c-4b95-b477-09272a2e47d2`, Fort Sumner `3fa1cd2c-be33-4bba-a65b-bbc786dcbd39`, Hondo `ce18fbb9-296d-4b40-ba66-f81a061051ac` |
| **Compliance** | **No.** Location and measurement are in one table, headers are capitalized, and there is no unit column. |

**Resource IDs have changed.** The 2021 snapshot listed GeoJSON and zipped-shapefile resources
(`roswell_wl.geojson`, `Roswel_wl.zip`, ...) and a tabular resource `5f64d411-...`, none matching the IDs
the 2022–2025 legacy loaders read. The dataset was rebuilt at least once, so match on name and format,
not IDs.

**Columns**, from the 2021 DataStore dump (the same schema the legacy loaders read):
`Site_ID, Date, Time, Location, DD_lat, DD_lon, DMS_lat, DMS_lon, DTWGS, Basin, Comment, UTM_East,
UTM_North`.

| Column | Notes |
|---|---|
| `Site_ID` | USGS-style 15-digit site number with a space, e.g. `"323405 104242601"`. Use as `source_id`, and as a candidate `alternate_id` with agency USGS. |
| `Date`, `Time` | Through the DataStore, `Time` comes back as `1899-12-30T00:00:00` (an Excel time cell typed as a timestamp) with no real time. Read from the file instead. Some rows' `Comment` says "Date and time approximate." |
| `Location` | PLSS township/range string, e.g. `20S.26E.17.34331`. Goes in `source_specific`. |
| `DD_lat`, `DD_lon` | Decimal degrees. The legacy loader **swaps them when `lat < 0`**, implying some uploads had them reversed. None are reversed in the 2021 dump, but keep the guard. |
| `DTWGS` | Depth to water below ground surface, in feet. |
| `Basin` | `Roswell`, `Hondo` or `Ft Sumner`. |

Each site appears once per measurement, so the shim derives the location file from the distinct
`Site_ID` rows. The 2021 Roswell-basin table held 1,003 rows, which matters because DIE's
`datastore_search` call without `limit`/`offset` returns only the first 100.

**Old FROST footprint (`st2`):** 222 Locations and 222 `Manual Groundwater Levels` Datastreams
(Sensor `Manual`), 2011 to about 2020-02. Agency code: `OSE-Roswell`, with `properties.basin`.

**Sample** (2021 DataStore dump, Roswell basin table):

```json
{"Site_ID": "323405 104242601", "Date": "2020-02-17T00:00:00", "Time": "1899-12-30T00:00:00",
 "Location": "20S.26E.17.34331", "DD_lat": 32.566778, "DD_lon": -104.408167, "DTWGS": 11.1,
 "Basin": "Roswell", "Comment": "Date and time approximate. Season range from Jan 21 to March 4, 2020."}
```

#### City of Roswell

| | |
|---|---|
| **On CKAN?** | **Unconfirmed.** The legacy loader (`stao/croswell/`, added 2025-03-18) read from a GCS bucket (`roswellbubbler`) and a checked-in locations CSV, not from CKAN. The Jan 2026 snapshot of the catalog's organizations page (66 orgs) has no `city-of-roswell` between `city-of-deming` and `city-of-santa-fe`. |
| **Compliance** | **Closest of the three.** The locations CSV already uses WDI field names, and the measurements are nearly compliant. |

**Locations** (`Roswell_locations.csv`), missing the required `elevation`, `elevation_units` and
`horizontal_datum`:

```csv
site_id,y_coord,x_coord,coordinate_system,county,well_depth,well_depth_unit,casing_diameter,casing_diameter_unit,primary_use,ose_pod_id
18,33.29592,-104.58409,decimal degrees,Chaves,850,ft,16,in,public supply,RA-4253S
```

**Measurements** (`SMW18_measurements_fixed.csv`), non-compliant in four ways: a space in
`measuring _agency`, an unnamed empty column, a combined non-zero-padded `timestamp` instead of separate
date and time columns, and no `depth_to_water_unit`:

```csv
depth_to_water,site_id,measuring _agency,,timestamp
124.1,18,City of Roswell,,2024-6-3T11:46:38
```

**Old FROST footprint (`st2`):** 1 Location (`Site-18`, with a geoconnex URI), 1 `Groundwater Levels`
Datastream (Sensor `Bubbler`, continuous), 2024-06-03 to 2025-03-15. Agency code: `CityOfRoswell`.

### Prior Art

| Code | What it does | Lesson |
|---|---|---|
| [NMWDI/CloudFunctions `stao/ckan_stao.py`](https://github.com/NMWDI/CloudFunctions/blob/main/stao/ckan_stao.py) | `package_show`, filter resources by name, `httpx.get(resource.url)`, `csv.DictReader`. | The recommended pattern already worked in production, after the 2025-03-19 move off the DataStore. No change detection or provenance. |
| [`stao/ebwpc/entities.py`](https://github.com/NMWDI/CloudFunctions/blob/main/stao/ebwpc/entities.py) | EBWPC locations from `EBWPC Well Locations` (UTM->lat/lon), manual + transducer datastreams per well, four hardcoded `strptime` formats. | Its comments list every known per-well quirk. Don't repeat its UTC stamping of local times. |
| [`stao/ose_roswell_basin/entities.py`](https://github.com/NMWDI/CloudFunctions/blob/main/stao/ose_roswell_basin/entities.py) | Three resource IDs, dedupes sites, swaps lat/lon when `lat < 0`. | Keep the lat/lon guard. |
| [`stao/croswell/entities.py`](https://github.com/NMWDI/CloudFunctions/blob/main/stao/croswell/entities.py) | Local locations CSV plus the `roswellbubbler` GCS bucket. | City of Roswell was never a CKAN source in the legacy system. |
| [DataIntegrationEngine `backend/connectors/ckan/source.py`](https://github.com/DataIntegrationGroup/DataIntegrationEngine/blob/main/backend/connectors/ckan/source.py) | OSE Roswell via `datastore_search?resource_id=...` with no `limit`/`offset`. | Silent truncation at CKAN's 100-row default. |

None of the three legacy agency loaders is wired into `main.py`. All were run by hand from `__main__`,
which fits how rarely the data moves.

---

## Open Questions

1. **Cloudflare exception.** Who at WDI owns the datHere relationship and files the request? How long is
   the time box before Plan B starts? Which Cloudflare feature issues the challenge (datHere can tell
   from a Ray ID)? If it is Bot Fight Mode, will datHere disable it or move to Super Bot Fight Mode?
   Should the exception match an API-token header (preferred) or egress IPs?
2. **Live re-verification** once access works (Plan A) or after the first browser pull (Plan B): each
   dataset's current resource list, format, `last_modified`, `size` and `datastore_active`. Done for
   EBWPC from a browser pull; OSE remains.
3. **EBWPC's one XLSX resource** (`E-9407-Archived`). Proposal: select CSV only and log skipped resources
   in metadata, and ask EBWPC to re-upload it as CSV per the best practices. Add `openpyxl` only if the
   well is needed sooner.
4. **Is City of Roswell on CKAN now?** If not, it is out of scope for the CKAN pipeline. Who owns the
   legacy `roswellbubbler` GCS bucket, and can we read it directly?
5. **Source timezone.** No source or WDI guidance states one, and the legacy loaders' UTC assumption is
   almost certainly wrong for field times. Don't assume `America/Denver`: EBWPC's loggers run on a fixed
   UTC−07:00 with no daylight saving (see [EBWPC](#ebwpc-estancia-basin-water-planning-committee)).
   Before choosing a zone for each source, check its timestamps for missing or repeated hours on DST
   dates. Record the assumption, and suggest WDI add a timezone field (NMBGMR's own data dictionary has
   `TimeDatum`).
6. **TOC readings** (EBWPC `hagerman`, `rubyshaw`, `e-6385`) need a measuring-point height to become BGS.
   Skip them (proposed), ask EBWPC for stick-up heights, or load them under a separate "below top of
   casing" observed property.
7. **Corrections older than the FROST watermark** are silently dropped (see
   [Transform](#transform-raw---wdi-shape---canonical)). Is that acceptable for sources that re-upload
   drift-corrected history? If not, the fix belongs in the loader, e.g. a per-source "reload datastream
   when the snapshot changed" mode.
8. **"Archived" wells.** Proposal: strip the `archived` suffix to get the `source_id`, record
   `is_archived` in `source_specific`, and load them wherever a location exists.
9. **Source keys and agency codes.** Keys proposed above. Old FROST used `EBWPC`, `OSE-Roswell` and
   `CityOfRoswell`, while the canonical model asks for upper-case codes (`PVACD`, `CABQ`, `EBID`). Keep
   the old codes for continuity, or normalize?
10. **Other catalog sources.** The same approach covers any water-level dataset on the catalog. Which other
    agencies (e.g. `nmbgmr`, `carlsbad-irrigation-district`, `city-of-santa-fe`, `city-of-deming`) are
    wanted?

---

## Proposed Follow-Up Tickets

1. **Access request (decides Plan A vs B):** through WDI, send datHere a Ray ID and request the exception
   described in [What to ask for](#what-to-ask-for), with an agreed time box. Once access works, run the
   live checks in Open Question 2 and update this doc.
1. **One-time browser pull:** save the EBWPC and OSE `package_show` JSON and resource files to a GCS
   landing prefix, for development now and as Plan B's first run if needed.
1. **`ckan_common` + `wdi_transform_common`:** shared discover/fingerprint/parse with a swappable fetch
   step (HTTP for Plan A, GCS landing for Plan B), and the WDI -> canonical adapter. Offline unit tests
   against a synthetic WDI-template fixture. No agency yet.
1. **Feedback to WDI** on the best-practices document: add a timezone field, and ask providers to keep
   resource IDs stable across updates (OSE's resources were recreated).
