# Source Mapping: EBWPC (NM Water Data Catalog, CKAN)

**Source key:** `ebwpc_ckan`
**Agency code:** `EBWPC`
**Response format:** `csv` (one file per well plus a locations file), discovered through CKAN `package_show` (`json`)
**Source timezone:** fixed UTC−07:00 (MST, no daylight saving). Inferred, not stated by the source. See
[Timestamps and timezone](#timestamps-and-timezone).
**Update frequency:** `irregular`. Measured semi-annually, but every well file was last re-uploaded on
2025-03-12 and the newest reading in any of them is 2024-04-26.

**Dataset:** `ebwpc-gw-monitoring` / `719844ad-46bf-46cf-a781-a111f114fe38` ("EBWPC Monitoring Well
Measurements"), organization `estancia-basin-water-planning-committee`. Transducer and hand-measured water
levels collected by Sandia National Labs (2007 to 2009), HydroResolutions (2009 to 2021-02) and John
Shomaker & Associates (2021-12 to present). Transducer data were drift-corrected against hand
measurements every 6 to 12 months.

Platform-level research (why the Action API plus file download, the Cloudflare block, Plan A/B) lives in
[`ckan_water_data_catalog.md`](ckan_water_data_catalog.md). This doc covers EBWPC only.

**At a glance**

- 21 wells (14 active, 7 archived), every one with a location once IDs are normalised.
- Up to two datastreams per well: transducer (`dtw`) and manual (`dtw-manual`), both depth to water.
- New canonical constants: `TRANSDUCER_SENSOR` and a "Manual Groundwater Levels" datastream template.
- Times are naive and on a fixed UTC−07:00 clock. Add 7 hours to get UTC.

---

## API Access

**Credentials: none.** The catalog is public CKAN and every read is unauthenticated.

**Blocked for scripts.** Cloudflare challenges every non-browser client on every path, while browsers get
through. Until datHere grants the exception described in
[The Cloudflare Challenge](ckan_water_data_catalog.md#the-cloudflare-challenge), use
[Plan B](ckan_water_data_catalog.md#plan-b-without-an-exception): save the files below from a browser into
GCS. The live examples in this doc were saved that way.

| Endpoint | URL |
|---|---|
| Dataset + resources | `GET https://catalog.newmexicowaterdata.org/api/3/action/package_show?id=719844ad-46bf-46cf-a781-a111f114fe38` |
| Well file | `GET {resource.url}`, e.g. `.../dataset/719844ad-.../resource/0c306f4d-1474-4d00-9d77-45b73513abfd/download/e-8428.csv` |
| Locations file | `GET {resource.url}` of `EBWPC Well Locations` (`2f186d78-02d0-4d89-bb4f-540acb398774`), currently `.../download/ebwpc_locations.csv` |

**Resources (22).**

- **21 well resources**, one per well, named by well ID. The 7 retired wells are suffixed `-Archived`. 20
  are CSV. `E-9407-Archived` is XLSX (`e-9407_hydrographdata_data.xlsx`).
- **1 locations resource**, `EBWPC Well Locations`.
- Select well resources by name and format, never by a fixed ID list. `format` is free text: `CSV` on
  most, `.csv` on `E-94077` and `Hagerman HQ-Archived`, `XLSX` on one. Normalise it (lower-case, strip
  the dot).

**`package_show` notes.**

- `last_modified`, `size` and `hash` (32 hex characters) are populated on every resource, so all three
  can feed the change fingerprint.
- `total_record_count` is the DataStore row count (75,660 for `E-8428`, matching the file).
- `spatial_full` is ~346 KB of GeoJSON, most of the 390 KB response. Ignore it.
- The dataset's `metadata_modified` (2026-03-18) moved during a catalog upgrade with no data change, so
  don't use it for change detection.

**Free-text well metadata.** Four well resources (`E-8428`, `Smith-1`, `E-7545`, `Magnum Steel`) describe
the well in their `description`: county, UTM coordinates, `Elevation NNNN (ft AMSL)`, depth and
formation. The other 17 give only a rough place ("About 5 miles south of Moriarty, NM"). Smith-1's
description carries E-10652's coordinates, so take coordinates from the locations file only.

---

## Location

**Source:** the `EBWPC Well Locations` CSV. Columns `NMBG_ID,UTM_Zone13N_Easting,UTM_Zone13N_Northing,Datum`,
CRLF line endings, 21 rows (14 active wells, then the 7 archived ones).

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | `NMBG_ID` | Trimmed, with the `-archived` suffix removed. Old FROST used the same names (`E-2298`, `Magnum Steel`). |
| `description` | str | Required | *(fixed)* | Fixed: `Location of well where measurements are made` |
| `encodingType` | str | Required | *(fixed)* | Fixed: `application/vnd.geo+json` |
| `location` (longitude) | float | Required | `UTM_Zone13N_Easting`, `UTM_Zone13N_Northing` | NAD83 / UTM zone 13N (EPSG:26913) → WGS84. See Unit Conversions. |
| `location` (latitude) | float | Required | `UTM_Zone13N_Easting`, `UTM_Zone13N_Northing` | As above |

**properties - standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `source_id` | str | Required | `NMBG_ID` | Same normalised value as `name` |
| `geoconnex` | str | Optional | *(not in source)* | - |
| `alternate_id` | [{id: str, agency: str}] \| None | Optional | *(not in source)* | The `E-nnnn` and `T-nnnn` IDs look like OSE Estancia-basin file numbers. Don't emit until confirmed. See Open Questions. |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `utm_easting` | float | `UTM_Zone13N_Easting` as published (whole metres) |
| `utm_northing` | float | `UTM_Zone13N_Northing` as published |
| `utm_zone` | str | Fixed `13N` (from the column names) |
| `horizontal_datum` | str | `Datum`. `NAD83` on every row. |
| `elevation` | {value: float, unit: "m"} \| None | Only 4 wells, parsed from the resource `description` (`Elevation 6712 (ft AMSL)`) and converted to metres to match `pvacd_hydrovu.md` and `ebid.md`. `None` for the other 17. See Open Questions. |
| `is_archived` | bool | True when `NMBG_ID` ends in `archived` |

**Matching measurements to locations.** The locations file, the resource names and the file's `site_id`
column spell the same well differently. Normalise all three the same way (trim, case-fold, drop a trailing
`archived` with its `-` or space) and join measurements on the normalised `site_id`. Log a warning if it
disagrees with the normalised resource name.

| Locations `NMBG_ID` | Resource name | File `site_id` | Normalised key |
|---|---|---|---|
| `E-1639-POD1` | `E-1639-POD1` | `e-1639-pod1` | `e-1639-pod1` |
| `E-50-1-archived` | `E-50-1-Archived` | `e-50-1 archived` | `e-50-1` |
| `Hagerman HQ-archived` | `Hagerman HQ-Archived` | `Hagerman HQ-archived` | `hagerman hq` |
| `Magnum Steel` | `Magnum Steel` | `Magnum Steel` | `magnum steel` |
| `E-9673 ` (trailing space) | `E-9673` | `e-9673` | `e-9673` |
| `E-2043` | `E-2034` | `e-2034` | **no match**, see below |

**`E-2043` is almost certainly `E-2034`.** The locations file has `E-2043` and no `E-2034`. The catalog
has resource `E-2034` (`site_id` `e-2034`) and no `E-2043`. Each is the only unmatched ID on its side.
Map `E-2043` to `E-2034` with a one-line alias in the shim, and ask EBWPC to correct the file.

**Every well has a location.** The legacy loader's "no location" notes came from three causes:
- the `E-2043` typo (`E-2034`)
- the trailing space (`E-9673`)
- the loader skipping rows that end in `archived` (`E-50-1`, `Greene-4`, `Lujan-1`)

This corrects the "Missing locations" bullet in `ckan_water_data_catalog.md`.

---

## Thing

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | *(fixed)* | Fixed: `Water Well`. Confirmed against old FROST. |
| `description` | str | Required | *(fixed)* | Fixed: `Well drilled or set into subsurface for the purposes of pumping water or monitoring groundwater`. Old FROST has `No Description`; follow the template. |

**properties - standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `agency` | str | Required | *(fixed)* | Fixed: `EBWPC`. Same code as old FROST. |
| `source_id` | str | Required | `NMBG_ID` | Same as Location |
| `alternate_id` | [{id: str, agency: str}] \| None | Optional | *(not in source)* | See Location |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `well_depth` | {value: float, unit: str} \| None | Not provided. The four descriptions that mention depth all say "Approximate Depth Unknown". |
| `screens` | [{top: float, bottom: float}] \| None | Not provided |
| `lithology` | str \| None | The formation from the same four descriptions (`Madera Formation`, `Alluvium/Yeso`, `Alluvium/Abo Formation`, `Alluvium`). Named after the WDI best-practice field. `None` elsewhere. |

---

## Sensor

| Existing Constant | Use for this source? |
|---|---|
| `MANUAL_SENSOR` (Manual) | **Yes**, for `manual_depth_bgs` |
| Transducer | **Yes**, for `transducer_depth_bgs`. Listed in `_mapping_template.md` but not yet defined in `canonical_constants.py`. |
| `HYDROVU_SENSOR` (VuLink) | No |
| All others (Pressure, Acoustic, VanEssenDiver, Bubbler, Satellite, Radio, RadioTower, AVFM, OneRain, NoSensor) | No |

**New sensor needed? Yes, `TRANSDUCER_SENSOR`.** Old FROST has it as `Sensors(13)`: `{"name":
"Transducer", "description": "No Description", "encodingType": "application/pdf", "metadata": "No
Metadata"}`. Following this repo's convention (`NO_DEFINITION` in the description slot, as in `ebid.md`):

```python
TRANSDUCER_SENSOR = CanonicalSensor(
    external_key="sensor-transducer",
    name="Transducer",
    description=NO_DEFINITION,
    encoding_type="application/pdf",
    metadata=NO_METADATA,
)
```

The make and model of the transducers isn't published. Adding the constant is a later ticket.

---

## ObservedProperty

| Existing Constant | Provided? | Source field/param code | Notes |
|---|---|---|---|
| Depth to Water Below Ground Surface | **Yes** | `transducer_depth_bgs`, `manual_depth_bgs` | `DTW_OBS_PROP`. Both old FROST datastreams per well point at it. |
| Groundwater Elevation | No | - | No elevations are published per reading |
| Groundwater Head | No | - | - |
| Adjusted Groundwater Head | No | - | - |
| Raw Depth to Water | No | - | - |
| OSERealTimeDischarge | No | - | - |
| OSERealTimeGageHeight | No | - | - |

**Not mapped: below-top-of-casing readings.** `manual_depth_BTOC` (`hagerman`, `rubyshaw`) and
`manual_depth_Below_TOC` (`e-6385`) are measured from the top of casing, not ground surface. That's 67
readings across three archived wells, and they would need a measuring-point height to become BGS. Skip
them and count them in asset metadata. See Open Questions.

**New observed property needed?** No.

---

## Datastream

One per (Thing, ObservedProperty, Sensor), so up to two per well. Names and descriptions match old FROST.

| | Transducer datastream | Manual datastream |
|---|---|---|
| Source column | `transducer_depth_bgs` | `manual_depth_bgs` |
| Suffix | `dtw` | `dtw-manual` |
| `name` | `Groundwater Levels` | `Manual Groundwater Levels` |
| `description` | `Measurement of groundwater depth in a water well, as measured below ground surface` | `Manual measurement of groundwater depth in a water well, as measured below ground surface` |
| Sensor | `TRANSDUCER_SENSOR` | `MANUAL_SENSOR` |
| `is_continuous` | `true` | `false` |

The transducer stream keeps the plain `dtw` suffix that the continuous HydroVu sources use, and the
manual one is qualified, mirroring old FROST's naming.

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | *(fixed)* | See table above |
| `description` | str | Required | *(fixed)* | See table above |
| `unitOfMeasurement` | JSON | Required | *(fixed)* | `UNIT_FOOT`. Confirmed against old FROST. |

`observationType` is `OM_Measurement`, confirmed.

**properties - standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `topic` | str \| None | Optional | *(not in source)* | `Water Quantity`, as on every old FROST EBWPC datastream |
| `is_provisional` | bool \| None | Optional | *(not in source)* | Not set. The dataset says transducer data were drift-corrected, but nothing marks individual values. |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `is_continuous` | bool | *(not in source)*. Old FROST sets `true` on the transducer stream. Same key as `ebid.md` and `san_acacia.md`. |

**New datastream template.** `canonical_constants.py` has `gwl_datastream_meta` ("Groundwater Levels")
only. Add a manual counterpart in the same later ticket as `TRANSDUCER_SENSOR`:

```python
def manual_gwl_datastream_meta(agency: str, location_name: str) -> dict:
    return {
        "name": "Manual Groundwater Levels",
        "description": "Manual measurement of groundwater depth in a water well, as measured below ground surface",
    }
```

**Datastream suffix(es):** `dtw` (transducer), `dtw-manual` (manual)

**How many datastreams per station?** Up to two. Create a datastream only when the well has at least one
usable reading of that type. Old FROST created both for every well and left many empty (`E-2043`,
`E-50-4`, `E-9673`).

| Wells | Transducer | Manual |
|---|---|---|
| `E-1639-POD1`, `E-2034`, `E-2298`, `E-50-4`, `E-7545`, `E-8428`, `E-94077`, `E-9673`, `Smith-1`, `T-6363` | yes | yes |
| `Osita Ranch`, `E-10652` (files not archived; per old FROST) | yes | yes |
| `E-00428` (file not archived; per old FROST) | no | yes |
| `Magnum Steel`, `E-50-1` (archived) | no | yes |
| `Greene-4`, `Lujan-1` (archived) | yes | no |
| `Hagerman HQ`, `Ruby Shaw WM`, `E-6385` (archived; TOC readings only) | no | no |
| `E-9407` (archived; XLSX) | unknown | unknown |

---

## Observation

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `phenomenonTime` | datetime (UTC) | Required | `measurement_date` + `measurement_time` | Naive local strings in several formats; add 7 hours for UTC. See [Timestamps and timezone](#timestamps-and-timezone). |
| `result` | float | Required | `transducer_depth_bgs` or `manual_depth_bgs` | Feet below ground surface, no conversion. One CSV row can yield two observations: 207 of the 335 manual readings share a row with a transducer reading. |
| `resultTime` | datetime | Optional | *(not in source)* | Set equal to `phenomenonTime`, as old FROST did |
| `resultQuality` | str \| None | Optional | *(not in source)* | `None` |
| `validTime` | period | Optional | *(not in source)* | Not applicable |

**parameters - standard keys:**

| Canonical Field | Type | Source Field | Notes |
|---|---|---|---|
| `measuring_agency` | str \| None | *(not in source)* | `None`. The dataset names its contractors by period, but the periods share the year 2009 and leave 2021-02 to 2021-12 uncovered. See Open Questions. |
| `measurement_method` | str \| None | *(not in source)* | `None`. The Sensor already says manual vs transducer, and the hand-measurement instrument isn't stated. |
| `data_source` | str \| None | *(not in source)* | Could be fixed `"NM Water Data Catalog"`, as `pvacd_hydrovu.md` and `ebid.md` propose for theirs |
| `water_level_status` | str \| None | *(not in source)* | `None` |
| `measurement_point_height` | float \| None | *(not in source)* | `None`. Its absence is why TOC readings are skipped. |
| `water_level_accuracy` | float \| None | *(not in source)* | `None` |

**parameters.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `time_missing` | bool | Set `true` on readings whose `measurement_time` is `N/A` or blank, which get 00:00 local. 33 manual readings. Omit otherwise. |

**Rows to drop**, counted in asset metadata. Figures are from the 17 archived well files.

- **No usable depth:** `N/A`, `#REF!`, `#VALUE!` or blank in the depth column. `N/A` runs from 1 row in
  some files up to 168 in `smith-1`.
- **No usable date:**
  - 4 manual readings in `e-9673` have `#VALUE!` for both date and time.
  - 7 manual readings in `e-8428` are dated `1/0/1900` (Excel's zero date).
- **Exact duplicates:** `e-50-1` repeats one reading. `e-2034`'s 533 duplicate rows all have no depth
  value, so they fall out under the first rule anyway.
- **Blank rows and trailing columns:** headers carry empty trailing columns (`...,manual_depth_bgs,,`).
  There are blank trailing rows (51 in `e-2034`, 49 in `e-94077`, 2 in `e-9673`), and `e-94077` has
  stray values, mostly a repeated date, in unnamed column 6 on 21 rows. Ignore unnamed columns.

### Timestamps and timezone

**Formats.** Dates come as `M/D/YYYY` or `M/D/YY` (two-digit years are 20xx), and `e-1639-pod1` mixes
both. Times come as `H:MM` (24-hour) or `h:mm:ss AM/PM`, or `N/A`/blank for date-only manual readings.
Parse the date and time separately, then combine.

**The clock is a fixed UTC−07:00 (MST), not America/Denver.**

- **The loggers ignore daylight saving.** All 12 archived files with transducer data have readings at
  02:xx on every spring-forward date covered: 77 dates. That hour doesn't exist on an America/Denver
  clock. None of the 84 fall-back dates repeats an 01:xx hour.
- **Manual readings are local daytime on the same clock.** Manual times cluster between 09:00 and 16:00,
  and most sit on logger rows.
- **Conversion:** attach `timezone(timedelta(hours=-7))` and convert to UTC, i.e. add 7 hours. Don't use
  `ZoneInfo("America/Denver")`, which would mis-shift summer readings and reject the 02:xx ones.
- This supersedes the DST-aware proposal in `ckan_water_data_catalog.md` (Open Question 5) for EBWPC.
  Confirm with John Shomaker & Associates (see Open Questions).

**Manual times are approximate.**
- 157 of the 302 manual readings that have a time are stamped exactly 00:00 or 12:00, which look like
  placeholders.
- 33 more have no time at all.
- Keep them as published, flagging only the missing-time ones.

**Old FROST EBWPC times are 7 hours off.** The legacy loader stamped these naive local times as UTC. For
example, E-8428's last transducer reading is `2024-04-26T08:18:00Z` in old FROST; it is
`2024-04-26T15:18:00Z` here.

---

## Unit Conversions

| Field | Source Unit | Canonical Unit | Conversion |
|---|---|---|---|
| `transducer_depth_bgs`, `manual_depth_bgs` | Feet below ground surface (implied by the `_bgs` names and by old FROST; there is no unit column) | Feet | None |
| `UTM_Zone13N_Easting`, `UTM_Zone13N_Northing` | Metres, NAD83 / UTM zone 13N (EPSG:26913) | WGS84 longitude, latitude (EPSG:4326) | `pyproj.Transformer.from_crs("EPSG:26913", "EPSG:4326", always_xy=True)`. Reproduces old FROST's coordinates exactly for all 14 active wells. |
| `measurement_date` + `measurement_time` | Naive, fixed UTC−07:00 | UTC | +7 hours |
| `elevation` (resource `description`, 4 wells) | Feet AMSL | Metres | ÷ 3.28084 (`METRES_TO_FEET`) |
| `manual_depth_BTOC`, `manual_depth_Below_TOC` | Feet below top of casing | - | Not mapped (needs a measuring-point height) |

`pyproj` is not a project dependency yet. Add it with `uv add pyproj` in the pipeline ticket.

---

## Raw Response Example

### `package_show` (live, trimmed)

Saved from a browser. This is a sanitised version of the live response. Removed:
- the contact person's name, email and phone, and `creator_user_id`
- `author`, `maintainer`, `spatial`, `spatial_full`, `tags`, `groups` and other unused dataset fields
- 19 of the 22 resources

Kept resources: one CSV well, the XLSX well and the locations file, exactly as returned.

```json
{
  "help": "https://catalog.newmexicowaterdata.org/api/3/action/help_show?name=package_show",
  "success": true,
  "result": {
    "id": "719844ad-46bf-46cf-a781-a111f114fe38",
    "name": "ebwpc-gw-monitoring",
    "title": "EBWPC Monitoring Well Measurements",
    "type": "dataset",
    "state": "active",
    "private": false,
    "owner_org": "71cb96f5-e3c6-43b4-b679-dbbe52130edc",
    "metadata_created": "2024-08-06T18:42:51.376786",
    "metadata_modified": "2026-03-18T21:11:06.241598",
    "num_resources": 22,
    "data_collection_frequency": "semi-annually",
    "data_collection_procedures": "Data collected by Sandia National Labs from 2007 to 2009\r\nData collected by HydroResolutions from 2009 to 2/2021\r\nData collected by John Shomaker & Associates from 12/2021 to present ",
    "data_quality_procedures": "Data was checked for transducer drift and corrected.  Transducer data were matched every 6-months to a year to and-collected water-level data",
    "preparation_method": "Data was checked for transducer drift and corrected",
    "notes": "Transducer recorded and hand-measured water-level data (active record)\r\n\r\nLocations:\r\nOsita Ranch: about 9 miles NW of Clines Corner, New Mexico\r\nE-8428: About 5 miles north of Tajique, NM\r\nSmith-1: About 3 miles north-northeast of Mountainair, NM\r\nE-10652: About 5 miles east-northeast of Mountainair, NM\r\nE-7545: About 3 miles east of Manzano, NM\r\nMagnum Steel: about 2 miles west-northwest of Moriarty, NM\r\nT-6363: 16 miles southeast of Mountainair, NM\r\nE-1639: between Progresso and Cedarvale, New Mexico\r\n",
    "organization": {
      "id": "71cb96f5-e3c6-43b4-b679-dbbe52130edc",
      "name": "estancia-basin-water-planning-committee",
      "title": "Estancia Basin Water Planning Committee"
    },
    "resources": [
      {
        "cache_last_updated": null,
        "cache_url": null,
        "created": "2024-08-12T20:40:12.731975",
        "datastore_active": true,
        "description": "About 5 miles north of Tajique, NM\r\nTorrance County, New Mexico\r\nUTM Easting 386426, Northing 3853603, Zone 13, NAD83\r\nElevation 6712 (ft AMSL), Approximate Depth Unknown,  Madera Formation\r\n",
        "format": "CSV",
        "hash": "e5375a486250c83edef9fdf908ab88e9",
        "id": "0c306f4d-1474-4d00-9d77-45b73513abfd",
        "last_modified": "2025-03-12T18:11:29.830723",
        "metadata_modified": "2025-03-12T18:11:36.629889",
        "mimetype": "text/csv",
        "mimetype_inner": null,
        "name": "E-8428",
        "package_id": "719844ad-46bf-46cf-a781-a111f114fe38",
        "position": 1,
        "preview": true,
        "preview_rows": 100,
        "resource_type": null,
        "size": 2530586,
        "state": "active",
        "total_record_count": 75660,
        "url": "https://catalog.newmexicowaterdata.org/dataset/719844ad-46bf-46cf-a781-a111f114fe38/resource/0c306f4d-1474-4d00-9d77-45b73513abfd/download/e-8428.csv",
        "url_type": "upload"
      },
      {
        "cache_last_updated": null,
        "cache_url": null,
        "created": "2024-10-16T14:47:42.739290",
        "datastore_active": true,
        "description": "About 6 miles uproad east of Lucy, NM",
        "format": "XLSX",
        "hash": "121fd174e4fce0f75feb33342b5ea422",
        "id": "be49076b-375e-4570-90e5-bcc9dc97ae04",
        "last_modified": "2024-10-16T14:47:42.384457",
        "metadata_modified": "2024-10-16T15:17:57.286527",
        "mimetype": null,
        "mimetype_inner": null,
        "name": "E-9407-Archived",
        "package_id": "719844ad-46bf-46cf-a781-a111f114fe38",
        "position": 16,
        "resource_type": null,
        "size": 1189499,
        "state": "active",
        "url": "https://catalog.newmexicowaterdata.org/dataset/719844ad-46bf-46cf-a781-a111f114fe38/resource/be49076b-375e-4570-90e5-bcc9dc97ae04/download/e-9407_hydrographdata_data.xlsx",
        "url_type": "upload"
      },
      {
        "cache_last_updated": null,
        "cache_url": null,
        "created": "2024-12-04T22:26:46.734422",
        "datastore_active": true,
        "description": "This table contains the locations of EBWPC wells in UTM Zone13 coordinates. ",
        "format": "CSV",
        "hash": "9ce5f2b0dd8483a392448e6fd89a04c4",
        "id": "2f186d78-02d0-4d89-bb4f-540acb398774",
        "last_modified": "2024-12-04T22:35:10.974237",
        "metadata_modified": "2024-12-04T22:35:11.054660",
        "mimetype": "text/csv",
        "mimetype_inner": null,
        "name": "EBWPC Well Locations",
        "package_id": "719844ad-46bf-46cf-a781-a111f114fe38",
        "position": 21,
        "resource_type": null,
        "size": 763,
        "state": "active",
        "url": "https://catalog.newmexicowaterdata.org/dataset/719844ad-46bf-46cf-a781-a111f114fe38/resource/2f186d78-02d0-4d89-bb4f-540acb398774/download/ebwpc_locations.csv",
        "url_type": "upload"
      }
    ]
  }
}
```

### Locations file (live, complete)

`ebwpc_locations.csv`, all 21 rows. The original has CRLF line endings. Note `E-2043` and the trailing
space after `E-9673`.

```csv
NMBG_ID,UTM_Zone13N_Easting,UTM_Zone13N_Northing,Datum
E-2298,388505,3892477,NAD83
E-94077,395066,3880536,NAD83
Osita Ranch,428285,3889072,NAD83
Magnum Steel,401055,3875642,NAD83
E-00428,399315,3865705,NAD83
E-50-4,405656,3861382,NAD83
E-2043,398176,3855674,NAD83
E-1639-POD1,424100,3812580,NAD83
T-6363,404197,3802110,NAD83
E-10652,393754,3824033,NAD83
Smith-1,387742,3824322,NAD83
E-7545,381162,3834482,NAD83
E-8428,386426,3853605,NAD83
E-9673 ,381972,3879947,NAD83
E-9407-archived,432560,3837007,NAD83
Lujan-1-archived,393864,3824679,NAD83
Ruby Shaw WM-archived,382687,3822107,NAD83
E-50-1-archived,405241,3864604,NAD83
E-6385-archived,402585,3874082,NAD83
Greene-4-archived,403139,3792977,NAD83
Hagerman HQ-archived,426891,3889512,NAD83
```

### Well files (excerpts)

From the Internet Archive's January 2026 copies. Their sizes match the live `package_show` `size`, and
every `last_modified` predates the capture, so they are the current files. All have CRLF line endings.
Rows are copied verbatim.

`e-8428.csv`, an active well with both depth columns and 24-hour times. The first two rows are manual only,
the third has both readings on one row, and the last is the newest reading in the dataset:

```csv
site_id,measurement_date,measurement_time,transducer_depth_bgs,manual_depth_bgs
e-8428,4/15/2009,0:00,,58.3
e-8428,6/4/2009,0:00,,57.22
e-8428,6/5/2009,10:00,57.17145,57.22
e-8428,4/26/2024,8:18,54.98248,56.08
```

`e-1639-pod1.csv`, with two-digit years, AM/PM times, an `N/A` time and depth, and a four-digit year on the
last row:

```csv
site_id,measurement_date,measurement_time,transducer_depth_bgs,manual_depth_bgs
e-1639-pod1,1/18/18,N/A,N/A,67.65
e-1639-pod1,4/13/18,9:00:00 AM,68.00145004,
e-1639-pod1,4/13/18,4:00:00 PM,67.91670368,67.63
e-1639-pod1,4/25/2024,3:12:00 PM,,68.61
```

`e-9673.csv`, with empty trailing columns and manual readings that lost their date:

```csv
site_id,measurement_date,measurement_time,transducer_depth_bgs,manual_depth_bgs,,,,
e-9673,7/24/2013,6:00,237.50416,,,,,
e-9673,#VALUE!,#VALUE!,N/A,237.03,,,,
e-9673,#VALUE!,#VALUE!,N/A,230.39,,,,
```

`e-6385.csv`, an archived well with only below-top-of-casing readings:

```csv
site_id,measurement_date,measurement_time,manual_depth_Below_TOC
E-6385-archived,2/18/2009,16:55,131.81
E-6385-archived,4/15/2009,16:06,136.16
```

---

## Open Questions

1. **Logger clock.** Confirm with John Shomaker & Associates that the transducers and field times are
   on MST (UTC−07:00) year-round, and not UTC. The evidence rules out daylight saving but can't tell a
   fixed MST clock from a fixed UTC one on its own.
2. **`E-2043` vs `E-2034`.** Confirm the typo with EBWPC and ask them to correct the locations file.
   Until then, alias it in the shim.
3. **Below-top-of-casing readings** (`hagerman`, `rubyshaw`, `e-6385`; 67 readings). Skip them
   (proposed), ask EBWPC for measuring-point heights, or load them under a separate observed property.
4. **Archived wells.** Proposal: load them like the rest, with `is_archived` set. Three have usable
   readings (`E-50-1`, `Greene-4`, `Lujan-1`), three have only TOC readings, and `E-9407` is XLSX. See
   the datastream table.
5. **`E-9407-Archived` is XLSX.** Proposal: skip it and log it, and ask EBWPC to re-upload it as CSV
   per the WDI best practices. Its contents are unknown because the file wasn't archived.
6. **`measuring_agency`.** Derive it from the dataset's contractor periods, or leave it `None`
   (proposed)? The periods overlap in 2009 and miss 2021-02 to 2021-12.
7. **`alternate_id`.** Are the `E-nnnn`/`T-nnnn` IDs OSE file numbers? If so, emit
   `[{id, agency: "NMOSE"}]`.
8. **Elevation and formation.** Parse the free-text descriptions (4 of 21 wells, proposed), or ask EBWPC
   to add `elevation`, `elevation_units` and `lithology` columns to the locations file per the WDI best
   practices? Elevation is a WDI required field that EBWPC doesn't otherwise provide.
9. **`data_source`.** Set a fixed `"NM Water Data Catalog"`, or leave `None`? Settle it the same way
   across sources.
