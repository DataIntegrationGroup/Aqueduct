# Source Mapping: BernCo HydroVu

**Source key:** `bernco_hydrovu`
**Agency code:** `BERNCO`
**Response format:** `json`
**Source timezone:** UTC (HydroVu `timestamp` is Unix epoch seconds). See the device-clock caveat below.
**Update frequency:** 4-hourly for most wells. Observed cadences across the 29 DTW locations: 4 h (19), 1 h (4), 6 h (3), 24 h (2), 1 min (1, `LVCC`)

**Credentials:** GCP Secret Manager, project `waterdatainitiative-271000` (project number
`95715287188`), secret name `hydrovu_bernco`, confirmed present 2026-08-24 and separate
from PVACD's `hydrovu_pvacd`. The payload is a JSON object with keys `id` and `secret`,
which is what `_resolve_hydrovu_credentials()` in `sources/hydrovu/dlt_pipeline.py`
already expects, so no credential-handling code has to change.

`deploy/30_dagster_gcp_auth.sh` grants `roles/secretmanager.secretAccessor` on
`hydrovu_pvacd` only. The Dagster service account needs the same grant on
`hydrovu_bernco` before this source can run in production.

**API endpoints confirmed live (2026-08-24):**

| Endpoint | URL |
|---|---|
| Auth | `POST https://hydrovu.com/public-api/oauth/token` |
| Locations | `GET https://www.hydrovu.com/public-api/v1/locations/list` |
| Readings | `GET https://www.hydrovu.com/public-api/v1/locations/{id}/data?startTime={unix_ts}` |
| Parameter/unit names | `GET https://www.hydrovu.com/public-api/v1/sispec/friendlynames` |

Machine-readable spec: `GET https://www.hydrovu.com/public-api/docs/spec` (OpenAPI 3.0.3).
The page at `https://www.hydrovu.com/public-api/docs/` is a Swagger UI shell that renders
it. Fetching that URL yields no documentation content.

**Auth:** OAuth2 client credentials, scopes `read:locations read:data`, tokens valid 3600 s.
`BearerAuth` (`shared/http.py`) already refreshes and retries once on 401.

**Pagination:** cursor-based via the `X-ISI-Start-Page` request header and `X-ISI-Next-Page`
response header (a 64-character opaque token). Pass `""` first; stop when the response
header is absent or empty. A readings page covers roughly a 2-day block of time, so the
reading count per page varies with logger cadence: 10 to 12 rows per page at a 4-hour
cadence, about 2,500 at `LVCC`'s 1-minute cadence. Keep every filter identical across
pages or the cursor is invalidated.

**Rate limit:** 1000 requests/minute per account, reported by the `x-isi-requests-this-minute`
and `x-isi-requests-timeout` response headers. The budget belongs to the credentials, not
the pipeline, so BernCo and PVACD contend only if they share a client ID (which they do not).

**Tenant shape (53 locations, 2026-08-24):**

| Device profile | Parameters | Locations | Carries DTW? |
|---|---|---|---|
| Aqua TROLL water-quality sonde | 1, 2, 4, 9, 10, 11, 12, 13, 14 | 22 | Yes |
| Level-only logger | 1, 2, 4 | 7 | Yes |
| VuLink gateway diagnostics only | 1, 16, 33 | 9 | **No** |
| VuLink gateway diagnostics + battery voltage | 1, 16, 26, 33 | 1 | **No** |
| No data in the last 60 days (HTTP 404) | — | 14 | — |

**35 of the 53 locations have carried `parameterId="4"` at some point; 29 report it within
the last 60 days.** The 10 VuLink-diagnostics locations report the gateway's own telemetry
(temperature, barometric pressure, battery) and hold no water-level data at all. Exclude
them from the ingest allowlist rather than filtering them out at transform time.

**Device-clock caveat:** the spec warns that VuLink timestamps are always UTC but
timestamps from a Cube or Tube "may appear in the device's local time." The evidence here
points to VuLink throughout. Many locations are named `*VuLink` or `*VL`, and
`parameterId` 16 (Baro) and 33 (Battery Level) are VuLink gateway telemetry. Treating
timestamps as UTC is correct for this tenant, but confirm with BernCo that the fleet holds
no Cube or Tube devices. A wrong answer shifts every observation by the UTC offset and
nothing in the pipeline would flag it.

**Reading consolidation:** a returned reading may be the *average* of several underlying
readings when the logger samples faster than once per minute. Minutes with no readings are
omitted rather than returned as nulls.

---

## Location

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | `locations[].name` | Use as-is, e.g. `"SierraVista-966932"` |
| `description` | str | Required | *(fixed)* | Fixed: `"Location of well where measurements are made"` |
| `encodingType` | str | Required | *(fixed)* | Fixed: `"application/geo+json"`, the default on `CanonicalLocation` |
| `location` (longitude) | float | Required | `locations[].gps.longitude` | GeoJSON coordinates[0] |
| `location` (latitude) | float | Required | `locations[].gps.latitude` | GeoJSON coordinates[1] |

**properties — standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `source_id` | str | Required | `locations[].id` | 64-bit integer, cast to `str` at the adapter boundary |
| `geoconnex` | str | Optional | *(not in API)* | Not returned by HydroVu |
| `alternate_id` | [{id: str, agency: str}] \| None | Optional | *(not in API)* | Not returned by HydroVu |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `hydrovu_description` | str | `locations[].description`. Populated on 26 of 53 locations; a 7-digit well/permit number such as `"1194043"`. Empty string on the other 27, the same pattern as PVACD, where it holds `"827276"` |
| `elevation` | {value: float, unit: "m"} \| None | **Not provided.** The `LatLng` schema carries only `latitude` and `longitude` |

**Three locations sit at In-Situ's factory-default coordinates.** `default-1191022`
(id `4905339374338048`), `default-817181` (id `6179866399342592`) and `default-969659`
(id `4657879867523072`) all report `40.58833, -105.06587`, which is In-Situ's Fort Collins,
Colorado headquarters. Nobody ever gave these devices a real position.
`default-969659` does carry live DTW data, so dropping it as inactive is probably not an
option; loading it would put a New Mexico groundwater observation in Colorado.
Either exclude all three from the allowlist or get real coordinates from BernCo first.
See Open Questions #5.

---

## Thing

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | *(fixed)* | Fixed: `"Water Well"`. See the caveat below |
| `description` | str | Required | *(fixed)* | Fixed: `"Well drilled or set into subsurface for the purposes of pumping water or monitoring groundwater"` |

**properties — standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `agency` | str | Required | *(fixed)* | Fixed: `"BERNCO"` |
| `source_id` | str | Required | `locations[].id` | Same as Location, cast to `str` |
| `alternate_id` | [{id: str, agency: str}] \| None | Optional | *(not in API)* | Not returned by HydroVu |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `hydrovu_description` | str | `locations[].description`. Well/permit number, or `""` |
| `well_depth` | {value: float, unit: "ft"} \| None | **Not provided by the HydroVu API.** |
| `screens` | [{top: float, bottom: float}] \| None | **Not provided by the HydroVu API.** |

**Not every location is a well.** `Carlito Springs Flume`, `Carlito Springs Baro`,
`Carlito Springs Lower Pool` and `Carlito Springs Well` are a surface-water and
barometric cluster. The flume and lower pool report `parameterId="5"` (Level: Elevation)
rather than depth to water, and the baro station reports only temperature and barometric
pressure. All four are dormant (see below), so the fixed `"Water Well"` Thing name is safe
for the initial DTW-only ingest. It stops being safe the moment those locations come back
or the ingest widens. Flagged in Open Questions #4.

**external_key convention:** `make_location_key("BERNCO", source_id)` >
`bernco-{location_id}`, e.g. `bernco-6255051791532032`. The Location and its Thing share
the key.

---

## Sensor

Shared constants — pick one or describe a new one.

Note: `MANUAL_SENSOR` and `HYDROVU_SENSOR` are the only `CanonicalSensor` objects in
canonical/canonical_constants.py.

| Candidate | Exists in `canonical_constants.py`? | Use for this source? |
|---|---|---|
| `HYDROVU_SENSOR` (VuLink) | Yes | **Yes.** Location naming (`*VuLink`, `*VL`) and the gateway telemetry parameters 16 (Baro) and 33 (Battery Level) both indicate VuLink transmitters |
| `MANUAL_SENSOR` (Manual) | Yes | No. This is a continuous logger feed, not manual soundings |
| Pressure, Transducer | No | Not ruled out by the hardware. Both describe BernCo's instruments accurately; the choice against them is a modeling convention. See the note below |
| Acoustic, Bubbler, AVFM | No | Wrong measurement principle. BernCo's level readings come from submerged pressure transducers, not acoustic ranging, bubbler lines, or flow meters |
| VanEssenDiver, OneRain | No | Other vendors' platforms. BernCo's data arrives through HydroVu |
| Satellite, Radio, RadioTower | No | Telemetry transport labels. The HydroVu API never reports which transport a VuLink uses, and Aqueduct does not model transport |
| NoSensor | No | A placeholder for unknown instrumentation. The platform is known here |

**Why not Pressure or Transducer, when these are pressure transducers?** Every DTW location
also reports `parameterId="2"` (Pressure, psi), and depth to water is derived from it, so a
sensor named Transducer would fairly describe the sensing element. Aqueduct models Sensor at
the telemetry platform instead, which is what `HYDROVU_SENSOR` (VuLink) names, and
`pvacd_hydrovu.md` makes the same call for identical hardware. Staying consistent with PVACD
matters more than instrument-level precision, because the two sources have to be comparable
once both are loaded into FROST. BernCo's own naming disagrees, for what it is worth: the
location called `Magnum Steel Transducer` is a VuLink gateway record carrying only
parameters 1, 16 and 33, while the sonde data sits under `Magnum Steel`.

**New sensor needed?** No. `HYDROVU_SENSOR` covers this source, the same choice
`pvacd_hydrovu.md` makes.

One thing to record for later. The 22 sonde locations return conductivity, salinity, TDS,
resistivity and density, which points to Aqua TROLL 600-class instruments rather than the
Level TROLLs behind PVACD. The canonical model has one VuLink sensor constant and does not
distinguish the instrument behind the gateway. That is fine while only DTW is ingested.
Revisit it if the water-quality parameters are brought in.

---

## ObservedProperty

Shared constants — check all that apply.

| Existing Constant | Provided? | Source field/param code                                              | Notes |
|---|---|----------------------------------------------------------------------|---|
| Depth to Water Below Ground Surface (`DTW_OBS_PROP`) | **Yes** | `parameterId="4"` > `"Level: Depth to Water"`, `unitId="35"` > `"m"` | The only property ingested. 29 locations in the last 60 days, 35 across all history |
| Groundwater Elevation (`ELEV_OBS_PROP`) | Historical only | `parameterId="5"` > `"Level: Elevation"`, `unitId="35"` > `"m"`      | Present at 2 locations (`Carlito Springs Flume`, `Carlito Springs Lower Pool`), both dormant. Not ingested initially |
| Groundwater Head | No | —                                                                    | No constant exists |
| Adjusted Groundwater Head | No | —                                                                    | No constant exists |
| Raw Depth to Water | No | —                                                                    | No constant exists |
| OSERealTimeDischarge | No | —                                                                    | No constant exists |
| OSERealTimeGageHeight | No | —                                                                    | No constant exists |

**Other parameters present in BernCo readings (not ingested):**

| parameterId | Name (from `/sispec/friendlynames`) | unitId | Unit | Locations | Notes |
|---|---|---|---|---|---|
| `"1"` | Temperature | `"1"` | C | 39 | Present nearly everywhere |
| `"2"` | Pressure | `"17"` | psi | 29 | Raw transducer pressure |
| `"3"` | Depth | `"35"` | m | 4 (historical) | **Not the same as parameter 4.** See the warning below |
| `"5"` | Level: Elevation | `"35"` | m | 2 (historical) | Would map to `ELEV_OBS_PROP` |
| `"9"` | Actual Conductivity | `"65"` | µS/cm | 22 | |
| `"10"` | Specific Conductivity | `"65"` | µS/cm | 22 | |
| `"11"` | Resistivity | `"81"` | Ω-cm | 22 | |
| `"12"` | Salinity | `"97"` | psu | 22 | |
| `"13"` | Total Dissolved Solids | `"117"` | mg/L | 22 | |
| `"14"` | Density | `"129"` | g/cm³ | 22 | |
| `"16"` | Baro | `"17"` | psi | 10 | VuLink gateway telemetry |
| `"26"` | Battery Voltage | `"163"` | V | 1 | |
| `"33"` | Battery Level | `"241"` | % | 10 | VuLink gateway telemetry |

No location uses a custom parameter. `customParameter` is `false` on every parameter at
every location, and BernCo's `/sispec/friendlynames` response is byte-identical to the
public demo tenant's (70 parameters, 97 units), so every ID resolves through the standard
SIS lookup.

> **Do not treat `parameterId="3"` (Depth) as depth to water.** Parameter 3 is the depth of
> the sensor below the water surface. Parameter 4 is the depth from the reference point
> down to water. They share `unitId="35"` (metres) and are easy to conflate, but they
> measure in opposite directions. Filtering on `parameter_id == "4"`, as
> `sources/hydrovu/adapter.py` already does, is correct.

**A location's parameter set changes over time.** `SP4VuLink-636814` reports parameter 3
in its oldest data (from 2019-11-05) and parameter 4 in recent data.
`E-55-POD 15-1193641VL` reports VuLink diagnostics historically and the full sonde suite
now. An allowlist built from "locations reporting parameter 4 today" will be wrong for
backfill: 35 locations have carried DTW at some point versus 29 today. This is the main
thing to get right before a historical load.

**New observed property needed?** No. `DTW_OBS_PROP` covers the ingested parameter, and
`ELEV_OBS_PROP` already exists if parameter 5 is added later.

---

## Datastream

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | *(fixed)* | From `gwl_datastream_meta()`: `"Groundwater Levels"` |
| `description` | str | Required | *(fixed)* | `"Measurement of groundwater depth in a water well, as measured below ground surface"` |
| `unitOfMeasurement` | JSON | Required | *(fixed)* | `UNIT_FOOT`, i.e. `{name: "Foot", symbol: "ft", definition: "http://www.qudt.org/vocab/unit/FT"}` |

**properties — standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `topic` | str \| None | Optional | *(not in API)* | Would have to be a fixed `"Water Quantity"`. See Open Questions #7 |
| `is_provisional` | bool \| None | Optional | *(not in API)* | HydroVu exposes no QC or publication status |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| *(none)* | — | HydroVu carries no datastream-level metadata |

**Datastream suffix(es):** `dtw`
**How many datastreams per station?** One. DTW only, matching PVACD. Add a second `gwe`
datastream only if the dormant `parameterId="5"` locations come back in scope.

**external_key convention:** `make_datastream_key("BERNCO", source_id, "dtw")` >
`bernco-{location_id}-dtw`, e.g. `bernco-6255051791532032-dtw`.

---

## Observation

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes                                                                                                                                                                  |
|---|---|---|---|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `phenomenonTime` | datetime (UTC) | Required | `readings[].timestamp` | Unix epoch seconds > `datetime.fromtimestamp(ts, tz=UTC)`. Valid without conversion for VuLink; see the device-clock caveat. **Filter sentinel timestamps.** See below |
| `result` | float | Required | `readings[].value` | Metres > feet, `× 3.28084`. May be a within-minute average when the logger samples faster than 1/min                                                                   |
| `resultTime` | datetime | Optional | *(not in API)* | The `Reading` schema has only `timestamp` and `value`                                                                                                                  |
| `resultQuality` | str \| None | Optional | *(not in API)* | HydroVu returns no QC flags, so always `None`                                                                                                                          |
| `validTime` | period | Optional | *(not in API)* | Not applicable                                                                                                                                                         |

**parameters — standard keys:**

| Canonical Field | Type | Source Field | Notes |
|---|---|---|---|
| `measuring_agency` | str \| None | *(not in API)* | Not available |
| `measurement_method` | str \| None | *(not in API)* | Could be a fixed `"Continuous Pressure Logger"`. See Open Questions #7 |
| `data_source` | str \| None | *(not in API)* | Could be a fixed `"HydroVu"`. See Open Questions #7 |
| `water_level_status` | str \| None | *(not in API)* | No dry-well indicator |
| `measurement_point_height` | float \| None | *(not in API)* | Not available |
| `water_level_accuracy` | float \| None | *(not in API)* | Not available |

**parameters.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `customParameter` | bool | `parameters[].customParameter`. `false` everywhere in this tenant. Same low value as for PVACD |

**Sentinel timestamps near the epoch.** Two locations return readings stamped at or near
Unix epoch 0, carrying real-looking values:

| Location | Location id | Parameters affected | Sentinel timestamp | Next real reading |
|---|---|---|---|---|
| `WhisperingPines-1002958` | `5617246532927488` | 1, 2, 4 | `0` (1970-01-01 00:00:00Z), DTW value `108.683` m | 2010-11-08 15:07Z |
| `E-55-POD 15-1173850TD` | `6725276880732160` | 1, 2, 3, 9, 10, 11, 12, 13, 14 | `960` (1970-01-01 00:16:00Z) | 2025-02-28 |

These are bad device clocks, not real 1970 measurements. Loaded as-is they become 1970
`phenomenonTime` values and drag any min-date or time-series axis with them. The adapter
should drop readings below a sanity floor, and the source's `initial_start_date` is the
natural threshold. The sentinel is not always exactly `0`, so a `timestamp > 0` check will
not catch both.

---

## Unit Conversions

| Field | Source Unit | Source Evidence | Canonical Unit | Conversion Factor |
|---|---|---|---|---|
| `result` (DTW reading) | metres | `unitId="35"` > `"m"` via `GET /v1/sispec/friendlynames` (2026-08-24). Verified as the only unit used for `parameterId="4"` across all 53 locations | feet | `× 3.28084` |

Identical to PVACD, so `METRES_TO_FEET = 3.28084` in `sources/hydrovu/adapter.py` carries
over unchanged. Reference factors for the other level units HydroVu can return, none of
which appear in this tenant today:

| unitId | Symbol | > feet |
|---|---|---|
| `"38"` | ft | none |
| `"34"` | cm | `× 0.0328084` |
| `"37"` | in | `÷ 12` |
| `"33"` | mm | `× 0.00328084` |

---

## Open Questions

1. **Multi-tenant HydroVu is unmodeled.** BernCo is the first case of two agencies on one
   source system, and nothing in the repo anticipates it:
   - `.dlt/config.toml` has a single flat `[sources.hydrovu]` block with one `gcp_secret`
     (`hydrovu_pvacd`) and one flat `location_ids` list.
   - `shared/source_registry.py` has one entry, `{"name": "hydrovu", "dataset": "raw_pvacd"}`.
   - `deploy/00_config.sh:54` hardcodes `SECRET_HYDROVU="hydrovu_pvacd"`, and the
     `secretmanager.secretAccessor` grant in `deploy/30_dagster_gcp_auth.sh` is scoped to
     that one secret.

   `docs/STORAGE_CONVENTIONS.md:128-134` covers only the inverse case (one agency, several
   source systems). Following that convention BernCo would land at
   `raw_bernco/hydrovu_readings/` with the vertical slice at `sources/bernco_hydrovu/`.
   One more thing to settle is whether the existing `hydrovu` source should become
   `pvacd_hydrovu` for symmetry. Its dlt pipeline name already is `pvacd_hydrovu`
   (`dlt_pipeline.py:503`) while the folder and registry entry are the bare `hydrovu`.
   This ticket changes no code and no config.

2. **HTTP 404 means "no data at or after `startTime`", not "no data endpoint."**
   `sources/hydrovu/dlt_pipeline.py:191-193` logs a 404 as "no data endpoint" and treats it
   as terminal for the run. I retried the 14 BernCo locations that 404 on a recent
   `startTime`, and 13 of them return full history at `startTime=0`. They are dormant, not
   endpoint-less. Only `SerenityMesa` (id `4562953333243904`) 404s at `startTime=0` and has
   no data at all. The handling itself (skip, don't advance the cursor) stays right, but
   the log message points a debugger in the wrong direction. Worth a docstring fix in the
   implementation ticket.

3. **The API has an `endTime` parameter that the code says it does not.**
   `sources/hydrovu/dlt_pipeline.py:130-137` states "The real API has no server-side
   end-time parameter (only startTime)" and implements a client-side cutoff for windowed
   backfill. The published OpenAPI spec lists `endTime` as a query parameter on
   `/locations/{id}/data` alongside `startTime`. If it works server-side it would cut the
   windowing code out of the backfill path. I did not test it. Flagging it for whoever
   picks up the backfill work.

4. **Which locations are wells?** The Carlito Springs cluster (flume, baro, lower pool) is
   surface water and barometric instrumentation, not wells, so the fixed
   `Thing.name = "Water Well"` would be wrong for them. All three are dormant today and the
   DTW-only ingest never touches them, but BernCo should confirm the intended scope before
   anyone draws up the allowlist.

5. **The three factory-default locations.** `default-1191022`, `default-817181` and
   `default-969659` report In-Situ's Fort Collins coordinates. `default-969659` has live
   DTW data. Does BernCo have real coordinates for these, or should they be excluded?

6. **`initial_start_date` for BernCo.** History runs far deeper than PVACD's. The earliest
   readings go back to 2009-05-18 (`BCFDWildlandSub-1091579`), with onsets spread across
   2009, 2011, 2013, 2014 (×4), 2015, 2016, 2018, 2023 (×2), 2024 (×8), 2025 (×6) and 2026.
   A full load from 2009 across 35 DTW locations is a big backfill: at roughly 2-day pages,
   one location-decade is about 1,800 requests. Someone has to decide how much history
   BernCo wants before `initial_start_date` gets set. Build that backfill allowlist from
   historical parameter coverage, not current (see ObservedProperty above).

7. **Where do `topic`, `is_provisional`, `is_continuous`, `measurement_method` and
   `data_source` come from?** None appears in any source API. `ebid.md`,
   `san_acacia.md` and `pvacd_hydrovu.md` all raise versions of this; `is_continuous` is
   used as a `source_specific` key by two of them but is not documented in
   `CANONICAL_MODEL.md`.

---

## Raw Response Example

Captured live 2026-08-24 from the BernCo tenant. Coordinates rounded to 3 d.p. (about
100 m), readings trimmed to three per parameter. No credential material included.

**Locations response** (`GET /v1/locations/list`, three of the 53 objects in the array:
a sonde well, a VuLink-diagnostics location, and one factory-default location):

```json
[
  {
    "id": 6255051791532032,
    "name": "SierraVista-966932",
    "description": "",
    "gps": {
      "latitude": 35.123,
      "longitude": -106.353
    }
  },
  {
    "id": 4890597735137280,
    "name": "E-94077-1193582VL (Anaya-1)",
    "description": "1193582",
    "gps": {
      "latitude": 35.062,
      "longitude": -106.151
    }
  },
  {
    "id": 4657879867523072,
    "name": "default-969659",
    "description": "",
    "gps": {
      "latitude": 40.588,
      "longitude": -105.066
    }
  }
]
```

**Readings response** (`GET /v1/locations/6255051791532032/data?startTime=1782000000`,
`SierraVista-966932`, the full sonde profile. `parameterId="4"` is the only one ingested):

```json
{
  "locationId": 6255051791532032,
  "parameters": [
    {
      "parameterId": "2",
      "unitId": "17",
      "customParameter": false,
      "readings": [
        {"timestamp": 1782346800, "value": 18.035131},
        {"timestamp": 1782361200, "value": 18.031544},
        {"timestamp": 1782375600, "value": 18.035131}
      ]
    },
    {
      "parameterId": "1",
      "unitId": "1",
      "customParameter": false,
      "readings": [
        {"timestamp": 1782346800, "value": 14.099037},
        {"timestamp": 1782361200, "value": 14.119316},
        {"timestamp": 1782375600, "value": 14.111549}
      ]
    },
    {
      "parameterId": "4",
      "unitId": "35",
      "customParameter": false,
      "readings": [
        {"timestamp": 1782346800, "value": 70.972789728},
        {"timestamp": 1782361200, "value": 70.975276896},
        {"timestamp": 1782375600, "value": 70.97276534400001}
      ]
    },
    {
      "parameterId": "9",
      "unitId": "65",
      "customParameter": false,
      "readings": [
        {"timestamp": 1782346800, "value": 1380.7738},
        {"timestamp": 1782361200, "value": 1381.4589},
        {"timestamp": 1782375600, "value": 1380.7633}
      ]
    },
    {
      "parameterId": "10",
      "unitId": "65",
      "customParameter": false,
      "readings": [
        {"timestamp": 1782346800, "value": 1743.8601},
        {"timestamp": 1782361200, "value": 1743.8723},
        {"timestamp": 1782375600, "value": 1743.3207}
      ]
    },
    {
      "parameterId": "12",
      "unitId": "97",
      "customParameter": false,
      "readings": [
        {"timestamp": 1782346800, "value": 0.8878466},
        {"timestamp": 1782361200, "value": 0.887878},
        {"timestamp": 1782375600, "value": 0.88757426}
      ]
    },
    {
      "parameterId": "13",
      "unitId": "117",
      "customParameter": false,
      "readings": [
        {"timestamp": 1782346800, "value": 1133.509},
        {"timestamp": 1782361200, "value": 1133.517},
        {"timestamp": 1782375600, "value": 1133.1241}
      ]
    },
    {
      "parameterId": "11",
      "unitId": "81",
      "customParameter": false,
      "readings": [
        {"timestamp": 1782346800, "value": 724.23157},
        {"timestamp": 1782361200, "value": 723.87244},
        {"timestamp": 1782375600, "value": 724.2371}
      ]
    },
    {
      "parameterId": "14",
      "unitId": "129",
      "customParameter": false,
      "readings": [
        {"timestamp": 1782346800, "value": 0.99992156},
        {"timestamp": 1782361200, "value": 0.9999187},
        {"timestamp": 1782375600, "value": 0.99991953}
      ]
    }
  ]
}
```

**Readings response, VuLink-diagnostics profile**
(`GET /v1/locations/4890597735137280/data?startTime=1782000000`, `E-94077-1193582VL`).
There is no `parameterId="4"`, so this location contributes no observations:

```json
{
  "locationId": 4890597735137280,
  "parameters": [
    {
      "parameterId": "1",
      "unitId": "1",
      "customParameter": false,
      "readings": [
        {"timestamp": 1782000000, "value": 39.63719177246094},
        {"timestamp": 1782003600, "value": 38.06060028076172},
        {"timestamp": 1782007200, "value": 35.42222595214844}
      ]
    },
    {
      "parameterId": "33",
      "unitId": "241",
      "customParameter": false,
      "readings": [
        {"timestamp": 1782000000, "value": 74.40609741210938},
        {"timestamp": 1782003600, "value": 74.38973236083984},
        {"timestamp": 1782007200, "value": 74.38914489746094}
      ]
    },
    {
      "parameterId": "16",
      "unitId": "17",
      "customParameter": false,
      "readings": [
        {"timestamp": 1782000000, "value": 11.57793941870145},
        {"timestamp": 1782003600, "value": 11.574908605372988},
        {"timestamp": 1782007200, "value": 11.574986500108066}
      ]
    }
  ]
}
```

The parameter array is not ordered by `parameterId`. Here it arrives 1, 33, 16. Nothing
should depend on its order.

**Friendlynames response** (`GET /v1/sispec/friendlynames`, the entries relevant to this
tenant). The full response has 70 parameters and 97 units:

```json
{
  "parameters": {
    "1": "Temperature",
    "2": "Pressure",
    "3": "Depth",
    "4": "Level: Depth to Water",
    "5": "Level: Elevation",
    "9": "Actual Conductivity",
    "10": "Specific Conductivity",
    "11": "Resistivity",
    "12": "Salinity",
    "13": "Total Dissolved Solids",
    "14": "Density",
    "16": "Baro",
    "26": "Battery Voltage",
    "33": "Battery Level"
  },
  "units": {
    "1": "C",
    "17": "psi",
    "35": "m",
    "65": "µS/cm",
    "81": "Ω-cm",
    "97": "psu",
    "117": "mg/L",
    "129": "g/cm³",
    "163": "V",
    "241": "%"
  }
}
```

**Token response** (`POST /public-api/oauth/token`, `grant_type=client_credentials`).
Shape only, no real token:

```json
{
  "access_token": "<opaque base64 string>",
  "token_type": "bearer",
  "expires_in": 3600,
  "scope": "read:locations read:data"
}
```
