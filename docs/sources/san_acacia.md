# Source Mapping: San Acacia (VanEssen GroundwaterOnline)

**Source key:** `san_acacia` (TBD — confirm before implementation)
**Agency code:** `SanAcaciaReach`
**Response format:** `json`
**Source timezone:** UTC
**Update frequency:** ~5 minutes (historical, from old FROST data)

**Credentials:** none — confirmed unauthenticated API, no key/token/header.

**API endpoints (base has a doubled `/api/api/` segment — confirmed, not a typo):**

| Endpoint | URL |
|---|---|
| All locations | `GET https://diver-hub.com/api/api/locations` |
| Locations by project | `GET .../locations/{projectName}` |
| Location detail | `GET .../locations/{projectName}/{id}` |
| Monitoring point (readings) | `GET .../monitoringPoint/{projectName}/{monitoringPointID}` — **currently returns 500 for every ID tried; escalated to VanEssen** |

Pagination: none — `/locations/{projectName}` returns all 33 wells in one response.

---

## Location

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | `name` | e.g. `"BRN E-01-A"` — use as-is |
| `description` | str | Required | *(fixed)* | Fixed: `"Location of well where measurements are made"` |
| `encodingType` | str | Required | *(fixed)* | Fixed: `"application/geo+json"` |
| `location` (longitude) | float | Required | `lng` | GeoJSON coordinates[0] |
| `location` (latitude) | float | Required | `lat` | GeoJSON coordinates[1] |

**properties — standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `source_id` | str | Required | `uid` | Already a string, e.g. `"sanacaciareach-40"` — confirmed via old FROST data. Not the bare `id`. Check `make_location_key()` doesn't re-prefix it (would double up). |
| `geoconnex` | str | Optional | *(not in VanEssen API)* | Present in old FROST data (real geoconnex.us URIs) — source TBD |
| `alternate_id` | [{id: str, agency: str}] \| None | Optional | possibly `monitoringPoints[].name` | `"SO-0125"`-style codes, maybe NM OSE POD numbers — unconfirmed |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `elevation` | — | Present in schema (`groundSurfaceData: [{fromDate, elevation}]` on the `monitoringPoint` endpoint), but as a time-series value, not a static property — and unconfirmed since that endpoint is currently broken. |

---

## Thing

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | *(fixed)* | Fixed: `"Water Well"` |
| `description` | str | Required | *(fixed)* | Fixed: `"Well drilled or set into subsurface for the purposes of pumping water or monitoring groundwater"` |

**properties — standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `agency` | str | Required | *(fixed)* | Fixed: `"SanAcaciaReach"` |
| `source_id` | str | Required | `uid` | Same as Location |
| `alternate_id` | [{id: str, agency: str}] \| None | Optional | see Location | Unconfirmed |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `well_depth` | {value: float, unit: "ft"} | `drillingDepth` is in **centimetres** — confirmed by back-calculation (all values divide evenly by `30.48`). Convert: `value_ft = drillingDepth / 30.48` |
| `number_of_screens` | int | `numberOfScreens` — a count only, not screen intervals. Canonical `screens` field stays `None`. |
| `purpose` | str | Always `"MonitoringWell"` |
| `is_active` | bool | `isActive` |
| `installation_date` | str | `installationDate`, e.g. `"2024-10-30T07:00:00"` — no timezone suffix |
| `monitoring_point_names` | list[str] | `monitoringPoints[].name`, e.g. `["SO-0125"]` |

`city`, `address`, `owner`, `stationGroup`, `imageLink` — always empty, not mapped.

---

## Sensor

| Existing Constant | Use for this source? |
|---|---|
| `HYDROVU_SENSOR` (VuLink) | No |
| Manual | No |

**New sensor needed? Yes** — `VanEssenDiver`. Confirmed as the only sensor
used across all San Acacia datastreams (queried old FROST data, filtered by
agency — exactly one match). Use this codebase's `NO_DEFINITION`/`NO_METADATA`
placeholders.

---

## ObservedProperty

| Existing Constant | Provided? | Source field/param code | Notes |
|---|---|---|---|
| Depth to Water Below Ground Surface | **Yes** | likely `gs` values | Confirmed — matches `DTW_OBS_PROP` exactly via old FROST data |
| Groundwater Elevation | No | possibly `vrd` values | Never historically loaded for this source |

**New observed property needed?** No — `DTW_OBS_PROP` already matches.

---

## Datastream

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | *(fixed)* | Confirmed: `"Groundwater Levels"` |
| `description` | str | Required | *(fixed)* | Confirmed: `"Measurement of groundwater depth in a water well, as measured below ground surface"` |
| `unitOfMeasurement` | JSON | Required | *(fixed)* | Confirmed: `UNIT_FOOT` |

**properties — standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `topic` | str \| None | Optional | *(not in VanEssen API)* | Old FROST value: `"Water Quantity"` — see Open Question below |
| `is_provisional` | bool \| None | Optional | *(not in VanEssen API)* | Old FROST value: `true` — see Open Question below |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `is_continuous` | bool | *(not in VanEssen API)* — new field, not yet documented in `CANONICAL_MODEL.md`. Old FROST value: `true` — see Open Question below. |

**Open question:** `topic`, `is_provisional`, and `is_continuous` don't appear
anywhere in VanEssen's real API (neither `/locations` nor `/monitoringPoint`).
The old FROST pipeline assigned them from somewhere else, unconfirmed where.
Should the new adapter reuse the same fixed values, or is that a decision the
team needs to make explicitly rather than inherit silently?

**Datastream suffix(es):** `dtw`

**How many datastreams per station?** One — DTW only, confirmed.

---

## Observation

Confirmed via old FROST data (downstream, not the raw VanEssen API — the
`monitoringPoint` endpoint is still broken):

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `phenomenonTime` | datetime (UTC) | Required | `ts` | ISO 8601 with `Z`, e.g. `"2025-03-19T23:50:00Z"`. Raw `ts` format (epoch s/ms) unconfirmed. |
| `result` | float | Required | `vrd` or `gs` | Already in feet (observed range ~1–11 ft). Raw API's native unit unconfirmed. |
| `resultQuality` | str \| None | Optional | *(derived)* | From which array a reading is in: `approvedWaterLevels*` vs `unApprovedWaterLevels*` |

---

## Unit Conversions

| Field | Source Unit | Source Evidence | Canonical Unit | Conversion Factor |
|---|---|---|---|---|
| `well_depth` (`drillingDepth`) | centimetres | Values divide evenly by `30.48` into clean feet | feet | `÷ 30.48` |
| `result` (`vrd` / `gs`) | feet (via old FROST) | Result values ~1–11, consistent with DTW in feet | feet | none — unconfirmed against raw API |

---

## Raw Response Example

**Locations** (`GET /api/api/locations/sanacaciareach`, real data, 2 of 33):

```json
{
  "locations": [
    {
      "projectName": "sanacaciareach",
      "id": 39,
      "uid": "sanacaciareach-39",
      "name": "BRN E-01-A",
      "lat": 34.003032,
      "lng": -106.869709,
      "isActive": true,
      "purpose": "MonitoringWell",
      "drillingDepth": 612.648,
      "numberOfScreens": 1,
      "installationDate": "2024-10-30T07:00:00",
      "monitoringPoints": [{"id": 39, "name": "SO-0125", "isActive": true}]
    },
    {
      "projectName": "sanacaciareach",
      "id": 43,
      "uid": "sanacaciareach-43",
      "name": "BRN W-05",
      "lat": 34.00067,
      "lng": -106.872017,
      "isActive": true,
      "purpose": "MonitoringWell",
      "drillingDepth": 581.8632,
      "numberOfScreens": 2,
      "installationDate": "2024-10-30T07:00:00",
      "monitoringPoints": [
        {"id": 43, "name": "SO-0144", "isActive": true},
        {"id": 44, "name": "SO-0145", "isActive": true}
      ]
    }
  ],
  "status": 200,
  "message": ""
}
```

**Monitoring point** (`GET .../monitoringPoint/{projectName}/{id}`) — **schema
only**, live endpoint currently returns 500:

```json
{
  "status": 200,
  "approvedWaterLevelsVrd": [{"ts": 0, "vrd": 0}],
  "unApprovedWaterLevelsVrd": [{"ts": 0, "vrd": 0}],
  "groundSurfaceData": [{"fromDate": "2026-07-16T19:26:00.385Z", "elevation": 0}],
  "approvedWaterLevelsGs": [{"ts": 0, "gs": 0}]
}
```

**Old FROST Datastream** (`st2.newmexicowaterdata.org`, downstream data):

```json
{
  "name": "Groundwater Levels",
  "description": "Measurement of groundwater depth in a water well, as measured below ground surface",
  "unitOfMeasurement": {"name": "Foot", "symbol": "ft", "definition": "http://www.qudt.org/vocab/unit/FT"},
  "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
  "properties": {"agency": "SanAcaciaReach", "is_continuous": true, "is_provisional": true, "topic": "Water Quantity"},
  "Sensor": {"name": "VanEssenDiver", "description": "No Description", "encodingType": "application/pdf", "metadata": "No Metadata"},
  "ObservedProperty": {"name": "Depth to Water Below Ground Surface", "definition": "No Definition", "description": "depth to water below ground surface"}
}
```

**Old FROST Observations** (`st2.newmexicowaterdata.org`, downstream data):

```json
{
  "value": [
    {"phenomenonTime": "2025-03-19T23:50:00Z", "result": 6.79817607055915},
    {"phenomenonTime": "2025-03-19T23:45:00Z", "result": 1.85933076816034},
    {"phenomenonTime": "2025-03-19T23:40:00Z", "result": 6.79817607055915}
  ]
}
```
