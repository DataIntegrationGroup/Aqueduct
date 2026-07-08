# Source Mapping: PVACD HydroVu

**Source key:** `pvacd_hydrovu`
**Agency code:** `PVACD`
**Response format:** `json`
**Source timezone:** UTC (HydroVu `timestamp` is Unix epoch seconds — no timezone conversion needed)
**Update frequency:** hourly (VuLink loggers transmit on a ~2-hour interval)

**Credentials:** stored in GCP Secret Manager under secret name `hydrovu_pvacd`. Never committed to git. See `.dlt/config.toml` for the `gcp_secret` key name.

**API endpoints confirmed live (2026-07-08):**

| Endpoint | URL |
|---|---|
| Auth | `POST https://hydrovu.com/public-api/oauth/token` |
| Locations | `GET https://www.hydrovu.com/public-api/v1/locations/list` |
| Readings | `GET https://www.hydrovu.com/public-api/v1/locations/{id}/data?startTime={unix_ts}` |
| Parameter/unit names | `GET https://www.hydrovu.com/public-api/v1/sispec/friendlynames` |

Pagination for both Locations and Readings: cursor-based via `X-ISI-Start-Page` request header and `X-ISI-Next-Page` response header. Pass `""` on the first request; stop when the response header is absent or empty.

---

## Location

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | `locations[].name` | e.g. `"Bartlett Level Troll"` — use as-is |
| `description` | str | Required | *(fixed)* | Fixed: `"Location of well where measurements are made"` |
| `encodingType` | str | Required | *(fixed)* | Fixed: `"application/geo+json"` |
| `location` (longitude) | float | Required | `locations[].gps.longitude` | GeoJSON coordinates[0] |
| `location` (latitude) | float | Required | `locations[].gps.latitude` | GeoJSON coordinates[1] |

**properties — standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `source_id` | str | Required | `locations[].id` | HydroVu returns an integer — cast to `str` at adapter boundary |
| `geoconnex` | str | Optional | *(not in API)* | Not returned by HydroVu |
| `alternate_id` | [{id: str, agency: str}] \| None | Optional | *(not in API)* | Not returned by HydroVu  |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `hydrovu_description` | str | `locations[].description` — the HydroVu description field (well number, e.g. `"827276"`) |
| `elevation` | {value: float, unit: str} \| None | **Not provided by HydroVu API.** `/locations/list` returns only `gps.latitude` and `gps.longitude` — no elevation field exists in the response. |

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
| `agency` | str | Required | *(fixed)* | Fixed: `"PVACD"` |
| `source_id` | str | Required | `locations[].id` | Same as Location — cast to `str` |
| `alternate_id` | [{id: str, agency: str}] \| None | Optional | *(not in API)* | Not returned by HydroVu API |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `hydrovu_description` | str | `locations[].description` — well number string (e.g. `"827276"`) |
| `well_depth` | {value: float, unit: "ft"} \| None | **Not provided by HydroVu API.** |
| `screens` | [{top: float, bottom: float}] \| None | **Not provided by HydroVu API.** |

---

## Sensor

Shared constants — pick one or describe a new one.

| Existing Constant | Use for this source? |
|---|---|
| `HYDROVU_SENSOR` (VuLink) | **Yes** — PVACD wells use VuLink transmitters with Level Troll pressure loggers |
| Manual | No |

**New sensor needed?** No — `HYDROVU_SENSOR` in `canonical_constants.py` covers this source.

---

## ObservedProperty

Shared constants — check all that apply.

| Existing Constant | Provided? | Source field/param code | Notes |
|---|---|---|---|
| Depth to Water Below Ground Surface | **Yes** | `parameterId="4"` → `"Level: Depth to Water"` (confirmed via `/sispec/friendlynames` 2026-07-08) | Only DTW is ingested |
| Groundwater Elevation | No | `parameterId="5"` exists in friendlynames but not present in PVACD readings | Not returned for PVACD wells |
| Groundwater Head | No | — | — |
| Adjusted Groundwater Head | No | — | — |
| Raw Depth to Water | No | — | — |
| OSERealTimeDischarge | No | — | — |
| OSERealTimeGageHeight | No | — | — |

**Other parameters present in PVACD readings (not ingested to canonical model):**

| parameterId | Name (from /sispec/friendlynames) | unitId | Unit | Ingested? |
|---|---|---|---|---|
| `"1"` | Temperature | `"1"` | C | No — filtered out |
| `"33"` | Battery Level | `"241"` | % | No — filtered out |

**New observed property needed?** No — `DTW_OBS_PROP` covers this source.

---

## Datastream

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | *(fixed)* | Fixed: `"Groundwater Levels"` |
| `description` | str | Required | *(fixed)* | Fixed: `"Measurement of groundwater depth in a water well, as measured below ground surface"` |
| `unitOfMeasurement` | JSON | Required | *(fixed)* | Fixed: `{name: "Foot", symbol: "ft", definition: "http://www.qudt.org/vocab/unit/FT"}` |

**properties — standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `topic` | str \| None | Optional | *(not in API)* | Could be set to `"Water Quantity"` as a fixed constant |
| `is_provisional` | bool \| None | Optional | *(not in API)* | HydroVu does not provide a QC/publication status field |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| *(none)* | — | No datastream-level source-specific fields available from HydroVu |

**Datastream suffix(es):** `dtw`

**How many datastreams per station?** One — DTW only.

---

## Observation

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `phenomenonTime` | datetime (UTC) | Required | `readings[].timestamp` | Unix epoch seconds → `datetime.fromtimestamp(ts, tz=UTC)`. No timezone conversion needed. |
| `result` | float | Required | `readings[].value` | Raw value in **metres** (`unitId="35"` → `"m"` confirmed via `/sispec/friendlynames`). Multiply by `3.28084` to convert to feet. |
| `resultTime` | datetime | Optional | *(not in API)* | HydroVu does not return a separate result recording time |
| `resultQuality` | str \| None | Optional | *(not in API)* | HydroVu does not return QC flags — always `None` for this source |
| `validTime` | period | Optional | *(not in API)* | Not applicable |

**parameters — standard keys:**

| Canonical Field | Type | Source Field | Notes |
|---|---|---|---|
| `measuring_agency` | str \| None | *(not in API)* | Not available from HydroVu |
| `measurement_method` | str \| None | *(not in API)* | Could be set to fixed `"Continuous Pressure Logger"` — see Open Questions |
| `data_source` | str \| None | *(not in API)* | Could be set to fixed `"HydroVu"` — see Open Questions |
| `water_level_status` | str \| None | *(not in API)* | HydroVu does not return a dry-well indicator |
| `measurement_point_height` | float \| None | *(not in API)* | Not available |
| `water_level_accuracy` | float \| None | *(not in API)* | Not available |

**parameters.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `customParameter` | bool | `parameters[].customParameter` — present in the raw readings response, always `false` for PVACD wells. Low value unless custom parameters are added to the device. |

---

## Unit Conversions

| Field | Source Unit | Source Evidence | Canonical Unit | Conversion Factor |
|---|---|---|---|---|
| `result` (DTW reading) | metres | `unitId="35"` → `"m"` confirmed via `GET /v1/sispec/friendlynames` (2026-07-08) | feet | `× 3.28084` |

---



## Raw Response Example

Sanitized sample from live API (2026-07-08). Coordinates rounded to 3 d.p. (~100 m precision).

**Locations response** (`GET /v1/locations/list`, one object from the array):

```json
{
  "id": 4745648669458432,
  "name": "Bartlett Level Troll",
  "description": "827276",
  "gps": {
    "latitude": 33.070,
    "longitude": -104.375
  }
}
```

**Readings response** (`GET /v1/locations/{id}/data`, DTW parameter shown):

```json
{
  "locationId": 4745648669458432,
  "parameters": [
    {
      "parameterId": "4",
      "unitId": "35",
      "customParameter": false,
      "readings": [
        {"timestamp": 1783288800, "value": 80.983},
        {"timestamp": 1783296000, "value": 81.048},
        {"timestamp": 1783303200, "value": 81.118}
      ]
    }
  ]
}
```

**Friendlynames response** (`GET /v1/sispec/friendlynames`, relevant entries):

```json
{
  "parameters": {
    "1": "Temperature",
    "4": "Level: Depth to Water",
    "33": "Battery Level"
  },
  "units": {
    "1": "C",
    "35": "m",
    "241": "%"
  }
}
```
