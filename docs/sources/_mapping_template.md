# Source Mapping: {AGENCY}

> Copy this template to `docs/sources/{source_key}.md` and fill it out.
> Fixed fields (same for every source) are pre-filled. Fill in the **Source Field** column for everything else.

**Source key:** `{source_key}`
**Agency code:** `{AGENCY}`
**Response format:** `json` / `csv` / `xml` / `geojson`
**Source timezone:** `UTC` / `US/Mountain` / ...
**Update frequency:** `hourly` / `daily` / `irregular`

---

## Location

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | | Human-readable site name |
| `description` | str | Required | | Fixed: `Location of well where measurements are made` |
| `encodingType` | str | Required | | Fixed: `application/vnd.geo+json` |
| `location` (longitude) | float | Required | | GeoJSON coordinates[0] |
| `location` (latitude) | float | Required | | GeoJSON coordinates[1] |

**properties — standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `source_id` | str | Required | | Stable ID, always string |
| `geoconnex` | str | Optional | | geoconnex.us URI if available |
| `alternate_id` | [{id: str, agency: str}] \| None | Optional | | Cross-reference IDs, e.g. `[{id: "NM-28258", agency: "NMBGMR"}]` |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| | | |

---

## Thing

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | | Fixed: `Water Well` |
| `description` | str | Required | | Fixed: `Well drilled or set into subsurface for the purposes of pumping water or monitoring groundwater` |

**properties — standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `agency` | str | Required | | Fixed: `{AGENCY}` |
| `source_id` | str | Required | | Same as Location |
| `alternate_id` | [{id: str, agency: str}] \| None | Optional | | Cross-reference IDs, e.g. `[{id: "BC-0002", agency: "NMBGMR"}]` |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `well_depth` | {value: float, unit: str} \| None | Always `{value: X, unit: "ft"}` — convert if needed |
| `screens` | [{top: float, bottom: float}] \| None | Screen intervals, or N/A |

---

## Sensor

Shared constants — pick one or describe a new one.

| Existing Constant | Use for this source? |
|---|---|
| VuLink | |
| Manual | |
| Pressure | |
| Acoustic | |
| VanEssenDiver | |
| Bubbler | |
| Transducer | |
| Satellite | |
| Radio | |
| RadioTower | |
| AVFM | |
| OneRain | |
| NoSensor | |

**New sensor needed?** Name and description:

---

## ObservedProperty

Shared constants — check all that apply.

| Existing Constant | Provided? | Source field/param code | Notes |
|---|---|---|---|
| Depth to Water Below Ground Surface | | | |
| Groundwater Elevation | | | |
| Groundwater Head | | | |
| Adjusted Groundwater Head | | | |
| Raw Depth to Water | | | |
| OSERealTimeDischarge | | | |
| OSERealTimeGageHeight | | | |

**New observed property needed?** Name, definition URI, and description:

---

## Datastream

One per (Thing, ObservedProperty, Sensor) combination.

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | | e.g. `Groundwater Levels` |
| `description` | str | Required | | e.g. `Measurement of groundwater depth in a water well, as measured below ground surface` |
| `unitOfMeasurement` | JSON | Required | | Fixed: `{name: Foot, symbol: ft, definition: ...}` |

**properties — standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `topic` | str \| None | Optional | | `Water Quantity` if applicable |
| `is_provisional` | bool \| None | Optional | | True if QC not completed |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| | | |

**Datastream suffix(es):** e.g. `dtw`, `gwe`, `discharge`

**How many datastreams per station?**

---

## Observation

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `phenomenonTime` | datetime | Required | | Timestamp — what format? What timezone? Conversion to UTC? |
| `result` | float | Required | | Value — what source unit? Conversion to feet? |
| `resultTime` | datetime | Optional | | When the result was recorded — often same as phenomenonTime |
| `resultQuality` | str \| None | Optional | | Publication status, e.g. `PROVISIONAL` / `APPROVED` / `ESTIMATED` / null |
| `validTime` | period | Optional | | Time period the result is valid for, if applicable |

**parameters — standard keys:**

| Canonical Field | Type | Source Field | Notes |
|---|---|---|---|
| `measuring_agency` | str \| None | | Who took the measurement |
| `measurement_method` | str \| None | | How it was taken |
| `data_source` | str \| None | | Which data system |
| `water_level_status` | str \| None | | Dry well flag if available |
| `measurement_point_height` | float \| None | | Height above ground surface |
| `water_level_accuracy` | float \| None | | Accuracy of measurement |

**parameters.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| | | |

---

## Unit Conversions

| Field | Source Unit | Canonical Unit | Conversion |
|---|---|---|---|
| | | | |

---

## Raw Response Example

Paste a sanitized example (one station, a few readings). This becomes test fixture data.

```
```

