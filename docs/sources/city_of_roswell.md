# Source Mapping: CityOfRoswell

**Source key:** `site_id`
**Agency code:** `CityOfRoswell`
**Response format:** `csv`
**Source timezone:** `US/Mountain`
**Update frequency:** `irregular`

Data accessed via google cloud storage in bucket `roswellbubbler`

---

## Location

**Standard SensorThings fields:**

| Canonical Field        | Type  | Status   | Source Field | Notes                                                 |
|------------------------|-------|----------|--------------|-------------------------------------------------------|
| `name`                 | str   | Required | Not in data  | Fixed: `Site-18`                                      |
| `description`          | str   | Required | Not in data  | Fixed: `Location of well where measurements are made` |
| `encodingType`         | str   | Required | Not in data  | Fixed: `application/vnd.geo+json`                     |
| `location` (longitude) | float | Required | Not in data  | Fixed: `-104.58409`                                   |
| `location` (latitude)  | float | Required | Not in data  | Fixed: `33.29592`                                     |

**properties — standard keys:**

| Canonical Field | Type                             | Status   | Source Field | Notes                                                            |
|-----------------|----------------------------------|----------|--------------|------------------------------------------------------------------|
| `source_id`     | int                              | Required | `site_id`    | Stable ID, always integer, always `18`                           |
| `geoconnex`     | str                              | Optional | Not in data  | geoconnex.us URI if available                                    |
| `alternate_id`  | [{id: str, agency: str}] \| None | Optional | Not in data  | Cross-reference IDs, e.g. `[{id: "NM-28258", agency: "NMBGMR"}]` |

**properties.source_specific:**

| Source Field | Type | Notes |
|--------------|------|-------|
|              |      |       |

---

## Thing

**Standard SensorThings fields:**

| Canonical Field | Type | Status   | Source Field | Notes                                                                                                    |
|-----------------|------|----------|--------------|----------------------------------------------------------------------------------------------------------|
| `name`          | str  | Required | Not in data  | Fixed: `Water Well`                                                                                      |
| `description`   | str  | Required | Not in data  | Fixed: `Well drilled or set into subsurface for the purposes of pumping water or monitoring groundwater` |

**properties — standard keys:**

| Canonical Field | Type                             | Status   | Source Field | Notes                                                           |
|-----------------|----------------------------------|----------|--------------|-----------------------------------------------------------------|
| `agency`        | str                              | Required | Not in data  | Fixed: `CityOfRoswell`                                          |
| `source_id`     | int                              | Required | `site_id`    | Same as Location                                                |
| `alternate_id`  | [{id: str, agency: str}] \| None | Optional | Not in data  | Cross-reference IDs, e.g. `[{id: "BC-0002", agency: "NMBGMR"}]` |

**properties.source_specific:**

| Source Field | Type                                  | Notes                                               |
|--------------|---------------------------------------|-----------------------------------------------------|
| `well_depth` | {value: float, unit: str} \| None     | Always `{value: X, unit: "ft"}` — convert if needed |
| `screens`    | [{top: float, bottom: float}] \| None | Screen intervals, or N/A                            |

---

## Sensor

Shared constants — pick one or describe a new one.

| Existing Constant | Use for this source? |
|-------------------|----------------------|
| VuLink            |                      |
| Manual            |                      |
| Pressure          |                      |
| Acoustic          |                      |
| VanEssenDiver     |                      |
| Bubbler           | Yes                  |
| Transducer        |                      |
| Satellite         |                      |
| Radio             |                      |
| RadioTower        |                      |
| AVFM              |                      |
| OneRain           |                      |
| NoSensor          |                      |

**New sensor needed?** Name and description: None

---

## ObservedProperty

Shared constants — check all that apply.

| Existing Constant                   | Provided? | Source field/param code | Notes |
|-------------------------------------|-----------|-------------------------|-------|
| Depth to Water Below Ground Surface | No        |                         |       |
| Groundwater Elevation               | No        |                         |       |
| Groundwater Head                    | No        |                         |       |
| Adjusted Groundwater Head           | No        |                         |       |
| Raw Depth to Water                  | Yes       | `depth_to_water`        |       |
| OSERealTimeDischarge                | No        |                         |       |
| OSERealTimeGageHeight               | No        |                         |       |

**New observed property needed?** Name, definition URI, and description: None

---

## Datastream

One per (Thing, ObservedProperty, Sensor) combination.

**Standard SensorThings fields:**

| Canonical Field     | Type | Status   | Source Field | Notes                                                                                     |
|---------------------|------|----------|--------------|-------------------------------------------------------------------------------------------|
| `name`              | str  | Required | Not in data  | e.g. `Groundwater Levels`                                                                 |
| `description`       | str  | Required | Not in data  | e.g. `Measurement of groundwater depth in a water well, as measured below ground surface` |
| `unitOfMeasurement` | JSON | Required | Not in data  | Fixed: `{name: Foot, symbol: ft, definition: ...}`                                        |

**properties — standard keys:**

| Canonical Field  | Type         | Status   | Source Field | Notes                          |
|------------------|--------------|----------|--------------|--------------------------------|
| `topic`          | str \| None  | Optional | Not in data  | `Water Quantity` if applicable |
| `is_provisional` | bool \| None | Optional | Not in data  | True if QC not completed       |

**properties.source_specific:**

| Source Field | Type | Notes |
|--------------|------|-------|
|              |      |       |

**Datastream suffix(es):** e.g. `dtw`, `gwe`, `discharge`

**How many datastreams per station?** None

---

## Observation

**Standard SensorThings fields:**

| Canonical Field  | Type        | Status   | Source Field     | Notes                                                                                                                                                       |
|------------------|-------------|----------|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `phenomenonTime` | datetime    | Required | `timestamp`      | Timestamp — ISO date time foramt, timezone not specified, likely mountain, sometimes there are duplicate entries for the same timestamp with the same value |
| `result`         | float       | Required | `depth_to_water` | Value — units not specified, appears to be ft                                                                                                               |
| `resultTime`     | datetime    | Optional | Not in data      | When the result was recorded — often same as phenomenonTime                                                                                                 |
| `resultQuality`  | str \| None | Optional | Not in data      | Publication status, e.g. `PROVISIONAL` / `APPROVED` / `ESTIMATED` / null                                                                                    |
| `validTime`      | period      | Optional | Not in data      | Time period the result is valid for, if applicable                                                                                                          |

**parameters — standard keys:**

| Canonical Field            | Type          | Source Field       | Notes                       |
|----------------------------|---------------|--------------------|-----------------------------|
| `measuring_agency`         | str \| None   | `measuring_agency` | Who took the measurement    |
| `measurement_method`       | str \| None   | Not in data        | How it was taken            |
| `data_source`              | str \| None   | Not in data        | Which data system           |
| `water_level_status`       | str \| None   | Not in data        | Dry well flag if available  |
| `measurement_point_height` | float \| None | Not in data        | Height above ground surface |
| `water_level_accuracy`     | float \| None | Not in data        | Accuracy of measurement     |

**parameters.source_specific:**

| Source Field | Type | Notes |
|--------------|------|-------|
|              |      |       |

---

## Unit Conversions

| Field | Source Unit | Canonical Unit | Conversion |
|-------|-------------|----------------|------------|
|       |             |                |            |

---

## Raw Response Example

Paste a sanitized example (one station, a few readings). This becomes test fixture data.

```csv
depth_to_water,site_id,measuring _agency,,timestamp
120.7 ,18 ,City of Roswell,2025-4-7T0:2:25
120.7 ,18 ,City of Roswell,2025-4-7T0:17:36
120.7 ,18 ,City of Roswell,2025-4-7T0:32:46
120.7 ,18 ,City of Roswell,2025-4-7T0:47:56
120.7 ,18 ,City of Roswell,2025-4-7T1:3:7
120.7 ,18 ,City of Roswell,2025-4-7T1:18:31
120.7 ,18 ,City of Roswell,2025-4-7T1:33:41
120.7 ,18 ,City of Roswell,2025-4-7T1:48:52
120.3 ,18 ,City of Roswell,2025-4-7T2:4:15
120.7 ,18 ,City of Roswell,2025-4-7T2:19:26
```

