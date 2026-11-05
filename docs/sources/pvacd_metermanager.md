# Source Mapping: PVACD Meter Manager

**Source key:** `pvacd_metermanager`
**Agency code:** `PVACD`
**Response format:** `json`
**Source timezone:** `UTC`
**Update frequency:** `irregular`

The data is served via a REST Api that can be access at `metermanager.pvacd.com/api/v1`. Doing so requires and API key
given in an Authorization header as a Bearer token. 

API Endpoints:

| Endpoint  | URL                                                                                                   |
|-----------|-------------------------------------------------------------------------------------------------------|
| Locations | `GET http://metermanager.pvacd.com/api/v1/aqueduct/wells`                                             |
| Readings  | `GET http://metermanager.pvacd.com/api/v1/aqueduct/wells/{id}/measurements?start_timestamp={unix_ts}` |

---

## Location

**Standard SensorThings fields:**

| Canonical Field        | Type  | Status   | Source Field | Notes                                                 |
|------------------------|-------|----------|--------------|-------------------------------------------------------|
| `name`                 | str   | Required | `name`       | Human-readable site name                              |
| `description`          | str   | Required |              | Fixed: `Location of well where measurements are made` |
| `encodingType`         | str   | Required |              | Fixed: `application/vnd.geo+json`                     |
| `location` (longitude) | float | Required | `longitude`  | GeoJSON coordinates[0]                                |
| `location` (latitude)  | float | Required | `latitude`   | GeoJSON coordinates[1]                                |

**properties — standard keys:**

| Canonical Field | Type                             | Status   | Source Field | Notes                                                            |
|-----------------|----------------------------------|----------|--------------|------------------------------------------------------------------|
| `source_id`     | str                              | Required | `id`         | Stable ID, always string                                         |
| `geoconnex`     | str                              | Optional | Not Provided | geoconnex.us URI if available                                    |
| `alternate_id`  | [{id: str, agency: str}] \| None | Optional | Not Provided | Cross-reference IDs, e.g. `[{id: "NM-28258", agency: "NMBGMR"}]` |

**properties.source_specific:**

| Source Field | Type | Notes |
|--------------|------|-------|
|              |      |       |

---

## Thing

**Standard SensorThings fields:**

| Canonical Field | Type | Status   | Source Field | Notes                                                                                                    |
|-----------------|------|----------|--------------|----------------------------------------------------------------------------------------------------------|
| `name`          | str  | Required |              | Fixed: `Water Well`                                                                                      |
| `description`   | str  | Required |              | Fixed: `Well drilled or set into subsurface for the purposes of pumping water or monitoring groundwater` |

**properties — standard keys:**

| Canonical Field | Type                             | Status   | Source Field | Notes                                                           |
|-----------------|----------------------------------|----------|--------------|-----------------------------------------------------------------|
| `agency`        | str                              | Required |              | Fixed: `PVACD`                                                  |
| `source_id`     | str                              | Required | `id`         | Same as Location                                                |
| `alternate_id`  | [{id: str, agency: str}] \| None | Optional | Not Provided | Cross-reference IDs, e.g. `[{id: "BC-0002", agency: "NMBGMR"}]` |

**properties.source_specific:**

| Source Field | Type                                  | Notes                                                       |
|--------------|---------------------------------------|-------------------------------------------------------------|
| `well_depth` | {value: float, unit: str} \| None     | Always `{value: X, unit: "ft"}`, source field `total_depth` |
| `screens`    | [{top: float, bottom: float}] \| None | Screen intervals, or N/A                                    |

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
| Bubbler           |                      |
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
| Depth to Water Below Ground Surface | Yes       | `value`                 |       |
| Groundwater Elevation               | No        |                         |       |
| Groundwater Head                    | No        |                         |       |
| Adjusted Groundwater Head           | No        |                         |       |
| Raw Depth to Water                  | No        |                         |       |
| OSERealTimeDischarge                | No        |                         |       |
| OSERealTimeGageHeight               | No        |                         |       |

**New observed property needed?** Name, definition URI, and description: None

---

## Datastream

One per (Thing, ObservedProperty, Sensor) combination.

**Standard SensorThings fields:**

| Canonical Field     | Type | Status   | Source Field                                       | Notes                                                                                     |
|---------------------|------|----------|----------------------------------------------------|-------------------------------------------------------------------------------------------|
| `name`              | str  | Required | `observed_property_name`                           | e.g. `Groundwater Levels`                                                                 |
| `description`       | str  | Required | `observed_property_description`                    | e.g. `Measurement of groundwater depth in a water well, as measured below ground surface` |
| `unitOfMeasurement` | JSON | Required | `unit_name`, `unit_name_short`, `unit_description` | Fixed: `{name: Foot, symbol: ft, definition: ...}`                                        |

**properties — standard keys:**

| Canonical Field  | Type         | Status   | Source Field | Notes                          |
|------------------|--------------|----------|--------------|--------------------------------|
| `topic`          | str \| None  | Optional | Not Provided | `Water Quantity` if applicable |
| `is_provisional` | bool \| None | Optional | Not Provided | True if QC not completed       |

**properties.source_specific:**

| Source Field | Type | Notes |
|--------------|------|-------|
|              |      |       |

**Datastream suffix(es):** e.g. `dtw`

**How many datastreams per station?** 1

---

## Observation

**Standard SensorThings fields:**

| Canonical Field  | Type        | Status   | Source Field | Notes                                                                    |
|------------------|-------------|----------|--------------|--------------------------------------------------------------------------|
| `phenomenonTime` | datetime    | Required | `timestamp`  | Timestamp — what format? What timezone? Conversion to UTC?               |
| `result`         | float       | Required | `value`      | Value — what source unit? Conversion to feet?                            |
| `resultTime`     | datetime    | Optional | Not Provided | When the result was recorded — often same as phenomenonTime              |
| `resultQuality`  | str \| None | Optional | Not Provided | Publication status, e.g. `PROVISIONAL` / `APPROVED` / `ESTIMATED` / null |
| `validTime`      | period      | Optional | Not Provided | Time period the result is valid for, if applicable                       |

**parameters — standard keys:**

| Canonical Field            | Type          | Source Field | Notes                       |
|----------------------------|---------------|--------------|-----------------------------|
| `measuring_agency`         | str \| None   | Not Provided | Who took the measurement    |
| `measurement_method`       | str \| None   | Not Provided | How it was taken            |
| `data_source`              | str \| None   | Not Provided | Which data system           |
| `water_level_status`       | str \| None   | Not Provided | Dry well flag if available  |
| `measurement_point_height` | float \| None | Not Provided | Height above ground surface |
| `water_level_accuracy`     | float \| None | Not Provided | Accuracy of measurement     |

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

### Well Locations

For a list of wells, send a GET request to `http://metermanager.pvacd.com/api/v1/aqueduct/wells`
```json
[
  {
    "id": 2603,
    "name": "Harrison Monitor Well #10",
    "total_depth": 800.0,
    "location_name": null,
    "latitude": 33.92219444444444,
    "longitude": -104.80441666666667
  }
]
```

### Measurements

For a list of measurements from a given Well, send a GET request to `http://metermanager.pvacd.com/api/v1/aqueduct/wells/2603/measurements?start_timestamp=946710000`.
This will give a list of all measurements for a well id, in this case well 2603, after the given start_timestamp. An 
end_timestamp parameter can also be provided to get all measurements between the start and end timestamps.
```json
[
  {
    "id": 25391,
    "well_id": 2603,
    "timestamp": "2025-01-31T17:50:24",
    "value": 621.36,
    "observed_property_name": "Depth to water",
    "observed_property_description": "Depth to water below ground surface",
    "observed_property_context": "well",
    "unit_name": "feet",
    "unit_name_short": "ft",
    "unit_description": "feet"
  }
]
```
