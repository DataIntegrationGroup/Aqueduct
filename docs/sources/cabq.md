# Source Mapping: CABQ

**Source key:** `cabq`
**Agency code:** `CABQ`
**Response format:** `json` / `geojson`
**Source timezone:** `UTC` (Field `measurement_date` is UNIX epoch milliseconds - no timezone conversion needed)
**Update frequency:** `irregular`

---

## Location

**Standard SensorThings fields:**

| Canonical Field        | Type  | Status   | Source Field | Notes                                                 |
|------------------------|-------|----------|--------------|-------------------------------------------------------|
| `name`                 | str   | Required | `loc_name`   | Human-readable site name                              |
| `description`          | str   | Required | (fixed)      | Fixed: `Location of well where measurements are made` |
| `encodingType`         | str   | Required | (fixed)      | Fixed: `application/vnd.geo+json`                     |
| `location` (longitude) | float | Required | `longitude`  | GeoJSON coordinates[0]                                |
| `location` (latitude)  | float | Required | `latitude`   | GeoJSON coordinates[1]                                |

**properties — standard keys:**

| Canonical Field | Type                             | Status   | Source Field   | Notes                                                            |
|-----------------|----------------------------------|----------|----------------|------------------------------------------------------------------|
| `source_id`     | str                              | Required | `sys_loc_code` |                                                                  |
| `geoconnex`     | str                              | Optional | (not in API)   | geoconnex.us URI if available                                    |
| `alternate_id`  | [{id: str, agency: str}] \| None | Optional | (not in API)   | Cross-reference IDs, e.g. `[{id: "NM-28258", agency: "NMBGMR"}]` |

**properties.source_specific:**

| Source Field   | Type   | Notes |
|----------------|--------|-------|
|                |        |       |

---

## Thing

**Standard SensorThings fields:**

| Canonical Field | Type | Status   | Source Field | Notes                                                                                                    |
|-----------------|------|----------|--------------|----------------------------------------------------------------------------------------------------------|
| `name`          | str  | Required | (fixed)      | Fixed: `Water Well`                                                                                      |
| `description`   | str  | Required | (fixed)      | Fixed: `Well drilled or set into subsurface for the purposes of pumping water or monitoring groundwater` |

**properties — standard keys:**

| Canonical Field | Type                             | Status   | Source Field   | Notes                                                           |
|-----------------|----------------------------------|----------|----------------|-----------------------------------------------------------------|
| `agency`        | str                              | Required | (fixed)        | Fixed: `CABQ`                                                   |
| `source_id`     | str                              | Required | `sys_loc_code` | Same as Location                                                |
| `alternate_id`  | [{id: str, agency: str}] \| None | Optional | (not in API)   | Cross-reference IDs, e.g. `[{id: "BC-0002", agency: "NMBGMR"}]` |

**properties.source_specific:**

| Source Field | Type                                  | Notes                                               |
|--------------|---------------------------------------|-----------------------------------------------------|
| `well_depth` | {value: float, unit: str} \| None     | Always `{value: X, unit: "ft"}` — convert if needed |
| `screens`    | [{top: float, bottom: float}] \| None | Screen intervals, or N/A                            |

---

## Sensor

Shared constants — pick one or describe a new one.

| Existing Constant | Use for this source?                                                |
|-------------------|---------------------------------------------------------------------|
| NoSensor          | `measurement_method` field indicates how depth measurement was made |

**New sensor needed?** No

---

## ObservedProperty

Shared constants — check all that apply.

| Existing Constant                   | Provided? | Source field/param code  | Notes                                |
|-------------------------------------|-----------|--------------------------|--------------------------------------|
| Depth to Water Below Ground Surface | yes       | `measured_depth_of_well` | Field exists but sometimes is `null` |
| Groundwater Elevation               | yes       | `reference_elevation`    |                                      |
| Groundwater Head                    | yes       | `water_level`            |                                      |
| Adjusted Groundwater Head           | yes       | `exact_elev`             |                                      |
| Raw Depth to Water                  | yes       | `water_depth`            |                                      |
| OSERealTimeDischarge                | no        |                          |                                      |
| OSERealTimeGageHeight               | no        |                          |                                      |

**New observed property needed?** No

---

## Datastream

One per (Thing, ObservedProperty, Sensor) combination.

**Standard SensorThings fields:**

| Canonical Field     | Type | Status   | Source Field | Notes                                                                                     |
|---------------------|------|----------|--------------|-------------------------------------------------------------------------------------------|
| `name`              | str  | Required | (fixed)      | e.g. `Groundwater Levels`                                                                 |
| `description`       | str  | Required | (fixed)      | e.g. `Measurement of groundwater depth in a water well, as measured below ground surface` |
| `unitOfMeasurement` | JSON | Required | (fixed)      | Fixed: `{name: Foot, symbol: ft, definition: ...}`                                        |

**properties — standard keys:**

| Canonical Field  | Type         | Status   | Source Field | Notes                          |
|------------------|--------------|----------|--------------|--------------------------------|
| `topic`          | str \| None  | Optional | (not in API) | `Water Quantity` if applicable |
| `is_provisional` | bool \| None | Optional | (not in API) | True if QC not completed       |

**properties.source_specific:**

| Source Field | Type | Notes |
|--------------|------|-------|
|              |      |       |

**Datastream suffix(es):** Not Applicable

**How many datastreams per station?** Not applicable

---

## Observation

**Standard SensorThings fields:**

| Canonical Field  | Type        | Status   | Source Field       | Notes                                                                     |
|------------------|-------------|----------|--------------------|---------------------------------------------------------------------------|
| `phenomenonTime` | datetime    | Required | `measurement_date` | Unix epoch milliseconds                                                   |
| `result`         | float       | Required | `water_depth`      | Raw value, units provided in `depth_unit` field, may be constant `ft msl` |
| `resultTime`     | datetime    | Optional | (not in API)       | Not applicable                                                            |
| `resultQuality`  | str \| None | Optional | (not in API)       | Not applicable                                                            |
| `validTime`      | period      | Optional | (not in API)       | Not applicable                                                            |

**parameters — standard keys:**

| Canonical Field            | Type          | Source Field         | Notes                                             |
|----------------------------|---------------|----------------------|---------------------------------------------------|
| `measuring_agency`         | str \| None   | `facility_code`      | Might be constant `COA`                           |
| `measurement_method`       | str \| None   | `measurement_method` | Often `null` for early entries                    |
| `data_source`              | str \| None   | (not in API)         | Not available                                     |
| `water_level_status`       | str \| None   | `dry_indicator_yn`   | always either `N` or `Y`, `null` in early entries |
| `measurement_point_height` | float \| None | (not in API)         | Not available                                     |
| `water_level_accuracy`     | float \| None | (not in API)         | Not available                                     |

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

Queried from [here](https://services.arcgis.com/CWv1abTnC3urn4bV/ArcGIS/rest/services/All_WL_Locations/FeatureServer/25/query)
with `OBJECTID` = 2000, out fields = *, and format = JSON. \
List of field definitions in `fields` is omitted to keep doc from being unnecessarily large. \
`GET https://services.arcgis.com/CWv1abTnC3urn4bV/ArcGIS/rest/services/All_WL_Locations/FeatureServer/25/query?where=OBJECTID%3D2000&outFields=*&f=pjson`
```json
{
  "objectIdFieldName" : "OBJECTID",
  "uniqueIdField" : {
    "name" : "OBJECTID",
    "isSystemMaintained" : true
  },
  "globalIdFieldName" : "",
  "fields" : [],
  "features" : [
    {
      "attributes" : {
        "OBJECTID" : 2000,
        "facility_id" : 1,
        "facility_code" : "COA",
        "sys_loc_code" : "IW4",
        "loc_name" : "LALF GROUNDWATER INJECTION WELL 4",
        "loc_group" : null,
        "loc_report_order" : null,
        "measurement_date" : 1391079600000,
        "reference_elev" : 5089.55,
        "water_level" : "4927.15",
        "exact_elev" : 4927.154,
        "measured_depth_of_well" : null,
        "depth_unit" : "ft msl",
        "batch_number" : null,
        "technician" : "KZ",
        "dry_indicator_yn" : "N",
        "measurement_method" : "Level Logger",
        "dip_or_elevation" : "dip",
        "remark" : null,
        "equipment_code" : null,
        "lnapl_cas_rn" : null,
        "lnapl_depth" : null,
        "lnapl_thickness" : null,
        "lnapl_density" : null,
        "water_depth" : 162.396,
        "dnapl_cas_rn" : null,
        "dnapl_depth" : null,
        "dnapl_thickness" : null,
        "task_code" : "COA GWL LL",
        "approval_code" : null,
        "x_coord" : 1536019.16999999,
        "y_coord" : 1517613.04,
        "longitude" : 35.170730266,
        "latitude" : -106.599332407
      }
    }
  ]
}
```

