# Source Mapping: BernCo Manual

**Source key:** `bernco_manual`
**Agency code:** `BERNCO`
**Response format:** `json` / `geojson`
**Source timezone:** `UTC` (Field `MSRMNT_Date` is UNIX epoch milliseconds - no timezone conversion needed)
**Update frequency:** `irregular`

**API endpoints**

| Endpoint     | URL                                                                                                             |
|--------------|-----------------------------------------------------------------------------------------------------------------|
| Locations    | `GET https://services6.arcgis.com/NiLPE6S5bwjCDk9X/ArcGIS/rest/services/Monitoring_Wells/FeatureServer/0/query` |
| Measurements | `GET https://services6.arcgis.com/NiLPE6S5bwjCDk9X/ArcGIS/rest/services/Monitoring_Wells/FeatureServer/1/query` |

---

## Location

**Standard SensorThings fields:**

| Canonical Field        | Type  | Status   | Source Field              | Notes                                                 |
|------------------------|-------|----------|---------------------------|-------------------------------------------------------|
| `name`                 | str   | Required | `Well_Name`               | Human-readable site name                              |
| `description`          | str   | Required | (fixed)                   | Fixed: `Location of well where measurements are made` |
| `encodingType`         | str   | Required | (fixed)                   | Fixed: `application/vnd.geo+json`                     |
| `location` (longitude) | float | Required | `Well_Location_Longitude` | GeoJSON coordinates[0]                                |
| `location` (latitude)  | float | Required | `Well_Location_Latitude`  | GeoJSON coordinates[1]                                |

**properties — standard keys:**

| Canonical Field | Type                             | Status   | Source Field | Notes                                                              |
|-----------------|----------------------------------|----------|--------------|--------------------------------------------------------------------|
| `source_id`     | str                              | Required | `GlobalID`   | Stable ID, always string (equivalent to `Well_ID` in measurements) |
| `geoconnex`     | str                              | Optional | (not in API) | geoconnex.us URI if available                                      |
| `alternate_id`  | [{id: str, agency: str}] \| None | Optional | `NMT_ID`     | Cross-reference IDs, e.g. `[{id: "NM-28258", agency: "NMBGMR"}]`   |

**properties.source_specific:**

| Source Field | Type | Notes |
|--------------|------|-------|
|              |      |       |

---

## Thing

**Standard SensorThings fields:**

| Canonical Field | Type | Status   | Source Field | Notes                                                                                                    |
|-----------------|------|----------|--------------|----------------------------------------------------------------------------------------------------------|
| `name`          | str  | Required | (fixed)      | Fixed: `Water Well`                                                                                      |
| `description`   | str  | Required | (fixed)      | Fixed: `Well drilled or set into subsurface for the purposes of pumping water or monitoring groundwater` |

**properties — standard keys:**

| Canonical Field | Type                             | Status   | Source Field | Notes                                                           |
|-----------------|----------------------------------|----------|--------------|-----------------------------------------------------------------|
| `agency`        | str                              | Required | (fixed)      | Fixed: `BERNCO`                                                 |
| `source_id`     | str                              | Required | `GlobalID`   | Same as Location                                                |
| `alternate_id`  | [{id: str, agency: str}] \| None | Optional | `NMT_ID`     | Cross-reference IDs, e.g. `[{id: "BC-0002", agency: "NMBGMR"}]` |

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
| NoSensor          | `Measurement_Method` field indicates how depth measurement was made |

**New sensor needed?** No

---

## ObservedProperty

Shared constants — check all that apply.

| Existing Constant                   | Provided? | Source field/param code          | Notes |
|-------------------------------------|-----------|----------------------------------|-------|
| Depth to Water Below Ground Surface | yes       | `Depth_To_Water_At_Msrmnt_Point` |       |
| Groundwater Elevation               | yes       | `Water_Level_Elevation`          |       |
| Groundwater Head                    | no        |                                  |       |
| Adjusted Groundwater Head           | no        |                                  |       |
| Raw Depth to Water                  | no        |                                  |       |
| OSERealTimeDischarge                | no        |                                  |       |
| OSERealTimeGageHeight               | no        |                                  |       |

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

**How many datastreams per station?** Not Applicable

---

## Observation

**Standard SensorThings fields:**

| Canonical Field  | Type        | Status   | Source Field                     | Notes                                     |
|------------------|-------------|----------|----------------------------------|-------------------------------------------|
| `phenomenonTime` | datetime    | Required | `MSRMNT_Date`                    | Unix epoch milliseconds                   |
| `result`         | float       | Required | `Depth_To_Water_At_Msrmnt_Point` | Raw value, units ft, no conversion needed |
| `resultTime`     | datetime    | Optional | (not in API)                     | Not applicable                            |
| `resultQuality`  | str \| None | Optional | (not in API)                     | Not applicable                            |
| `validTime`      | period      | Optional | (not in API)                     | Not applicable                            |

**parameters — standard keys:**

| Canonical Field            | Type          | Source Field         | Notes            |
|----------------------------|---------------|----------------------|------------------|
| `measuring_agency`         | str \| None   | (not in API)         | Not available    |
| `measurement_method`       | str \| None   | `Measurement_Method` | How it was taken |
| `data_source`              | str \| None   | (Not in API)         | Not available    |
| `water_level_status`       | str \| None   | (Not in API)         | Not available    |
| `measurement_point_height` | float \| None | (Not in API)         | Not available    |
| `water_level_accuracy`     | float \| None | (Not in API)         | Not available    |

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

### Location
The following api call gets a list of all locations, the sample result just contains the first location returned. \
List of field definitions in `fields` is omitted to keep doc from being unnecessarily large. \
`GET https://services6.arcgis.com/NiLPE6S5bwjCDk9X/ArcGIS/rest/services/Monitoring_Wells/FeatureServer/0/query?where=OBJECTID%3E%3D0&outFields=*&returnDistinctValues=true&f=pjson`
```json
{
  "objectIdFieldName" : "OBJECTID",
  "uniqueIdField" : {
    "name" : "OBJECTID",
    "isSystemMaintained" : true
  },
  "globalIdFieldName" : "GlobalID",
  "geometryType" : "esriGeometryPoint",
  "spatialReference" : {
    "wkid" : 4326,
    "latestWkid" : 4326
  },
  "fields" : [],
  "features" : [
    {
      "attributes" : {
        "OBJECTID" : 1,
        "Well_IDKey" : 373,
        "Well_Name" : "9-Mile Hill LF",
        "Well_Depth" : 740,
        "Well_Casing_Stickup" : 0,
        "Well_Location_Latitude" : 35.071526,
        "Well_Location_Longitude" : -106.778477,
        "Well_Location_Elevation" : 5642.196,
        "Well_OSE_Permit" : null,
        "Well_Type" : "MW",
        "GlobalID" : "4f92c895-6b41-42d6-be6b-508ee44812fa",
        "NMT_ID" : "BC-0364",
        "IndexDesignation" : "M",
        "AquiferCode" : "112SNTF",
        "ScreenInterval" : null,
        "CreationDate" : 1715203555377,
        "Creator" : "ccarsrud_EH",
        "EditDate" : 1760969152268,
        "Editor" : "pheinstein_NRS"
      },
      "geometry" : {
        "x" : -106.77847660904175,
        "y" : 35.071525524302793
      }
    }, {
      
    }
  ]
}
```

### Reading:
This api call returns a list of all measurements for the location in the previous example, only showing the first result. \
List of field definitions in `fields` is omitted to keep doc from being unnecessarily large. \
`GET https://services6.arcgis.com/NiLPE6S5bwjCDk9X/ArcGIS/rest/services/Monitoring_Wells/FeatureServer/1/query?where=Well_ID%3D%274f92c895-6b41-42d6-be6b-508ee44812fa%27&outFields=*&f=pjson`
```json
{
  "objectIdFieldName" : "OBJECTID",
  "uniqueIdField" : {
    "name" : "OBJECTID",
    "isSystemMaintained" : true
  },
  "globalIdFieldName" : "GlobalID",
  "fields" : [],
  "features" : [
    {
      "attributes" : {
        "OBJECTID" : 854,
        "Well_Name" : "9-Mile Hill LF",
        "Measurement_Method" : "Unknown",
        "MSRMNT_Date" : 1573689600000,
        "Depth_To_Water_At_Msrmnt_Point" : 711.11,
        "Comments" : "",
        "Last_Update" : 1613088000000,
        "Well_ID" : "4f92c895-6b41-42d6-be6b-508ee44812fa",
        "GlobalID" : "5500de13-5617-4cf8-a886-c6248bfd0419",
        "WaterLevelElevation" : 4931.086,
        "dtw_NEGATIVE" : -711.11,
        "Observer" : null,
        "MeasuringPt" : null,
        "CreationDate" : 1715203556034,
        "Creator" : "ccarsrud_EH",
        "EditDate" : 1784669722735,
        "Editor" : "ccarsrud_EH"
      }
    }, {
      
    }
  ]
}
```
