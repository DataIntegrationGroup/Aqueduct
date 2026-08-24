# Source Mapping: EBID (OneRain Contrail)

**Source key:** `ebid`
**Agency code:** `EBID`
**Response format:** `xml`
**Source timezone:** UTC (Contrail defaults to UTC when `timezone` is omitted)
**Update frequency:** ~30 minutes (a half-hourly RTU report with clock drift)

The retired Airbyte connector used `xmltodict`; the stdlib `xml.etree.ElementTree`
handles this envelope fine. `data_time` and `receive_time` arrive as naive
`YYYY-MM-DD HH:MM:SS` strings with no offset suffix, so stamp them UTC at the
adapter boundary. Cadence measured from consecutive `phenomenonTime` values in
the old FROST records: `19:08:55`, `19:38:54`, `20:08:56`, `20:38:35`, `21:08:21`.

**Credentials: not yet held.** Every Data Exchange method except `GetTime` and
`GetStatus` requires a Contrail `system_key` GUID. Without one the API answers
`<onerain><error>Use of System Key required</error></onerain>`.

Routes to a key:

1. Recover it from the retired NMWDI Airbyte connector's saved source config.
   Its [`spec.yaml`](https://github.com/NMWDI/airbyte/blob/master/airbyte-integrations/connectors/source-onerain-api/source_onerain_api/spec.yaml)
   takes exactly two fields, `url_base` and `system_key`, so a working key
   existed.
1. Ask EBID
1. OneRain/AEM support: `contrail.support@onerain.com`.

A key grants access to one Output System, which is a named subset of sensors
rather than the whole instance. Confirm the key EBID issues actually covers the
monitoring-well DTW sensors.

Store the key in GCP Secret Manager.

**API endpoints confirmed live (2026-08-20):**

| Endpoint | URL |
|---|---|
| Base | `http://onerain.ebid-nm.org:8080/OneRain/DataAPI` |
| Health / clock | `GET ...?method=GetTime` -> `<time>2026-08-20 07:05:21</time>` |
| Version | `GET ...?method=GetStatus` -> `Servlet version: 4.1.003 2012-05-02` |
| Site metadata | `GET ...?method=GetSiteMetaData&system_key={key}` |
| Sensor metadata | `GET ...?method=GetSensorMetaData&system_key={key}&or_site_id={id}` |
| Readings | `GET ...?method=GetSensorData&system_key={key}&or_site_id={id}&or_sensor_id={id}&data_start={ts}&data_end={ts}` |

Notes that constrain the ingest ticket:

- **Plain HTTP on port 8080.** No TLS, no auth header. The `system_key` travels
  as a query parameter. There is no HTTPS listener at all:
  `https://onerain.ebid-nm.org/OneRain/DataAPI` returns the web portal's 404
  page rather than the API.
- **The servlet path is case-sensitive.** `/OneRain/DataAPI` must match exactly.
  Parameter names are case-insensitive and method names are forgiving in
  practice, but the path is neither.
- **No pagination.** Slice by time window instead. The retired Airbyte connector
  used 10-day windows per (site, sensor) pair.
- **Enforced limits.** 5000 rows per response, truncated silently, so the client
  has to detect the threshold itself. A single-sensor request may span 31 days.
  A request that could match two or more sensors is capped at 24 hours.
- **Unknown parameters are ignored rather than rejected.** A typo in `data_end`
  defaults it to *now* and can produce an enormous request. Validate parameter
  names in the pipeline code.
- **Two ID namespaces.** `or_site_id`/`or_sensor_id` are Contrail's internal
  numeric IDs. `site_id`/`sensor_id` are the agency's alias string IDs. Mixing
  both in one request is rejected outright. Requesting by alias returns both
  sets; requesting by Contrail ID returns only Contrail IDs. That matters,
  because `source_id` maps from the alias `site_id` (see Location below). Call
  `GetSensorData` with `or_site_id` and the alias is missing from the response,
  so the adapter has to join it back from `GetSiteMetaData`.
- **`concise=true`** collapses each row to a comma-delimited string. Don't use
  it. The doc states field order is not stable.

NMWDI EBID ingest code:

| Component | Role |
|---|---|
| [NMWDI/airbyte `source-onerain-api`](https://github.com/NMWDI/airbyte/tree/master/airbyte-integrations/connectors/source-onerain-api) | Extract: three streams (`GetSiteMetaData` -> `GetSensorMetaData` -> `GetSensorData`) landing in BigQuery. Its `schemas/*.json` carry field-level descriptions for every column and are the best available field dictionary. |
| [NMWDI/CloudFunctions `stao/ebid/entities.py`](https://github.com/NMWDI/CloudFunctions/blob/master/stao/ebid/entities.py) | Transform: BigQuery -> SensorThings. Every mapping decision below comes from this file. |
| [st2.newmexicowaterdata.org](https://st2.newmexicowaterdata.org/FROST-Server/v1.1/Things?$filter=properties/agency%20eq%20%27EBID%27) | The result: 6 EBID Things, Locations and Datastreams, with 3,165 observations per datastream spanning 2024-06-30 to 2024-09-04. Every "old FROST" value quoted below came off this server. |

---

## Location

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | `GetSiteMetaData.site_id` | Upper-cased alias site ID, e.g. `"209-0000-M19R"`. The old pipeline called `record['site_id'].upper()`; values are already upper-case in practice. Not the human-readable `location` string. |
| `description` | str | Required | *(fixed)* | Fixed: `"Location of well where measurements are made"` |
| `encodingType` | str | Required | *(fixed)* | Fixed: `"application/geo+json"`. |
| `location` (longitude) | float | Required | `GetSiteMetaData.longitude_dec` | GeoJSON coordinates[0], decimal degrees, e.g. `-106.85369` |
| `location` (latitude) | float | Required | `GetSiteMetaData.latitude_dec` | GeoJSON coordinates[1], decimal degrees, e.g. `32.38739` |

Coordinates arrive as strings in the XML. The old pipeline cast them with
`cast(latitude_dec as FLOAT64)`; do the same at the adapter boundary. Do not
append elevation as a Z coordinate (see `CANONICAL_MODEL.md` §3).

**properties: standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `source_id` | str | Required | `GetSiteMetaData.site_id` | The alias ID (`"209-0000-M19R"`), not `or_site_id`. Confirmed against old FROST, where `properties.source_id == properties.site_id`. Already a string, so no cast. |
| `geoconnex` | str | Optional | *(not in API)* | Not a source field. Old FROST values look like `https://geoconnex.us/nmwdi/st/locations/{frost_location_id}`, minted downstream *after* insert from the FROST-assigned ID. Do not map this from the API. |
| `alternate_id` | [{id: str, agency: str}] \| None | Optional | *(not in API)* | Not returned. The `site_id` format (`209-0000-M19R`) resembles an EBID internal well tag. Whether it cross-references an NMBGMR or OSE POD number is unconfirmed. |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `or_site_id` | int | `GetSiteMetaData.or_site_id`, Contrail's numeric site ID, e.g. `1164`. Every subsequent request needs it, so the adapter must carry it through. Old FROST stored it at the top level of `properties`; under the current canonical model it belongs in `source_specific`. |
| `onerain_location` | str | `GetSiteMetaData.location`, the human-readable site description, e.g. `"Monitoring Well - MES_19R"`. Named `onerain_location` to avoid colliding with the SensorThings `location` geometry. It also doubles as the monitoring-well filter (see ObservedProperty). |
| `elevation` | {value: float, unit: "m"} \| None | See Unit Conversions. Two candidate sources, both problematic. |
| `system_id` | int | `GetSiteMetaData.system_id`, the Contrail *input* system the site belongs to. |
| `client_id` | str | `GetSiteMetaData.client_id`, the GUID of the Contrail client owning that input system. Low value; include for traceability or drop it. |

The vendor documents `GetSiteMetaData.owner` as deprecated, and it returns the
literal string `"DEPRECATED"`. Do not map it.

---

## Thing

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | *(fixed)* | Fixed: `"Water Well"` |
| `description` | str | Required | *(fixed)* | Fixed: `"Well drilled or set into subsurface for the purposes of pumping water or monitoring groundwater"` |

**properties: standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `agency` | str | Required | *(fixed)* | Fixed: `"EBID"`. Confirmed: old FROST Things carry exactly `{"agency": "EBID"}` and nothing else. |
| `source_id` | str | Required | `GetSiteMetaData.site_id` | Same as Location |
| `alternate_id` | [{id: str, agency: str}] \| None | Optional | *(not in API)* | Not returned |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `well_depth` | {value: float, unit: "ft"} \| None | Not provided. Contrail is a telemetry system and stores no well-construction data. Always `None`. |
| `screens` | [{top: float, bottom: float}] \| None | Not provided. Always `None`. |

---

## Sensor

Shared constants

| Existing Constant | Use for this source? |
|---|---|
| `HYDROVU_SENSOR` (VuLink) | No |
| `MANUAL_SENSOR` (Manual) | No. EBID readings are telemetered on a fixed interval, not hand-measured. |
| OneRain | **Yes** |
| All others (Pressure, Acoustic, VanEssenDiver, Bubbler, Transducer, Satellite, Radio, RadioTower, AVFM, NoSensor) | No |

**New sensor needed? Yes, `ONERAIN_SENSOR`.** The `OneRain` sensor appears in
`_mapping_template.md` and lives in old FROST as `Sensors(10)`, but nothing
defines it in
[`canonical_constants.py`](../../src/aqueduct_dagster/canonical/canonical_constants.py),
which holds only `MANUAL_SENSOR` and `HYDROVU_SENSOR` today. It is the one new
canonical constant this source needs. Proposed definition, matching the old
FROST record and following the existing `external_key` style:

```python
ONERAIN_SENSOR = CanonicalSensor(
    external_key="sensor-onerain",
    name="OneRain",
    description=NO_DEFINITION,
    encoding_type="application/pdf",
    metadata=NO_METADATA,
)
```

Old FROST `Sensors(10)` reads `{"name": "OneRain", "description": "No
Description", "encodingType": "application/pdf", "metadata": "No Metadata"}`.
Its `description` is `"No Description"`, while `MANUAL_SENSOR` and
`HYDROVU_SENSOR` in this repo both put `NO_DEFINITION` (`"No Definition"`) in
the description slot. Follow this repo's convention, not the old server's.

Adding the constant is a later ticket.

---

## ObservedProperty

Shared constants

| Existing Constant | Provided? | Source field/param code | Notes |
|---|---|---|---|
| Depth to Water Below Ground Surface | **Yes** | `sensor_class = 102`, historically `or_sensor_id = 4` | The only property in scope. Confirmed via old FROST, where all 6 EBID datastreams point at `ObservedProperties(1)` = `Depth to Water Below Ground Surface`. |
| Groundwater Elevation | No | — | Never loaded for EBID |
| Groundwater Head | No | — | — |
| Adjusted Groundwater Head | No | — | — |
| Raw Depth to Water | No | — | `GetSensorData.raw_value` is the pre-conversion telemetry count, not a second water-level property. Keep it in `parameters.source_specific` rather than giving it its own ObservedProperty. |
| OSERealTimeDischarge | No | — | — |
| OSERealTimeGageHeight | No | — | — |

**Selecting monitoring wells.** The old pipeline used two filters together, and
both are needed:

1. `sensor_class = 102` on `GetSensorMetaData`. The old code's comment reads
   *"only get sites with depth to water"*. **102 is absent from the vendor's
   standard sensor-class table**, which lists 10/11 rain, 20 stage, 25 flow, 30
   air temp, 199 battery and so on. The vendor doc warns that *"these may vary
   for your particular instance of Contrail"*, so 102 is EBID-local. Verify it
   against a real `GetSensorMetaData` response before relying on it.
1. `'Well' in GetSiteMetaData.location`. The site's human-readable name must
   contain the substring `Well`. All six historical sites are named
   `Monitoring Well - MES_<n>R`. The old pipeline skipped any site that failed
   this test; its `STREAM_GAUGE` branch is commented out and was never finished.

The old observation query also hardcoded `or_sensor_id = 4`. Treat that as a
coincidence of the historical six sites rather than a rule, and discover the DTW
sensor per site from `GetSensorMetaData` instead.

**Out of scope.** EBID's Contrail instance also carries rain, stage, flow and
weather sensors; the portal is primarily a storm-water and flood system. This
pipeline ingests groundwater monitoring wells only.

**New observed property needed?** No. `DTW_OBS_PROP` covers this source.

---

## Datastream

One per (Thing, ObservedProperty, Sensor) combination.

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `name` | str | Required | *(fixed)* | Fixed: `"Groundwater Levels"`. Confirmed against all 6 old FROST datastreams. |
| `description` | str | Required | *(fixed)* | Fixed: `"Measurement of groundwater depth in a water well, as measured below ground surface"`. Confirmed. |
| `unitOfMeasurement` | JSON | Required | *(fixed)* | Fixed: `UNIT_FOOT` = `{name: "Foot", symbol: "ft", definition: "http://www.qudt.org/vocab/unit/FT"}`. Confirmed. |

`observationType` is `OM_Measurement`, confirmed.

**properties: standard keys:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `topic` | str \| None | Optional | *(not in API)* | Old FROST value `"Water Quantity"`, identical on all 6 datastreams. See Open Questions. |
| `is_provisional` | bool \| None | Optional | *(not in API)* | Never set for EBID. `GetSensorData.data_quality` is the closest source signal (see Observation). |

**properties.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `is_continuous` | bool | *(not in API)*. Old FROST value `true` on all 6 datastreams. `san_acacia.md` flags this same field as undocumented in `CANONICAL_MODEL.md`; keep the name consistent with that source. See Open Questions. |

**Datastream suffix(es):** `dtw`

**How many datastreams per station?** One, DTW only. Confirmed: each of the 6
EBID Things in old FROST has exactly one Datastream.

---

## Observation

**Standard SensorThings fields:**

| Canonical Field | Type | Status | Source Field | Notes |
|---|---|---|---|---|
| `phenomenonTime` | datetime (UTC) | Required | `GetSensorData.data_time` | Naive `"%Y-%m-%d %H:%M:%S"` string -> `datetime.strptime(...).replace(tzinfo=UTC)`. Requests must omit `timezone=` so Contrail returns UTC. This is the dlt incremental cursor; the old Airbyte connector used `data_time` too. |
| `result` | float | Required | `GetSensorData.data_value` | **`result = data_value * -1`**. **UNVERIFIED, see Unit Conversions.** Already in feet, so no unit scaling. Old FROST results sit between 14.63 and 14.65 ft, consistent with shallow DTW. |
| `resultTime` | datetime | Optional | `GetSensorData.data_time` | Set equal to `phenomenonTime`. Confirmed: old FROST observations have identical `phenomenonTime` and `resultTime`. `receive_time` is the database write time, not a measurement result time, so it belongs in `parameters.source_specific`. |
| `resultQuality` | str \| None | Optional | `GetSensorData.data_quality` | Single-character flag; the vendor example shows `A`. The old pipeline discarded it. Mapping it is a proposed improvement, pending confirmation of the value domain. See Open Questions. |
| `validTime` | period | Optional | *(not in API)* | Not applicable |

**parameters: standard keys:**

| Canonical Field | Type | Source Field | Notes |
|---|---|---|---|
| `measuring_agency` | str \| None | *(not in API)* | Could be fixed `"EBID"`, but `Thing.properties.agency` already carries it. Leave `None`. |
| `measurement_method` | str \| None | *(not in API)* | Not available. `GetSensorMetaData.sensor_type` describes the telemetry source rather than the measurement method, and it is per-sensor metadata, not per-observation. |
| `data_source` | str \| None | *(not in API)* | Could be fixed `"OneRain Contrail"`. See Open Questions. |
| `water_level_status` | str \| None | *(not in API)* | Contrail has no dry-well indicator |
| `measurement_point_height` | float \| None | *(not in API)* | Not available per observation. `GetSensorMetaData.reference` is a static per-sensor datum offset, not a per-reading measuring-point height. |
| `water_level_accuracy` | float \| None | *(not in API)* | Not available |

**parameters.source_specific:**

| Source Field | Type | Notes |
|---|---|---|
| `raw_value` | float | `GetSensorData.raw_value`, the value as supplied by the field device, before Contrail applies `slope`/`offset`/`reference` conversion or validation. Worth keeping: it is the only way to spot a mid-history recalibration. |
| `receive_time` | str | `GetSensorData.receive_time`, when Contrail wrote the record, distinct from `data_time`. Latency between the two ran about an hour in the vendor's example. |
| `or_sensor_id` | int | `GetSensorData.or_sensor_id`, which physical sensor produced the reading |

---

## Unit Conversions

| Field                                       | Source Unit | Source Evidence | Canonical Unit | Conversion Factor |
|---------------------------------------------|---|---|---|---|
| `result` (DTW reading)                      | feet, sign inverted | **UNVERIFIED.** Inferred from `_transform_value(self, v, record): return v * -1` in the old transform, plus old FROST results being positive (`14.6496`) where DTW below ground would be reported negative | feet | **`× -1`** |
| `elevation` - `GetSensorMetaData.reference` | feet | Old transform: `(cast(reference as FLOAT64)/3.28084) as reference`, then passed as the geometry elevation | metres | `÷ 3.28084` |
| `elevation` - `GetSiteMetaData.elevation`   | feet (assumed) | Old FROST `properties.elevation`: `0.0` for five of six sites, `3896.0` for one | — | Unreliable, see below |

**On the sign flip.** Nothing needs rescaling. Contrail already applies each
sensor's `slope` and `offset` to produce `data_value`, and DTW comes out in
feet. The sign is the only transform. The old pipeline negated unconditionally,
which means EBID's Contrail almost certainly stores depth to water as a negative
number, with the water surface below the reference datum. **Confirm this against
a live `GetSensorData` response before implementing.** A wrongly applied
negation produces plausible-looking data that is off by a factor of -1 and fails    
no test.

**On elevation.** Two candidate sources disagree, and neither is trustworthy as
it stands. `GetSiteMetaData.elevation` came back `0.0` for five of six sites, so
it is effectively unpopulated. The old pipeline used
`GetSensorMetaData.reference` instead, converting feet to metres. But the
Airbyte schema documents `reference` as *"used in data conversion; additive
value"*, meaning a sensor datum offset that only coincidentally resembles a
ground elevation. The old FROST geometries carry two coordinates, not three, so
`reference` was null or zero as well and the elevation never landed. Treat
elevation as unavailable from this source until a real `GetSensorMetaData`
response proves otherwise.

---

## Open Questions

1. **Sign convention (blocking).** Is `data_value` really negative for DTW?
   Resolve this from a live `GetSensorData` response before anyone writes
   adapter code.
1. **`topic` and `is_continuous` provenance.** Old FROST carries
   `{"is_continuous": true, "topic": "Water Quantity"}` identically on all 6
   EBID datastreams, but the old transform code sets `properties = {}`. Those
   values came from somewhere else, either a later revision or a manual patch.
   `san_acacia.md` raises the same question for the same two fields. Should the
   new adapter reuse the values, or is this a team decision rather than
   something to inherit silently? Worth settling once for both sources.
1. **`data_quality` -> `resultQuality`.** The old pipeline dropped `data_quality`
   entirely. The vendor example shows `A`, presumably *Approved*, and the
   Airbyte schema calls it a data quality flag without giving a value domain.
   Confirm the domain from a live response, then decide whether it populates
   `resultQuality`, drives `is_provisional`, or both.
1. **`data_source` parameter.** Set a fixed `"OneRain Contrail"` on every
   observation, or leave `None`? `pvacd_hydrovu.md` raises the same question for
   `"HydroVu"`. Answer both the same way.
1. **Current site list.** The six wells in old FROST are `or_site_id` 1164,
   1165, 1171, 1179, 1180, 1181. The site linked in the ticket
   ([`site_id=1146`](https://onerain.ebid-nm.org/site/?site_id=1146)) falls
   outside that set, so EBID has monitoring wells that were never ingested.
   Enumerate the current set from `GetSiteMetaData` once the key is available
   rather than assuming the historical six.
1. **Backfill horizon.** Old FROST data spans 2024-06-30 to 2024-09-04 only, and
   the old pipeline started from `datetime.strptime('2020', '%Y')`. How far back
   does EBID's Contrail retain data? The vendor doc notes that readings older
   than 30 days come from `device_reading_history`, and that the Data Exchange
   *"should not be used for large archival data requests"*. A full backfill may
   have to come from EBID as a file rather than through this API.

---

## Raw Response Example

> **RECONSTRUCTED. NOT A CAPTURED RESPONSE.** No `system_key` was available
> when this was written, so these payloads combine three verified inputs: the
> XML envelope and field order from the vendor doc (`DataExchangeAPI_Get.pdf`
> v8.2), the field names and types from the retired Airbyte connector's
> `schemas/*.json`, and real EBID values read back off the old FROST server. The
> two labels below separate the real values from the placeholders. Replace this
> whole section with genuine captures before using it as fixture data.

**Real, recovered from old FROST:** `or_site_id=1164`, `site_id=209-0000-M19R`,
`location=Monitoring Well - MES_19R`, `latitude_dec=32.38739`,
`longitude_dec=-106.85369`, and the five `data_time`/`data_value` pairs, negated
back to their presumed raw form.

**Placeholder:** `system_id`, `client_id`, everything in `GetSensorMetaData`
except the IDs, and all `receive_time`, `raw_value` and `data_quality` values.

**Site metadata** (`GET ...?method=GetSiteMetaData&system_key={key}&or_site_id=1164`):

```xml
<onerain>
  <response>
    <general>
      <row>
        <or_site_id>1164</or_site_id>
        <site_id>209-0000-M19R</site_id>
        <location>Monitoring Well - MES_19R</location>
        <owner>DEPRECATED</owner>
        <system_id>101</system_id>
        <client_id>00000000-0000-0000-0000-000000000000</client_id>
        <latitude_dec>32.38739</latitude_dec>
        <longitude_dec>-106.85369</longitude_dec>
        <elevation>0</elevation>
      </row>
    </general>
  </response>
</onerain>
```

**Sensor metadata** (`GET ...?method=GetSensorMetaData&system_key={key}&or_site_id=1164`).
Field list and descriptions come from the Airbyte schema. The DTW sensor is the
`sensor_class=102` row:

```xml
<onerain>
  <response>
    <general>
      <row>
        <site_id>209-0000-M19R</site_id>
        <sensor_id>4</sensor_id>
        <or_site_id>1164</or_site_id>
        <or_sensor_id>4</or_sensor_id>
        <location>Monitoring Well - MES_19R</location>
        <description>Depth to Water</description>
        <sensor_class>102</sensor_class>
        <sensor_type>SDI-12</sensor_type>
        <units>ft</units>
        <translate>false</translate>
        <precision>4</precision>
        <last_time>2024-09-04 21:08:21</last_time>
        <last_value>-14.6496</last_value>
        <last_time_received>2024-09-04 21:12:03</last_time_received>
        <last_value_received>-14.6496</last_value_received>
        <last_raw_value>-14.6496</last_raw_value>
        <last_raw_value_received>-14.6496</last_raw_value_received>
        <change_time>2024-06-30 21:39:11</change_time>
        <normal>1</normal>
        <active>1</active>
        <valid>1</valid>
        <validation>NONE</validation>
        <value_max>0</value_max>
        <value_min>-100</value_min>
        <delta_pos>0</delta_pos>
        <delta_neg>0</delta_neg>
        <time_max>0</time_max>
        <time_min>0</time_min>
        <slope>1</slope>
        <offset>0</offset>
        <reference>0</reference>
        <utc_offset>0</utc_offset>
        <conversion>NONE</conversion>
      </row>
    </general>
  </response>
</onerain>
```

The Airbyte schema documents seven more fields that carry no signal:
`change_rate`, `time_min_consec_zeros`, `rate_pos`, `rate_neg`, `using_dst`,
`usage` and `protocol`. They are omitted here. Expect them in a real response
and ignore them.

**Readings** (`GET ...?method=GetSensorData&system_key={key}&or_site_id=1164&or_sensor_id=4&data_start=2024-09-04%2019:00:00&data_end=2024-09-04%2022:00:00`):

```xml
<onerain>
  <response>
    <general>
      <row>
        <or_site_id>1164</or_site_id>
        <or_sensor_id>4</or_sensor_id>
        <receive_time>2024-09-04 19:12:04</receive_time>
        <data_time>2024-09-04 19:08:55</data_time>
        <data_value>-14.63910000</data_value>
        <raw_value>-14.63910000</raw_value>
        <data_quality>A</data_quality>
      </row>
      <row>
        <or_site_id>1164</or_site_id>
        <or_sensor_id>4</or_sensor_id>
        <receive_time>2024-09-04 19:42:01</receive_time>
        <data_time>2024-09-04 19:38:54</data_time>
        <data_value>-14.62860000</data_value>
        <raw_value>-14.62860000</raw_value>
        <data_quality>A</data_quality>
      </row>
      <row>
        <or_site_id>1164</or_site_id>
        <or_sensor_id>4</or_sensor_id>
        <receive_time>2024-09-04 20:12:07</receive_time>
        <data_time>2024-09-04 20:08:56</data_time>
        <data_value>-14.63910000</data_value>
        <raw_value>-14.63910000</raw_value>
        <data_quality>A</data_quality>
      </row>
      <row>
        <or_site_id>1164</or_site_id>
        <or_sensor_id>4</or_sensor_id>
        <receive_time>2024-09-04 20:41:58</receive_time>
        <data_time>2024-09-04 20:38:35</data_time>
        <data_value>-14.63910000</data_value>
        <raw_value>-14.63910000</raw_value>
        <data_quality>A</data_quality>
      </row>
      <row>
        <or_site_id>1164</or_site_id>
        <or_sensor_id>4</or_sensor_id>
        <receive_time>2024-09-04 21:11:44</receive_time>
        <data_time>2024-09-04 21:08:21</data_time>
        <data_value>-14.64960000</data_value>
        <raw_value>-14.64960000</raw_value>
        <data_quality>A</data_quality>
      </row>
    </general>
  </response>
</onerain>
```

An empty result set returns
`<onerain><response><general /></response></onerain>`, so the adapter must
handle the self-closing `<general/>` element, not just an absent `<row>`.

Errors come back as `<onerain><error>...</error></onerain>` with **HTTP 200**.
Status-code checks are not enough; the parser has to look for an `<error>`
element.

**Capturing the real payloads.** Once a `system_key` exists, export it to the
shell rather than pasting it into a file, then run the three commands below.

```bash
export EBID_SYSTEM_KEY='<paste-key-here>'
```

```bash
curl -sS "http://onerain.ebid-nm.org:8080/OneRain/DataAPI?method=GetSiteMetaData&system_key=$EBID_SYSTEM_KEY" -o ebid_site_metadata.xml
```

```bash
curl -sS "http://onerain.ebid-nm.org:8080/OneRain/DataAPI?method=GetSensorMetaData&system_key=$EBID_SYSTEM_KEY&or_site_id=1164" -o ebid_sensor_metadata.xml
```

```bash
curl -sS "http://onerain.ebid-nm.org:8080/OneRain/DataAPI?method=GetSensorData&system_key=$EBID_SYSTEM_KEY&or_site_id=1164&or_sensor_id=4&data_start=2024-09-04%2000:00:00&data_end=2024-09-04%2023:59:59" -o ebid_sensor_data.xml
```

Re-verify against those captures, in this order:

1. **The sign of `data_value`.** The one mapping that silently corrupts data if
   it is wrong (Open Question 1).
1. `sensor_class` for the DTW sensors. Is it really `102`, and is `or_sensor_id`
   still `4`?
1. The `units` string on the DTW sensor. Confirm `ft`, not `m` or `in`.
1. The `data_quality` value domain. Which flags actually appear?
1. That `data_time` is genuinely UTC. Cross-check a reading's `data_time`
   against `GetTime`, which returns UTC, and against the 30-minute reporting
   cadence.
1. `reference` and `elevation`. Does either carry a usable ground elevation (see
   Unit Conversions)?
1. The current site list. Which sites exist now, versus the historical six (Open
   Question 5)?

Sanitize before committing as a fixture. The `system_key` never appears in the
response body, but `client_id` is a Contrail GUID, so replace it the way the
reconstructed example above does.
