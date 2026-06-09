# Canonical Model

## 1. What Is the Canonical Model

The canonical model is the fixed shape that all source data must be converted to before it reaches FROST. It does not change when a new source is added. Every source writes an adapter that maps its raw data to this shape.

```
Sources                  Adapter            Canonical Model        FROST
──────────────────────   ───────────────    ───────────────────    ──────────────────────
PVACD HydroVu        →                  →                      →  SensorThings API
CABQ CKAN            →   one per source  →   (never changes)    →  (only sees canonical
Source C, D …        →                  →                      →   shape)
```

---

## 2. The SensorThings Entities — In Water Data Terms

The canonical model is made up of **6 required entities** and **1 optional entity**, defined by the OGC SensorThings standard. Every adapter must produce the 6 required ones.

| # | Entity | Simple Terms | In water data Terms|
|---|--------|---------------|-------------------|
| 1 | **Thing** | The physical object being monitored | A groundwater well (e.g. `Zumwalt level`, `COA-0001`). This is the well itself — not the measurement. Every well is one Thing. |
| 2 | **Location** | Where the Thing is in the world | Lat/lon coordinates of the well plus its elevation. Stored as a GeoJSON Point. Elevation always in metres. |
| 3 | **Sensor** | The instrument or method that made the measurement | The measurement method — e.g. `Manual Measurement (steel-tape)` or `Continuous Pressure Logger (HydroVu)`. Currently a constant shared by all sources. |
| 4 | **ObservedProperty** | What is being measured | `Depth to Water Below Ground Surface`, or `Water Level Elevation (NAVD88)`. Each is defined with a URI from an established ontology. Constants shared by all sources. |
| 5 | **Datastream** | A time series linking one Thing + one Sensor + one ObservedProperty | One well can have multiple Datastreams — e.g. one for depth to water and one for water elevation. Each Datastream also defines the unit (feet) and observation type (`OM_Measurement`). |
| 6 | **Observation** | A single measurement: value + timestamp | One reading — e.g. `depth = 45.3 ft at 2026-06-03T06:00:00Z`. Always UTC. Can carry optional metadata like `measurement_method` or `dry_indicator`. |
| 7 | **FeatureOfInterest** *(optional)* | The real-world feature the observation is about | The aquifer or water body the observation is about. |

---

## 3. The Three Files

The canonical model lives in three Python files. Both POCs import from the same files.

| File | What it contains | Who uses it |
|------|-----------------|-------------|
| `canonical_model.py` | The 7 dataclasses — one per SensorThings entity. The shape every adapter must produce. | Both POC adapters import from here. `frost_loader.py` also imports from here. This file has no source-specific code — it is purely the shape definition. |
| `canonical_constants.py` | Shared values — units (feet, metres), sensor types (`MANUAL_SENSOR`, `CONTINUOUS_LOGGER`), observed properties (`DTW_OBS_PROP`, `ELEV_OBS_PROP`), and key-building functions. | Both POC adapters import from here. If a constant is missing, add it here — never define a constant inside an adapter. |
| `base_adapter.py` | Abstract base class. Defines the interface every adapter must implement: `extract()`, `to_thing()`, `to_observations()`. | Every source adapter inherits from `BaseAdapter`. The pipeline calls `adapter.run()` — the same call regardless of source. |

### How the files relate

```
canonical_model.py          ← defines the shapes (dataclasses)
canonical_constants.py      ← defines shared values (units, sensors, obs properties)
base_adapter.py             ← defines the interface every adapter must follow

# Each source adapter imports all three:
cabq_adapter.py     → inherits BaseAdapter → produces CanonicalBundle
pvacd_adapter.py    → inherits BaseAdapter → produces CanonicalBundle
source_c_adapter.py → inherits BaseAdapter → produces CanonicalBundle  (future)

# The loader imports canonical_model.py only:
frost_loader.py     → consumes CanonicalBundle → writes to FROST
```
