"""
canonical_constants.py
Shared constants for the Aqueduct canonical model.

Every source adapter imports from here.
Never define units, sensors, or observed properties inside an adapter.
If a constant is missing, add it here — do not work around it.

Lines marked ⚠ are open questions for the team to resolve.
"""

from aqueduct_dagster.canonical.canonical_model import CanonicalSensor, CanonicalObservedProperty

# ── Placeholders ──────────────────────────────────────────────────────────────

NO_DEFINITION = "No Definition"
NO_METADATA   = "No Metadata"

# ── Observation type ──────────────────────────────────────────────────────────

OM_Measurement = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"

# ── Units of measurement ──────────────────────────────────────────────────────

UNIT_FOOT = {
    "name": "Foot",
    "symbol": "ft",
    "definition": "http://www.qudt.org/vocab/unit/FT",
}

UNIT_METRE = {
    "name": "metre",
    "symbol": "m",
    "definition": "http://qudt.org/vocab/unit/M",
}

UNIT_CFS = {
    "name": "cubic foot per second",
    "symbol": "cfs",
    "definition": "http://qudt.org/vocab/unit/FT3-PER-SEC",
}

# ⚠ OPEN: should observations always be stored in feet, or preserve the source unit?

# ── Sensors ───────────────────────────────────────────────────────────────────

MANUAL_SENSOR = CanonicalSensor(
    external_key="sensor-manual-measurement",
    name="Manual",
    description=NO_DEFINITION,
    encoding_type="application/pdf",
    metadata=NO_METADATA,
)

HYDROVU_SENSOR = CanonicalSensor(
    external_key="sensor-hydrovu-vulink",
    name="VuLink",
    description=NO_DEFINITION,
    encoding_type="application/pdf",
    metadata=NO_METADATA,
)

# ── Observed properties ───────────────────────────────────────────────────────

DTW_OBS_PROP = CanonicalObservedProperty(
    external_key="observed-property-depth-to-water-bgs",
    name="Depth to Water Below Ground Surface",
    definition=NO_DEFINITION,
    description="depth to water below ground surface",
)

ELEV_OBS_PROP = CanonicalObservedProperty(
    external_key="observed-property-water-level-elevation-navd88",
    name="Water Level Elevation (NAVD88)",
    definition=NO_DEFINITION,   # ⚠ replace with real URI
    description="Groundwater elevation relative to NAVD88, in feet",
)

# ── Datastream name templates ─────────────────────────────────────────────────

def gwl_datastream_meta(agency: str, location_name: str) -> dict:
    return {
        "name": "Groundwater Levels",
        "description": "Measurement of groundwater depth in a water well, as measured below ground surface",
    }

def gwe_datastream_meta(agency: str, location_name: str) -> dict:
    return {
        "name": "Groundwater Elevations",
        "description": f"Water level elevation relative to NAVD88 — {agency} {location_name}",
    }

# ── external_key conventions ──────────────────────────────────────────────────
# Location/Thing:  f"{agency_lower}-{source_id}"           e.g. "pvacd-123"
# Datastream:      f"{agency_lower}-{source_id}-{suffix}"  e.g. "pvacd-123-dtw"

def make_location_key(agency: str, source_id: str) -> str:
    return f"{agency.lower()}-{source_id}"

def make_datastream_key(agency: str, source_id: str, suffix: str) -> str:
    return f"{agency.lower()}-{source_id}-{suffix}"