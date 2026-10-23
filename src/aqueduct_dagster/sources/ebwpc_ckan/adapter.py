"""
EBWPC's agency shim and adapter. The shim translates EBWPC's raw CKAN rows into
the WDI water-level shape. The mapping itself is WdiWaterLevelAdapter's in
ckan_transform_common.py, shared by every CKAN agency.

EBWPC-specific quirks handled here:
- Each well ID is spelled three ways between the locations file, resource name and the site_id column (e.g. E-50-1-archived vs E-50-1-Archived vs e-50-1 archived)
- E-2034 appears to be typo'd as E-2043 in the locations file
- Locations are UTM only
- Elevation and formation appear only in four resource descriptions
- Transducer and manual depths are in separate columns
- Dates and times come in mixed formats
- Times are on a fixed UTC−07:00 clock.

external_key: "ebwpc-{well}" for Location/Thing, where {well} is the
locations file's NMBG_ID trimmed with "-archived" dropped (case kept);
"ebwpc-{well}-dtw" for the transducer datastream and "ebwpc-{well}-dtw-manual"
for the manual one, e.g. "ebwpc-E-8428-dtw-manual".
"""

from __future__ import annotations

import logging
import math
import re
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone

from pyproj import Transformer

from aqueduct_dagster.sources.ckan_transform_common import WdiWaterLevelAdapter, clean_cell

logger = logging.getLogger(__name__)

AGENCY = "EBWPC"
#: The loggers ignore daylight saving (readings at 02:xx on every spring-forward
#: date), so this is a fixed MST clock, not America/Denver.
SOURCE_TZ = timezone(timedelta(hours=-7))

# The locations file lists E-2034 as E-2043; the catalog has only E-2034.
_ALIASES = {"e-2043": "E-2034"}
_ARCHIVED = re.compile(r"[\s-]*archived$", re.IGNORECASE)

_DEPTH_COLUMNS = {"transducer_depth_bgs": "transducer", "manual_depth_bgs": "manual"}
# Measured from the top of casing, not ground surface: without a stick-up
# height they can't become depth below ground surface.
_TOC_COLUMNS = ("manual_depth_btoc", "manual_depth_below_toc")
_DATE_FORMATS = ("%m/%d/%Y", "%m/%d/%y")
_TIME_FORMATS = ("%H:%M", "%H:%M:%S", "%I:%M:%S %p", "%I:%M %p")
# "Elevation 6712 (ft AMSL), Approximate Depth Unknown,  Madera Formation"
_ELEVATION = re.compile(r"Elevation\s+([\d,]+(?:\.\d+)?)\s*\(\s*ft\s+AMSL\s*\)(.*)", re.IGNORECASE)

# NAD83 / UTM zone 13N -> WGS84. Reproduces old FROST's coordinates for every active well.
_UTM13N_TO_WGS84 = Transformer.from_crs("EPSG:26913", "EPSG:4326", always_xy=True)


class EbwpcCkanAdapter(WdiWaterLevelAdapter):
    AGENCY = AGENCY
    SOURCE_TZ = SOURCE_TZ


def well_id(raw: str) -> str:
    """The well's source_id: trimmed, "-archived" dropped, case kept, E-2043 corrected."""
    cleaned = _ARCHIVED.sub("", raw.strip()).strip()
    return _ALIASES.get(cleaned.casefold(), cleaned)


def site_key(raw: str) -> str:
    """The join key across the locations file, resource names and the site_id
    column, which spell one well three ways (E-50-1-archived, E-50-1-Archived,
    e-50-1 archived)."""
    return well_id(raw).casefold()


def parse_description(description: str) -> tuple[float | None, str | None]:
    """(elevation in ft AMSL, formation) from a well resource's free-text
    description. Four wells describe themselves this way; the rest give neither."""
    match = _ELEVATION.search(description)
    if match is None:
        return None, None
    elevation = float(match.group(1).replace(",", ""))
    segments = [segment.strip() for segment in match.group(2).split(",")]
    formations = [segment for segment in segments if segment and "depth" not in segment.casefold()]
    return elevation, formations[-1] if formations else None


def ebwpc_locations(location_rows: list[dict], resource_rows: list[dict]) -> dict[str, dict]:
    """WDI location records keyed by site_key, from the landed EBWPC Well
    Locations rows (column names as dlt normalised them) plus ckan_resources
    for each well's description. A row without usable coordinates is skipped:
    the best practices say such sites aren't loaded."""
    descriptions = {
        site_key(str(row["resource_name"])): str(row.get("resource_description") or "")
        for row in resource_rows
        if row.get("resource_name")
    }

    locations: dict[str, dict] = {}
    for row in location_rows:
        nmbg_id = clean_cell(row.get("nmbg_id"))
        try:
            if nmbg_id is None:
                raise ValueError("no NMBG_ID")
            easting = float(str(row.get("utm_zone13_n_easting")))
            northing = float(str(row.get("utm_zone13_n_northing")))
        except ValueError as exc:
            logger.warning("Skipping location row %r: %s", row, exc)
            continue

        key = site_key(nmbg_id)
        if key in locations:
            logger.warning("Location %r appears twice — keeping the later row", nmbg_id)
        source_id = well_id(nmbg_id)
        longitude, latitude = _UTM13N_TO_WGS84.transform(easting, northing)
        elevation_ft, lithology = parse_description(descriptions.get(key, ""))
        locations[key] = {
            "site_id": source_id,
            "site_name": source_id,
            "longitude": longitude,
            "latitude": latitude,
            "elevation": elevation_ft,
            "elevation_units": "ft",
            "lithology": lithology,
            "location_source_specific": {
                "utm_easting": easting,
                "utm_northing": northing,
                "utm_zone": "13N",
                "horizontal_datum": clean_cell(row.get("datum")),
                "is_archived": bool(_ARCHIVED.search(nmbg_id)),
            },
            "thing_source_specific": {},
        }
    return locations


def ebwpc_measurements(rows: list[dict]) -> tuple[list[dict], Counter[str]]:
    """Unpivots raw well rows into WDI measurements, up to two per row (one per
    depth column), with site_id as its site_key. Returns them with a tally of
    what was left out: rows yielding none, by reason (no_usable_depth,
    no_site_id, missing_or_junk_date, unparseable_date), and toc_reading_skipped."""
    measurements: list[dict] = []
    dropped: Counter[str] = Counter()
    warned: set[str] = set()

    for row in rows:
        toc_readings = sum(1 for column in _TOC_COLUMNS if clean_cell(row.get(column)) is not None)
        if toc_readings:
            dropped["toc_reading_skipped"] += toc_readings
        depths = {
            kind: depth
            for column, kind in _DEPTH_COLUMNS.items()
            if (depth := _parse_depth(row.get(column))) is not None
        }
        if not depths:
            if not toc_readings:
                dropped["no_usable_depth"] += 1
            continue

        raw_site = clean_cell(row.get("site_id"))
        if raw_site is None:
            dropped["no_site_id"] += 1
            continue
        key = site_key(raw_site)
        resource_name = str(row.get("resource_name") or "")
        if resource_name and site_key(resource_name) != key and resource_name not in warned:
            warned.add(resource_name)
            logger.warning(
                "Resource %s: site_id %r doesn't match the resource name — joining on site_id",
                resource_name,
                raw_site,
            )

        raw_date = clean_cell(row.get("measurement_date"))
        if raw_date is None:
            dropped["missing_or_junk_date"] += 1
            continue
        day = _parse_date(raw_date)
        if day is None:
            dropped["unparseable_date"] += 1
            continue
        clock = _parse_time(row.get("measurement_time"))

        for kind, depth in depths.items():
            measurements.append(
                {
                    "site_id": key,
                    "measurement_date": day.isoformat(),
                    "measurement_time": clock.isoformat() if clock else None,
                    "depth_to_water": depth,
                    "depth_to_water_unit": "ft",
                    "measurement_type": kind,
                }
            )
    return measurements, dropped


def _parse_depth(value: object) -> float | None:
    text = clean_cell(value)
    if text is None:
        return None
    try:
        depth = float(text)
    except ValueError:
        return None
    return depth if math.isfinite(depth) else None


def _parse_date(text: str) -> date | None:
    """M/D/YYYY or M/D/YY. None for impossible dates such as 1/0/1900 (Excel's
    zero date). A four-digit year keeps its century; a two-digit one takes
    strptime's pivot (00-68 -> 2000-2068, 69-99 -> 1969-1999), which puts every
    EBWPC reading (2007 onward) in the 2000s."""
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _parse_time(value: object) -> time | None:
    """H:MM, H:MM:SS or h:mm[:ss] AM/PM. None (time missing) for N/A, blank or unreadable."""
    text = clean_cell(value)
    if text is None:
        return None
    for fmt in _TIME_FORMATS:
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return None
