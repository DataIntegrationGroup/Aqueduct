"""
PvacdHydroVuAdapter: grouped PVACD HydroVu parquet rows -> CanonicalBundles
for FROST. The mapping itself is vendor-level and lives in
hydrovu_transform_common.py's HydroVuDtwAdapter; this just sets AGENCY.

external_key: "pvacd-{location_id}" for Location/Thing, "pvacd-{location_id}-dtw"
for the datastream, e.g. "pvacd-4745648669458432-dtw".
"""

from __future__ import annotations

from aqueduct_dagster.sources.hydrovu_transform_common import HydroVuDtwAdapter


class PvacdHydroVuAdapter(HydroVuDtwAdapter):
    AGENCY = "PVACD"
