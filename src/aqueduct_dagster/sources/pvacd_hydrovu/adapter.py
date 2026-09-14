"""
sources/pvacd_hydrovu/adapter.py

PvacdHydroVuAdapter: grouped PVACD HydroVu parquet rows -> CanonicalBundles for FROST.

The mapping itself is vendor-level and lives in sources/hydrovu_transform_common.py,

Called by pvacd_hydrovu/transform.py, which:
  1. Reads raw parquet from GCS
  2. Filters to DTW rows (parameter_id="4") before grouping
  3. Groups filtered rows by location_id into one record per location
  4. Passes records to PvacdHydroVuAdapter(records).run()

external_key convention: "pvacd-{location_id}" for the Location and its Thing,
"pvacd-{location_id}-dtw" for the datastream, e.g. "pvacd-4745648669458432-dtw".
The record shape and the DTW mapping are documented on HydroVuDtwAdapter.
"""

from __future__ import annotations

from aqueduct_dagster.sources.hydrovu_transform_common import HydroVuDtwAdapter


class PvacdHydroVuAdapter(HydroVuDtwAdapter):
    """Adapter for PVACD HydroVu groundwater level data."""

    AGENCY = "PVACD"
