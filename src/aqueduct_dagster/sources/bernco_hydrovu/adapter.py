"""
sources/bernco_hydrovu/adapter.py

BerncoHydroVuAdapter: grouped BernCo HydroVu parquet rows -> CanonicalBundles for FROST.

The mapping is vendor-level and lives in sources/hydrovu_transform_common.py, shared
with the pvacd_hydrovu tenant: both tenants' raw rows are written by the same code in
hydrovu_common.py, so they map to canonical identically. All this module supplies is
BernCo's agency code. Mapping decisions are recorded in docs/sources/bernco_hydrovu.md.

Called by bernco_hydrovu/transform.py, which:
  1. Reads raw parquet from GCS
  2. Filters to DTW rows (parameter_id="4") before grouping
  3. Groups filtered rows by location_id into one record per location
  4. Passes records to BerncoHydroVuAdapter(records, min_timestamp=...).run()

external_key convention: "bernco-{location_id}" for the Location and its Thing,
"bernco-{location_id}-dtw" for the datastream, e.g. "bernco-6255051791532032-dtw".
The record shape and the DTW mapping are documented on HydroVuDtwAdapter.

Sanity floor: two BernCo locations return readings stamped at or near Unix epoch 0
carrying plausible values — bad device clocks, not 1970 measurements.
WhisperingPines-1002958 (id 5617246532927488) reports DTW 108.683 m at timestamp 0,
and E-55-POD 15-1173850TD (id 6725276880732160) at timestamp 960. The sentinel is not
always exactly 0, so `timestamp > 0` does not catch both. The transform passes the
source's initial_start_date as min_timestamp and readings below it are dropped.
"""

from __future__ import annotations

from aqueduct_dagster.sources.hydrovu_transform_common import HydroVuDtwAdapter


class BerncoHydroVuAdapter(HydroVuDtwAdapter):
    """Adapter for BernCo HydroVu groundwater level data."""

    AGENCY = "BERNCO"
