"""
BerncoHydroVuAdapter: grouped BernCo HydroVu parquet rows -> CanonicalBundles
for FROST. The mapping lives in hydrovu_transform_common.py's HydroVuDtwAdapter,
shared with pvacd_hydrovu; this just sets AGENCY. See docs/sources/bernco_hydrovu.md
for mapping decisions.

external_key: "bernco-{location_id}" for Location/Thing, "bernco-{location_id}-dtw"
for the datastream, e.g. "bernco-6255051791532032-dtw".

Sanity floor: two locations return readings near Unix epoch 0 with plausible
values — bad device clocks, not real 1970 measurements (WhisperingPines-1002958
at timestamp 0, E-55-POD 15-1173850TD at timestamp 960; not always exactly 0,
so `timestamp > 0` won't catch both). Transform passes initial_start_date as
min_timestamp to drop them.
"""

from __future__ import annotations

from aqueduct_dagster.sources.hydrovu_transform_common import HydroVuDtwAdapter


class BerncoHydroVuAdapter(HydroVuDtwAdapter):
    """Adapter for BernCo HydroVu groundwater level data."""

    AGENCY = "BERNCO"
