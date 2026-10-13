"""Static configuration: which gauges, locks and parameters this project tracks.

VERIFY in the week-2 spike: monitoring-location IDs and the Corps Locks source.
Values marked VERIFY are best-known starting points, not confirmed endpoints.
"""

from __future__ import annotations

from dataclasses import dataclass

USGS_API_BASE = "https://api.waterdata.usgs.gov/ogcapi/v0"
"""New USGS Water Data API. The legacy waterservices.usgs.gov endpoints are retired."""

# USGS parameter codes
PARAM_DISCHARGE_CFS = "00060"
PARAM_GAGE_HEIGHT_FT = "00065"
PARAM_WATER_TEMP_C = "00010"


@dataclass(frozen=True)
class Gauge:
    monitoring_location_id: str
    name: str
    river: str
    parameter_codes: tuple[str, ...] = (PARAM_DISCHARGE_CFS, PARAM_GAGE_HEIGHT_FT)


# VERIFY each ID at https://waterdata.usgs.gov/monitoring-location/<id>/
GAUGES: tuple[Gauge, ...] = (
    Gauge("USGS-05587450", "Mississippi River at Grafton, IL", "Upper Mississippi"),
    Gauge("USGS-07010000", "Mississippi River at St. Louis, MO", "Upper Mississippi"),
    Gauge("USGS-07020500", "Mississippi River at Chester, IL", "Upper Mississippi"),
    Gauge("USGS-05586100", "Illinois River at Valley City, IL", "Illinois"),
)


# VERIFY: lock names as they appear in the Corps Locks data. Used to filter the
# national feed down to the St. Louis area for the first iteration.
TARGET_LOCKS: tuple[str, ...] = (
    "Mel Price",
    "Chain of Rocks",
    "Lock 25",
    "Lock 24",
    "LaGrange",
    "Peoria",
)

# VERIFY: fill in after inspecting the Corps Locks site in the week-2 spike.
# Each entry: (source_name, url). The collector saves responses as-is.
CORPS_LOCKS_SOURCES: tuple[tuple[str, str], ...] = ()
