"""Static configuration: which gauges, locks and parameters this project tracks.

USGS gauge IDs verified 2026-10-13. VERIFY in the week-2 spike: the Corps Locks source.
Values marked VERIFY are best-known starting points, not confirmed endpoints.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

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


# Verified 2026-10-13: all four return 00060 and 00065 from the `continuous` collection.
GAUGES: tuple[Gauge, ...] = (
    Gauge("USGS-05587450", "Mississippi River at Grafton, IL", "Upper Mississippi"),
    Gauge("USGS-07010000", "Mississippi River at St. Louis, MO", "Upper Mississippi"),
    Gauge("USGS-07020500", "Mississippi River at Chester, IL", "Upper Mississippi"),
    Gauge("USGS-05586100", "Illinois River at Valley City, IL", "Illinois"),
)


WATERWAY_LOCKS_URL = (
    "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Locks/FeatureServer/0"
)
"""USACE Waterway Locks via BTS geodata (ArcGIS FeatureServer). 234 chamber rows nationally."""

# St. Louis-area locks as (RIVERCD, LOCKCD) from Waterway Locks, verified 2026-10-13.
# Match on codes, not names: the source spells them "CHAINS OF ROCKS L/D 27", "LOCK & DAM 25".
TARGET_LOCKS: dict[tuple[str, str], str] = {
    ("MI", "27"): "Chain of Rocks (L&D 27)",
    ("MI", "26"): "Mel Price (L&D 26)",
    ("MI", "25"): "Lock and Dam 25",
    ("MI", "24"): "Lock and Dam 24",
    ("IL", "08"): "LaGrange",
    ("IL", "07"): "Peoria",
}

# USACE NDC "Corps Locks" public Data Web Services (LPMS), documented at
# https://ndc.ops.usace.army.mil/ords/r/lpms/corps-locks/data-web-services
# Responses are served as text/html even when the body is JSON, and lock_delay_json is
# not always valid JSON, so the collector lands bodies as-is and bronze parses them.
CORPS_LOCKS_BASE = "https://ndc.ops.usace.army.mil/ords/lpms"


def corps_locks_sources(today: date) -> list[tuple[str, str]]:
    """(source_name, url) pairs for one collector run.

    traffic_report and lock_queue_json keep only a rolling ~30 days, so they must be
    collected at least weekly or history is lost. Status and delay are point-in-time
    snapshots: their history exists only if we collect them on a schedule.
    """
    rivers = sorted({river for river, _ in TARGET_LOCKS})
    sources: list[tuple[str, str]] = []
    for river, lock in sorted(TARGET_LOCKS):
        q = f"in_river={river}&in_lock={lock}"
        sources.append((f"traffic_{river}{lock}", f"{CORPS_LOCKS_BASE}/json/traffic_report?{q}"))
        sources.append((f"queue_{river}{lock}", f"{CORPS_LOCKS_BASE}/json/lock_queue_json?{q}"))
    sources.append(
        ("status", f"{CORPS_LOCKS_BASE}/json/lock_status_report?in_river_codes={':'.join(rivers)}")
    )
    sources.append(("delay", f"{CORPS_LOCKS_BASE}/lock_delay_json"))
    begin = today - timedelta(days=35)
    sources.append(
        ("stall_stoppage",
         f"{CORPS_LOCKS_BASE}/stall_stoppage_json?begin_date={begin:%d%m%Y}&end_date={today:%d%m%Y}")
    )
    return sources
