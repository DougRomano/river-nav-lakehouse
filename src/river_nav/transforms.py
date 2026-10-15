"""Pure transforms used by the pipeline, written without Spark so they unit-test fast.

The Spark pipeline applies the same rules with column expressions; these
functions are the executable specification of those rules.
"""

from __future__ import annotations

from bisect import bisect_right
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta

USGS_NO_DATA_SENTINELS = {-999999.0, -999999}


# ---------------------------------------------------------------------------
# Gauge readings
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class GaugeReading:
    monitoring_location_id: str
    parameter_code: str
    observed_at_utc: datetime
    value: float | None
    is_provisional: bool


def parse_gauge_feature(feature: dict) -> GaugeReading:
    """Turn one GeoJSON feature from the `continuous` collection into a typed reading.

    - Times are normalized to UTC.
    - USGS no-data sentinels become None.
    - Readings flagged provisional (approval status not 'Approved') are marked,
      because USGS revises them later: silver must upsert, not append.
    """
    props = feature.get("properties", {})
    raw_value = props.get("value")
    value = None if raw_value in (None, "") else float(raw_value)
    if value in USGS_NO_DATA_SENTINELS:
        value = None

    approval = str(props.get("approval_status", "")).strip().lower()
    return GaugeReading(
        monitoring_location_id=props["monitoring_location_id"],
        parameter_code=props["parameter_code"],
        observed_at_utc=to_utc(props["time"]),
        value=value,
        is_provisional=approval != "approved",
    )


def to_utc(ts: str) -> datetime:
    """Parse an ISO-8601 timestamp (with Z or offset) and return it in UTC."""
    dt = datetime.fromisoformat(ts)  # 3.11+ accepts a trailing Z
    if dt.tzinfo is None:
        raise ValueError(f"timestamp has no offset: {ts!r}")
    return dt.astimezone(UTC)


def latest_per_key(readings: Iterable[GaugeReading]) -> list[GaugeReading]:
    """Deduplicate re-delivered readings: one row per (location, parameter, time).

    When USGS revises a provisional value, the approved one wins; otherwise
    the last one seen wins. Mirrors the silver-layer AUTO CDC / MERGE rule.
    """
    best: dict[tuple[str, str, datetime], GaugeReading] = {}
    for r in readings:
        key = (r.monitoring_location_id, r.parameter_code, r.observed_at_utc)
        current = best.get(key)
        if current is None or (current.is_provisional or not r.is_provisional):
            best[key] = r
    return sorted(best.values(), key=lambda r: (r.monitoring_location_id,
                                                r.parameter_code, r.observed_at_utc))


# ---------------------------------------------------------------------------
# Lock dimension (SCD Type 2)
# ---------------------------------------------------------------------------

OPEN_END = date(9999, 12, 31)

@dataclass(frozen=True)
class LockVersion:
    lock_id: str
    name: str
    river: str
    chamber_count: int
    status: str
    valid_from: date
    valid_to: date = OPEN_END
    is_current: bool = True

    def attributes(self) -> tuple:
        return (self.name, self.river, self.chamber_count, self.status)


def apply_scd2(
    existing: list[LockVersion],
    incoming: list[LockVersion],
    effective: date,
) -> list[LockVersion]:
    """Apply an SCD Type 2 load: close changed rows, insert new versions.

    - Unchanged locks: untouched.
    - Changed attributes: current row closed at effective - 1 day, new row opened.
    - New locks: inserted as current.
    - Locks missing from incoming: left as-is (a reference file is not a delete feed).
    """
    current = {v.lock_id: v for v in existing if v.is_current}
    result = list(existing)

    for new in incoming:
        old = current.get(new.lock_id)
        if old is not None and old.attributes() == new.attributes():
            continue
        if old is not None:
            idx = result.index(old)
            result[idx] = replace(old, valid_to=effective - timedelta(days=1), is_current=False)
        result.append(replace(new, valid_from=effective, valid_to=OPEN_END, is_current=True))

    return sorted(result, key=lambda v: (v.lock_id, v.valid_from))


# ---------------------------------------------------------------------------
# Lock queue delay
# ---------------------------------------------------------------------------

def delay_minutes(arrived_at: datetime, started_lockage_at: datetime | None) -> float | None:
    """Queue delay in minutes; None while still waiting; negative values are data errors."""
    if started_lockage_at is None:
        return None
    minutes = (started_lockage_at - arrived_at).total_seconds() / 60
    if minutes < 0:
        raise ValueError("lockage started before arrival")
    return round(minutes, 1)


# ---------------------------------------------------------------------------
# 15-minute grid interpolation
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Reading:
    observed_at_utc: datetime
    value: float | None

def interpolate_at(
    readings: list[Reading], grid: list[datetime]
) -> list[tuple[datetime, float | None]]:
    """Value at each grid time, linearly interpolated between the nearest readings."""
    ordered = sorted(readings, key=lambda r: r.observed_at_utc)
    times = [r.observed_at_utc for r in ordered]
    found: list[tuple[datetime, float | None]] = []
    for t in grid:
        # Last reading at or before t: TOP 1 WHERE observed_at <= t ORDER BY observed_at DESC.
        i = bisect_right(times, t) - 1
        if i < 0:
            continue
        left = ordered[i]
        if left.observed_at_utc == t:
            found.append((t, left.value))
            continue
        if i + 1 >= len(ordered):
            found.append((t, left.value))
            continue
        right = ordered[i + 1]
        if left.value is None or right.value is None:
            found.append((t, None))
            continue
        span = (right.observed_at_utc - left.observed_at_utc).total_seconds()
        fraction = (t - left.observed_at_utc).total_seconds() / span
        found.append((t, left.value + fraction * (right.value - left.value)))
    return found

def floor_15(ts: datetime) -> datetime:
    """Floor to the nearest 15-minute mark."""
    return ts.replace(second=0, microsecond=0, minute=ts.minute // 15 * 15)

def ceil_15(ts: datetime) -> datetime:
    """Ceil to the nearest 15-minute mark."""
    return ts.replace(second=0, microsecond=0, minute=ts.minute // 15 * 15 + 15)

def interpolate_angle(start_deg: float, end_deg: float, fraction: float) -> float:
    """Interpolate between two angles, wrapping around at 360°."""
    raise NotImplementedError