"""Pure transforms used by the pipeline, written without Spark so they unit-test fast.

The Spark pipeline applies the same rules with column expressions; these
functions are the executable specification of those rules.
"""

from __future__ import annotations

from bisect import bisect_right
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta

import pandas as pd

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

def interpolate_frame(
    readings: pd.DataFrame, grid: list[datetime], by: str
) -> pd.DataFrame:
    """Value at each grid time, within each `by` group.

    Backward as-of is the bisect_right lookup: last row with observed_at_utc
    <= grid_time. Forward as-of, with exact matches off, is the first strictly
    later row. A grid time with no backward match is dropped. An exact grid
    time, or one with no forward match, keeps the backward value. A time
    between two readings is left + fraction * (right - left).
    """
    groups = readings[[by]].drop_duplicates()
    grid_frame = groups.merge(
        pd.DataFrame({"grid_time": pd.to_datetime(list(grid), utc=True)}), how="cross"
    ).sort_values([by, "grid_time"], kind="mergesort")
    ordered = readings.copy()
    ordered["observed_at_utc"] = pd.to_datetime(ordered["observed_at_utc"], utc=True)
    # Stable sort keeps equal timestamps in input order. Backward as-of then
    # takes the last duplicate (bisect_right). Forward as-of takes the first
    # row strictly later, ordered[i + 1], which is the first of the next timestamp.
    ordered = ordered.sort_values([by, "observed_at_utc"], kind="mergesort")

    left = pd.merge_asof(
        grid_frame,
        ordered.rename(columns={"observed_at_utc": "left_time", "value": "left_value"}),
        left_on="grid_time",
        right_on="left_time",
        by=by,
        direction="backward",
    )
    right = pd.merge_asof(
        grid_frame,
        ordered.rename(columns={"observed_at_utc": "right_time", "value": "right_value"}),
        left_on="grid_time",
        right_on="right_time",
        by=by,
        direction="forward",
        allow_exact_matches=False,
    )
    matched = left.merge(right, on=[by, "grid_time"], how="left")
    # bisect_right - 1 < 0: no reading at or before this grid time.
    matched = matched.dropna(subset=["left_time"])
    exact = matched["left_time"].eq(matched["grid_time"])
    # i + 1 >= len(ordered): nothing strictly after this grid time.
    no_right = matched["right_time"].isna()
    span_s = (matched["right_time"] - matched["left_time"]).dt.total_seconds()
    fraction = (matched["grid_time"] - matched["left_time"]).dt.total_seconds() / span_s
    blended = matched["left_value"] + fraction * (
        matched["right_value"] - matched["left_value"]
    )
    matched["value"] = blended.where(~exact & ~no_right, matched["left_value"])
    return matched[[by, "grid_time", "value"]].reset_index(drop=True)

def floor_15(ts: datetime) -> datetime:
    """Floor to the nearest 15-minute mark."""
    return ts.replace(second=0, microsecond=0, minute=ts.minute // 15 * 15)

def ceil_15(ts: datetime) -> datetime:
    """Ceil to the nearest 15-minute mark. A time already on a boundary stays put."""
    floored = floor_15(ts)
    if floored == ts:
        return ts
    return floored + timedelta(minutes=15)

def interpolate_angle(start_deg: float, end_deg: float, fraction: float) -> float:
    """Shortest-arc blend, in degrees. Result is in [0, 360)."""
    delta = (end_deg - start_deg + 180) % 360 - 180
    return (start_deg + fraction * delta) % 360