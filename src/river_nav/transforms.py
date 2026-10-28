"""Pure transforms used by the pipeline, written without Spark so they unit-test fast.

The Spark pipeline applies the same rules with column expressions; these
functions are the executable specification of those rules.
"""

from __future__ import annotations

from bisect import bisect_right
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

USGS_NO_DATA_SENTINELS = {-999999.0, -999999}

# Instantaneous value. A daily mean (00003) and any other statistic are a
# different series and must not be parsed into the same gauge readings.
INSTANTANEOUS_STATISTIC_ID = "00011"

# The gauges report about every 30 minutes. Two hours is four missed intervals.
MAX_GAP = timedelta(hours=2)


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


def is_instantaneous(statistic_id: str | None) -> bool:
    """True when the reading belongs on the instantaneous series.

    ``00011`` is instantaneous. A missing id is kept, because a page can omit
    the field. Any other id is a different series.
    """
    return statistic_id in (None, INSTANTANEOUS_STATISTIC_ID)


def parse_gauge_feature(feature: dict) -> GaugeReading | None:
    """Turn one GeoJSON feature from the `continuous` collection into a typed reading.

    Returns None when ``statistic_id`` names something other than instantaneous.
    See ``is_instantaneous``.

    - Times are normalized to UTC.
    - USGS no-data sentinels become None.
    - Readings flagged provisional (approval status not 'Approved') are marked,
      because USGS revises them later: silver must upsert, not append.
    """
    props = feature.get("properties", {})
    if not is_instantaneous(props.get("statistic_id")):
        return None
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
    readings: list[Reading],
    grid: list[datetime],
    max_gap: timedelta = MAX_GAP,
) -> list[tuple[datetime, float | None]]:
    """Value at each grid time, linearly interpolated between the nearest readings.

    Callers must dedupe first (``latest_per_key``). This function does not.
    The timestamp sort is stable, so equal timestamps stay in input order:
    the left neighbor is the last row in the list, and the right neighbor is
    the first. That is not the most recent revision.

    A gap longer than ``max_gap`` (default two hours) is left empty. A grid
    time between two readings that far apart is None, and so is a grid time
    more than ``max_gap`` after the last reading. An exact timestamp keeps
    its value. A shorter tail after the last reading still carries that
    value forward.
    """
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
            age = t - left.observed_at_utc
            found.append((t, left.value if age <= max_gap else None))
            continue
        right = ordered[i + 1]
        if right.observed_at_utc - left.observed_at_utc > max_gap:
            found.append((t, None))
            continue
        if left.value is None or right.value is None:
            found.append((t, None))
            continue
        span = (right.observed_at_utc - left.observed_at_utc).total_seconds()
        fraction = (t - left.observed_at_utc).total_seconds() / span
        found.append((t, left.value + fraction * (right.value - left.value)))
    return found

def interpolate_frame(
    readings: pd.DataFrame,
    grid: list[datetime],
    by: str,
    max_gap: timedelta = MAX_GAP,
) -> pd.DataFrame:
    """Value at each grid time, within each `by` group.

    Backward as-of is the bisect_right lookup: last row with observed_at_utc
    <= grid_time. Forward as-of, with exact matches off, is the first strictly
    later row. A grid time with no backward match is dropped. An exact grid
    time keeps the backward value. A time between two readings is
    left + fraction * (right - left), unless those readings are more than
    ``max_gap`` apart (default two hours), in which case the value is missing.
    With no forward match, the backward value carries forward only while the
    grid time is within ``max_gap`` of that reading.

    Callers must dedupe first (``latest_per_key``). This function does not.
    The time sort is stable, so equal timestamps stay in input order:
    backward as-of takes the last row in the list, and forward as-of takes
    the first strictly later row. That is not the most recent revision.
    """
    groups = readings[[by]].drop_duplicates()
    grid_frame = groups.merge(
        pd.DataFrame({"grid_time": pd.to_datetime(list(grid), utc=True)}), how="cross"
    ).sort_values("grid_time", kind="mergesort")
    ordered = readings.copy()
    ordered["observed_at_utc"] = pd.to_datetime(ordered["observed_at_utc"], utc=True)
    # merge_asof requires the on-key itself to be monotonic. Sorting by gauge
    # first makes time go backwards at each gauge boundary. by= still partitions
    # the match. A stable time sort keeps equal timestamps in input order, so
    # backward as-of takes the last duplicate in that gauge and forward as-of
    # takes the first strictly later row in that gauge (ordered[i + 1]).
    ordered = ordered.sort_values("observed_at_utc", kind="mergesort")

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
    max_gap_s = max_gap.total_seconds()
    since_left_s = (matched["grid_time"] - matched["left_time"]).dt.total_seconds()
    span_s = (matched["right_time"] - matched["left_time"]).dt.total_seconds()
    fraction = since_left_s / span_s
    # A null on either side stays null. NaN propagates, so no is-None guard.
    # interpolate_at needs the guard because 8.0 - None raises TypeError.
    blended = matched["left_value"] + fraction * (
        matched["right_value"] - matched["left_value"]
    )
    matched["value"] = blended.where(~exact & ~no_right, matched["left_value"])
    # A bracket wider than max_gap is empty all the way across, including next
    # to the left reading. After the last reading, the gap is the time since it.
    # span_s is NaN when there is no right neighbor, and NaN > max_gap is false.
    too_wide = ~exact & span_s.gt(max_gap_s)
    stale = no_right & since_left_s.gt(max_gap_s)
    matched["value"] = matched["value"].mask(too_wide | stale)
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

def fifteen_minute_grid(start: datetime, end: datetime) -> list[datetime]:
    """15-minute marks from ceil(start) through floor(end), inclusive.

    A mark before start would be dropped for the gauge that begins latest.
    A mark after end would sit past the gauge that ends earliest.
    """
    first = ceil_15(start)
    last = floor_15(end)
    if first > last:
        return []
    marks: list[datetime] = []
    current = first
    while current <= last:
        marks.append(current)
        current += timedelta(minutes=15)
    return marks

def interpolate_angle(start_deg: float, end_deg: float, fraction: float) -> float:
    """Shortest-arc blend, in degrees. Result is in [0, 360)."""
    delta = (end_deg - start_deg + 180) % 360 - 180
    return (start_deg + fraction * delta) % 360


# ---------------------------------------------------------------------------
# Corps Locks lockages (USACE NDC traffic_report)
# ---------------------------------------------------------------------------

# The feed labels every time "CST", but the clock is local Central time.
# Checked 2026-10-28 on 65,880 landed timestamps: read as fixed CST, 22 of them
# fall after the moment the file was pulled. Read as America/Chicago, none do.
LPMS_TIME_ZONE = ZoneInfo("America/Chicago")
LPMS_TIME_FORMAT = "%m/%d/%y %H:%M"


def parse_lpms_time(text: str | None) -> datetime | None:
    """Parse an LPMS time like "10/05/26 12:06" (local Central) and return UTC.

    Blank or missing values become None. In the repeated hour when daylight
    time ends, the first occurrence (daylight time) is used, because the feed
    gives no way to tell the two apart.
    """
    if text is None or not text.strip():
        return None
    local = datetime.strptime(text.strip(), LPMS_TIME_FORMAT).replace(tzinfo=LPMS_TIME_ZONE)
    return local.astimezone(UTC)


@dataclass(frozen=True)
class Lockage:
    river_code: str
    lock_no: str
    vessel_no: str
    vessel_name: str | None
    direction: str
    arrival_utc: datetime
    start_utc: datetime | None
    end_utc: datetime | None
    num_barges: int | None
    number_processed: int | None
    is_hazardous: bool | None
    ingested_at: datetime
    cut_seq: int = 0

    @property
    def lock_code(self) -> str:
        """Same key as silver_lock_history.lock_code, for example "MI-26"."""
        return f"{self.river_code}-{self.lock_no}"

    @property
    def lockage_id(self) -> str:
        """One lockage: lock, vessel, arrival and cut. Stable across pulls."""
        return f"{self.lock_code}|{self.vessel_no}|{self.arrival_utc:%Y%m%dT%H%MZ}|{self.cut_seq}"


def _optional_int(value: object) -> int | None:
    return None if value in (None, "") else int(value)


def parse_lockage_record(record: dict, ingested_at: datetime) -> Lockage:
    """Turn one traffic_report object into a typed Lockage.

    Raises ValueError when the record has no arrival time; that row cannot be
    placed in time and is dropped by the silver expectation.
    """
    arrival = parse_lpms_time(record.get("arrivalDate"))
    if arrival is None:
        raise ValueError("lockage has no arrivalDate")
    hazard = (record.get("hazardCode") or "").strip().upper()
    return Lockage(
        river_code=record["riverCode"].strip(),
        lock_no=record["lockNo"].strip(),
        vessel_no=record["vesselNo"].strip(),
        vessel_name=(record.get("vesselName") or "").strip() or None,
        direction=record["direction"].strip(),
        arrival_utc=arrival,
        start_utc=parse_lpms_time(record.get("solDate")),
        end_utc=parse_lpms_time(record.get("endOfLockage")),
        num_barges=_optional_int(record.get("numBarges")),
        number_processed=_optional_int(record.get("numberProcessed")),
        is_hazardous={"Y": True, "N": False}.get(hazard),
        ingested_at=ingested_at,
    )


def latest_snapshot_lockages(lockages: Iterable[Lockage]) -> list[Lockage]:
    """Keep the newest version of each vessel arrival, then number its cuts.

    The feed is a rolling 30-day window, pulled daily, so one arrival appears
    in many pulls. A later pull can revise start and end times or the barge
    count. One arrival can also have several rows: a long tow locks through in
    cuts. Neither the start time nor the cut count is stable across pulls, so
    the rule works on the whole arrival:

    - Group by lock, vessel and arrival time.
    - Keep only the rows from the newest pull that contains that arrival.
    - Number those rows 1..n by start time (rows with no start sort last).

    An arrival that has rolled out of the newest pull keeps its last version.
    Mirrors silver_lockages in the pipeline.
    """
    groups: dict[tuple[str, str, datetime], list[Lockage]] = {}
    for row in lockages:
        groups.setdefault((row.lock_code, row.vessel_no, row.arrival_utc), []).append(row)

    result: list[Lockage] = []
    for rows in groups.values():
        newest = max(r.ingested_at for r in rows)
        latest = [r for r in rows if r.ingested_at == newest]
        latest.sort(key=lambda r: (r.start_utc is None, r.start_utc or r.arrival_utc))
        result.extend(replace(r, cut_seq=i) for i, r in enumerate(latest, start=1))
    return sorted(result, key=lambda r: (r.lock_code, r.arrival_utc, r.vessel_no, r.cut_seq))

