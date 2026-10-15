"""Put the four USGS gauges on one 15-minute gage-height grid.

Fetches parameter 00065 (gage height, feet), interpolates with interpolate_frame,
and checks every cell against interpolate_at.

  python scripts/align_gauges.py
  python scripts/align_gauges.py --days 2
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta

import pandas as pd
import requests

from river_nav.config import GAUGES, PARAM_GAGE_HEIGHT_FT
from river_nav.transforms import (
    GaugeReading,
    Reading,
    fifteen_minute_grid,
    interpolate_at,
    interpolate_frame,
    latest_per_key,
    parse_gauge_feature,
)
from river_nav.usgs_client import continuous_url, fetch_pages

INSTANTANEOUS = "00011"


def main() -> int:
    days = 2
    if len(sys.argv) == 3 and sys.argv[1] == "--days":
        days = int(sys.argv[2])
    elif len(sys.argv) != 1:
        print("usage: python scripts/align_gauges.py [--days N]", file=sys.stderr)
        return 2

    end = datetime.now(UTC).replace(microsecond=0)
    start = end - timedelta(days=days)
    fetched, skipped_stats = _fetch(start, end)
    if not fetched:
        print("no gage-height readings returned", file=sys.stderr)
        return 1

    deduped = latest_per_key(fetched)
    by_gauge = {g.monitoring_location_id: [] for g in GAUGES}
    for reading in deduped:
        by_gauge[reading.monitoring_location_id].append(
            Reading(reading.observed_at_utc, reading.value)
        )

    missing = [g.name for g in GAUGES if not by_gauge[g.monitoring_location_id]]
    if missing:
        print(f"no readings for: {', '.join(missing)}", file=sys.stderr)
        return 1

    overlap_start = max(s[0].observed_at_utc for s in by_gauge.values())
    overlap_end = min(s[-1].observed_at_utc for s in by_gauge.values())
    grid = fifteen_minute_grid(overlap_start, overlap_end)
    if not grid:
        print("gauges do not overlap on a 15-minute mark", file=sys.stderr)
        return 1

    frame = pd.DataFrame(
        [
            {"gauge_id": gauge_id, "observed_at_utc": r.observed_at_utc, "value": r.value}
            for gauge_id, readings in by_gauge.items()
            for r in readings
        ]
    )
    aligned = interpolate_frame(frame, grid, by="gauge_id")
    if not _matches_pure(aligned, by_gauge, grid):
        return 1

    _report(by_gauge, grid, aligned, start, end, skipped_stats)
    return 0


def _fetch(start: datetime, end: datetime) -> tuple[list[GaugeReading], int]:
    readings: list[GaugeReading] = []
    skipped = 0
    with requests.Session() as session:
        for gauge in GAUGES:
            url = continuous_url(gauge.monitoring_location_id, PARAM_GAGE_HEIGHT_FT, start, end)
            for page in fetch_pages(url, session):
                for feature in page.get("features", []):
                    props = feature.get("properties", {})
                    stat = props.get("statistic_id")
                    if stat not in (None, INSTANTANEOUS):
                        skipped += 1
                        continue
                    readings.append(parse_gauge_feature(feature))
            print(f"fetched {gauge.monitoring_location_id}", flush=True)
    return readings, skipped


def _matches_pure(
    aligned: pd.DataFrame, by_gauge: dict[str, list[Reading]], grid: list[datetime]
) -> bool:
    for gauge_id, readings in by_gauge.items():
        got = aligned.loc[aligned["gauge_id"] == gauge_id]
        pure = interpolate_at(readings, grid)
        if len(got) != len(pure):
            print(f"{gauge_id}: asof {len(got)} rows, pure {len(pure)}", file=sys.stderr)
            return False
        for ts, raw, (want_t, want_v) in zip(got["grid_time"], got["value"], pure, strict=True):
            if ts.to_pydatetime() != want_t:
                print(f"{gauge_id}: time {ts} != {want_t}", file=sys.stderr)
                return False
            if want_v is None:
                if not pd.isna(raw):
                    print(f"{gauge_id}: expected null at {want_t}", file=sys.stderr)
                    return False
            elif pd.isna(raw) or abs(float(raw) - want_v) > 1e-9:
                print(f"{gauge_id}: {raw} != {want_v} at {want_t}", file=sys.stderr)
                return False
    return True


def _report(
    by_gauge: dict[str, list[Reading]],
    grid: list[datetime],
    aligned: pd.DataFrame,
    start: datetime,
    end: datetime,
    skipped_stats: int,
) -> None:
    names = {g.monitoring_location_id: g.name.split(" at ", 1)[1].split(",")[0] for g in GAUGES}
    print(f"\nGage height (00065, ft), requested {_stamp(start)} .. {_stamp(end)}")
    if skipped_stats:
        print(f"skipped {skipped_stats} non-instantaneous rows")
    for gauge in GAUGES:
        readings = by_gauge[gauge.monitoring_location_id]
        gaps = [
            (b.observed_at_utc - a.observed_at_utc).total_seconds() / 60
            for a, b in zip(readings, readings[1:], strict=False)
        ]
        raw_times = {r.observed_at_utc for r in readings}
        exact = sum(t in raw_times for t in grid)
        label = names[gauge.monitoring_location_id]
        print(
            f"{label:12} {len(readings):4} readings, "
            f"median gap {pd.Series(gaps).median():.0f} min (max {max(gaps):.0f}), "
            f"{exact} of {len(grid)} slots exact, {len(grid) - exact} interpolated"
        )

    wide = (
        aligned.pivot(index="grid_time", columns="gauge_id", values="value")
        .rename(columns=names)
        .reindex(columns=[names[g.monitoring_location_id] for g in GAUGES])
        .sort_index()
    )
    print(f"\nShared grid {_stamp(grid[0])} .. {_stamp(grid[-1])}, {len(grid)} slots")
    print("pure Python match on every cell")
    shown = pd.concat([wide.head(4), wide.tail(2)])
    print(shown.to_string(float_format=lambda v: f"{v:,.2f}"))

    sample_id = GAUGES[1].monitoring_location_id
    raw_times = {r.observed_at_utc for r in by_gauge[sample_id]}
    sample_t = next(t for t in grid if t not in raw_times)
    left = max(
        (r for r in by_gauge[sample_id] if r.observed_at_utc <= sample_t),
        key=lambda r: r.observed_at_utc,
    )
    right = min(
        (r for r in by_gauge[sample_id] if r.observed_at_utc > sample_t),
        key=lambda r: r.observed_at_utc,
    )
    value = wide.loc[pd.Timestamp(sample_t), names[sample_id]]
    print(
        f"\n{names[sample_id]} {_stamp(sample_t)}: "
        f"{left.value:.2f} ft at {_stamp(left.observed_at_utc)}, "
        f"{right.value:.2f} ft at {_stamp(right.observed_at_utc)} -> {value:.2f} ft"
    )


def _stamp(ts: datetime) -> str:
    return f"{ts:%Y-%m-%d %H:%MZ}"


if __name__ == "__main__":
    raise SystemExit(main())
