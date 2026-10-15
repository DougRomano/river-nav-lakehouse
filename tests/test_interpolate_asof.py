"""pandas merge_asof spelling of interpolate_at.

Same readings as tests/test_interpolate.py. Each test is added only after the
previous one is green, and the frame result must match the pure-Python list.
"""

from datetime import UTC, datetime

import pandas as pd
import pytest

from river_nav.transforms import Reading, interpolate_at, interpolate_frame


def _ts(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 10, 13, hour, minute, second, tzinfo=UTC)


def _frame(readings: list[Reading], gauge: str = "g1") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "gauge_id": gauge,
            "observed_at_utc": [r.observed_at_utc for r in readings],
            "value": [r.value for r in readings],
        }
    )


def _as_tuples(frame: pd.DataFrame) -> list[tuple[datetime, float | None]]:
    found: list[tuple[datetime, float | None]] = []
    for ts, val in zip(frame["grid_time"], frame["value"], strict=True):
        py = ts.to_pydatetime()
        found.append((py, None if pd.isna(val) else float(val)))
    return found


def _assert_matches(readings: list[Reading], grid: list[datetime], gauge: str = "g1") -> None:
    got = _as_tuples(interpolate_frame(_frame(readings, gauge), grid, by="gauge_id"))
    want = interpolate_at(readings, grid)
    assert len(got) == len(want)
    for (got_t, got_v), (want_t, want_v) in zip(got, want, strict=True):
        assert got_t == want_t
        if want_v is None:
            assert got_v is None
        else:
            assert got_v == pytest.approx(want_v)


def test_exact_grid_point_matches_interpolate_at():
    readings = [Reading(_ts(10, 0), 4.0)]
    _assert_matches(readings, [_ts(10, 0)])


def test_halfway_point_matches_interpolate_at():
    readings = [Reading(_ts(10, 0), 4.0), Reading(_ts(10, 30), 8.0)]
    _assert_matches(readings, [_ts(10, 15)])


def test_point_before_first_reading_is_skipped():
    readings = [Reading(_ts(10, 15), 4.0)]
    _assert_matches(readings, [_ts(10, 0), _ts(10, 15)])


def test_point_after_last_reading_carries_forward():
    readings = [Reading(_ts(10, 0), 4.0)]
    _assert_matches(readings, [_ts(10, 0), _ts(10, 30)])


def test_duplicate_timestamps_use_the_last_value():
    readings = [
        Reading(_ts(10, 0), 4.0),
        Reading(_ts(10, 0), 6.0),
        Reading(_ts(10, 30), 10.0),
    ]
    _assert_matches(readings, [_ts(10, 15)])
