from datetime import UTC, datetime, timedelta

import pytest

from river_nav.transforms import (
    Reading,
    ceil_15,
    fifteen_minute_grid,
    floor_15,
    interpolate_angle,
    interpolate_at,
)


def _ts(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 10, 13, hour, minute, second, tzinfo=UTC)


def test_exact_grid_point_returns_reading_value():
    readings = [Reading(_ts(10, 0), 4.0)]
    result = interpolate_at(readings, [_ts(10, 0)])
    assert result == [(_ts(10, 0), pytest.approx(4.0))]


def test_halfway_point_returns_midpoint():
    readings = [Reading(_ts(10, 0), 4.0), Reading(_ts(10, 30), 8.0)]
    result = interpolate_at(readings, [_ts(10, 15)])
    assert result == [(_ts(10, 15), pytest.approx(6.0))]


def test_point_before_first_reading_is_skipped():
    readings = [Reading(_ts(10, 15), 4.0)]
    result = interpolate_at(readings, [_ts(10, 0), _ts(10, 15)])
    assert result == [(_ts(10, 15), pytest.approx(4.0))]


def test_point_after_last_reading_carries_forward():
    readings = [Reading(_ts(10, 0), 4.0)]
    result = interpolate_at(readings, [_ts(10, 0), _ts(10, 30)])
    assert result == [
        (_ts(10, 0), pytest.approx(4.0)),
        (_ts(10, 30), pytest.approx(4.0)),
    ]


def test_duplicate_timestamps_use_the_last_value():
    # Last in the list, because the sort is stable. Not the latest revision.
    # Callers dedupe with latest_per_key before they get here.
    readings = [
        Reading(_ts(10, 0), 4.0),
        Reading(_ts(10, 0), 6.0),
        Reading(_ts(10, 30), 10.0),
    ]
    result = interpolate_at(readings, [_ts(10, 15)])
    assert result == [(_ts(10, 15), pytest.approx(8.0))]


def test_gap_longer_than_two_hours_is_missing():
    # Grafton offline for two days must not become a straight line from 4 to 8,
    # and the last reading must not carry forward past the two-hour limit.
    start = _ts(10, 0)
    end = start + timedelta(days=2)
    readings = [Reading(start, 4.0), Reading(end, 8.0)]
    between = start + timedelta(hours=1)
    after = end + timedelta(hours=3)
    assert interpolate_at(readings, [between, end, after]) == [
        (between, None),
        (end, pytest.approx(8.0)),
        (after, None),
    ]


def test_gap_of_two_hours_still_fills():
    # Longer than two hours is empty. A gap of exactly two hours still blends,
    # and the last reading still carries forward out to that same limit.
    start = _ts(10, 0)
    end = start + timedelta(hours=2)
    readings = [Reading(start, 4.0), Reading(end, 8.0)]
    assert interpolate_at(readings, [start + timedelta(hours=1), end + timedelta(hours=2)]) == [
        (start + timedelta(hours=1), pytest.approx(6.0)),
        (end + timedelta(hours=2), pytest.approx(8.0)),
    ]


def test_missing_value_on_either_side_yields_none():
    left_missing = [Reading(_ts(10, 0), None), Reading(_ts(10, 30), 8.0)]
    right_missing = [Reading(_ts(10, 0), 4.0), Reading(_ts(10, 30), None)]
    exact_missing = [Reading(_ts(10, 0), None)]

    assert interpolate_at(left_missing, [_ts(10, 15)]) == [(_ts(10, 15), None)]
    assert interpolate_at(right_missing, [_ts(10, 15)]) == [(_ts(10, 15), None)]
    assert interpolate_at(exact_missing, [_ts(10, 0)]) == [(_ts(10, 0), None)]


def test_floor_15_drops_minutes_and_seconds():
    assert floor_15(_ts(10, 7, 30)) == _ts(10, 0)


def test_ceil_15_keeps_an_exact_boundary():
    assert ceil_15(_ts(10, 7, 30)) == _ts(10, 15)
    assert ceil_15(_ts(10, 15)) == _ts(10, 15)


def test_fifteen_minute_grid_spans_inclusive_marks():
    assert fifteen_minute_grid(_ts(10, 7, 30), _ts(10, 46)) == [
        _ts(10, 15),
        _ts(10, 30),
        _ts(10, 45),
    ]
    assert fifteen_minute_grid(_ts(10, 0), _ts(10, 30)) == [
        _ts(10, 0),
        _ts(10, 15),
        _ts(10, 30),
    ]
    assert fifteen_minute_grid(_ts(10, 1), _ts(10, 14)) == []


def test_interpolate_angle_takes_the_short_arc():
    assert interpolate_angle(355, 10, 0.5) == pytest.approx(2.5)
    assert interpolate_angle(10, 20, 0.5) == pytest.approx(15)
