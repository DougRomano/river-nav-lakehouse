from datetime import UTC, datetime

import pytest

from river_nav.transforms import Reading, ceil_15, floor_15, interpolate_angle, interpolate_at


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
    readings = [
        Reading(_ts(10, 0), 4.0),
        Reading(_ts(10, 0), 6.0),
        Reading(_ts(10, 30), 10.0),
    ]
    result = interpolate_at(readings, [_ts(10, 15)])
    assert result == [(_ts(10, 15), pytest.approx(8.0))]


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


def test_interpolate_angle_takes_the_short_arc():
    assert interpolate_angle(355, 10, 0.5) == pytest.approx(2.5)
    assert interpolate_angle(10, 20, 0.5) == pytest.approx(15)
