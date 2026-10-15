from datetime import UTC, datetime

import pytest

from river_nav.transforms import Reading, interpolate_at


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
