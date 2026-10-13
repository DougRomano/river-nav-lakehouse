from datetime import UTC, date, datetime

import pytest

from river_nav.transforms import (
    OPEN_END,
    GaugeReading,
    LockVersion,
    apply_scd2,
    delay_minutes,
    latest_per_key,
    parse_gauge_feature,
    to_utc,
)
from river_nav.usgs_client import continuous_url, next_link


def feature(value, status="Provisional", time="2026-10-13T08:15:00-05:00"):
    return {
        "properties": {
            "monitoring_location_id": "USGS-07010000",
            "parameter_code": "00065",
            "time": time,
            "value": value,
            "approval_status": status,
        }
    }


# --- gauge parsing ---------------------------------------------------------

def test_parse_converts_to_utc_and_types_value():
    r = parse_gauge_feature(feature("12.34"))
    assert r.observed_at_utc == datetime(2026, 10, 13, 13, 15, tzinfo=UTC)
    assert r.value == pytest.approx(12.34)
    assert r.is_provisional


def test_parse_maps_no_data_sentinel_to_none():
    assert parse_gauge_feature(feature(-999999)).value is None
    assert parse_gauge_feature(feature("")).value is None


def test_parse_marks_approved_readings():
    assert not parse_gauge_feature(feature("1.0", status="Approved")).is_provisional


def test_to_utc_rejects_naive_timestamps():
    with pytest.raises(ValueError):
        to_utc("2026-10-13T08:15:00")


# --- dedupe / revisions ----------------------------------------------------

def _reading(value, provisional):
    return GaugeReading("USGS-07010000", "00065",
                        datetime(2026, 10, 13, 13, 15, tzinfo=UTC), value, provisional)


def test_latest_per_key_approved_beats_provisional_regardless_of_order():
    approved, provisional = _reading(10.0, False), _reading(9.5, True)
    assert latest_per_key([approved, provisional]) == [approved]
    assert latest_per_key([provisional, approved]) == [approved]


def test_latest_per_key_last_provisional_wins():
    first, second = _reading(9.0, True), _reading(9.5, True)
    assert latest_per_key([first, second]) == [second]


# --- SCD2 ------------------------------------------------------------------

def _lock(status="OPERABLE", chambers=2):
    return LockVersion("L025", "Lock 25", "Upper Mississippi", chambers, status,
                       valid_from=date(2026, 1, 1))


def test_scd2_unchanged_is_noop():
    existing = [_lock()]
    assert apply_scd2(existing, [_lock()], date(2026, 10, 1)) == existing


def test_scd2_change_closes_old_and_opens_new():
    out = apply_scd2([_lock()], [_lock(status="INOPERABLE")], date(2026, 10, 1))
    old, new = out
    assert (old.is_current, old.valid_to) == (False, date(2026, 9, 30))
    assert (new.is_current, new.valid_from, new.valid_to) == (True, date(2026, 10, 1), OPEN_END)
    assert new.status == "INOPERABLE"


def test_scd2_new_lock_inserted_and_missing_lock_kept():
    other = LockVersion("L027", "Chain Of Rocks", "Upper Mississippi", 2, "OPERABLE",
                        valid_from=date(2026, 1, 1))
    out = apply_scd2([_lock()], [other], date(2026, 10, 1))
    assert {v.lock_id for v in out} == {"L025", "L027"}
    assert all(v.is_current for v in out)


# --- delay -----------------------------------------------------------------

def test_delay_minutes():
    arrive = datetime(2026, 10, 13, 8, 0, tzinfo=UTC)
    start = datetime(2026, 10, 13, 10, 30, tzinfo=UTC)
    assert delay_minutes(arrive, start) == 150.0
    assert delay_minutes(arrive, None) is None
    with pytest.raises(ValueError):
        delay_minutes(start, arrive)


# --- USGS client -----------------------------------------------------------

def test_continuous_url_has_utc_interval():
    url = continuous_url("USGS-07010000", "00065",
                         datetime(2026, 10, 13, 0, 0), datetime(2026, 10, 13, 2, 0))
    assert "collections/continuous/items" in url
    assert "time=2026-10-13T00%3A00%3A00Z%2F2026-10-13T02%3A00%3A00Z" in url


def test_continuous_url_rejects_bad_window():
    with pytest.raises(ValueError):
        continuous_url("x", "00065", datetime(2026, 1, 2), datetime(2026, 1, 1))


def test_next_link():
    assert next_link({"links": [{"rel": "next", "href": "https://n"}]}) == "https://n"
    assert next_link({"links": [{"rel": "self", "href": "https://s"}]}) is None
