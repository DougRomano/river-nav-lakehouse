from datetime import UTC, datetime

import pytest

from river_nav.transforms import (
    delay_minutes,
    latest_snapshot_lockages,
    parse_lockage_record,
    parse_lpms_time,
)

PULL_1 = datetime(2026, 10, 27, 11, 7, tzinfo=UTC)
PULL_2 = datetime(2026, 10, 28, 11, 7, tzinfo=UTC)


def record(arrival="10/24/26 12:00", sol="10/24/26 13:58", end="10/24/26 16:25", **extra):
    base = {
        "riverCode": "MI", "lockNo": "26", "vesselNo": "0608409",
        "vesselName": "DANIEL P MECKLENBORG ", "direction": "D",
        "arrivalDate": arrival, "solDate": sol, "endOfLockage": end,
        "timezone": "CST", "numBarges": 9, "numberProcessed": 9, "hazardCode": "N",
    }
    return base | extra


# --- time zone ---------------------------------------------------------------

def test_daylight_time_is_utc_minus_5():
    assert parse_lpms_time("10/05/26 12:06") == datetime(2026, 10, 5, 17, 6, tzinfo=UTC)


def test_standard_time_is_utc_minus_6():
    assert parse_lpms_time("12/05/26 12:06") == datetime(2026, 12, 5, 18, 6, tzinfo=UTC)


def test_repeated_hour_takes_the_daylight_reading():
    # Nov 1, 2026: 01:30 happens twice. The feed cannot say which; we take the first.
    assert parse_lpms_time("11/01/26 01:30") == datetime(2026, 11, 1, 6, 30, tzinfo=UTC)


@pytest.mark.parametrize("blank", [None, "", "   "])
def test_blank_time_is_none(blank):
    assert parse_lpms_time(blank) is None


# --- parsing -----------------------------------------------------------------

def test_parse_types_the_record():
    lockage = parse_lockage_record(record(), PULL_1)
    assert lockage.lock_code == "MI-26"
    assert lockage.vessel_name == "DANIEL P MECKLENBORG"
    assert lockage.arrival_utc == datetime(2026, 10, 24, 17, 0, tzinfo=UTC)
    assert (lockage.num_barges, lockage.is_hazardous) == (9, False)


def test_parse_keeps_missing_counts_as_none():
    lockage = parse_lockage_record(
        record(numBarges=None, numberProcessed=None, hazardCode=None, solDate=None), PULL_1
    )
    assert lockage.num_barges is None
    assert lockage.number_processed is None
    assert lockage.is_hazardous is None
    assert lockage.start_utc is None


def test_parse_rejects_a_record_without_arrival():
    with pytest.raises(ValueError):
        parse_lockage_record(record(arrival=""), PULL_1)


# --- revisions and cuts --------------------------------------------------------

def test_newer_pull_replaces_a_revised_arrival():
    old = parse_lockage_record(record(sol="10/24/26 16:30", numberProcessed=6), PULL_1)
    new = parse_lockage_record(record(sol="10/24/26 13:58", numberProcessed=9), PULL_2)
    [kept] = latest_snapshot_lockages([new, old])
    assert kept.ingested_at == PULL_2
    assert kept.number_processed == 9
    assert kept.cut_seq == 1


def test_arrival_that_rolled_out_of_the_window_is_kept():
    old_only = parse_lockage_record(record(arrival="09/27/26 08:00", sol="09/27/26 09:00"), PULL_1)
    current = parse_lockage_record(record(), PULL_2)
    kept = latest_snapshot_lockages([old_only, current])
    assert {k.ingested_at for k in kept} == {PULL_1, PULL_2}


def test_cuts_of_one_arrival_are_numbered_by_start_time():
    second = parse_lockage_record(record(sol="10/24/26 15:10"), PULL_2)
    first = parse_lockage_record(record(sol="10/24/26 13:58"), PULL_2)
    kept = latest_snapshot_lockages([second, first])
    assert [k.cut_seq for k in kept] == [1, 2]
    assert kept[0].start_utc < kept[1].start_utc
    assert kept[0].lockage_id != kept[1].lockage_id


def test_newer_pull_with_fewer_cuts_drops_the_old_extra_cut():
    old = [
        parse_lockage_record(record(sol="10/24/26 13:58"), PULL_1),
        parse_lockage_record(record(sol="10/24/26 15:10"), PULL_1),
    ]
    new = [parse_lockage_record(record(sol="10/24/26 14:05"), PULL_2)]
    kept = latest_snapshot_lockages(old + new)
    assert [(k.ingested_at, k.cut_seq) for k in kept] == [(PULL_2, 1)]


def test_wait_is_arrival_to_start_of_lockage():
    lockage = parse_lockage_record(record(arrival="10/24/26 12:00", sol="10/24/26 13:58"), PULL_2)
    assert delay_minutes(lockage.arrival_utc, lockage.start_utc) == 118.0
