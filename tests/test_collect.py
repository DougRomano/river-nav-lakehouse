from datetime import UTC, datetime, timedelta

from river_nav.collect import BACKFILL_DAYS, collect_usgs, land, usgs_backfill_calls
from river_nav.config import GAUGES, PARAM_DISCHARGE_CFS, PARAM_GAGE_HEIGHT_FT


def test_land_writes_the_ingest_date_partition(tmp_path):
    run_at = datetime(2026, 10, 15, 19, 48, tzinfo=UTC)
    path = land(
        tmp_path,
        "usgs_continuous",
        '{"features": []}',
        run_at,
        suffix="_USGS-07010000_00065_0",
    )
    assert path == (
        tmp_path
        / "usgs_continuous"
        / "ingest_date=2026-10-15"
        / "usgs_continuous_20261015T194800Z_USGS-07010000_00065_0.json"
    )
    assert path.read_text() == '{"features": []}'


def test_land_writes_bytes_unchanged(tmp_path):
    run_at = datetime(2026, 10, 15, tzinfo=UTC)
    path = land(tmp_path, "corps_locks_status", b"not-json", run_at)
    assert path.read_bytes() == b"not-json"


def test_usgs_backfill_is_one_call_per_gauge_and_parameter():
    end = datetime(2026, 10, 15, 19, 48, tzinfo=UTC)
    calls = usgs_backfill_calls(end)
    assert len(calls) == len(GAUGES) * 2 == 8
    assert {gauge_id for gauge_id, _, _, _ in calls} == {g.monitoring_location_id for g in GAUGES}
    assert {code for _, code, _, _ in calls} == {PARAM_DISCHARGE_CFS, PARAM_GAGE_HEIGHT_FT}
    start = end - timedelta(days=BACKFILL_DAYS)
    windows = {(window_start, window_end) for _, _, window_start, window_end in calls}
    assert windows == {(start, end)}


def test_collect_usgs_lands_every_page(tmp_path, monkeypatch):
    def fake_pages(url, session=None):
        yield {"features": [{}, {}], "links": []}
        yield {"features": [{}], "links": []}

    monkeypatch.setattr("river_nav.collect.fetch_pages", fake_pages)
    end = datetime(2026, 10, 15, tzinfo=UTC)
    pulls = collect_usgs(tmp_path, None, end - timedelta(days=1), end, end)
    assert len(pulls) == 8
    assert sum(pull.pages for pull in pulls) == 16
    assert sum(pull.features for pull in pulls) == 24
    landed = list(tmp_path.glob("usgs_continuous/ingest_date=2026-10-15/*.json"))
    assert len(landed) == 16
