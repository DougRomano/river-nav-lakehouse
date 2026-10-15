from datetime import UTC, datetime, timedelta

from river_nav.collect import (
    BACKFILL_DAYS,
    collect_usgs,
    collect_waterway_locks,
    land,
    usgs_backfill_calls,
)
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


def test_land_nested_source_names_the_file_after_the_last_segment(tmp_path):
    run_at = datetime(2026, 10, 15, 20, 0, tzinfo=UTC)
    path = land(tmp_path, "reference/waterway_locks", b"a,b\n1,2\n", run_at, ext="csv")
    assert path == (
        tmp_path
        / "reference"
        / "waterway_locks"
        / "ingest_date=2026-10-15"
        / "waterway_locks_20261015T200000Z.csv"
    )


class _Response:
    def __init__(self, content: bytes):
        self.content = content

    def raise_for_status(self):
        pass


class _Session:
    def __init__(self, content: bytes):
        self.content = content
        self.calls: list[str] = []

    def get(self, url, **kwargs):
        self.calls.append(url)
        return _Response(self.content)


def test_collect_waterway_locks_lands_the_csv_as_received(tmp_path):
    body = "\ufeffOBJECTID,NDCCODE\n1,3020090601\n".encode()
    session = _Session(body)
    run_at = datetime(2026, 10, 15, 20, 0, tzinfo=UTC)
    path = collect_waterway_locks(tmp_path, session, run_at, url="https://example.test/locks.csv")
    assert session.calls == ["https://example.test/locks.csv"]
    assert path.parent == tmp_path / "reference" / "waterway_locks" / "ingest_date=2026-10-15"
    assert path.read_bytes() == body  # BOM and all: bronze keeps payloads as received
