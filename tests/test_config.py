from datetime import date

from river_nav.config import TARGET_LOCKS, corps_locks_sources


def test_corps_locks_sources_cover_every_target_lock():
    sources = dict(corps_locks_sources(date(2026, 10, 13)))
    for river, lock in TARGET_LOCKS:
        assert f"in_river={river}&in_lock={lock}" in sources[f"traffic_{river}{lock}"]
        assert f"queue_{river}{lock}" in sources


def test_stall_stoppage_window_uses_ddmmyyyy():
    url = dict(corps_locks_sources(date(2026, 10, 13)))["stall_stoppage"]
    assert url.endswith("begin_date=08092026&end_date=13102026")
