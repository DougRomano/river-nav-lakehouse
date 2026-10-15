"""Land raw API payloads. Bronze reads the files; nothing is parsed here.

Layout: `<base>/<source>/ingest_date=YYYY-MM-DD/<name>_<utc-stamp>[_<suffix>].<ext>`

`source` may be nested (`reference/waterway_locks`); `name` is its last segment.

The base path is an argument. Locally it is `data/raw`. On Databricks it is the
landing Volume. Same layout either way.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import requests

from river_nav.config import GAUGES, WATERWAY_LOCKS_CSV_URL
from river_nav.usgs_client import continuous_url, fetch_pages

BACKFILL_DAYS = 400


def land(
    base: str | Path,
    source: str,
    payload: str | bytes,
    run_at: datetime,
    *,
    suffix: str = "",
    ext: str = "json",
) -> Path:
    """Write one payload exactly as received, under the caller's base path."""
    stamp = f"{run_at:%Y%m%dT%H%M%SZ}"
    folder = Path(base) / source / f"ingest_date={run_at:%Y-%m-%d}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{Path(source).name}_{stamp}{suffix}.{ext}"
    if isinstance(payload, bytes):
        path.write_bytes(payload)
    else:
        path.write_text(payload)
    return path


def usgs_backfill_calls(
    end: datetime, days: int = BACKFILL_DAYS
) -> list[tuple[str, str, datetime, datetime]]:
    """One window per gauge and parameter. Four gauges and two parameters is eight calls."""
    if days < 1:
        raise ValueError("days must be positive")
    start = end - timedelta(days=days)
    return [
        (gauge.monitoring_location_id, code, start, end)
        for gauge in GAUGES
        for code in gauge.parameter_codes
    ]


@dataclass(frozen=True)
class UsgsPull:
    monitoring_location_id: str
    parameter_code: str
    pages: int
    features: int


def _land_gauge_window(
    base: str | Path,
    session: requests.Session | None,
    monitoring_location_id: str,
    parameter_code: str,
    start: datetime,
    end: datetime,
    run_at: datetime,
) -> UsgsPull:
    """Land every page of one gauge, parameter and window."""
    url = continuous_url(monitoring_location_id, parameter_code, start, end)
    pages = 0
    features = 0
    for i, page in enumerate(fetch_pages(url, session)):
        features += len(page.get("features", []))
        page["_collector"] = {
            "source_url": url,
            "collected_at_utc": run_at.isoformat(),
        }
        land(
            base,
            "usgs_continuous",
            json.dumps(page),
            run_at,
            suffix=f"_{monitoring_location_id}_{parameter_code}_{i}",
        )
        pages += 1
    return UsgsPull(monitoring_location_id, parameter_code, pages, features)


def collect_usgs(
    base: str | Path,
    session: requests.Session | None,
    start: datetime,
    end: datetime,
    run_at: datetime,
) -> list[UsgsPull]:
    """Land every page of gage height and discharge for each tracked gauge."""
    return [
        _land_gauge_window(
            base, session, gauge.monitoring_location_id, code, start, end, run_at
        )
        for gauge in GAUGES
        for code in gauge.parameter_codes
    ]


def backfill_usgs(
    base: str | Path,
    session: requests.Session | None,
    end: datetime,
    days: int = BACKFILL_DAYS,
) -> list[UsgsPull]:
    """History load: one request per window from ``usgs_backfill_calls``.

    Default is 400 days, four gauges, discharge and gage height. Each tuple
    is requested as written.
    """
    return [
        _land_gauge_window(base, session, gauge_id, code, start, window_end, end)
        for gauge_id, code, start, window_end in usgs_backfill_calls(end, days)
    ]


def collect_waterway_locks(
    base: str | Path,
    session: requests.Session,
    run_at: datetime,
    url: str = WATERWAY_LOCKS_CSV_URL,
) -> Path:
    """Land the USACE Waterway Locks CSV, as received, under `reference/waterway_locks`.

    Lock characteristics change rarely, so this is a periodic full snapshot, not a
    window. Each landed file is a new version for the SCD2 flow in silver.
    """
    resp = session.get(url, timeout=60, allow_redirects=True)
    resp.raise_for_status()
    return land(base, "reference/waterway_locks", resp.content, run_at, ext="csv")
