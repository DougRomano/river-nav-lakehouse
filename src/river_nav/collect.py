"""Land raw API payloads. Bronze reads the files; nothing is parsed here.

Layout: `<base>/<source>/ingest_date=YYYY-MM-DD/<source>_<utc-stamp>[_<suffix>].json`

The base path is an argument. Locally it is `data/raw`. On Databricks it is the
landing Volume. Same layout either way.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import requests

from river_nav.config import GAUGES
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
    path = folder / f"{source}_{stamp}{suffix}.{ext}"
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


def collect_usgs(
    base: str | Path,
    session: requests.Session | None,
    start: datetime,
    end: datetime,
    run_at: datetime,
) -> list[UsgsPull]:
    """Land every page of gage height and discharge for each tracked gauge."""
    pulls: list[UsgsPull] = []
    for gauge in GAUGES:
        for code in gauge.parameter_codes:
            url = continuous_url(gauge.monitoring_location_id, code, start, end)
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
                    suffix=f"_{gauge.monitoring_location_id}_{code}_{i}",
                )
                pages += 1
            pulls.append(UsgsPull(gauge.monitoring_location_id, code, pages, features))
    return pulls


def backfill_usgs(
    base: str | Path,
    session: requests.Session,
    end: datetime,
    days: int = BACKFILL_DAYS,
) -> list[UsgsPull]:
    """History load: one request per gauge and parameter, default 400 days."""
    calls = usgs_backfill_calls(end, days)
    start = calls[0][2]
    return collect_usgs(base, session, start, end, end)
