"""Backfill USGS gage height and discharge for the four gauges.

One request per gauge and parameter, 400 days by default. Each request may
return more than one page. Files land under data/raw in the Volume layout.

  python scripts/backfill_usgs.py
  python scripts/backfill_usgs.py --days 400
"""

from __future__ import annotations

import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import requests

from river_nav.collect import BACKFILL_DAYS, backfill_usgs

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"
ENV = Path(__file__).resolve().parents[1] / ".env"


def main() -> int:
    days = BACKFILL_DAYS
    if len(sys.argv) == 3 and sys.argv[1] == "--days":
        days = int(sys.argv[2])
    elif len(sys.argv) != 1:
        print("usage: python scripts/backfill_usgs.py [--days N]", file=sys.stderr)
        return 2

    _load_api_key()
    end = datetime.now(UTC).replace(microsecond=0)
    started = time.perf_counter()
    with requests.Session() as session:
        pulls = backfill_usgs(RAW, session, end, days)
    elapsed = time.perf_counter() - started

    print(f"\nUSGS backfill, {days} days, ending {end:%Y-%m-%d %H:%MZ}")
    for pull in pulls:
        print(
            f"{pull.monitoring_location_id} {pull.parameter_code}: "
            f"{pull.pages} pages, {pull.features:,} features"
        )
    print(
        f"{len(pulls)} calls, {sum(p.pages for p in pulls)} pages, "
        f"{sum(p.features for p in pulls):,} features, {elapsed:.0f}s"
    )
    return 0


def _load_api_key() -> None:
    """Read USGS_API_KEY from .env when the shell has not exported it."""
    if os.environ.get("USGS_API_KEY") or not ENV.exists():
        return
    for line in ENV.read_text().splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        if key.strip() == "USGS_API_KEY":
            os.environ["USGS_API_KEY"] = value.strip().strip('"').strip("'")
            return


if __name__ == "__main__":
    raise SystemExit(main())
