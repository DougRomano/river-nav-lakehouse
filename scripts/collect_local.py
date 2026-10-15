"""Stopgap collector: land Corps Locks responses under data/raw/ until the Databricks job runs.

Same folder layout as the Volume, so the files can be uploaded later with `databricks fs cp`.
Run daily at minimum (traffic and queue keep only ~30 days). ~4 min: 5 requests/min limit.
  python scripts/collect_local.py
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import requests

from river_nav.collect import land
from river_nav.config import corps_locks_sources
from river_nav.corps_locks_client import RateLimited, polite_get

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"


def main() -> int:
    run_at = datetime.now(UTC)
    failures = 0
    with requests.Session() as session:
        for name, url in corps_locks_sources(run_at.date()):
            source = f"corps_locks_{name}"
            try:
                body = polite_get(session, url)
            except (requests.RequestException, RateLimited) as exc:
                failures += 1
                print(f"FAILED {source}: {exc}", file=sys.stderr)
                continue
            path = land(RAW, source, body, run_at)
            print(f"landed {source}: {path.stat().st_size:,} bytes", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
