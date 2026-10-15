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

from river_nav.config import corps_locks_sources
from river_nav.corps_locks_client import RateLimited, polite_get

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"


def main() -> int:
    run_at = datetime.now(UTC)
    stamp = f"{run_at:%Y%m%dT%H%M%SZ}"
    failures = 0
    with requests.Session() as session:
        for name, url in corps_locks_sources(run_at.date()):
            source = f"corps_locks_{name}"
            folder = RAW / source / f"ingest_date={run_at:%Y-%m-%d}"
            try:
                body = polite_get(session, url)
            except (requests.RequestException, RateLimited) as exc:
                failures += 1
                print(f"FAILED {source}: {exc}", file=sys.stderr)
                continue
            folder.mkdir(parents=True, exist_ok=True)
            (folder / f"{source}_{stamp}.json").write_bytes(body)
            print(f"landed {source}: {len(body):,} bytes", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
