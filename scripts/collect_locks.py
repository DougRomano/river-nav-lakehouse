"""Land the USACE Waterway Locks CSV under data/raw/reference/waterway_locks/.

Lock characteristics change rarely. Run this once now, then whenever the BTS layer's
last-edit date moves. Same folder layout as the Volume, so the file can be uploaded
later with `databricks fs cp`.
  python scripts/collect_locks.py
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import requests

from river_nav.collect import collect_waterway_locks

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"


def main() -> int:
    run_at = datetime.now(UTC)
    with requests.Session() as session:
        try:
            path = collect_waterway_locks(RAW, session, run_at)
        except requests.RequestException as exc:
            print(f"FAILED waterway_locks: {exc}", file=sys.stderr)
            return 1
    print(f"landed {path.relative_to(RAW.parent.parent)}: {path.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
