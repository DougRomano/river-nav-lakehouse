# Databricks notebook source
# MAGIC %md
# MAGIC # Collect raw snapshots
# MAGIC Lands raw API responses in the landing Volume, exactly as received.
# MAGIC Bronze ingests these files with Auto Loader. Nothing is parsed here.
# MAGIC
# MAGIC Layout: `<raw_volume>/<source>/ingest_date=YYYY-MM-DD/<source>_<utc-timestamp>[_<n>].json`

# COMMAND ----------

dbutils.widgets.text("raw_volume", "/Volumes/river_nav_dev/nav_raw/landing")
dbutils.widgets.text("lookback_hours", "2")

raw_volume = dbutils.widgets.get("raw_volume")
lookback_hours = int(dbutils.widgets.get("lookback_hours"))

# COMMAND ----------

import json
import os
import sys
from datetime import datetime, timedelta, UTC

# Make src/ importable when run from the bundle's workspace files
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

import requests

from river_nav.config import CORPS_LOCKS_SOURCES, GAUGES
from river_nav.usgs_client import continuous_url, fetch_pages

run_at = datetime.now(UTC)
stamp = run_at.strftime("%Y%m%dT%H%M%SZ")
partition = f"ingest_date={run_at:%Y-%m-%d}"


def land(source: str, payload: str | bytes, suffix: str = "", ext: str = "json") -> str:
    folder = f"{raw_volume}/{source}/{partition}"
    os.makedirs(folder, exist_ok=True)
    path = f"{folder}/{source}_{stamp}{suffix}.{ext}"
    mode = "wb" if isinstance(payload, bytes) else "w"
    with open(path, mode) as f:
        f.write(payload)
    return path


# COMMAND ----------

# MAGIC %md ## USGS gauges (overlapping window so revisions and late data are re-captured)

# COMMAND ----------

session = requests.Session()
start = run_at - timedelta(hours=lookback_hours)
landed = 0

for gauge in GAUGES:
    for code in gauge.parameter_codes:
        url = continuous_url(gauge.monitoring_location_id, code, start, run_at)
        for i, page in enumerate(fetch_pages(url, session)):
            page["_collector"] = {"source_url": url, "collected_at_utc": run_at.isoformat()}
            land("usgs_continuous",
                 json.dumps(page),
                 suffix=f"_{gauge.monitoring_location_id}_{code}_{i}")
            landed += 1

print(f"USGS: landed {landed} pages")

# COMMAND ----------

# MAGIC %md ## Corps Locks snapshots
# MAGIC Populate `CORPS_LOCKS_SOURCES` in `river_nav/config.py` after the week-2 spike.

# COMMAND ----------

if not CORPS_LOCKS_SOURCES:
    print("Corps Locks: no sources configured yet — skipping")
else:
    for name, url in CORPS_LOCKS_SOURCES:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        ctype = resp.headers.get("content-type", "")
        ext = "json" if "json" in ctype else ("csv" if "csv" in ctype else "html")
        land(f"corps_locks_{name}", resp.content, ext=ext)
        print(f"Corps Locks: landed {name} ({ext})")
