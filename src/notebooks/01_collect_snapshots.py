# Databricks notebook source
# MAGIC %md
# MAGIC # Collect raw snapshots
# MAGIC Lands raw API responses in the landing Volume, exactly as received.
# MAGIC Bronze ingests these files with Auto Loader. Nothing is parsed here.
# MAGIC
# MAGIC Layout: `<raw_volume>/<source>/ingest_date=YYYY-MM-DD/<source>_<utc-timestamp>[_<n>].json`

# COMMAND ----------

dbutils.widgets.text("raw_volume", "/Volumes/river_nav_dev/nav_raw/landing")
dbutils.widgets.text("lookback_hours", "6")  # USGS lags ~2.5 h; overlap is deduped in silver

raw_volume = dbutils.widgets.get("raw_volume")
lookback_hours = int(dbutils.widgets.get("lookback_hours"))

# COMMAND ----------

import os
import sys
from datetime import datetime, timedelta, UTC

# Make src/ importable when run from the bundle's workspace files
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

import requests

from river_nav.collect import collect_usgs, land
from river_nav.config import corps_locks_sources
from river_nav.corps_locks_client import polite_get

run_at = datetime.now(UTC)


# COMMAND ----------

# MAGIC %md ## USGS gauges (overlapping window so revisions and late data are re-captured)

# COMMAND ----------

session = requests.Session()
start = run_at - timedelta(hours=lookback_hours)
pulls = collect_usgs(raw_volume, session, start, run_at, run_at)
print(f"USGS: landed {sum(pull.pages for pull in pulls)} pages")

# COMMAND ----------

# MAGIC %md ## Corps Locks (USACE NDC Data Web Services)
# MAGIC Traffic and queue keep only ~30 days; status and delay are snapshots. Bodies are
# MAGIC landed as-is (served as text/html, and lock_delay_json is not always valid JSON).

# COMMAND ----------

for name, url in corps_locks_sources(run_at.date()):
    body = polite_get(session, url)  # 5 requests/min limit; ~4 min per run
    land(raw_volume, f"corps_locks_{name}", body, run_at)
    print(f"Corps Locks: landed {name} ({len(body):,} bytes)")
