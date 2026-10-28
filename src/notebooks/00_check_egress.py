# Databricks notebook source
# MAGIC %md
# MAGIC # Check outbound access
# MAGIC Free Edition serverless compute reaches only a limited set of trusted domains.
# MAGIC Run this once before the collector job. It makes one small request to each source.
# MAGIC
# MAGIC - **All OK:** the collector can run on Databricks. Turn off the Mac `launchd` job after
# MAGIC   the first scheduled run lands files in the Volume.
# MAGIC - **Any BLOCKED:** keep collecting on the Mac and upload with
# MAGIC   `databricks fs cp -r data/raw dbfs:/Volumes/<catalog>/<schema>/landing --overwrite`.

# COMMAND ----------

import requests

CHECKS = {
    "USGS Water Data API": (
        "https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items"
        "?monitoring_location_id=USGS-07010000&parameter_code=00065&limit=1&f=json"
    ),
    # One call only: this service allows 5 requests per minute.
    "USACE Corps Locks": "https://ndc.ops.usace.army.mil/ords/lpms/lock_delay_json",
    "BTS Waterway Locks": (
        "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Locks/FeatureServer/0"
        "?f=json"
    ),
}

results = []
for name, url in CHECKS.items():
    try:
        resp = requests.get(url, timeout=30)
        body = resp.content[:200]
        if b"Rate limit exceeded" in body:
            status = "OK (reachable, but rate-limited right now)"
        elif resp.ok:
            status = "OK"
        else:
            status = f"HTTP {resp.status_code}"
        results.append((name, status, len(resp.content)))
    except requests.RequestException as exc:
        results.append((name, f"BLOCKED: {type(exc).__name__}", 0))

display(spark.createDataFrame(results, "source string, status string, bytes long"))  # noqa: F821
