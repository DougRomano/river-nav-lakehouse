# Databricks notebook source
# MAGIC %md
# MAGIC # Check outbound access
# MAGIC Free Edition serverless compute reaches only a limited set of trusted domains.
# MAGIC "ConnectionError" alone can mean four different things, so this notebook tests each
# MAGIC layer for every host and reports the first one that fails:
# MAGIC
# MAGIC | Layer | Fails as | Usually means |
# MAGIC | --- | --- | --- |
# MAGIC | DNS | `gaierror` | The name does not resolve from serverless (allowlist by DNS) |
# MAGIC | TCP 443 | timeout / refused | Firewall drops or rejects the connection |
# MAGIC | TLS | SSL error / reset | A proxy cuts the handshake for hosts not on the list |
# MAGIC | HTTP | status code / body | Reachable; any problem is the API itself |
# MAGIC
# MAGIC It also tries other hostnames that serve the same data, and two controls.
# MAGIC Run it twice, a few minutes apart, to rule out a one-off failure.

# COMMAND ----------

import os
import socket
import ssl
import time
from urllib.parse import urlparse

import requests

TARGETS = [
    # (label, url). Corps Locks allows 5 requests per minute: only one call to it here.
    ("USGS Water Data API", "https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items"
     "?monitoring_location_id=USGS-07010000&parameter_code=00065&limit=1&f=json"),
    ("USACE Corps Locks (ndc.ops)", "https://ndc.ops.usace.army.mil/ords/lpms/lock_delay_json"),
    ("USACE Corps Locks site (corpslocks)", "https://corpslocks.usace.army.mil/"),
    ("BTS Waterway Locks (ArcGIS services7)",
     "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Locks/FeatureServer/0?f=json"),
    ("BTS Waterway Locks (geodata.bts.gov hub)",
     "https://geodata.bts.gov/datasets/usdot::waterway-locks/about"),
    ("ArcGIS Hub API", "https://hub.arcgis.com/api/v3/datasets?filter%5Bslug%5D=usdot::waterway-locks"),
    ("NOAA NCEI (weather, later)", "https://www.ncei.noaa.gov/"),
    # Controls: one that should work, one that should not.
    ("Control: pypi.org", "https://pypi.org/simple/requests/"),
    ("Control: example.com", "https://example.com/"),
]


def check(url: str) -> dict:
    host = urlparse(url).hostname
    row = {"host": host, "dns": "", "tcp": "", "tls": "", "http": "", "first_failure": ""}

    try:
        addrs = sorted({a[4][0] for a in socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)})
        row["dns"] = ", ".join(addrs[:3])
    except OSError as exc:
        row["dns"] = f"FAIL {type(exc).__name__}: {exc}"
        row["first_failure"] = "DNS"
        return row

    try:
        start = time.perf_counter()
        sock = socket.create_connection((host, 443), timeout=10)
        row["tcp"] = f"ok {1000 * (time.perf_counter() - start):.0f} ms"
    except OSError as exc:
        row["tcp"] = f"FAIL {type(exc).__name__}: {exc}"
        row["first_failure"] = "TCP"
        return row

    try:
        with ssl.create_default_context().wrap_socket(sock, server_hostname=host) as tls:
            row["tls"] = f"ok {tls.version()}"
    except (OSError, ssl.SSLError) as exc:
        row["tls"] = f"FAIL {type(exc).__name__}: {exc}"
        row["first_failure"] = "TLS"
        return row

    try:
        resp = requests.get(url, timeout=30)
        body = resp.content[:200]
        note = " (rate-limited body)" if b"Rate limit exceeded" in body else ""
        row["http"] = f"{resp.status_code}, {len(resp.content):,} bytes{note}"
        if not resp.ok:
            row["first_failure"] = "HTTP"
    except requests.RequestException as exc:
        cause = exc.__context__ or exc.__cause__
        row["http"] = f"FAIL {type(exc).__name__}: {cause or exc}"[:300]
        row["first_failure"] = "HTTP"
    return row


proxies = {k: v for k, v in os.environ.items() if "proxy" in k.lower()}
print("Proxy settings in the environment:", proxies or "none")

rows = [{"source": label, **check(url)} for label, url in TARGETS]
display(spark.createDataFrame(rows))  # noqa: F821
