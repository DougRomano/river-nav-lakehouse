"""Minimal client for the USGS Water Data OGC API (continuous / instantaneous values).

Only builds URLs and pages through results; it never parses beyond the GeoJSON
envelope, so bronze keeps the payload exactly as received.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime
from urllib.parse import urlencode

import requests

from river_nav.config import USGS_API_BASE

DEFAULT_TIMEOUT_S = 30


def continuous_url(
    monitoring_location_id: str,
    parameter_code: str,
    start: datetime,
    end: datetime,
    limit: int = 10_000,
) -> str:
    """Build a request URL for the `continuous` collection (15-minute readings)."""
    if start >= end:
        raise ValueError("start must be before end")
    params = {
        "monitoring_location_id": monitoring_location_id,
        "parameter_code": parameter_code,
        "time": f"{_iso(start)}/{_iso(end)}",
        "limit": limit,
        "f": "json",
    }
    return f"{USGS_API_BASE}/collections/continuous/items?{urlencode(params)}"


def fetch_pages(url: str, session: requests.Session | None = None) -> Iterator[dict]:
    """Yield each GeoJSON page, following `rel=next` links until exhausted."""
    s = session or requests.Session()
    api_key = os.environ.get("USGS_API_KEY")
    headers = {"X-Api-Key": api_key} if api_key else {}
    next_url: str | None = url
    while next_url:
        resp = s.get(next_url, headers=headers, timeout=DEFAULT_TIMEOUT_S)
        resp.raise_for_status()
        page = resp.json()
        yield page
        next_url = next_link(page)


def next_link(page: dict) -> str | None:
    """Return the `next` link from an OGC API page, if any."""
    for link in page.get("links", []):
        if link.get("rel") == "next" and link.get("href"):
            return link["href"]
    return None


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
