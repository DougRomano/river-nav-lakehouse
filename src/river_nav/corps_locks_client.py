"""Polite client for the USACE NDC Corps Locks Data Web Services.

The service allows 5 requests per minute and signals a breach with HTTP 200 and a body of
{"error": "Rate limit exceeded. ..."}, so status codes alone can't be trusted.
"""

from __future__ import annotations

import time
from collections.abc import Callable

import requests

MIN_INTERVAL_S = 13.0  # 60 s / 5 requests, with margin
RATE_LIMIT_MARKER = b"Rate limit exceeded"


class RateLimited(Exception):
    """The service answered 200 but with its rate-limit error body."""


def is_rate_limited(body: bytes) -> bool:
    return RATE_LIMIT_MARKER in body[:200]


def polite_get(
    session: requests.Session,
    url: str,
    *,
    retries: int = 3,
    sleep: Callable[[float], None] = time.sleep,
) -> bytes:
    """GET with a fixed pause before each call; retry after a minute on a rate-limit body."""
    for attempt in range(retries + 1):
        sleep(MIN_INTERVAL_S if attempt == 0 else 61.0)
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        if not is_rate_limited(resp.content):
            return resp.content
    raise RateLimited(url)
