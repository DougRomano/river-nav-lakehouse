import pytest

from river_nav.corps_locks_client import RateLimited, is_rate_limited, polite_get

LIMITED = b'{"error": "Rate limit exceeded. Maximum 5 requests per minute allowed."}'


class FakeResponse:
    def __init__(self, content):
        self.content = content

    def raise_for_status(self):
        pass


class FakeSession:
    def __init__(self, bodies):
        self.bodies = list(bodies)

    def get(self, url, timeout):
        return FakeResponse(self.bodies.pop(0))


def test_detects_rate_limit_body():
    assert is_rate_limited(LIMITED)
    assert not is_rate_limited(b'[{"lockNo": "26"}]')


def test_retries_after_rate_limit_then_succeeds():
    waits = []
    body = polite_get(FakeSession([LIMITED, b"[]"]), "u", sleep=waits.append)
    assert body == b"[]"
    assert waits == [13.0, 61.0]


def test_gives_up_after_retries():
    with pytest.raises(RateLimited):
        polite_get(FakeSession([LIMITED] * 3), "u", retries=2, sleep=lambda s: None)
