import email.utils

from lyrelog._transport import Backoff, classify, parse_retry_after


def test_2xx_with_rejected_lines_is_delivered():
    outcome = classify(200, b'{"accepted":2,"rejected":[{"line":1,"error":"bad"}]}', None)
    assert outcome.kind == "delivered" and len(outcome.rejected) == 1


def test_unreadable_2xx_is_retried():
    assert classify(200, b"<html>", None).kind == "retry"


def test_4xx_drops_and_auth_stops():
    assert classify(400, b"", None).kind == "drop"
    assert classify(413, b"", None).kind == "drop"
    assert classify(415, b"", None).kind == "drop"
    assert classify(401, b"", None).kind == "unauthorized"
    assert classify(403, b"", None).kind == "unauthorized"


def test_temporary_failures_retry_with_retry_after():
    outcome = classify(429, b"", "7")
    assert outcome.kind == "retry" and outcome.retry_after == 7
    assert classify(503, b"", None).kind == "retry"
    assert classify(599, b"", None).kind == "retry"
    assert classify(408, b"", None).kind == "retry"


def test_retry_after_as_http_date():
    now = 1_800_000_000.0
    header = email.utils.formatdate(now + 30, usegmt=True)
    assert abs(parse_retry_after(header, now=now) - 30) < 1
    assert parse_retry_after("garbage") is None
    assert parse_retry_after("-1") is None


def test_backoff_doubles_from_one_second_to_five_minutes():
    high = lambda a, b: b  # noqa: E731
    low = lambda a, b: a  # noqa: E731
    assert Backoff.delay(0, rand=high) == 1
    assert Backoff.delay(3, rand=high) == 8
    assert Backoff.delay(30, rand=high) == 300
    assert Backoff.delay(3, rand=low) == 4  # equal jitter: never below half
    assert Backoff.delay(0, retry_after=60, rand=high) == 60


import pytest

from lyrelog._transport import DEFAULT_ENDPOINT, normalize_endpoint


@pytest.mark.parametrize("raw, base", [
    ("https://ingest.lyrelog.com", "https://ingest.lyrelog.com"),
    ("https://ingest.lyrelog.com/", "https://ingest.lyrelog.com"),
    ("https://ingest.lyrelog.com/v1", "https://ingest.lyrelog.com"),
    ("https://ingest.lyrelog.com/v1/", "https://ingest.lyrelog.com"),
    ("https://ingest.lyrelog.com/v1/events:batch", "https://ingest.lyrelog.com"),
    ("  HTTPS://Ingest.Lyrelog.com  ", "https://ingest.lyrelog.com"),
    ("https://proxy.example.com/lyrelog", "https://proxy.example.com/lyrelog"),
    ("http://localhost:8090", "http://localhost:8090"),
    ("http://127.0.0.1:9", "http://127.0.0.1:9"),
    ("http://[::1]:8090/v1", "http://[::1]:8090"),
    (None, DEFAULT_ENDPOINT),
    ("", DEFAULT_ENDPOINT),
])
def test_endpoints_are_normalised(raw, base):
    assert normalize_endpoint(raw) == base


@pytest.mark.parametrize("raw", [
    "ingest.lyrelog.com",            # no scheme: would retry forever
    "http://ingest.lyrelog.com",     # the key would travel in clear
    "ftp://ingest.lyrelog.com",
    "https://",
    "https://ingest.lyrelog.com?x=1",
])
def test_bad_endpoints_are_refused(raw):
    assert normalize_endpoint(raw) is None


def test_retry_after_is_clamped_to_an_hour():
    assert parse_retry_after("inf") == 3600
    assert parse_retry_after("999999") == 3600
    assert parse_retry_after("nan") is None
    assert parse_retry_after("1e400") == 3600
    assert parse_retry_after("9" * 5000) == 3600
    assert parse_retry_after("Fri, 31 Dec 9999 23:59:59 GMT", now=1_800_000_000.0) == 3600
    assert Backoff.delay(10 ** 9, retry_after=float("inf"), rand=lambda a, b: b) == 3600
    assert parse_retry_after(email.utils.formatdate(1_900_000_000.0, usegmt=True), now=1_800_000_000.0) == 3600


def test_3xx_is_a_drop():
    assert classify(307, b"", None).kind == "drop"
    assert classify(301, b"", None).kind == "drop"


def test_delivered_counts_what_the_server_accepted():
    outcome = classify(200, b'{"accepted":3,"rejected":[{"line":4,"error":"bad"}]}', None)
    assert outcome.accepted == 3


@pytest.mark.parametrize("status", [200, 500])
def test_the_response_read_is_capped_at_1_mib(stub, status):
    from lyrelog._transport import MAX_RESPONSE_BYTES, UrllibTransport

    stub.respond(status, b"x" * (3 << 20))
    code, data, _ = UrllibTransport().send(stub.url + "/v1/events:batch", b"", {}, 5)
    assert code == status
    assert len(data) == MAX_RESPONSE_BYTES == 1 << 20
