import inspect
import json

import pytest
import collections
import logging
import os
import threading
import time

import lyrelog
from lyrelog import _client


def test_events_reach_the_ingest_with_the_wire_shape(stub, make_client):
    client = make_client(release="1.2.3", app_id="tilefall-api", environment="Staging")
    client.set_user("u_1")
    client.info("checkout_done", category="billing", attrs={"amount": 499}, payload={"cart": [1]})
    assert client.flush(5)
    request = stub.requests[0]
    assert request["path"] == "/v1/events:batch"
    assert request["headers"]["Authorization"] == "Bearer lyr_ing_test"
    assert request["headers"]["Content-Type"] == "application/x-ndjson"
    assert request["headers"]["Content-Encoding"] == "gzip"
    event = request["lines"][0]
    assert set(event) == {"event_id", "ts", "level", "category", "message", "user_id", "install_id",
                          "session_id", "attrs", "payload", "resource"}
    assert event["level"] == "info" and event["message"] == "checkout_done"
    assert event["user_id"] == "u_1" and event["attrs"] == {"amount": 499}
    resource = event["resource"]
    assert resource["platform"] == "python"
    assert resource["sdk"] == "lyrelog-python/0.9.0"
    assert resource["environment"] == "staging"
    assert resource["app_version"] == "1.2.3" and resource["app_id"] == "tilefall-api"
    assert set(resource) <= {"app_id", "app_version", "app_build", "os_name", "os_version", "device_model",
                             "locale", "sdk", "environment", "platform"}


def test_environment_from_the_environment_variable(monkeypatch, stub):
    monkeypatch.setenv("LYRELOG_ENVIRONMENT", "development")
    assert lyrelog.Client(endpoint=stub.url, api_key="k").resource["environment"] == "development"
    monkeypatch.delenv("LYRELOG_ENVIRONMENT")
    assert lyrelog.Client(endpoint=stub.url, api_key="k", platform="API").resource["platform"] == "api"
    assert lyrelog.Client(endpoint=stub.url, api_key="k").resource["environment"] == "production"


def test_minimum_level_and_per_call_user(stub, make_client):
    client = make_client(minimum_level="info")
    client.set_user("everyone")
    client.debug("hidden")
    client.info("shown", user_id="u_9")
    client.warning("anon", user_id=None)
    client.flush(5)
    assert [e["message"] for e in stub.events] == ["shown", "anon"]
    assert stub.events[0]["user_id"] == "u_9" and "user_id" not in stub.events[1]


def test_batches_split_at_500_events(stub, make_client):
    client = make_client()
    for i in range(1201):
        client.info("e", attrs={"i": i})
    assert client.flush(10)
    sizes = [len(r["lines"]) for r in stub.requests]
    # A full batch may go out while the loop is still logging, so the later splits vary.
    assert max(sizes) == 500 and sum(sizes) == 1201 and len(sizes) >= 3
    assert [e["attrs"]["i"] for e in stub.events] == list(range(1201))


def test_batches_split_by_size(stub, make_client):
    client = make_client()
    for _ in range(40):
        client.info("big", payload={"blob": "x" * 30_000})
    assert client.flush(10)
    assert len(stub.requests) >= 3
    assert all(len(r["raw"]) <= 512 * 1024 for r in stub.requests)
    assert len(stub.events) == 40


def test_a_full_batch_is_sent_without_waiting_for_the_timer(stub, make_client):
    client = make_client(flush_interval=60)
    for _ in range(500):
        client.info("e")
    assert stub.wait_requests(1, timeout=5)


def test_the_timer_sends(stub, make_client):
    client = make_client(flush_interval=0.2)
    client.info("tick")
    assert stub.wait_requests(1, timeout=5)


def test_queue_cap_drops_the_oldest_and_says_so(stub, make_client):
    client = make_client(max_queue=3)
    for i in range(5):  # the worker only wakes on its 60 s timer or at 500 events
        client.info(f"m{i}")
    assert client.stats()["dropped"] == 2
    assert client.flush(5)
    messages = [e["message"] for e in stub.events]
    assert messages == ["lyrelog_events_dropped", "m2", "m3", "m4"]
    assert stub.events[0]["attrs"] == {"count": 2, "max_queue": 3, "max_queue_bytes": 16 * 1024 * 1024}


def test_rejected_lines_are_not_resent(stub, make_client):
    stub.respond(200, {"accepted": 1, "rejected": [{"line": 2, "error": "bad"}]})
    client = make_client()
    client.info("a")
    client.info("b")
    assert client.flush(5)
    assert client.flush(5)
    assert len(stub.requests) == 1
    assert client.stats()["rejected"] == 1 and client.stats()["delivered"] == 1


def test_400_and_413_drop_the_batch(stub, make_client):
    stub.respond(400, {"error": "bad"})
    stub.respond(413, {"error": "too big"})
    client = make_client()
    client.info("first")
    client.flush(5)
    client.info("second")
    client.flush(5)
    client.info("third")
    client.flush(5)
    assert [r["lines"][0]["message"] for r in stub.requests] == ["first", "second", "third"]
    assert client.stats()["discarded"] == 2


def test_401_stops_sending_but_keeps_the_data(stub, make_client):
    stub.respond(401, {"error": "revoked"})
    client = make_client()
    client.info("a")
    client.flush(5)
    client.info("b")
    assert client.flush(1) is False
    assert len(stub.requests) == 1
    stats = client.stats()
    assert stats["disabled"] is True and stats["queued"] == 2


def test_429_retries_the_same_bytes_after_retry_after(stub, make_client):
    stub.respond(429, {"error": "slow down"}, {"Retry-After": "120"})
    client = make_client()
    client.info("once")
    client.flush(5)
    assert client._pending is not None
    wait = client._not_before - time.monotonic()
    assert 110 < wait <= 120
    # The timer respects it, and so does a flush (the server asked)...
    client._round(force=False)
    assert client.flush(5) is False
    assert len(stub.requests) == 1
    # ...once the time has passed, the very same bytes go (same event id).
    client._retry_until = 0.0
    client.flush(5)
    assert len(stub.requests) == 2
    assert stub.requests[0]["raw"] == stub.requests[1]["raw"]
    assert client._pending is None


def test_5xx_and_network_errors_back_off(stub, make_client):
    stub.respond(503, {"error": "down"})
    client = make_client()
    client.info("x")
    client.flush(5)
    assert client._pending is not None and client._attempt == 1
    assert 0.5 <= client._not_before - time.monotonic() <= 1.0
    client.flush(5)
    assert client._pending is None and client._attempt == 0

    dead = lyrelog.Client(endpoint="http://127.0.0.1:9", api_key="k", flush_interval=60)
    dead.info("x")
    dead.flush(5)
    assert dead._pending is not None and dead._attempt == 1
    dead.shutdown(timeout=1)


def test_shutdown_flushes_and_ignores_later_events(stub, make_client):
    client = make_client()
    for i in range(10):
        client.info("bye", attrs={"i": i})
    client.shutdown(timeout=5)
    assert len(stub.events) == 10
    client.info("after")
    client.shutdown()
    assert len(stub.events) == 10


def test_atexit_hook_flushes_every_client(stub, make_client):
    client = make_client()
    client.info("exit")
    _client._shutdown_all()
    assert [e["message"] for e in stub.events] == ["exit"]


def test_logging_from_many_threads(stub, make_client):
    client = make_client()

    def work(n):
        for i in range(200):
            client.info("t", attrs={"n": n, "i": i})

    threads = [threading.Thread(target=work, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert client.flush(10)
    assert len(stub.events) == 1600
    assert len({e["event_id"] for e in stub.events}) == 1600


def test_never_raises_on_bad_input(stub, make_client):
    client = make_client()

    class Boom:
        def __str__(self):
            raise RuntimeError("no")

    client.info(Boom(), category=Boom(), attrs={"x": Boom()}, payload=Boom())
    client.info("bad attrs", attrs="not a dict")
    client.log("nonsense", "level")  # type: ignore[arg-type]
    assert client.flush(5)
    assert [e["message"] for e in stub.events][-2:] == ["bad attrs", "level"]


def test_capture_error_defaults_to_the_current_one(stub, make_client):
    client = make_client()
    try:
        raise ValueError("broken")
    except ValueError:
        client.capture_error(category="jobs", attrs={"job": "sync"})
    client.capture_error()  # nothing being handled: no event
    client.flush(5)
    [event] = stub.events
    assert event["level"] == "error" and event["message"] == "exception"
    assert event["attrs"] == {"job": "sync", "error_type": "ValueError", "error_message": "broken"}
    assert event["payload"]["type"] == "ValueError" and event["payload"]["message"] == "broken"
    assert "raise ValueError" in event["payload"]["traceback"]


def test_session_rolls_after_idle(stub, make_client):
    client = make_client(session_timeout=0.05)
    client.info("a")
    first = client.session_id
    time.sleep(0.1)
    client.info("b")
    client.flush(5)
    assert stub.events[0]["session_id"] == first
    assert stub.events[1]["session_id"] != first
    assert stub.events[0]["install_id"] == stub.events[1]["install_id"]


def test_module_level_api(stub):
    early = lyrelog.logger("early")  # made before init
    lyrelog.info("ignored before init")
    client = lyrelog.init(endpoint=stub.url, api_key="k", flush_interval=60)
    assert lyrelog.get_client() is client
    lyrelog.set_user("u_2")
    early.warning("works", attrs={"a": 1})
    lyrelog.critical("crit", category="c")
    try:
        1 / 0
    except ZeroDivisionError:
        lyrelog.capture_error()
    assert lyrelog.flush(5)
    lyrelog.shutdown()
    assert [(e.get("category"), e["message"], e["user_id"]) for e in stub.events] == [
        ("early", "works", "u_2"), ("c", "crit", "u_2"), (None, "exception", "u_2"),
    ]


def test_missing_configuration_is_a_quiet_no_op():
    client = lyrelog.Client(endpoint="", api_key="")
    client.info("nothing")
    assert client.flush(1) is False
    client.shutdown()


# Forking with the sender thread alive is exactly the case under test.
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
def test_fork_gets_a_fresh_worker_and_queue(stub, make_client):
    if not hasattr(os, "fork"):
        return
    client = make_client()
    client.info("parent")
    read, write = os.pipe()
    pid = os.fork()
    if pid == 0:  # child
        try:
            ok = client.stats()["queued"] == 0 and client._worker is None
            client.info("child")
            ok = ok and client.flush(5)
            os.write(write, b"1" if ok else b"0")
        finally:
            os._exit(0)
    os.close(write)
    result = os.read(read, 1)
    os.waitpid(pid, 0)
    assert result == b"1"
    assert client.flush(5)
    messages = sorted(e["message"] for e in stub.events)
    assert messages == ["child", "parent"]
    child = next(e for e in stub.events if e["message"] == "child")
    parent = next(e for e in stub.events if e["message"] == "parent")
    assert child["install_id"] != parent["install_id"]


def test_queue_is_bounded_by_bytes_too(stub, make_client):
    client = make_client(max_queue_bytes=200_000)
    for i in range(10):  # ~60 KB each: only the newest three fit in 200 KB
        client.info(f"m{i}", payload={"blob": "x" * 60_000})
    stats = client.stats()
    assert stats["dropped"] == 7
    assert client._queued_bytes <= 200_000
    assert client.flush(5)
    messages = [e["message"] for e in stub.events]
    assert messages == ["lyrelog_events_dropped", "m7", "m8", "m9"]
    assert stub.events[0]["attrs"]["count"] == 7
    assert stub.events[0]["attrs"]["max_queue_bytes"] == 200_000


def test_queue_bytes_default_is_16_mib(stub, make_client):
    assert make_client()._max_queue_bytes == 16 * 1024 * 1024


def _deep_raise(depth, message):
    if depth == 0:
        raise ValueError(message)
    _deep_raise(depth - 1, message)


def test_a_long_exception_message_keeps_the_traceback(stub, make_client):
    client = make_client()
    try:
        _deep_raise(5, "é" * 100_000)  # 200 KB of UTF-8, 100k characters
    except ValueError:
        client.capture_error()
    client.flush(5)
    [event] = stub.events
    payload = event["payload"]
    assert "lyrelog_truncated" not in payload
    assert len(payload["message"].encode()) <= 8 * 1024
    assert "_deep_raise" in payload["traceback"] and "raise ValueError" in payload["traceback"]
    encoded = json.dumps(payload, ensure_ascii=False).encode()
    assert len(encoded) < 64 * 1024


def test_a_multibyte_traceback_is_budgeted_in_bytes(stub, make_client):
    client = make_client()
    try:
        error = RuntimeError("fondo")
        error.add_note("ñ" * 40_000)  # 40k characters (under the old 48k-character cap), 80 KB
        raise error
    except RuntimeError:
        client.capture_error()
    client.flush(5)
    [event] = stub.events
    payload = event["payload"]
    assert "lyrelog_truncated" not in payload
    assert "raise error" in payload["traceback"] and "RuntimeError: fondo" in payload["traceback"]
    assert len(json.dumps(payload, ensure_ascii=False).encode()) < 64 * 1024


def test_lone_surrogates_and_huge_ints_still_send(stub, make_client):
    client = make_client()
    huge = 10 ** 5000  # past Python's 4,300-digit str() limit
    client.info("odd", attrs={"s": "a\ud800b", "n": huge, "big": 2 ** 70, "ok": 7},
                payload={"s": "x\udfffy", "\ud800k": 1, "n": huge, "list": [huge]})
    assert client.flush(5)
    [event] = stub.events
    assert event["attrs"]["s"] == "a?b" and event["attrs"]["ok"] == 7
    assert isinstance(event["attrs"]["n"], str) and isinstance(event["attrs"]["big"], str)
    assert event["payload"]["s"] == "x?y" and event["payload"]["?k"] == 1
    assert isinstance(event["payload"]["n"], str) and isinstance(event["payload"]["list"][0], str)
    assert client.stats()["failed"] == 0


def test_an_event_that_cannot_be_shaped_is_counted(stub, make_client, monkeypatch):
    client = make_client()

    def broken(*args, **kwargs):
        raise RuntimeError("bug")

    monkeypatch.setattr(_client.prepare, "payload", broken)
    client.info("lost", payload={"a": 1})
    assert client.stats()["failed"] == 1


def _finishes(fn, timeout=3.0):
    done = threading.Event()

    def run():
        fn()
        done.set()

    threading.Thread(target=run, daemon=True).start()
    return done.wait(timeout)


def test_logging_while_the_sdk_holds_its_lock_does_not_deadlock(stub, make_client):
    # A signal handler or a finalizer can run on a thread that is inside the SDK, with the
    # queue lock held. Simulate it: logging from inside the queue's append.
    client = make_client()

    class Reentrant(collections.deque):
        def append(self, item):
            client.warning("from a finalizer")
            super().append(item)

    client._queue = Reentrant()
    assert _finishes(lambda: client.info("outer"))
    client._queue = collections.deque(client._queue)
    assert client.flush(5)
    assert [e["message"] for e in stub.events] == ["outer"]
    assert client.stats()["reentrant"] == 1


def test_user_code_logging_during_shaping_is_dropped_not_looped(stub, make_client):
    client = make_client()

    class Chatty:
        def __str__(self):
            client.info("inner")
            return "chatty"

    assert _finishes(lambda: client.info("outer", attrs={"x": Chatty()}))
    assert client.flush(5)
    assert [e["message"] for e in stub.events] == ["outer"]
    assert stub.events[0]["attrs"]["x"] == "chatty"


def test_the_drop_note_is_built_outside_the_lock(stub, make_client):
    client = make_client(max_queue=1)
    client.info("a")
    client.info("b")
    held = []
    original = client._line

    def spy(*args, **kwargs):
        held.append(client._lock._is_owned())
        return original(*args, **kwargs)

    client._line = spy
    assert client.flush(5)
    assert held == [False]
    note = stub.events[0]
    assert note["message"] == "lyrelog_events_dropped"


def test_flush_and_shutdown_from_a_signal_handler_inside_the_sdk_return(stub, make_client):
    client = make_client()
    results = []

    class Interrupting(collections.deque):
        def append(self, item):  # a signal handler runs here, with the lock held
            results.append(client.flush(5))
            client.shutdown(timeout=5)
            super().append(item)

    client._queue = Interrupting()
    started = time.monotonic()
    assert _finishes(lambda: client.info("outer"))
    assert time.monotonic() - started < 2
    assert results == [False]


def test_the_drop_note_carries_no_user(stub, make_client):
    client = make_client(max_queue=1)
    client.set_user("u_secret")
    client.info("a")
    client.info("b")
    assert client.flush(5)
    note, event = stub.events
    assert note["message"] == "lyrelog_events_dropped" and "user_id" not in note
    assert event["user_id"] == "u_secret"


def test_the_default_endpoint_is_the_hosted_ingest():
    client = lyrelog.Client(api_key="lyr_ing_x")
    assert client._endpoint == "https://ingest.lyrelog.com/v1/events:batch"
    assert not client.stats()["disabled"]
    client._closed = True  # nothing to send


def test_v1_in_the_endpoint_is_not_doubled(stub):
    client = lyrelog.Client(endpoint=stub.url + "/v1", api_key="k", flush_interval=60)
    client.info("a")
    assert client.flush(5)
    assert stub.requests[0]["path"] == "/v1/events:batch"
    client.shutdown()


def test_an_invalid_endpoint_disables_the_client_loudly(caplog):
    with caplog.at_level(logging.WARNING, logger="lyrelog"):
        client = lyrelog.Client(endpoint="ingest.lyrelog.com", api_key="lyr_ing_x")
    assert client.stats()["disabled"] is True
    assert any("ingest.lyrelog.com" in r.getMessage() and r.levelno == logging.WARNING for r in caplog.records)
    client.info("x")
    assert client.flush(1) is False


def test_redirects_are_not_followed_and_the_key_stays_home(stub, make_client, caplog):
    from conftest import Stub

    elsewhere = Stub()
    try:
        stub.respond(307, {}, {"Location": elsewhere.url + "/v1/events:batch"})
        client = make_client()
        client.info("a")
        with caplog.at_level(logging.WARNING, logger="lyrelog"):
            assert client.flush(5) is False
        assert elsewhere.requests == []
        assert client.stats()["delivered"] == 0 and client.stats()["discarded"] == 1
        assert any("307" in r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
    finally:
        elsewhere.close()


def test_a_404_warns_once(stub, make_client, caplog):
    stub.respond(404, {"error": "not found"})
    stub.respond(404, {"error": "not found"})
    client = make_client()
    with caplog.at_level(logging.WARNING, logger="lyrelog"):
        client.info("a")
        client.flush(5)
        client.info("b")
        client.flush(5)
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "404" in r.getMessage()]
    assert len(warnings) == 1


def test_repeated_network_failures_warn_once(caplog):
    dead = lyrelog.Client(endpoint="http://127.0.0.1:9", api_key="k", flush_interval=60)
    dead.info("x")
    with caplog.at_level(logging.WARNING, logger="lyrelog"):
        for _ in range(5):
            dead.flush(2)
    dead.shutdown(timeout=1)
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "127.0.0.1:9" in r.getMessage()]
    assert len(warnings) == 1


def test_the_api_has_no_catch_all_keyword_arguments():
    import inspect

    names = ("log", "trace", "debug", "info", "notice", "warning", "error", "critical", "capture_error",
             "capture_exception")
    functions = [getattr(lyrelog, name) for name in names]
    functions += [getattr(lyrelog.Client, name) for name in names]
    functions += [getattr(lyrelog.Logger, name) for name in names]
    for function in functions:
        parameters = inspect.signature(function).parameters
        kinds = {p.kind for p in parameters.values()}
        assert inspect.Parameter.VAR_KEYWORD not in kinds, function
        assert "user_id" in parameters, function
        for name in ("category", "attrs", "payload", "message", "level"):
            if name in parameters and not (name in ("message", "level") and "exc" not in parameters):
                assert parameters[name].kind is inspect.Parameter.KEYWORD_ONLY, (function, name)


def test_keyword_options_reach_the_client(stub):
    lyrelog.init(endpoint=stub.url, api_key="k", flush_interval=60)
    lyrelog.info("a", user_id="u_a")
    lyrelog.logger("jobs").info("b", user_id="u_b")
    try:
        raise KeyError("k")
    except KeyError:
        lyrelog.capture_error(message="sync_failed", level="critical", user_id="u_c")
        lyrelog.logger("jobs").capture_error(message="job_failed", level="warning")
    assert lyrelog.flush(5)
    assert [(e["message"], e["level"], e.get("user_id")) for e in stub.events] == [
        ("a", "info", "u_a"), ("b", "info", "u_b"), ("sync_failed", "critical", "u_c"),
        ("job_failed", "warning", None),
    ]


def test_init_needs_only_the_key():
    client = lyrelog.init(api_key="lyr_ing_x")
    assert client._endpoint == "https://ingest.lyrelog.com/v1/events:batch"
    client._closed = True


def test_flush_says_whether_the_events_were_delivered(stub, make_client):
    client = make_client()
    assert client.flush() is True  # nothing to send
    client.info("a")
    assert client.flush() is True
    stub.respond(503, {"error": "down"})
    client.info("b")
    assert client.flush() is False  # still waiting for a retry
    assert client.flush() is True  # the retry went through
    stub.respond(400, {"error": "bad"})
    client.info("c")
    assert client.flush() is False  # dropped, not delivered


def test_flush_has_a_default_timeout():
    import inspect

    assert inspect.signature(lyrelog.Client.flush).parameters["timeout"].default == 5.0
    assert inspect.signature(lyrelog.flush).parameters["timeout"].default == 5.0


def test_flush_returns_at_its_timeout_when_the_network_hangs(stub):
    class Slow:
        def send(self, url, body, headers, timeout):
            time.sleep(3)
            return 200, b'{"accepted":1,"rejected":[]}', None

    client = lyrelog.Client(endpoint=stub.url, api_key="k", flush_interval=60, transport=Slow())
    client.info("a")
    started = time.monotonic()
    assert client.flush(0.3) is False
    assert time.monotonic() - started < 1.5
    client.shutdown(timeout=5)


def test_an_event_over_the_ingests_1_mib_per_line_is_dropped_not_sent():
    from lyrelog._client import Client

    sent = []

    class Transport:
        def send(self, url, body, headers, timeout):
            import gzip, json
            lines = gzip.decompress(body).splitlines()
            for raw in lines:
                sent.append(json.loads(raw)["message"])
            return 200, json.dumps({"accepted": len(lines), "rejected": []}).encode(), None

    client = Client(endpoint="https://ingest.test", api_key="lyr_ing_test", transport=Transport())
    big = {f"k{i}": ["x" * 1000] * 64 for i in range(60)}
    client.info("huge", attrs=big)
    client.info("small")
    client.flush(timeout=5)
    client.shutdown()
    assert "huge" not in sent and "small" in sent
    assert client.stats()["dropped"] >= 1


QUOTA_ERROR = ("this organization's free plan includes 1000000 events a month and they're used: "
               "upgrade it, or events resume on 1 December")


def _quota_429(stub, retry_after="1"):
    stub.respond(429, {"error": QUOTA_ERROR, "plan": "free", "limit": 1000000, "used": 1000123},
                 {"Retry-After": retry_after, "Content-Type": "application/json"})


def test_the_monthly_quota_stop_warns_once_until_a_batch_is_accepted(stub, make_client, caplog):
    client = make_client(max_queue=1234, max_queue_bytes=2 * 1024 * 1024)

    def quota_warnings():
        return [r for r in caplog.records
                if r.levelno == logging.WARNING and QUOTA_ERROR in r.getMessage()]

    with caplog.at_level(logging.WARNING, logger="lyrelog"):
        # A rate-limit 429 (no "plan") and a 429 that isn't JSON: retried, no quota warning.
        stub.respond(429, {"error": "rate limit for this project"}, {"Retry-After": "1"})
        stub.respond(429, b"slow down", {"Retry-After": "1"})
        client.info("a")
        client.flush(5)
        client._retry_until = 0.0  # as if each Retry-After had passed
        client.flush(5)
        assert quota_warnings() == []

        # Two quota stops in a row: one warning; the batch is kept and retried as before.
        _quota_429(stub, "3600")
        client._retry_until = 0.0
        client.flush(5)
        assert client._pending is not None
        assert 3590 < client._not_before - time.monotonic() <= 3600
        assert 3590 < client._retry_until - time.monotonic() <= 3600
        client._retry_until = 0.0  # as if the hour had passed
        _quota_429(stub)
        client.flush(5)
        assert len(quota_warnings()) == 1
        # Accepted: the same bytes went out, and the next quota stop warns again.
        client._retry_until = 0.0
        client.flush(5)
        assert client._pending is None
        assert len({r["raw"] for r in stub.requests}) == 1
        _quota_429(stub)
        client.info("b")
        client.flush(5)
        assert len(quota_warnings()) == 2

    message = quota_warnings()[0].getMessage()
    assert message == (
        f"Lyrelog: {QUOTA_ERROR}. Events are kept and sent when sending resumes; how many wait "
        "depends on the platform (here: in memory, up to 1234 events and 2 MiB, oldest dropped "
        "first; lost when the process ends).")
    assert "lyr_ing_test" not in caplog.text
    assert not any(r.levelno == logging.WARNING and "attempts to send" in r.getMessage()
                   for r in caplog.records)


def test_flush_and_shutdown_honour_the_servers_retry_after(stub, make_client):
    # A serverless handler flushes after every invocation: while the server asked to wait
    # (503 or 429 with Retry-After), a flush must not send again, or every invocation would
    # hit an ingest that is already overloaded or over quota.
    stub.respond(503, {"error": "busy"}, {"Retry-After": "120"})
    client = make_client()
    client.info("a")
    assert client.flush(5) is False
    assert client.flush(5) is False
    client.info("b")
    assert client.flush(5) is False
    client.shutdown(timeout=2)
    assert len(stub.requests) == 1


def test_a_user_or_category_that_cannot_be_printed_keeps_the_event(stub, make_client):
    class Boom:
        def __str__(self):
            raise RuntimeError("no")

    client = make_client()
    client.info("bad_user", user_id=Boom())
    client.info("bad_category", category=Boom())
    assert client.flush(5)
    assert [e["message"] for e in stub.events] == ["bad_user", "bad_category"]
    assert "user_id" not in stub.events[0] and "category" not in stub.events[1]
    assert client.stats()["failed"] == 0


def test_before_send_cannot_make_a_line_the_ingest_rejects(stub, make_client):
    from datetime import datetime, timezone

    edits = iter([
        {"event_id": "not-a-uuid", "ts": "2026-11-21 10:00:00"},  # both refused by the ingest
        {"event_id": 42, "ts": "yesterday"},
        {"ts": datetime(2026, 11, 21, 10, 0, 0, 123000, tzinfo=timezone.utc)},
    ])
    client = make_client(before_send=lambda event: {**event, **next(edits)})
    for name in ("a", "b", "c"):
        client.info(name)
    assert client.flush(5)
    import re
    uuid = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    rfc3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")
    for event in stub.events:
        assert uuid.match(event["event_id"]), event["event_id"]
        assert rfc3339.match(event["ts"]), event["ts"]
    assert stub.events[2]["ts"] == "2026-11-21T10:00:00.123Z"


def test_an_unusable_endpoint_is_not_logged_with_its_credentials(caplog):
    with caplog.at_level(logging.DEBUG, logger="lyrelog"):
        client = lyrelog.Client(endpoint="https://lyr_srv_secret123@ingest.lyrelog.com/?key=lyr_srv_secret456",
                                api_key="lyr_srv_key")
    assert client.stats()["disabled"] is True
    assert caplog.records  # still warned
    assert "lyr_srv_secret" not in caplog.text
    assert "ingest.lyrelog.com" in caplog.text


def test_a_key_read_from_a_file_with_its_newline_works_and_is_never_logged(stub, caplog):
    # Secret files and env vars often end in a newline. http.client refuses that header and its
    # ValueError quotes the whole value: the key must neither break sending nor reach the log.
    client = lyrelog.Client(endpoint=stub.url, api_key="lyr_srv_secretkey\n", flush_interval=60)
    client.info("a")
    assert client.flush(5)
    assert stub.requests[0]["headers"]["Authorization"] == "Bearer lyr_srv_secretkey"
    client.shutdown()

    broken = lyrelog.Client(endpoint=stub.url, api_key="lyr_srv_secret\nkey", flush_interval=60)
    with caplog.at_level(logging.DEBUG, logger="lyrelog"):
        broken.info("b")
        for _ in range(_client.WARN_AFTER_FAILURES):
            broken._not_before = 0.0
            broken.flush(5)
    assert any("attempts to send" in r.getMessage() for r in caplog.records)
    assert "lyr_srv_secret" not in caplog.text and "nkey" not in caplog.text
    broken.shutdown(timeout=1)


def test_a_key_passed_as_the_endpoint_is_not_logged(caplog):
    # Positional arguments are refused now; a key still pasted into endpoint= is not logged.
    with caplog.at_level(logging.WARNING, logger="lyrelog"):
        client = lyrelog.init(endpoint="lyr_srv_secretkey")
    assert client.stats()["disabled"] is True
    assert any("api_key=" in r.getMessage() for r in caplog.records)
    assert "secretkey" not in caplog.text


def test_options_and_fields_are_keyword_only(stub):
    # A key passed by position can't land in endpoint=, nor attrs in category=.
    with pytest.raises(TypeError):
        lyrelog.Client("https://ingest.lyrelog.com", "lyr_srv_x")  # type: ignore[misc]
    with pytest.raises(TypeError):
        lyrelog.init("lyr_srv_x")  # type: ignore[misc]
    client = lyrelog.Client(endpoint=stub.url, api_key="k", flush_interval=60)
    with pytest.raises(TypeError):
        client.info("m", "billing")  # type: ignore[misc]
    with pytest.raises(TypeError):
        client.logger("billing").info("m", {"a": 1})  # type: ignore[misc]
    with pytest.raises(TypeError):
        client.capture_error(ValueError("x"), "billing")  # type: ignore[misc]
    for function in (lyrelog.init, lyrelog.Client.__init__):
        for parameter in inspect.signature(function).parameters.values():
            if parameter.name != "self":
                assert parameter.kind is inspect.Parameter.KEYWORD_ONLY, (function, parameter.name)
    client.shutdown(timeout=1)


def test_capture_exception_is_a_deprecated_alias_of_capture_error(stub):
    client = lyrelog.init(endpoint=stub.url, api_key="k", flush_interval=60)
    error = ValueError("broken")
    with pytest.warns(DeprecationWarning, match="capture_error"):
        client.capture_exception(error, category="jobs")
    with pytest.warns(DeprecationWarning, match="capture_error"):
        lyrelog.capture_exception(error, category="jobs")
    with pytest.warns(DeprecationWarning, match="capture_error"):
        lyrelog.logger("jobs").capture_exception(error)
    client.capture_error(error, category="jobs")
    assert lyrelog.flush(5)
    shaped = [(e["category"], e["message"], e["level"], e["attrs"]) for e in stub.events]
    assert len(shaped) == 4 and len(set(map(repr, shaped))) == 1
    assert shaped[0][3] == {"error_type": "ValueError", "error_message": "broken"}


def test_capture_exception_never_raises_even_with_warnings_as_errors(stub, make_client):
    client = make_client()
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        client.capture_exception(ValueError("x"))
    assert client.flush(5) and len(stub.events) == 1


def test_set_user_is_per_thread_and_per_task(stub, make_client):
    import asyncio

    client = make_client()
    barrier = threading.Barrier(2)

    def request(user):
        client.set_user(user)
        barrier.wait()  # both users are set before either logs
        client.info("served", attrs={"for": user})

    threads = [threading.Thread(target=request, args=(u,)) for u in ("u_a", "u_b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    async def handle(user):
        client.set_user(user)
        await asyncio.sleep(0.01)  # the other task runs and sets its own user meanwhile
        client.info("served", attrs={"for": user})

    async def main():
        await asyncio.gather(handle("u_c"), handle("u_d"))

    asyncio.run(main())
    client.info("main")  # this thread set no user
    assert client.flush(5)
    for event in stub.events:
        assert event.get("user_id") == event.get("attrs", {}).get("for"), event


def test_set_default_user_is_process_wide_and_set_user_wins(stub, make_client):
    client = make_client()
    client.set_default_user("svc")
    worker = threading.Thread(target=lambda: client.info("from_thread"))
    worker.start()
    worker.join()
    client.set_user("u_1")
    client.info("own")
    client.set_user(None)
    client.info("back_to_default")
    client.set_default_user(None)
    client.info("nobody")
    assert client.flush(5)
    assert [(e["message"], e.get("user_id")) for e in stub.events] == [
        ("from_thread", "svc"), ("own", "u_1"), ("back_to_default", "svc"), ("nobody", None)]


def test_module_set_default_user(stub):
    lyrelog.init(endpoint=stub.url, api_key="k", flush_interval=60)
    lyrelog.set_default_user("svc")
    worker = threading.Thread(target=lambda: lyrelog.info("a"))
    worker.start()
    worker.join()
    assert lyrelog.flush(5)
    assert stub.events[0]["user_id"] == "svc"


def test_levels_are_typed_and_transport_is_public():
    import typing

    assert typing.get_args(lyrelog.Level) == ("trace", "debug", "info", "notice", "warning", "error",
                                              "critical")
    assert typing.get_type_hints(lyrelog.Client.log)["level"] == lyrelog.Level
    assert typing.get_type_hints(lyrelog.info)  # resolvable at runtime
    assert typing.get_type_hints(lyrelog.Client.__init__)["transport"] == typing.Optional[lyrelog.Transport]
    assert {"Level", "Transport", "capture_error", "set_default_user"} <= set(lyrelog.__all__)


@pytest.mark.parametrize("value, expected", [
    (0.01, 1.0), (0, 1.0), (-5, 1.0), (1.5, 1.5), (float("inf"), 3600.0), (1e300, 3600.0),
    (float("nan"), 5.0),
])
def test_flush_interval_is_clamped(value, expected):
    client = lyrelog.Client(endpoint="", api_key="", flush_interval=value)
    assert client._interval == expected
    client.shutdown()


def test_an_infinite_flush_interval_does_not_kill_the_worker(stub):
    client = lyrelog.Client(endpoint=stub.url, api_key="k", flush_interval=float("inf"))
    client.info("a")
    time.sleep(0.3)
    assert client._worker is not None and client._worker.is_alive()
    assert client.flush(5)
    client.shutdown(timeout=2)


def test_a_quota_429_is_not_a_failed_attempt(stub, make_client, caplog):
    client = make_client()
    with caplog.at_level(logging.WARNING, logger="lyrelog"):
        stub.respond(503, {"error": "down"})
        stub.respond(503, {"error": "down"})
        client.info("a")
        client.flush(5)
        client.flush(5)
        assert (client._failures, client._attempt) == (2, 2)
        _quota_429(stub, "120")
        client.flush(5)
        assert (client._failures, client._attempt) == (2, 2)  # unchanged: no backoff, no count
        assert 110 < client._not_before - time.monotonic() <= 120
        assert 110 < client._retry_until - time.monotonic() <= 120
        # Without Retry-After it waits a minute, still not counted.
        client._retry_until = 0.0
        stub.respond(429, {"error": QUOTA_ERROR, "plan": "free"})
        client.flush(5)
        assert (client._failures, client._attempt) == (2, 2)
        assert 50 < client._retry_until - time.monotonic() <= 60
    assert not any("attempts to send" in r.getMessage() for r in caplog.records)
    client._retry_until = 0.0
    assert client.flush(5)


def test_the_network_warning_comes_back_after_a_success(caplog):
    answers = ["fail"] * 3 + ["ok"] + ["fail"] * 3

    class Flaky:
        def send(self, url, body, headers, timeout):
            if answers.pop(0) == "fail":
                raise OSError("connection refused")
            return 200, b'{"accepted":1,"rejected":[]}', None

    client = lyrelog.Client(endpoint="https://ingest.test", api_key="k", flush_interval=60, transport=Flaky())
    with caplog.at_level(logging.WARNING, logger="lyrelog"):
        client.info("a")
        for _ in range(4):  # three failures (one warning), then delivered
            client.flush(5)
        assert client._pending is None
        client.info("b")
        for _ in range(3):
            client.flush(5)
    client.shutdown(timeout=0)
    warnings = [r for r in caplog.records if "attempts to send" in r.getMessage()]
    assert len(warnings) == 2
    assert all(r.name == "lyrelog" for r in warnings)  # the SDK's own channel, never the app's


def test_a_round_that_raises_wakes_the_waiting_flush(stub):
    client = lyrelog.Client(endpoint=stub.url, api_key="k", flush_interval=1)

    def broken(force):
        raise RuntimeError("bug")

    client._round = broken  # type: ignore[method-assign]
    client.info("a")
    started = time.monotonic()
    assert client.flush(3) is False
    assert time.monotonic() - started < 1.0
    del client._round
    client.shutdown(timeout=3)


def test_exit_gives_every_client_one_shared_deadline():
    release = threading.Event()

    class Hanging:
        def send(self, url, body, headers, timeout):
            release.wait(10)
            return 200, b'{"accepted":1,"rejected":[]}', None

    clients = [lyrelog.Client(endpoint="https://ingest.test", api_key="k", flush_interval=60,
                              transport=Hanging()) for _ in range(3)]
    for client in clients:
        client.info("a")
    started = time.monotonic()
    _client._shutdown_all(timeout=1.0)
    elapsed = time.monotonic() - started
    release.set()
    assert elapsed < 1.5, elapsed  # not a second per client
