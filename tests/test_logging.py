import json
import logging

import pytest

import lyrelog
from lyrelog._logging import level_name


@pytest.fixture
def py_logger(stub, make_client):
    client = make_client()
    handler = lyrelog.LoggingHandler(level=logging.DEBUG, client=client)
    log = logging.getLogger("app.billing")
    log.setLevel(1)
    log.propagate = False
    log.addHandler(handler)
    yield log, client
    log.removeHandler(handler)


def test_levels_map():
    assert level_name(5) == "trace"
    assert level_name(logging.DEBUG) == "debug"
    assert level_name(logging.INFO) == "info"
    assert level_name(25) == "notice"
    assert level_name(logging.WARNING) == "warning"
    assert level_name(logging.ERROR) == "error"
    assert level_name(logging.CRITICAL) == "critical"


def test_records_become_events(stub, py_logger):
    log, client = py_logger
    log.info("charged %s cents", 499, extra={"order_id": "o_1", "user_id": "u_7", "payload": {"raw": 1}})
    log.log(5, "below debug: dropped by the handler level")
    client.flush(5)
    [event] = stub.events
    assert event["category"] == "app.billing"
    assert event["level"] == "info"
    assert event["message"] == "charged 499 cents"
    assert event["user_id"] == "u_7"
    assert event["attrs"] == {"order_id": "o_1", "msg_template": "charged %s cents"}
    assert event["payload"]["data"] == {"raw": 1}
    assert event["payload"]["source"]["function"] == "test_records_become_events"


def test_exc_info_goes_to_the_payload(stub, py_logger):
    log, client = py_logger
    try:
        {}["missing"]
    except KeyError:
        log.exception("lookup_failed")
    client.flush(5)
    [event] = stub.events
    assert event["level"] == "error"
    assert event["attrs"]["error_type"] == "KeyError"
    assert "KeyError" in event["payload"]["exception"]["traceback"]


def test_the_sdk_never_forwards_its_own_logging(stub, make_client):
    client = make_client()
    handler = lyrelog.LoggingHandler(client=client)
    root = logging.getLogger()
    root.addHandler(handler)
    old = root.level
    root.setLevel(logging.DEBUG)
    try:
        logging.getLogger("lyrelog").warning("internal")
        stub.respond(200, {"accepted": 0, "rejected": [{"line": 1, "error": "bad"}]})
        logging.getLogger("other").info("outside")
        client.flush(5)  # the worker logs the rejection at debug: must not loop back
        client.flush(5)
    finally:
        root.removeHandler(handler)
        root.setLevel(old)
    assert [e["message"] for e in stub.events] == ["outside"]
    assert len(stub.requests) == 1


def test_handler_follows_the_default_client(stub):
    handler = lyrelog.LoggingHandler()
    log = logging.getLogger("svc")
    log.propagate = False
    log.addHandler(handler)
    try:
        log.warning("before init")  # no client: ignored quietly
        lyrelog.init(endpoint=stub.url, api_key="k", flush_interval=60)
        log.warning("after init")
        lyrelog.flush(5)
    finally:
        log.removeHandler(handler)
    assert [e["message"] for e in stub.events] == ["after init"]


def test_a_long_exception_from_logging_keeps_its_traceback(stub, py_logger):
    log, client = py_logger
    try:
        raise ValueError("x" * 200_000)
    except ValueError:
        log.exception("boom", stack_info=True, extra={"payload": {"blob": "y" * 30_000}})
    client.flush(5)
    [event] = stub.events
    payload = event["payload"]
    assert "lyrelog_truncated" not in payload
    assert "raise ValueError" in payload["exception"]["traceback"]
    assert len(payload["exception"]["message"].encode()) <= 8 * 1024
    assert payload["source"]["function"] == "test_a_long_exception_from_logging_keeps_its_traceback"
    assert len(json.dumps(payload, ensure_ascii=False).encode()) < 64 * 1024


# uvicorn 0.38's LOGGING_CONFIG: "uvicorn" has its own handler and propagate=False, so a
# handler on the root logger never sees uvicorn.error (the tracebacks of unhandled errors).
UVICORN_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"default": {"class": "logging.NullHandler"}, "access": {"class": "logging.NullHandler"}},
    "loggers": {
        "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "uvicorn.error": {"level": "INFO"},
        "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
    },
}


@pytest.fixture
def clean_logging():
    import logging.config

    names = ["", "uvicorn", "uvicorn.error", "uvicorn.access", "app.x"]
    saved = {n: (logging.getLogger(n).handlers[:], logging.getLogger(n).level, logging.getLogger(n).propagate)
             for n in names}
    logging.config.dictConfig(UVICORN_CONFIG)
    yield
    for n, (handlers, level, propagate) in saved.items():
        log = logging.getLogger(n)
        log.handlers[:] = handlers
        log.setLevel(level)
        log.propagate = propagate


def test_install_logging_reaches_uvicorn_error_once(stub, make_client, clean_logging):
    client = make_client()
    lyrelog.install_logging(client=client)
    lyrelog.install_logging(client=client)  # twice: still one handler per logger
    logging.getLogger("uvicorn.error").error("Exception in ASGI application")
    logging.getLogger("uvicorn.access").info("GET / 200")  # the middleware records requests
    logging.getLogger("app.x").info("from the app")
    logging.getLogger("app.x").debug("below INFO")
    client.flush(5)
    assert sorted((e["category"], e["message"]) for e in stub.events) == [
        ("app.x", "from the app"), ("uvicorn.error", "Exception in ASGI application"),
    ]


def test_install_logging_without_uvicorn_only_touches_root(stub, make_client, clean_logging):
    client = make_client()
    handler = lyrelog.install_logging(level=logging.WARNING, uvicorn=False, client=client)
    assert handler in logging.getLogger().handlers
    assert handler not in logging.getLogger("uvicorn").handlers
    lyrelog.uninstall_logging()
    assert handler not in logging.getLogger().handlers
