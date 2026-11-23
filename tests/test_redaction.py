import logging
from collections.abc import Mapping

import lyrelog
from lyrelog import _prepare as prepare


class Headers(Mapping):
    """Like starlette's or httpx's Headers: a Mapping, not a dict."""

    def __init__(self, items):
        self._items = dict(items)

    def __getitem__(self, key):
        return self._items[key]

    def __iter__(self):
        return iter(self._items)

    def __len__(self):
        return len(self._items)

    def __repr__(self):
        return f"Headers({self._items!r})"


def test_secrets_are_redacted_by_name_in_attrs():
    attrs = prepare.attributes({
        "Authorization": "Bearer abc", "password": "p", "user": {"api_key": "k", "name": "ana"},
        "X-Api-Key": "k2", "csrf_token": "c", "token_count": 5, "max_tokens": 3, "tokens": ["a"],
        "headers": Headers({"Cookie": "sid=1", "Accept": "json"}),
    })
    assert attrs["Authorization"] == attrs["password"] == attrs["X-Api-Key"] == attrs["csrf_token"] == "[redacted]"
    assert attrs["user"] == {"api_key": "[redacted]", "name": "ana"}
    assert attrs["token_count"] == 5 and attrs["max_tokens"] == 3 and attrs["tokens"] == ["a"]
    assert attrs["headers"] == {"Cookie": "[redacted]", "Accept": "json"}


def test_secrets_are_redacted_by_name_in_the_payload():
    out = prepare.payload({
        "request": {"headers": Headers({"authorization": "Bearer x", "set-cookie": "a"}),
                    "pairs": [("Authorization", "Bearer y"), ("Accept", "*/*")]},
        "body": {"refresh_token": "r", "client_secret": "s", "items": [{"password": "p"}]},
    })
    assert "Bearer" not in str(out)
    assert out["request"]["headers"] == {"authorization": "[redacted]", "set-cookie": "[redacted]"}
    assert out["request"]["pairs"] == [["Authorization", "[redacted]"], ["Accept", "*/*"]]
    assert out["body"] == {"refresh_token": "[redacted]", "client_secret": "[redacted]",
                           "items": [{"password": "[redacted]"}]}


def test_redaction_can_be_changed_or_turned_off(stub, make_client):
    plain = make_client(redact_keys=())
    plain.info("a", attrs={"password": "p"})
    extra = make_client(redact_keys=[*lyrelog.DEFAULT_REDACT_KEYS, "ssn"])
    extra.info("b", attrs={"ssn": "1", "password": "p", "keep": 1})
    plain.flush(5)
    extra.flush(5)
    by = {e["message"]: e["attrs"] for e in stub.events}
    assert by["a"] == {"password": "p"}
    assert by["b"] == {"ssn": "[redacted]", "password": "[redacted]", "keep": 1}


def test_before_send_can_edit_or_drop_events(stub, make_client):
    seen = []

    def before_send(event):
        seen.append(event["message"])
        if event["message"] == "noise":
            return None
        if event["message"] == "broken":
            raise RuntimeError("hook bug")
        event["attrs"] = {**event.get("attrs", {}), "region": "eu"}
        if "payload" in event and "traceback" in event["payload"]:
            event["payload"]["traceback"] = "hidden"
        lyrelog.get_client()  # harmless
        client.info("logged inside the hook")  # dropped, never loops
        return event

    client = make_client(before_send=before_send)
    client.info("kept", attrs={"a": 1})
    client.info("noise")
    client.info("broken")
    try:
        raise ValueError("secret in a message")
    except ValueError:
        client.capture_error()
    assert client.flush(5)
    assert [e["message"] for e in stub.events] == ["kept", "exception"]
    assert stub.events[0]["attrs"] == {"a": 1, "region": "eu"}
    assert stub.events[1]["payload"]["traceback"] == "hidden"
    stats = client.stats()
    assert stats["filtered"] == 1 and stats["failed"] == 1
    assert seen == ["kept", "noise", "broken", "exception"]


def test_a_filter_that_rewrites_exc_text_is_respected(stub, make_client):
    client = make_client()

    class Scrub(logging.Filter):
        def filter(self, record):
            record.exc_text = "Traceback scrubbed"
            return True

    handler = lyrelog.LoggingHandler(client=client)
    handler.addFilter(Scrub())
    log = logging.getLogger("scrubbed")
    log.propagate = False
    log.addHandler(handler)
    try:
        try:
            raise ValueError("token=abc")
        except ValueError:
            log.exception("failed")
    finally:
        log.removeHandler(handler)
    client.flush(5)
    [event] = stub.events
    assert event["payload"]["exception"]["traceback"] == "Traceback scrubbed"
    assert event["attrs"]["error_type"] == "ValueError"


def test_header_pairs_are_redacted_in_attrs_too():
    # request.headers.items() / httpx's .raw: a list of (name, value) pairs, as in the payload.
    attrs = prepare.attributes({"headers": [("Authorization", "Bearer abc"), ("Accept", "json")]})
    assert "abc" not in str(attrs)
    assert attrs["headers"] == ['["Authorization","[redacted]"]', '["Accept","json"]']
