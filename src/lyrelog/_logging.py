"""Forwarding the standard library's ``logging`` to Lyrelog."""

from __future__ import annotations

import logging
import threading
from typing import Any, Optional

from . import _prepare as prepare
from ._client import _UNSET, Client, exception_payload
from ._prepare import Level

# Every attribute a plain LogRecord has; anything else came from ``extra=``.
_STANDARD = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime", "taskName"}
_guard = threading.local()
MAX_LOGGED_EXCEPTION_BYTES = 48 * 1024
MAX_STACK_BYTES = 8 * 1024


def level_name(levelno: int) -> Level:
    if levelno >= logging.CRITICAL:
        return "critical"
    if levelno >= logging.ERROR:
        return "error"
    if levelno >= logging.WARNING:
        return "warning"
    if levelno > logging.INFO:
        return "notice"  # custom levels between INFO and WARNING (e.g. 25)
    if levelno >= logging.INFO:
        return "info"
    if levelno >= logging.DEBUG:
        return "debug"
    return "trace"


class LoggingHandler(logging.Handler):
    """A ``logging.Handler`` that sends records to Lyrelog.

    - the logger's name becomes the category (``uvicorn.error``, ``app.billing``),
    - the level maps to the closest Lyrelog level (below DEBUG is ``trace``, between INFO and
      WARNING is ``notice``),
    - the formatted message becomes the message (the template, when there are arguments, goes
      to ``attrs.msg_template`` so you can group by it),
    - ``extra=`` fields become attrs, except ``user_id`` (the event's user) and ``payload``
      (kept as ``payload.data``),
    - the payload also carries ``source`` (file, line, function),
    - ``exc_info`` becomes the payload's ``exception`` (type, message, traceback).

    Records from the SDK itself (the ``lyrelog`` logger and its worker thread) are ignored, so
    it can never loop.
    """

    def __init__(self, level: int = logging.NOTSET, client: Optional[Client] = None) -> None:
        super().__init__(level)
        self._client = client

    def _target(self) -> Optional[Client]:
        if self._client is not None:
            return self._client
        from . import _default_client

        return _default_client()

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(_guard, "active", False):
            return
        # Attached to several loggers on one path (root, uvicorn, uvicorn.error): send once.
        seen = record.__dict__.setdefault("_lyrelog_seen", set())
        if id(self) in seen:
            return
        seen.add(id(self))
        name = record.name or ""
        if name == "lyrelog" or name.startswith("lyrelog."):
            return
        if threading.current_thread().name == "lyrelog-worker":
            return
        client = self._target()
        if client is None:
            return
        _guard.active = True
        try:
            attrs: dict[str, Any] = {}
            payload: dict[str, Any] = {}
            user_id: Any = None
            has_user = False
            data: Any = None
            has_data = False
            for key, value in record.__dict__.items():
                if key in _STANDARD or key.startswith("_"):
                    continue
                if key == "user_id":
                    user_id, has_user = value, True
                elif key == "payload":
                    data, has_data = value, True
                else:
                    attrs[key] = value
            try:
                text = record.getMessage()
            except Exception:
                text = str(record.msg)
            if record.args:
                attrs.setdefault("msg_template", str(record.msg))
            # Budget the 64 KiB payload so the parts that matter survive: the source, then the
            # exception (up to 48 KiB), then the stack (8 KiB, its end), then extra= payload.
            payload["source"] = {"file": record.pathname, "line": record.lineno, "function": record.funcName}
            if record.exc_info and record.exc_info[1] is not None:
                details = exception_payload(record.exc_info[1], MAX_LOGGED_EXCEPTION_BYTES)
                if record.exc_text:  # a Filter (or another handler's Formatter) already wrote it
                    room = MAX_LOGGED_EXCEPTION_BYTES - prepare.json_bytes({**details, "traceback": ""})
                    details["traceback"] = prepare.json_tail(str(record.exc_text), room)
                payload["exception"] = details
                attrs.setdefault("error_type", details["type"])
                attrs.setdefault("error_message", details["message"])
            elif record.exc_text:
                payload["exception"] = {"traceback": prepare.json_tail(str(record.exc_text),
                                                                       MAX_LOGGED_EXCEPTION_BYTES)}
            if record.stack_info:
                payload["stack"] = prepare.json_tail(str(record.stack_info), MAX_STACK_BYTES)
            if has_data:
                room = prepare.MAX_PAYLOAD_BYTES - 1024 - prepare.json_bytes(payload)
                payload["data"] = prepare.payload(data, max(1024, room), redact=client._redact)
            client.log(level_name(record.levelno), text, category=name or None, attrs=attrs, payload=payload,
                       user_id=user_id if has_user else _UNSET)
        except Exception:  # never raise into the code that logged
            pass
        finally:
            _guard.active = False


UVICORN_LOGGERS = ("uvicorn", "uvicorn.error")
_installed: list[tuple[logging.Logger, LoggingHandler]] = []
_install_lock = threading.Lock()


def install_logging(
    level: int = logging.INFO,
    *,
    uvicorn: bool = True,
    client: Optional[Client] = None,
) -> LoggingHandler:
    """Forwards the ``logging`` module to Lyrelog and returns the handler.

    Adds one ``LoggingHandler`` to the root logger and, with ``uvicorn=True``, to ``uvicorn`` and
    ``uvicorn.error``: uvicorn's own logging config sets ``propagate=False`` on ``uvicorn``, so
    the root logger never sees its records (the tracebacks of unhandled errors included). A
    record reaching the handler by two of those loggers is sent once. ``uvicorn.access`` is left
    out: record requests in a middleware instead.

    The root logger is lowered to ``level`` when it is above it, like ``logging.basicConfig``,
    so ``INFO`` records reach the handler. Calling it again replaces the earlier handler.
    uvicorn replaces its loggers' handlers when it applies its config, so call this after that:
    at import of the app module run by ``uvicorn app:app`` (uvicorn configures first) or in the
    app's lifespan startup.
    """
    handler = LoggingHandler(level=level, client=client)
    targets = [logging.getLogger()]
    if uvicorn:
        targets += [logging.getLogger(name) for name in UVICORN_LOGGERS]
    with _install_lock:
        _remove_installed_locked()
        for target in targets:
            target.addHandler(handler)
            _installed.append((target, handler))
    root = logging.getLogger()
    if root.level > level:  # NOTSET (0) already lets everything through
        root.setLevel(level)
    return handler


def uninstall_logging() -> None:
    """Removes what ``install_logging`` added."""
    with _install_lock:
        _remove_installed_locked()


def _remove_installed_locked() -> None:
    for target, handler in _installed:
        target.removeHandler(handler)
    _installed.clear()
