"""Lyrelog for Python: structured logs from your services to Lyrelog, the hosted log service.

    import lyrelog
    lyrelog.init(api_key="lyr_srv_...")  # endpoint defaults to https://ingest.lyrelog.com
    lyrelog.info("checkout_done", category="billing", attrs={"amount_cents": 499})
"""

from __future__ import annotations

import os
import threading
from typing import Any, Iterable, Mapping, Optional

from ._client import (
    _UNSET,
    FLUSH_INTERVAL,
    FLUSH_TIMEOUT,
    MAX_QUEUE_BYTES,
    SHUTDOWN_TIMEOUT,
    BeforeSend,
    Client,
    Logger,
    _deprecated,
)
from ._prepare import DEFAULT_REDACT_KEYS, Level
from ._logging import LoggingHandler, install_logging, uninstall_logging
from ._transport import SDK, SDK_VERSION, Transport

__all__ = [
    "Client",
    "Logger",
    "LoggingHandler",
    "install_logging",
    "uninstall_logging",
    "init",
    "get_client",
    "trace",
    "debug",
    "info",
    "notice",
    "warning",
    "error",
    "critical",
    "log",
    "logger",
    "set_user",
    "set_default_user",
    "capture_error",
    "capture_exception",
    "flush",
    "shutdown",
    "SDK",
    "DEFAULT_REDACT_KEYS",
    "BeforeSend",
    "Level",
    "Transport",
    "__version__",
]

__version__ = SDK_VERSION

_default: Optional[Client] = None
_default_lock = threading.Lock()


def _default_client() -> Optional[Client]:
    return _default


def _fresh_default_lock() -> None:
    # Another thread may have held it at the moment of the fork; in the child it never unlocks.
    global _default_lock
    _default_lock = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_fresh_default_lock)


def init(
    *,
    endpoint: Optional[str] = None,
    api_key: Optional[str] = None,
    environment: Optional[str] = None,
    platform: Optional[str] = None,
    release: Optional[str] = None,
    app_id: Optional[str] = None,
    minimum_level: Level = "debug",
    flush_interval: float = FLUSH_INTERVAL,
    max_queue: int = 10_000,
    max_queue_bytes: int = MAX_QUEUE_BYTES,
    before_send: Optional[BeforeSend] = None,
    redact_keys: Optional[Iterable[str]] = None,
) -> Client:
    """Starts the default client (replacing, and flushing, any earlier one) and returns it."""
    global _default
    client = Client(
        endpoint=endpoint,
        api_key=api_key,
        environment=environment,
        platform=platform,
        release=release,
        app_id=app_id,
        minimum_level=minimum_level,
        flush_interval=flush_interval,
        max_queue=max_queue,
        max_queue_bytes=max_queue_bytes,
        before_send=before_send,
        redact_keys=redact_keys,
    )
    with _default_lock:
        previous, _default = _default, client
    if previous is not None:
        previous.shutdown()
    return client


def get_client() -> Optional[Client]:
    """The default client, or None before ``init``."""
    return _default


def log(level: Level, message: str, *, category: Optional[str] = None,
        attrs: Optional[Mapping[str, Any]] = None, payload: Any = None,
        user_id: Optional[str] = _UNSET) -> None:
    client = _default
    if client is not None:
        client.log(level, message, category=category, attrs=attrs, payload=payload, user_id=user_id)


def trace(message: str, *, category: Optional[str] = None, attrs: Optional[Mapping[str, Any]] = None,
          payload: Any = None, user_id: Optional[str] = _UNSET) -> None:
    log("trace", message, category=category, attrs=attrs, payload=payload, user_id=user_id)


def debug(message: str, *, category: Optional[str] = None, attrs: Optional[Mapping[str, Any]] = None,
          payload: Any = None, user_id: Optional[str] = _UNSET) -> None:
    log("debug", message, category=category, attrs=attrs, payload=payload, user_id=user_id)


def info(message: str, *, category: Optional[str] = None, attrs: Optional[Mapping[str, Any]] = None,
         payload: Any = None, user_id: Optional[str] = _UNSET) -> None:
    log("info", message, category=category, attrs=attrs, payload=payload, user_id=user_id)


def notice(message: str, *, category: Optional[str] = None, attrs: Optional[Mapping[str, Any]] = None,
           payload: Any = None, user_id: Optional[str] = _UNSET) -> None:
    log("notice", message, category=category, attrs=attrs, payload=payload, user_id=user_id)


def warning(message: str, *, category: Optional[str] = None, attrs: Optional[Mapping[str, Any]] = None,
            payload: Any = None, user_id: Optional[str] = _UNSET) -> None:
    log("warning", message, category=category, attrs=attrs, payload=payload, user_id=user_id)


def error(message: str, *, category: Optional[str] = None, attrs: Optional[Mapping[str, Any]] = None,
          payload: Any = None, user_id: Optional[str] = _UNSET) -> None:
    log("error", message, category=category, attrs=attrs, payload=payload, user_id=user_id)


def critical(message: str, *, category: Optional[str] = None, attrs: Optional[Mapping[str, Any]] = None,
             payload: Any = None, user_id: Optional[str] = _UNSET) -> None:
    log("critical", message, category=category, attrs=attrs, payload=payload, user_id=user_id)


def logger(category: str) -> Logger:
    """A logger for one area; it follows the default client, so it can be made before ``init``."""
    return Logger(category)


def set_user(user_id: Optional[str]) -> None:
    """The user of the events logged from now on in this context: this thread, or this asyncio
    task and the tasks it starts. Concurrent requests on a server keep their own. ``None``
    clears it (back to ``set_default_user``'s)."""
    client = _default
    if client is not None:
        client.set_user(user_id)


def set_default_user(user_id: Optional[str]) -> None:
    """The user of every event whose context set none, in every thread: for a script or a
    worker that serves one person. ``None`` clears it."""
    client = _default
    if client is not None:
        client.set_default_user(user_id)


def capture_error(
    exc: Optional[BaseException] = None,
    *,
    category: Optional[str] = None,
    attrs: Optional[Mapping[str, Any]] = None,
    message: str = "exception",
    level: Level = "error",
    user_id: Optional[str] = _UNSET,
) -> None:
    """Logs ``exc`` (default: the exception being handled) with type, message and traceback."""
    if exc is None:
        import sys

        exc = sys.exc_info()[1]
    client = _default
    if client is not None:
        client.capture_error(exc, category=category, attrs=attrs, message=message, level=level, user_id=user_id)


def capture_exception(
    exc: Optional[BaseException] = None,
    *,
    category: Optional[str] = None,
    attrs: Optional[Mapping[str, Any]] = None,
    message: str = "exception",
    level: Level = "error",
    user_id: Optional[str] = _UNSET,
) -> None:
    """Deprecated: ``capture_error``, the name every Lyrelog SDK uses."""
    _deprecated("capture_exception", "capture_error")
    if exc is None:
        import sys

        exc = sys.exc_info()[1]
    capture_error(exc, category=category, attrs=attrs, message=message, level=level, user_id=user_id)


def flush(timeout: Optional[float] = FLUSH_TIMEOUT) -> bool:
    """Sends what was logged so far, waiting at most ``timeout`` seconds. True when all of it
    reached the ingest; False when some is still queued or was lost. It blocks: from asyncio,
    ``await asyncio.to_thread(lyrelog.flush)``."""
    client = _default
    return client.flush(timeout) if client is not None else True


def shutdown(timeout: Optional[float] = SHUTDOWN_TIMEOUT) -> None:
    """Flushes and stops the default client. Also runs at exit."""
    global _default
    with _default_lock:
        client, _default = _default, None
    if client is not None:
        client.shutdown(timeout)
