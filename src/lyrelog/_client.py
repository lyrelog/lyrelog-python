"""The client: capture on the caller's thread, one daemon worker that batches and sends."""

from __future__ import annotations

import atexit
import contextvars
import json
import logging
import math
import os
import platform as _platform
import threading
import time
import traceback
import uuid
import warnings
import weakref
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Mapping, Optional

from . import _prepare as prepare
from ._prepare import Level
from ._transport import (
    BATCH_PATH,
    SDK,
    Backoff,
    Outcome,
    Transport,
    UrllibTransport,
    build_request,
    classify,
    normalize_endpoint,
)

MAX_BATCH_EVENTS = 500
MAX_BATCH_BYTES = 512 * 1024
# The ingest refuses one line over 1 MiB; such an event is dropped here and never sent.
MAX_LINE_BYTES = 1024 * 1024
MAX_EXCEPTION_MESSAGE_BYTES = 8 * 1024
MAX_EXCEPTION_BYTES = 60 * 1024  # the whole exception payload, under the 64 KiB payload limit
MAX_TRACEBACK_PART_BYTES = 8 * 1024  # one frame or one exception line inside the traceback
REQUEST_TIMEOUT = 15.0
FLUSH_TIMEOUT = 5.0  # a flush never blocks a caller (an event loop) for minutes
SHUTDOWN_TIMEOUT = 5.0  # at exit, for every client together
FLUSH_INTERVAL = 5.0
MIN_FLUSH_INTERVAL = 1.0
MAX_FLUSH_INTERVAL = 3600.0  # a bigger one (inf) would overflow the worker's wait
QUOTA_WAIT = 60.0  # a quota 429 without Retry-After: the next try, not counted as a failure
MAX_QUEUE_BYTES = 16 * 1024 * 1024
SESSION_TIMEOUT = 30 * 60.0
WARN_AFTER_FAILURES = 3  # consecutive failed attempts before one warning

# The SDK's own diagnostics. LoggingHandler never forwards this logger, so it can't loop.
internal_log = logging.getLogger("lyrelog")

_UNSET: Any = object()

BeforeSend = Callable[[dict[str, Any]], Optional[Mapping[str, Any]]]

# Set while this thread is inside the SDK (and always on the worker). An event logged then (from a
# signal handler, a finalizer, a __str__ that logs) is dropped and counted, never queued: the
# thread may be holding the queue lock halfway through an update.
_inside = threading.local()
_clients: "weakref.WeakSet[Client]" = weakref.WeakSet()


def _size(count: int) -> str:
    """Bytes for a person: 16 MiB, 512 KiB, 100 bytes."""
    for unit, scale in (("GiB", 1024 ** 3), ("MiB", 1024 ** 2), ("KiB", 1024)):
        if count >= scale:
            value = count / scale
            return f"{value:.0f} {unit}" if value == int(value) else f"{value:.1f} {unit}"
    return f"{count} bytes"


def _describe_endpoint(raw: Optional[str]) -> str:
    """An endpoint for a warning, without what may be a credential: user info, query, fragment."""
    text = str(raw or "").strip().split("?")[0].split("#")[0]
    scheme, sep, rest = text.partition("://")
    if not sep:
        scheme, rest = "", text
    return f"{scheme}{sep}{rest.rpartition('@')[2]}"


def _interval(value: Any) -> float:
    """``flush_interval`` in seconds, between 1 s and an hour; anything unusable is 5 s."""
    try:
        seconds = float(value)
    except (TypeError, ValueError, OverflowError):
        return FLUSH_INTERVAL
    if math.isnan(seconds):
        return FLUSH_INTERVAL
    return min(MAX_FLUSH_INTERVAL, max(MIN_FLUSH_INTERVAL, seconds))


def _deprecated(old: str, new: str) -> None:
    try:
        warnings.warn(f"lyrelog: {old} is deprecated, use {new}", DeprecationWarning, stacklevel=3)
    except Exception:  # -W error turns it into one: never raise into the caller
        pass


def _new_id() -> str:
    return str(uuid.uuid4())


def detected_environment() -> str:
    return prepare.name_rule(os.environ.get("LYRELOG_ENVIRONMENT")) or "production"


def build_resource(
    environment: Optional[str],
    platform: Optional[str],
    release: Optional[str],
    app_id: Optional[str],
) -> dict[str, str]:
    values = {
        "app_id": app_id,
        "app_version": release,
        "os_name": _platform.system(),
        "os_version": _platform.release(),
        "device_model": _platform.machine(),
        "sdk": SDK,
        "environment": prepare.name_rule(environment) or detected_environment(),
        "platform": prepare.name_rule(platform) or "python",
    }
    # Unknown keys are rejected by the server; empty ones are simply left out.
    return {key: prepare.resource_value(value) for key, value in values.items() if value}


def exception_payload(exc: BaseException, max_bytes: int = MAX_EXCEPTION_BYTES) -> dict[str, Any]:
    """Type, message and traceback, in at most ``max_bytes`` of JSON.

    Budgeted in bytes, not characters: the message is cut to 8 KB, each part of the traceback
    (a frame, the exception line, a note) to 8 KB, and the traceback keeps its end, where the
    innermost frames are.
    """
    kind = type(exc)
    module = kind.__module__
    name = kind.__qualname__ if module in ("builtins", "__main__") else f"{module}.{kind.__qualname__}"
    try:
        text = str(exc)
    except Exception:
        text = f"<unprintable {kind.__name__}>"
    text = prepare.json_head(text, MAX_EXCEPTION_MESSAGE_BYTES)
    try:
        parts = traceback.format_exception(kind, exc, exc.__traceback__)
    except Exception:
        parts = []
    trace = "".join(_cap_part(part) for part in parts)
    budget = max_bytes - prepare.json_bytes({"type": name, "message": text, "traceback": ""})
    if prepare.json_bytes(trace) > budget:
        marker = "...(truncated)\n"
        trace = marker + prepare.json_tail(trace, budget - prepare.json_bytes(marker))
    return {"type": name, "message": text, "traceback": trace}


def _cap_part(part: str) -> str:
    if len(part) * 4 <= MAX_TRACEBACK_PART_BYTES:  # fast path: can't be over in UTF-8
        return part
    cut = prepare.json_head(part, MAX_TRACEBACK_PART_BYTES)
    return cut if cut == part else cut + "...(truncated)\n"


class Client:
    """A Lyrelog client. Logging never blocks on the network and never raises.

    Most apps use the module-level functions (``lyrelog.init``, ``lyrelog.info``...), which
    drive one default ``Client``. Make your own when you need two projects or a test double.
    """

    def __init__(
        self,
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
        transport: Optional[Transport] = None,
        session_timeout: float = SESSION_TIMEOUT,
        before_send: Optional[BeforeSend] = None,
        redact_keys: Optional[Iterable[str]] = None,
    ) -> None:
        base = normalize_endpoint(endpoint)
        self._endpoint = f"{base}{BATCH_PATH}" if base else ""
        # A key from a secret file or an env var often ends in a newline, which no header takes.
        self._api_key = str(api_key or "").strip()
        self._minimum = prepare.LEVEL_RANK.get(prepare.level(minimum_level), 1)
        self._interval = _interval(flush_interval)
        self._max_queue = max(1, int(max_queue))
        self._max_queue_bytes = max(1, int(max_queue_bytes))
        self._transport: Transport = transport or UrllibTransport()
        self._session_timeout = float(session_timeout)
        self._before_send = before_send
        self._redact = prepare.redact_set(redact_keys)
        self.resource = build_resource(environment, platform, release, app_id)
        # set_user is per context (a thread, an asyncio task); set_default_user for the process.
        self._user: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
            f"lyrelog_user_{id(self):x}", default=None)
        self._default_user: Optional[str] = None
        self._closed = False
        self._init_runtime()
        # Uploader state, touched only by the worker thread.
        self._disabled = not base or not self._api_key
        self._warned: set[str] = set()
        self._failures = 0
        if not base and str(endpoint).strip().startswith("lyr_"):
            internal_log.warning("lyrelog: the endpoint looks like a key (never logged); pass it as "
                                 "api_key=, events will not be sent")
        elif not base:
            internal_log.warning("lyrelog: the endpoint %r can't be used (it needs https://, or http:// "
                                 "for localhost), events will not be sent", _describe_endpoint(endpoint))
        elif not self._api_key:
            internal_log.warning("lyrelog: no api_key, events will not be sent")
        _clients.add(self)

    def _init_runtime(self) -> None:
        """Everything that must be fresh in a forked child."""
        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self._session_lock = threading.RLock()
        self._queue: deque[bytes] = deque()
        self._queued_bytes = 0
        self._waiters: list[threading.Event] = []
        self._worker: Optional[threading.Thread] = None
        self._busy = False  # the worker is in a round (sending), outside the lock
        self._parking = False  # stopping the worker for a fork
        self._install_id = _new_id()
        self._session_id = _new_id()
        self._last_activity: Optional[float] = None
        # (lines, count, events): a batch waiting for a retry; ``events`` leaves out the drop note.
        self._pending: Optional[tuple[bytes, int, int]] = None
        # Positions in the queue's history, so flush() can tell whether its events got through.
        self._enqueued_total = 0
        self._resolved_total = 0  # left the SDK: delivered, dropped or discarded (oldest first)
        self._lost_total = 0  # of those, dropped or discarded
        self._attempt = 0
        self._not_before = 0.0  # the backoff: no timer round before it (flush and shutdown try anyway)
        self._retry_until = 0.0  # the server's Retry-After: no attempt at all before it
        self._stats = {"queued": 0, "dropped": 0, "delivered": 0, "rejected": 0, "discarded": 0, "failed": 0,
                       "reentrant": 0, "filtered": 0}
        self._unreported_drops = 0

    def _after_fork_in_child(self) -> None:
        # The parent still owns (and will send) what was queued before the fork.
        self._init_runtime()

    # --- the API ------------------------------------------------------------------------

    @property
    def install_id(self) -> str:
        return self._install_id

    @property
    def session_id(self) -> str:
        return self._session_id

    def set_user(self, user_id: Optional[str]) -> None:
        """The user of the events logged from now on in this context: this thread, or this
        asyncio task (and the tasks it starts). Other requests a server handles at the same
        time keep their own. ``None`` clears it, back to ``set_default_user``'s."""
        try:
            self._user.set(prepare.identifier(user_id))
        except Exception:
            pass

    def set_default_user(self, user_id: Optional[str]) -> None:
        """The user of every event whose context set none (``set_user``), in every thread: for
        a script or a worker that serves one person. ``None`` clears it."""
        try:
            self._default_user = prepare.identifier(user_id)
        except Exception:
            self._default_user = None

    def _current_user(self) -> Optional[str]:
        user = self._user.get()
        return user if user is not None else self._default_user

    def log(
        self,
        level: Level,
        message: str,
        *,
        category: Optional[str] = None,
        attrs: Optional[Mapping[str, Any]] = None,
        payload: Any = None,
        user_id: Optional[str] = _UNSET,
    ) -> None:
        if getattr(_inside, "active", False):
            self._count("reentrant")
            return
        _inside.active = True
        try:
            name = prepare.level(level)
            if prepare.LEVEL_RANK[name] < self._minimum or self._closed:
                return
            line = self._line(name, message, category, attrs, payload,
                              self._current_user() if user_id is _UNSET else user_id, touch=True)
            if line is not None:
                self._enqueue(line)
        except Exception:  # logging must never break the caller
            self._count("failed")
        finally:
            _inside.active = False

    def trace(self, message: str, *, category: Optional[str] = None, attrs: Optional[Mapping[str, Any]] = None,
              payload: Any = None, user_id: Optional[str] = _UNSET) -> None:
        self.log("trace", message, category=category, attrs=attrs, payload=payload, user_id=user_id)

    def debug(self, message: str, *, category: Optional[str] = None, attrs: Optional[Mapping[str, Any]] = None,
              payload: Any = None, user_id: Optional[str] = _UNSET) -> None:
        self.log("debug", message, category=category, attrs=attrs, payload=payload, user_id=user_id)

    def info(self, message: str, *, category: Optional[str] = None, attrs: Optional[Mapping[str, Any]] = None,
             payload: Any = None, user_id: Optional[str] = _UNSET) -> None:
        self.log("info", message, category=category, attrs=attrs, payload=payload, user_id=user_id)

    def notice(self, message: str, *, category: Optional[str] = None, attrs: Optional[Mapping[str, Any]] = None,
               payload: Any = None, user_id: Optional[str] = _UNSET) -> None:
        self.log("notice", message, category=category, attrs=attrs, payload=payload, user_id=user_id)

    def warning(self, message: str, *, category: Optional[str] = None, attrs: Optional[Mapping[str, Any]] = None,
                payload: Any = None, user_id: Optional[str] = _UNSET) -> None:
        self.log("warning", message, category=category, attrs=attrs, payload=payload, user_id=user_id)

    def error(self, message: str, *, category: Optional[str] = None, attrs: Optional[Mapping[str, Any]] = None,
              payload: Any = None, user_id: Optional[str] = _UNSET) -> None:
        self.log("error", message, category=category, attrs=attrs, payload=payload, user_id=user_id)

    def critical(self, message: str, *, category: Optional[str] = None, attrs: Optional[Mapping[str, Any]] = None,
                 payload: Any = None, user_id: Optional[str] = _UNSET) -> None:
        self.log("critical", message, category=category, attrs=attrs, payload=payload, user_id=user_id)

    def capture_error(
        self,
        exc: Optional[BaseException] = None,
        *,
        category: Optional[str] = None,
        attrs: Optional[Mapping[str, Any]] = None,
        message: str = "exception",
        level: Level = "error",
        user_id: Optional[str] = _UNSET,
    ) -> None:
        """Logs ``exc`` (default: the exception being handled) with its type, message and traceback."""
        try:
            if exc is None:
                import sys

                exc = sys.exc_info()[1]
            if exc is None:
                return
            details = exception_payload(exc)
            merged = dict(attrs or {})
            merged.setdefault("error_type", details["type"])
            merged.setdefault("error_message", details["message"])
            self.log(level, message, category=category, attrs=merged, payload=details, user_id=user_id)
        except Exception:
            pass

    def capture_exception(
        self,
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
        self.capture_error(exc, category=category, attrs=attrs, message=message, level=level, user_id=user_id)

    def logger(self, category: str) -> "Logger":
        return Logger(category, client=self)

    def flush(self, timeout: Optional[float] = FLUSH_TIMEOUT) -> bool:
        """Sends what was logged before this call, waiting at most ``timeout`` seconds.

        True when all of it reached the ingest (a 2xx). False when it is still queued (timeout,
        network down, waiting for a retry, key refused) or was lost (queue full, 4xx, redirect).
        """
        try:
            if self._holds_lock():  # the worker couldn't run until we return
                return False
            with self._lock:
                if self._closed:
                    return not self._queue and self._pending is None
                if self._disabled or threading.current_thread() is self._worker:
                    return False
                if not self._queue and self._pending is None:
                    return True
                if not self._ensure_worker_locked():
                    return False
                done = threading.Event()
                self._waiters.append(done)
                target, lost_before = self._enqueued_total, self._lost_total
                self._cond.notify_all()
            if not done.wait(timeout):
                return False
            with self._lock:
                return self._resolved_total >= target and self._lost_total == lost_before
        except Exception:
            return False

    def shutdown(self, timeout: Optional[float] = SHUTDOWN_TIMEOUT) -> None:
        """Flushes and stops the worker. Later events are ignored. Safe to call twice."""
        try:
            interrupted = self._holds_lock()
            with self._lock:
                if self._closed:
                    return
                self._closed = True
                worker = self._worker
                if worker is None and (self._queue or self._pending) and not self._disabled:
                    worker = self._start_worker_locked()
                self._cond.notify_all()
            # Interrupted (a signal handler) while this thread held the lock: the worker can't take
            # it until we return, so waiting would only burn the timeout.
            if worker is not None and worker is not threading.current_thread() and not interrupted:
                worker.join(timeout)
        except Exception:
            pass

    close = shutdown

    def stats(self) -> dict[str, Any]:
        """Counters for this process: queued now, dropped (queue full), delivered, rejected
        (lines the server refused), discarded (batches dropped after a 4xx or 3xx), failed
        (events that could not be shaped or whose ``before_send`` raised), filtered (dropped by
        ``before_send``), reentrant (logged from inside the SDK, e.g. a signal handler),
        disabled (bad key or endpoint)."""
        with self._lock:
            out: dict[str, Any] = dict(self._stats)
            out["queued"] = len(self._queue) + (self._pending[2] if self._pending else 0)
        out["disabled"] = self._disabled
        return out

    # --- capture ------------------------------------------------------------------------

    def _holds_lock(self) -> bool:
        """True when this thread already holds the lock: re-entered from a signal handler or a
        finalizer halfway through an update."""
        try:
            return bool(self._lock._is_owned())  # type: ignore[attr-defined]
        except Exception:
            return False

    def _count(self, name: str) -> None:
        try:
            with self._lock:
                self._stats[name] += 1
        except Exception:
            pass

    def _current_session(self, now: float, touch: bool) -> str:
        with self._session_lock:
            if touch:
                if self._last_activity is not None and now - self._last_activity > self._session_timeout:
                    self._session_id = _new_id()
                self._last_activity = now
            return self._session_id

    def _event(self, level: str, message: Any, category: Any, attrs: Any, payload: Any,
               user_id: Any, touch: bool) -> dict[str, Any]:
        event: dict[str, Any] = {
            "event_id": _new_id(),
            "ts": prepare.timestamp(datetime.now(timezone.utc)),
            "level": level,
            "message": prepare.message(message),
        }
        cat = prepare.category(category)
        if cat:
            event["category"] = cat
        user = prepare.identifier(user_id)
        if user:
            event["user_id"] = user
        event["install_id"] = self._install_id
        event["session_id"] = self._current_session(time.monotonic(), touch)
        shaped = prepare.attributes(attrs, self._redact)
        if shaped:
            event["attrs"] = shaped
        body = prepare.payload(payload, redact=self._redact)
        if body is not None:
            event["payload"] = body
        event["resource"] = self.resource
        return event

    def _line(self, level: str, message: Any, category: Any, attrs: Any, payload: Any,
              user_id: Any, touch: bool, hook: bool = True) -> Optional[bytes]:
        """One NDJSON line, or None when ``before_send`` dropped the event."""
        event = self._event(level, message, category, attrs, payload, user_id, touch)
        if hook and self._before_send is not None:
            edited = self._run_before_send(event)
            if edited is None:
                return None
            event = edited
        text = json.dumps(event, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        return text.encode("utf-8", "replace")

    def _run_before_send(self, event: dict[str, Any]) -> Optional[dict[str, Any]]:
        hook = self._before_send
        assert hook is not None
        try:
            result = hook(event)
        except Exception:
            self._count("failed")  # fail closed: a broken redaction hook must not leak
            internal_log.debug("lyrelog: before_send raised, event dropped", exc_info=True)
            return None
        if result is None:
            self._count("filtered")
            return None
        if not isinstance(result, Mapping):
            self._count("failed")
            return None
        # The hook may have added anything: fit it to the limits again.
        out: dict[str, Any] = {
            # An id or time the ingest would refuse keeps the SDK's own.
            "event_id": prepare.event_id(result.get("event_id")) or event["event_id"],
            "ts": prepare.event_ts(result.get("ts")) or event["ts"],
            "level": prepare.level(result.get("level", event["level"])),
            "message": prepare.message(result.get("message", event["message"])),
        }
        for key, shape in (("category", prepare.category), ("user_id", prepare.identifier)):
            value = shape(result.get(key))
            if value:
                out[key] = value
        out["install_id"] = event["install_id"]
        out["session_id"] = event["session_id"]
        attrs = prepare.attributes(result.get("attrs"), self._redact)
        if attrs:
            out["attrs"] = attrs
        body = prepare.payload(result.get("payload"), redact=self._redact)
        if body is not None:
            out["payload"] = body
        out["resource"] = event["resource"]
        return out

    def _enqueue(self, line: bytes) -> None:
        with self._lock:
            if self._closed:
                return
            if len(line) > MAX_LINE_BYTES:
                self._enqueued_total += 1
                self._lose_locked(1)
                return
            # Bounded by count and by bytes: the oldest go first.
            while self._queue and (len(self._queue) >= self._max_queue
                                   or self._queued_bytes + len(line) > self._max_queue_bytes):
                oldest = self._queue.popleft()
                self._queued_bytes -= len(oldest)
                self._lose_locked(1)
            self._enqueued_total += 1
            if len(line) > MAX_LINE_BYTES or len(line) > self._max_queue_bytes:  # the ingest refuses it, or it can't fit
                self._lose_locked(1)
                return
            self._queue.append(line)
            self._queued_bytes += len(line)
            if not self._disabled:
                self._ensure_worker_locked()
                if len(self._queue) >= MAX_BATCH_EVENTS or self._queued_bytes >= MAX_BATCH_BYTES:
                    self._cond.notify_all()

    def _lose_locked(self, events: int) -> None:
        """Events pushed out of a full queue."""
        self._stats["dropped"] += events
        self._unreported_drops += events
        self._resolved_total += events
        self._lost_total += events

    # --- the worker ---------------------------------------------------------------------

    def _ensure_worker_locked(self) -> bool:
        if self._parking:  # a fork is about to happen; the parent restarts it afterwards
            return True
        if self._worker is not None and self._worker.is_alive():
            return True
        return self._start_worker_locked() is not None

    def _start_worker_locked(self) -> Optional[threading.Thread]:
        worker = threading.Thread(target=self._run, name="lyrelog-worker", daemon=True)
        try:
            worker.start()
        except RuntimeError:  # interpreter shutting down
            return None
        self._worker = worker
        return worker

    def _park_worker(self) -> None:
        """Before a fork: stop an idle worker, so the fork doesn't copy a process with a thread
        in it (gunicorn --preload forks after the app logged in the master)."""
        with self._lock:
            worker = self._worker
            if worker is None or not worker.is_alive() or self._busy or self._closed:
                return
            self._parking = True
            self._cond.notify_all()
        worker.join(0.5)

    def _unpark_worker(self) -> None:
        with self._lock:
            if not self._parking:
                return
            self._parking = False
            if (self._queue or self._pending or self._waiters) and not self._disabled and not self._closed:
                self._ensure_worker_locked()

    def _wants_round_locked(self) -> bool:
        if self._closed or self._waiters:
            return True
        if self._disabled or (self._pending is not None and time.monotonic() < self._not_before):
            return False
        return len(self._queue) >= MAX_BATCH_EVENTS or self._queued_bytes >= MAX_BATCH_BYTES

    def _run(self) -> None:
        _inside.active = True
        while True:
            try:
                with self._cond:
                    self._busy = False
                    deadline = time.monotonic() + self._interval
                    while not self._wants_round_locked() and not self._parking:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        self._cond.wait(remaining)
                    closing = self._closed
                    if self._parking and not closing and not self._waiters:
                        if self._worker is threading.current_thread():
                            self._worker = None
                        return
                    self._busy = True
                    waiters, self._waiters = self._waiters, []
                try:
                    self._round(force=closing or bool(waiters))
                finally:  # a round that raised must not leave a flush waiting for its timeout
                    for waiter in waiters:
                        waiter.set()
                if closing:
                    with self._lock:
                        for waiter in self._waiters:
                            waiter.set()
                        self._waiters = []
                    return
            except Exception:  # never let the worker die
                internal_log.debug("lyrelog: worker error", exc_info=True)
                time.sleep(self._interval)

    def _drop_note(self) -> Optional[bytes]:
        """The ``lyrelog_events_dropped`` line, built outside the lock (it shapes an event)."""
        with self._lock:
            count, self._unreported_drops = self._unreported_drops, 0
        if not count:
            return None
        return self._line("warning", "lyrelog_events_dropped", "lyrelog",
                          {"count": count, "max_queue": self._max_queue,
                           "max_queue_bytes": self._max_queue_bytes},
                          None, None, touch=False, hook=False)

    def _take_batch_locked(self, note: Optional[bytes] = None) -> Optional[tuple[bytes, int, int]]:
        lines: list[bytes] = []
        size = 0
        if note is not None:
            lines.append(note)
            size += len(note) + 1
        while self._queue and len(lines) < MAX_BATCH_EVENTS:
            line = self._queue[0]
            if lines and size + len(line) + 1 > MAX_BATCH_BYTES:
                break
            self._queue.popleft()
            self._queued_bytes -= len(line)
            lines.append(line)
            size += len(line) + 1
        if not lines:
            return None
        return b"\n".join(lines) + b"\n", len(lines), len(lines) - (1 if note is not None else 0)

    def _round(self, force: bool) -> None:
        """Sends batches oldest first until the queue is empty or a request fails.

        ``force`` (flush, shutdown) ignores the backoff for one attempt, never the server's
        ``Retry-After``.
        """
        while not self._disabled:
            if self._pending is None:
                note = self._drop_note()
                with self._lock:
                    self._pending = self._take_batch_locked(note)
                if self._pending is None:
                    return
            elif time.monotonic() < self._retry_until or (not force and time.monotonic() < self._not_before):
                return
            lines, count, events = self._pending
            outcome = self._send(lines)
            if outcome.kind == "delivered":
                with self._lock:
                    self._pending = None
                    self._resolved_total += events
                    self._stats["delivered"] += min(count, outcome.accepted)
                    self._stats["rejected"] += len(outcome.rejected)
                self._attempt = 0
                self._not_before = 0.0
                self._retry_until = 0.0
                self._failures = 0
                self._warned.discard("unreachable")
                self._warned.discard("quota")
                if outcome.rejected:
                    first = outcome.rejected[0]
                    internal_log.debug("lyrelog: %d line(s) rejected, first: %s", len(outcome.rejected), first)
            elif outcome.kind == "drop":
                with self._lock:
                    self._pending = None
                    self._stats["discarded"] += count
                    self._resolved_total += events
                    self._lost_total += events
                self._warn_drop(outcome.status, count)
            elif outcome.kind == "unauthorized":
                # Keep the data in memory (bounded), but stop sending until the next start.
                self._disabled = True
                internal_log.warning("lyrelog: the ingest key was refused (HTTP %d), sending stopped",
                                     outcome.status)
                return
            elif outcome.quota:
                # The monthly quota: the ingest is up and answered, so it isn't a failed attempt
                # (no backoff, no "unreachable" count). Wait for its Retry-After, warn once
                # until a batch is accepted again.
                wait = outcome.retry_after if outcome.retry_after is not None else QUOTA_WAIT
                self._not_before = time.monotonic() + wait
                self._retry_until = time.monotonic() + wait
                if "quota" not in self._warned:
                    self._warned.add("quota")
                    self._warn_quota(outcome.quota)
                return
            else:
                self._not_before = time.monotonic() + Backoff.delay(self._attempt, outcome.retry_after)
                if outcome.retry_after:  # asked by the server: flush and shutdown wait for it too
                    self._retry_until = time.monotonic() + outcome.retry_after
                self._attempt += 1
                self._failures += 1
                if self._failures >= WARN_AFTER_FAILURES and "unreachable" not in self._warned:
                    self._warned.add("unreachable")
                    reason = outcome.error or f"HTTP {outcome.status}"
                    internal_log.warning("lyrelog: %d attempts to send to %s failed (%s), still retrying",
                                         self._failures, self._endpoint, reason)
                return

    def _warn_quota(self, error: str) -> None:
        internal_log.warning(
            "Lyrelog: %s. Events are kept and sent when sending resumes; how many wait depends on the "
            "platform (here: in memory, up to %d events and %s, oldest dropped first; lost when the "
            "process ends).", error.rstrip("."), self._max_queue, _size(self._max_queue_bytes))

    def _warn_drop(self, status: int, count: int) -> None:
        key = f"http-{status}"
        if key in self._warned:
            internal_log.debug("lyrelog: batch of %d dropped (HTTP %d)", count, status)
            return
        self._warned.add(key)
        if 300 <= status < 400:
            hint = "a redirect, not followed (it would send the key to another host): set the final URL"
        elif status == 404:
            hint = "check the endpoint: it should be the ingest's base URL, like https://ingest.lyrelog.com"
        else:
            hint = "the server refused the batch itself"
        internal_log.warning("lyrelog: %s answered HTTP %d, %d events dropped; %s",
                             self._endpoint, status, count, hint)

    def _send(self, lines: bytes) -> Outcome:
        try:
            body, headers = build_request(self._api_key, lines)
            status, data, retry_after = self._transport.send(self._endpoint, body, headers, REQUEST_TIMEOUT)
            return classify(status, data, retry_after)
        except Exception as error:
            # http.client quotes a header it refuses whole, the key included: never let it out.
            text = f"{type(error).__name__}: {error}"
            key = self._api_key
            if key:  # as is, and as its str or bytes repr shows it
                for form in (key, repr(key)[1:-1], repr(key.encode("utf-8", "replace"))[2:-1]):
                    text = text.replace(form, "[key]")
            return Outcome("retry", error=text[:300])


class Logger:
    """Events for one area of the app, like ``logging.getLogger("checkout")``.

    Without a client it uses the default one at call time, so it can be made at import,
    before ``lyrelog.init``.
    """

    def __init__(self, category: str, client: Optional[Client] = None) -> None:
        self.category = category
        self._client = client

    def _target(self) -> Optional[Client]:
        if self._client is not None:
            return self._client
        from . import _default_client

        return _default_client()

    def log(self, level: Level, message: str, *, attrs: Optional[Mapping[str, Any]] = None, payload: Any = None,
            user_id: Optional[str] = _UNSET) -> None:
        client = self._target()
        if client is not None:
            client.log(level, message, category=self.category, attrs=attrs, payload=payload, user_id=user_id)

    def trace(self, message: str, *, attrs: Optional[Mapping[str, Any]] = None, payload: Any = None,
              user_id: Optional[str] = _UNSET) -> None:
        self.log("trace", message, attrs=attrs, payload=payload, user_id=user_id)

    def debug(self, message: str, *, attrs: Optional[Mapping[str, Any]] = None, payload: Any = None,
              user_id: Optional[str] = _UNSET) -> None:
        self.log("debug", message, attrs=attrs, payload=payload, user_id=user_id)

    def info(self, message: str, *, attrs: Optional[Mapping[str, Any]] = None, payload: Any = None,
             user_id: Optional[str] = _UNSET) -> None:
        self.log("info", message, attrs=attrs, payload=payload, user_id=user_id)

    def notice(self, message: str, *, attrs: Optional[Mapping[str, Any]] = None, payload: Any = None,
               user_id: Optional[str] = _UNSET) -> None:
        self.log("notice", message, attrs=attrs, payload=payload, user_id=user_id)

    def warning(self, message: str, *, attrs: Optional[Mapping[str, Any]] = None, payload: Any = None,
                user_id: Optional[str] = _UNSET) -> None:
        self.log("warning", message, attrs=attrs, payload=payload, user_id=user_id)

    def error(self, message: str, *, attrs: Optional[Mapping[str, Any]] = None, payload: Any = None,
              user_id: Optional[str] = _UNSET) -> None:
        self.log("error", message, attrs=attrs, payload=payload, user_id=user_id)

    def critical(self, message: str, *, attrs: Optional[Mapping[str, Any]] = None, payload: Any = None,
                 user_id: Optional[str] = _UNSET) -> None:
        self.log("critical", message, attrs=attrs, payload=payload, user_id=user_id)

    def capture_error(
        self,
        exc: Optional[BaseException] = None,
        *,
        attrs: Optional[Mapping[str, Any]] = None,
        message: str = "exception",
        level: Level = "error",
        user_id: Optional[str] = _UNSET,
    ) -> None:
        """Logs ``exc`` (default: the exception being handled) in this logger's category."""
        if exc is None:
            import sys

            exc = sys.exc_info()[1]
        client = self._target()
        if client is not None:
            client.capture_error(exc, category=self.category, attrs=attrs, message=message, level=level,
                                 user_id=user_id)

    def capture_exception(
        self,
        exc: Optional[BaseException] = None,
        *,
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
        self.capture_error(exc, attrs=attrs, message=message, level=level, user_id=user_id)


def _before_fork() -> None:
    for client in list(_clients):
        try:
            client._park_worker()
        except Exception:
            pass


def _after_fork_in_parent() -> None:
    for client in list(_clients):
        try:
            client._unpark_worker()
        except Exception:
            pass


def _after_fork_in_child() -> None:
    for client in list(_clients):
        try:
            client._after_fork_in_child()
        except Exception:
            pass


def _shutdown_all(timeout: float = SHUTDOWN_TIMEOUT) -> None:
    """At exit: every client gets one last attempt, all within ``timeout`` together, so a
    process with several clients and the network down still exits in 5 s."""
    deadline = time.monotonic() + timeout
    clients = list(_clients)
    for client in clients:  # all stop taking events and start their last round at once
        client.shutdown(timeout=0)
    for client in clients:
        try:
            worker = client._worker
            if worker is not None and worker is not threading.current_thread():
                worker.join(max(0.0, deadline - time.monotonic()))
        except Exception:
            pass


if hasattr(os, "register_at_fork"):
    os.register_at_fork(before=_before_fork, after_in_parent=_after_fork_in_parent,
                        after_in_child=_after_fork_in_child)
atexit.register(_shutdown_all)
