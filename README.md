# Lyrelog for Python

Structured logs from your Python services to [Lyrelog](https://lyrelog.com), the hosted log
service. Python 3.11+, standard library only, typed (`py.typed`).

## Install

```bash
uv add lyrelog==0.9.0          # or: pip install lyrelog==0.9.0
```

## Use

```python
import lyrelog

# Once, at startup. The endpoint defaults to the hosted ingest, https://ingest.lyrelog.com.
lyrelog.init(
    api_key="lyr_srv_...",          # a server key: it can append events, never read them
    release="2.0.1",                # goes to resource.app_version
    app_id="orders-api",
)

lyrelog.set_user("u_123")          # optional; this thread's or asyncio task's following events
lyrelog.info("checkout_done", category="billing", attrs={"order_id": "o_42", "latency_ms": 412})
lyrelog.error("payment_declined", category="billing", attrs={"code": 51}, payload={"response": raw})

# One logger per area of the app.
billing = lyrelog.logger("billing")
billing.warning("refund_denied", attrs={"order_id": order.id}, user_id=order.user_id)

try:
    sync()
except Exception:
    lyrelog.capture_error(category="jobs")   # type, message and traceback in the payload

lyrelog.flush()                    # rarely needed: it sends every 5 s and at exit
```

- `message`: a stable event name (`checkout_done`), not prose.
- `category`: the area of the app (`billing`, `auth`, `jobs`); the dashboard filters by it.
- `attrs`: small fields to filter and chart by: ids, durations, codes, flags.
- `payload`: the big stuff you only read when you open one event: responses, traces.
- `user_id=`: on any call, for that event only.

`category`, `attrs` and `payload` are keyword-only, and so is every option of `init` and
`Client`: a key can't end up in `endpoint` by position, and a type checker catches a misspelt
name.

Use a server key, `lyr_srv_...`, from the project's API keys page: it is a secret, so keep it in
the server's environment. Its events are verified. An ingest key, `lyr_ing_...`, works too, but
its events aren't. A key read from a file with its trailing newline works as is.

Levels: `trace`, `debug`, `info`, `notice`, `warning`, `error`, `critical`, typed as
`lyrelog.Level` (a `Literal`), so a type checker catches `"warn"`. `capture_error` defaults to the
exception being handled and logs `exception` at `error` with `attrs.error_type` and
`attrs.error_message` (the same in every Lyrelog SDK); `message=` and `level=` change both.
`capture_exception` is the old name: it still works, with a `DeprecationWarning`.

`lyrelog.Client(endpoint=..., api_key=..., ...)` is the same thing without the module-level
default, for a second project or tests: every function above is a method on it.
`transport=` takes any `lyrelog.Transport`, for tests.

### Users

`set_user` is per context: the thread, or the asyncio task, that calls it (and the tasks it
starts). Requests a server handles at the same time each keep their own, so set it at the start
of a request. A thread from a pool keeps it for its next request, so clear it at the end with
`set_user(None)`. A thread you start doesn't inherit it.

`set_default_user` sets the user for every event whose context set none, in every thread: for a
script or a worker that serves one person. `set_user` wins over it, `set_user(None)` falls back
to it, and `user_id=` on a call wins over both (`user_id=None` sends no user).

## The `logging` module

```python
import logging
import lyrelog

lyrelog.init(api_key=...)
lyrelog.install_logging(level=logging.INFO)   # a LoggingHandler on the root logger (and uvicorn's)

log = logging.getLogger("app.matches")
log.info("match %s closed", match_id, extra={"players": 3, "user_id": uid})
log.exception("push_failed")       # inside an except: the traceback goes in the payload
```

`install_logging` also lowers the root logger to `level` when it is above it, like
`logging.basicConfig(level=logging.INFO)`: without that, the root logger's default `WARNING`
drops `INFO` records before any handler sees them. To wire it by hand instead:

```python
logging.basicConfig(level=logging.INFO)
logging.getLogger().addHandler(lyrelog.LoggingHandler(level=logging.INFO))
```

| `logging` | Lyrelog |
|---|---|
| logger name (`app.matches`, `uvicorn.error`) | `category` |
| `DEBUG` / `INFO` / `WARNING` / `ERROR` / `CRITICAL` | same; below DEBUG is `trace`, between INFO and WARNING is `notice` |
| the formatted message | `message`; with arguments, the template goes to `attrs.msg_template` to group by |
| `extra=` fields | `attrs`, except `user_id` (the event's user) and `payload` (`payload.data`) |
| `exc_info` | `payload.exception` (type, message, traceback) and `attrs.error_type`, `attrs.error_message`; a `record.exc_text` set by a `logging.Filter` replaces the traceback |
| `stack_info` | `payload.stack` (its last 8 KiB) |
| file, line, function | `payload.source` |

The handler never forwards the SDK's own `lyrelog` logger or anything logged from its worker
thread, so it can't loop. A record that reaches it through two loggers is sent once.

## FastAPI (uvicorn)

```python
import logging
import os
import time
from contextlib import asynccontextmanager

import lyrelog
from fastapi import FastAPI, Request

lyrelog.init(
    api_key=os.environ["LYRELOG_KEY"],
    platform="api",                 # instead of "python", to tell it apart from a worker
    release=os.environ.get("GIT_SHA"),
    app_id="orders-api",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # After uvicorn applied its logging config (it replaces its loggers' handlers).
    lyrelog.install_logging(level=logging.INFO)
    yield
    lyrelog.shutdown()


app = FastAPI(lifespan=lifespan)
http = lyrelog.logger("http")


@app.middleware("http")
async def record_request(request: Request, call_next):
    start = time.perf_counter()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    except Exception:
        http.capture_error(attrs={"method": request.method, "path": request.url.path})
        raise
    finally:
        route = request.scope.get("route")   # set by the router once it matched
        http.log(
            "error" if status >= 500 else "warning" if status >= 400 else "info",
            "http_request_served",
            attrs={
                "method": request.method,
                "route": getattr(route, "path", "unmatched"),   # /orders/{order_id}, not the id
                "status": status,
                "duration_ms": round((time.perf_counter() - start) * 1000, 1),
            },
        )
```

Why `install_logging` and not a handler on the root logger: uvicorn's logging config gives the
`uvicorn` logger its own handler and `propagate=False`, so `uvicorn.error` (which carries the
tracebacks of unhandled exceptions) never reaches the root logger. `install_logging` attaches
the handler to the root logger and to `uvicorn` and `uvicorn.error` (`uvicorn=False` to skip
them). It leaves `uvicorn.access` out: the middleware above records each request with its route
template. uvicorn applies its config when it starts, so a handler added before that is removed:
install it in the lifespan (as above) or at import of the module `uvicorn app:app` loads.

The route template keeps ids out of the attribute, so the dashboard can count by route. Nothing
here waits on the network: each `log` copies the event into memory and returns.

With gunicorn or `uvicorn --workers`, each worker process gets its own sender (see Forks below).

## Django

```python
# settings.py
import os

import lyrelog

lyrelog.init(api_key=os.environ.get("LYRELOG_KEY"), platform="web", app_id="shop")

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {"class": "logging.StreamHandler"},
        "lyrelog": {"class": "lyrelog.LoggingHandler", "level": "INFO"},
    },
    # django.request (500s with their tracebacks) propagates up to the root logger.
    "root": {"handlers": ["console", "lyrelog"], "level": "INFO"},
}
```

The handler looks up the default client when a record arrives, so the order of `init` and
`LOGGING` doesn't matter. Record requests with a middleware like the FastAPI one if you want
per-route counts.

## Serverless and short scripts

The worker sends every `flush_interval` from a background thread. On AWS Lambda, Cloud Run jobs
and the like, the process is frozen between invocations and the thread doesn't run, so flush at
the end of each one:

```python
def handler(event, context):
    try:
        return work(event)
    finally:
        lyrelog.flush(timeout=2)   # keep it under the function's remaining time
```

`flush()` returns `True` only when everything logged before the call reached the ingest, and
`False` when something is still queued (timeout, network down, a retry pending, key refused) or
was lost (queue full, a 4xx). It waits at most 5 s by default; each request has a 15 s timeout of
its own, so a short `timeout` returns on time and the send carries on in the background. While
the server asked to wait (`Retry-After`), neither `flush` nor `shutdown` sends.

`flush()` blocks the calling thread. In asyncio code, run it off the event loop:

```python
await asyncio.to_thread(lyrelog.flush)
```

## Configuration

| Option | Default | Meaning |
|---|---|---|
| `endpoint` | `https://ingest.lyrelog.com` | the ingest's base URL. `https://` only (`http://` for `localhost`, to test against a local server); a trailing `/v1` or `/v1/events:batch` is taken off. An unusable one disables sending with a warning. |
| `api_key` | required | the project's server key `lyr_srv_...` (or an ingest key `lyr_ing_...`, whose events aren't verified) |
| `environment` | `LYRELOG_ENVIRONMENT`, else `production` | `production`, `staging`, `development`, or any name: lowercase letters, digits, `-` and `_`, up to 32 (normalised for you) |
| `platform` | `python` | which part of the product sent it: `api`, `worker`... (same rule) |
| `release` | none | `resource.app_version` |
| `app_id` | none | `resource.app_id`: the service name |
| `minimum_level` | `debug` | events below it are dropped before queueing |
| `flush_interval` | `5.0` s | how often the worker sends; between 1 s and an hour (smaller is raised to 1 s, larger or `inf` lowered to an hour) |
| `max_queue` | `10_000` events | oldest dropped beyond it |
| `max_queue_bytes` | 16 MiB | oldest dropped beyond it, whichever cap comes first |
| `before_send` | none | `before_send(event) -> event or None`, see Privacy |
| `redact_keys` | `lyrelog.DEFAULT_REDACT_KEYS` | field names whose values are replaced by `[redacted]`; `()` turns it off |

An empty `api_key` gives a client that accepts events and sends nothing, so a missing
environment variable in development doesn't crash the app.

Every event also carries `os_name` (`platform.system()`), `os_version` (`platform.release()`),
`device_model` (`platform.machine()`), `sdk` (`lyrelog-python/0.9.0`), an `install_id` and a
`session_id` (both per process; the session is new after 30 minutes without an event).

## Privacy

Values of fields named like a secret are replaced by `"[redacted]"` before an event is queued,
in `attrs` and in `payload`, at any depth, in `Mapping` objects such as request headers and in
`(name, value)` header pairs. A name matches when, lowercased and without `-`, `_`, `.` and
spaces, it is or ends with one of `authorization`, `cookie`, `password`, `passwd`, `secret`,
`token`, `apikey`, `privatekey`, `credential(s)`, `accesskey`, `signature`: `X-Api-Key`,
`Set-Cookie` and `csrf_token` match, `token_count` and `max_tokens` don't. Change the list with
`redact_keys=[*lyrelog.DEFAULT_REDACT_KEYS, "ssn"]`.

Redaction goes by field name, so it can't see a secret inside free text: a message, an exception
message or a traceback. For those, `before_send` gets every event (exceptions included, with the
traceback in `payload.traceback`, or `payload.exception.traceback` from `logging`) as a dict on
the caller's thread, and returns it edited, or `None` to drop it:

```python
def scrub(event):
    if event.get("category") == "health":
        return None                       # counted in stats()["filtered"]
    trace = event.get("payload", {}).get("traceback")
    if trace:
        event["payload"]["traceback"] = TOKEN_PATTERN.sub("[redacted]", trace)
    return event

lyrelog.init(api_key=..., before_send=scrub)
```

What it returns is fitted to the limits again (an `event_id` that isn't a UUID or a `ts` that
isn't an RFC 3339 time keeps the SDK's own). If it raises, the event is dropped (counted in
`stats()["failed"]`) rather than sent unredacted. Events logged from inside it are ignored.

## What it does for you

| | |
|---|---|
| Never blocks, never raises | A log call shapes the event, puts one line in memory and returns. Anything odd you pass (objects, NaN, cycles, lone surrogates, huge ints) is turned into something valid. Logging from a signal handler or a finalizer while the SDK is busy on that thread drops that event (`stats()["reentrant"]`) instead of deadlocking. |
| Batches | Up to 500 events or 512 KiB per request, gzip, 15 s timeout. A full batch goes out at once; otherwise every `flush_interval`. |
| Retries | 1 s doubling to 5 min, with jitter, honouring `Retry-After` (at most an hour). The same bytes are resent, so the server collapses duplicates. A response body is read up to 1 MiB. |
| Responses | 2xx: delivered (lines the server rejected are counted, never resent). 400, 404, 413 and other 4xx: the batch is dropped, with a warning the first time for each status. 3xx: never followed (a redirect would carry the key to another host), dropped with a warning. 401/403: sending stops until the next start, events stay in memory. 408, 429, 5xx, network errors: retried; one warning after three failed attempts in a row, then nothing until a batch is accepted again. A 429 for the organization's monthly quota (its JSON body has a `plan`) waits for its `Retry-After` (a minute without one) but isn't a failed attempt: no backoff, not counted toward that warning. It has one warning of its own (the server's text and how many events can wait here), and warns again only after a batch has been accepted. |
| Caps | At most `max_queue` events and `max_queue_bytes` wait; past either the oldest go, and a `lyrelog_events_dropped` warning with the count goes out with the next batch. |
| Exit | `shutdown()` runs at normal interpreter exit (atexit): one last attempt to send everything, up to 5 s for all clients together. atexit does not run after `os._exit()` (multiprocessing's fork children, some job runners), on `SIGTERM` without a handler, or on `SIGKILL`: call `lyrelog.shutdown()` yourself in those exit paths (a worker's exit hook, a `SIGTERM` handler). |
| Threads and forks | Thread-safe. After `os.fork` the child gets a fresh queue, worker, install and session (the parent keeps sending what it had). An idle worker is stopped for the fork and restarted in the parent, so `gunicorn --preload` doesn't fork a live SDK thread. |
| Counters | `client.stats()`: queued, dropped, delivered, rejected, discarded, failed, filtered, reentrant, disabled. |

### Debugging delivery

Problems that lose events (an unusable endpoint, a refused key, a 404, a redirect, an unreachable
ingest) are logged once at `WARNING` on the `lyrelog` logger; with no logging configured Python
prints those to stderr. To see every batch decision:

```python
logging.basicConfig()
logging.getLogger("lyrelog").setLevel(logging.DEBUG)
```

`LoggingHandler` never forwards the `lyrelog` logger, so this can't loop back into Lyrelog.

## Limits

Events are fitted to the server's limits before they are queued, so a line is never rejected for
something the SDK could fix:

| | |
|---|---|
| `message` | 2,048 bytes (cut on a character boundary; empty becomes `(empty)`) |
| `category` | 64 bytes |
| `user_id` | 128 bytes |
| `attrs` | strings 1,024 bytes; numbers (ints beyond 64 bits become strings), booleans, null; lists, tuples and sets stay arrays, with up to 64 items, each turned into a string; nested objects 4 levels (deeper is kept as a JSON string); 200 leaves, the rest counted in `lyrelog_dropped_attrs`; dots in names become `_`, leading `_` removed, and when two names end up the same the one that needed no rewriting is kept and the other counted |
| `payload` | any JSON up to 64 KiB serialised; larger becomes `{"lyrelog_truncated": true, "bytes": N, "preview": "<first 1 KiB>"}` |
| exceptions | budgeted in bytes to stay under the payload limit: the message up to 8 KB, each frame up to 8 KB, and the traceback keeps its end (the innermost frames) |

## Tests

```bash
uv run pytest
for v in 3.11 3.12 3.13 3.14 3.15; do uv run --python $v pytest -q; done
uv run mypy --strict src
LYRELOG_E2E_URL=http://localhost:8090 LYRELOG_E2E_KEY=lyr_ing_... uv run pytest tests/test_e2e.py
```

## Licence

MIT.
