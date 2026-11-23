"""Sending one batch and deciding what to do with it. The classifier and backoff are pure."""

from __future__ import annotations

import email.utils
import gzip
import json
import math
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Protocol

SDK_NAME = "lyrelog-python"
SDK_VERSION = "1.0.0"
SDK = f"{SDK_NAME}/{SDK_VERSION}"

DEFAULT_ENDPOINT = "https://ingest.lyrelog.com"
BATCH_PATH = "/v1/events:batch"
MAX_RETRY_AFTER = 3600.0
# The ingest answers a few hundred bytes; a proxy or a wrong endpoint may answer much more.
MAX_RESPONSE_BYTES = 1 << 20
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


def normalize_endpoint(raw: Optional[str]) -> Optional[str]:
    """The ingest's base URL, or None when it can't be used.

    Empty means the hosted ingest. ``https`` is required (``http`` only for localhost, for a
    local server), and a trailing ``/v1`` or ``/v1/events:batch`` is taken off, since the SDK
    adds the path itself.
    """
    text = (raw or "").strip()
    if not text:
        return DEFAULT_ENDPOINT
    try:
        parts = urllib.parse.urlsplit(text)
        host = parts.hostname
        port = parts.port
    except ValueError:
        return None
    scheme = parts.scheme.lower()
    if not host or parts.query or parts.fragment or parts.username or parts.password:
        return None
    if scheme != "https" and not (scheme == "http" and (host in _LOCAL_HOSTS or host.endswith(".localhost"))):
        return None
    path = parts.path.rstrip("/")
    for suffix in (BATCH_PATH, "/v1"):
        if path.endswith(suffix):
            path = path[: -len(suffix)].rstrip("/")
            break
    netloc = f"[{host}]" if ":" in host else host
    if port is not None:
        netloc += f":{port}"
    return f"{scheme}://{netloc}{path}"


@dataclass(frozen=True)
class Outcome:
    """What to do with a batch after a request.

    ``kind`` is one of:
    - ``delivered``: stored; ``rejected`` lines were invalid and are gone for good (never resent).
    - ``retry``: temporary problem; keep the same bytes, try again after ``retry_after`` or a backoff.
    - ``drop``: the batch itself is wrong (400, 413, 415...): resending would fail the same way.
    - ``unauthorized``: unknown, revoked or read-only key; stop sending until the next start.
    """

    kind: str
    status: int = 0
    rejected: tuple[Any, ...] = ()
    retry_after: Optional[float] = None
    accepted: int = 0
    error: str = ""  # a network error, for the warning
    quota: str = ""  # a 429 for the organization's monthly quota: the server's "error" text


def parse_retry_after(value: Optional[str], now: Optional[float] = None) -> Optional[float]:
    """``Retry-After`` is either seconds or an HTTP date."""
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        pass
    else:
        # "inf" or a year would park the queue forever: never wait more than an hour.
        return min(seconds, MAX_RETRY_AFTER) if seconds >= 0 and not math.isnan(seconds) else None
    try:
        moment = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if moment is None:
        return None
    current = time.time() if now is None else now
    return min(MAX_RETRY_AFTER, max(0.0, moment.timestamp() - current))


def classify(status: int, body: bytes, retry_after: Optional[str], now: Optional[float] = None) -> Outcome:
    if 200 <= status < 300:
        # A 2xx we can't read may not mean "stored": retry; the server collapses duplicates.
        try:
            answer = json.loads(body.decode("utf-8"))
            accepted = answer["accepted"]
            rejected = answer.get("rejected") or []
            if not isinstance(accepted, int) or not isinstance(rejected, list):
                raise ValueError
        except Exception:
            return Outcome("retry", status=status)
        return Outcome("delivered", status=status, rejected=tuple(rejected), accepted=max(0, accepted))
    if status in (401, 403):
        return Outcome("unauthorized", status=status)
    if status in (408, 425, 429) or status >= 500:
        quota = quota_error(body) if status == 429 else ""
        return Outcome("retry", status=status, retry_after=parse_retry_after(retry_after, now), quota=quota)
    return Outcome("drop", status=status)  # 3xx (never followed), 400, 404, 413, 415...


def quota_error(body: bytes) -> str:
    """The server's text when a 429 is the monthly quota stop (its JSON has a ``plan``), else "".

    The rate-limit 429 is just ``{"error": ...}``; anything unreadable is a plain 429.
    """
    try:
        answer = json.loads(body.decode("utf-8"))
    except Exception:
        return ""
    if not isinstance(answer, dict) or "plan" not in answer:
        return ""
    error = answer.get("error")
    text = error.strip() if isinstance(error, str) else ""
    return (text or "this organization's monthly event quota is used")[:500]


class Backoff:
    """1 s doubling to 5 min, with jitter, never sooner than the server asked."""

    base = 1.0
    cap = 300.0

    @classmethod
    def delay(
        cls,
        attempt: int,
        retry_after: Optional[float] = None,
        rand: Callable[[float, float], float] = random.uniform,
    ) -> float:
        ceiling: float = min(cls.cap, cls.base * float(2 ** min(max(attempt, 0), 16)))
        # Equal jitter: somewhere in the upper half, so a retry never fires right away.
        jittered = ceiling / 2 + float(rand(0.0, ceiling / 2))
        return max(jittered, min(retry_after or 0.0, MAX_RETRY_AFTER))


class Transport(Protocol):
    """What sends one batch. ``Client(transport=...)`` takes one, for tests or another HTTP stack."""

    def send(self, url: str, body: bytes, headers: Mapping[str, str], timeout: float) -> tuple[int, bytes, Optional[str]]:
        """Returns (status, body, Retry-After header). Raises on a network error."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """A redirect would turn the POST into a GET and carry the Bearer key to the Location host:
    never follow one; the 3xx comes back as the answer."""

    def redirect_request(self, req: urllib.request.Request, fp: Any, code: int, msg: str,
                         headers: Any, newurl: str) -> None:
        return None


class UrllibTransport:
    def __init__(self) -> None:
        self._opener = urllib.request.build_opener(_NoRedirect())

    def send(self, url: str, body: bytes, headers: Mapping[str, str], timeout: float) -> tuple[int, bytes, Optional[str]]:
        request = urllib.request.Request(url, data=body, headers=dict(headers), method="POST")
        try:
            with self._opener.open(request, timeout=timeout) as response:  # noqa: S310 (our endpoint)
                return int(response.status), response.read(MAX_RESPONSE_BYTES), response.headers.get("Retry-After")
        except urllib.error.HTTPError as error:
            try:
                data = error.read(MAX_RESPONSE_BYTES)
            except Exception:
                data = b""
            return error.code, data, error.headers.get("Retry-After") if error.headers else None


def build_request(api_key: str, lines: bytes) -> tuple[bytes, dict[str, str]]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/x-ndjson",
        "User-Agent": SDK,
    }
    try:
        body = gzip.compress(lines, compresslevel=6, mtime=0)
        headers["Content-Encoding"] = "gzip"
    except Exception:
        body = lines
    return body, headers
