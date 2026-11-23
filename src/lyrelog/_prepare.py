"""Fitting an event inside the ingest's limits before it is queued.

Mirrors lyrelog-swift's EventPreparation (and lyrelog's internal/events/events.go): never drop an
event for its shape. Long strings are cut on a character boundary, extra attributes are dropped
and counted in ``lyrelog_dropped_attrs``, an oversized payload is replaced by a short summary.
Pure functions.
"""

from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from collections.abc import Iterable, Mapping
from typing import Any, Literal, get_args

MAX_MESSAGE_BYTES = 2048
MAX_CATEGORY_BYTES = 64
MAX_ID_BYTES = 128
MAX_RESOURCE_BYTES = 64
MAX_ATTRIBUTES = 200
MAX_ATTRIBUTE_STRING_BYTES = 1024
MAX_ARRAY_ITEMS = 64
MAX_DEPTH = 4
MAX_PAYLOAD_BYTES = 64 * 1024
MAX_NAME_BYTES = 128
DROPPED_MARKER = "lyrelog_dropped_attrs"

Level = Literal["trace", "debug", "info", "notice", "warning", "error", "critical"]
LEVELS: tuple[Level, ...] = get_args(Level)
LEVEL_RANK: dict[str, int] = {name: rank for rank, name in enumerate(LEVELS)}

_NAME_RULE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")

# --- redaction ----------------------------------------------------------------------------

# A field is redacted when its name, lowercased and without "-", "_", "." or spaces, is one of
# these or ends with one: "X-Api-Key", "csrf_token" and "Set-Cookie" match; "token_count" and
# "max_tokens" don't.
DEFAULT_REDACT_KEYS: tuple[str, ...] = (
    "authorization", "cookie", "password", "passwd", "secret", "token", "apikey", "privatekey",
    "credential", "credentials", "accesskey", "signature",
)
REDACTED = "[redacted]"
_SEPARATORS = re.compile(r"[-_. ]")


def _normal(name: str) -> str:
    return _SEPARATORS.sub("", name.lower())


def redact_set(keys: Iterable[str] | None) -> frozenset[str]:
    """The normalised names to redact; None means the defaults, an empty list turns it off."""
    chosen = DEFAULT_REDACT_KEYS if keys is None else keys
    return frozenset(n for n in (_normal(str(k)) for k in chosen) if n)


DEFAULT_REDACT = redact_set(None)


def is_secret(name: Any, redact: frozenset[str]) -> bool:
    if not redact or not isinstance(name, str):
        return False
    normal = _normal(name)
    return any(normal.endswith(key) for key in redact)


def truncate(text: str, max_bytes: int) -> str:
    """Cut to at most ``max_bytes`` of UTF-8 without splitting a character."""
    data = text.encode("utf-8", "replace")  # a lone surrogate becomes "?" instead of failing
    if len(data) <= max_bytes:
        return data.decode("utf-8")
    return data[:max_bytes].decode("utf-8", "ignore")


def clean(text: str) -> str:
    """``text`` as valid UTF-8: a lone surrogate (which makes ``json.dumps(...).encode()`` fail)
    becomes ``?``."""
    if text.isascii():
        return text
    try:
        text.encode("utf-8")
        return text
    except UnicodeEncodeError:
        return text.encode("utf-8", "replace").decode("utf-8")


MIN_INT = -(2 ** 63)
MAX_INT = 2 ** 64 - 1


def number(value: int) -> int | str:
    """An int JSON and the server can take as a number; anything bigger becomes a string.

    Past 4,300 digits ``json.dumps`` raises (Python's int-to-str limit), so huge ones are written
    in hex, which has no limit.
    """
    if MIN_INT <= value <= MAX_INT:
        return int(value)
    try:
        return str(int(value))
    except ValueError:
        return f"{int(value):#x}"


def message(value: Any) -> str:
    text = value if isinstance(value, str) else _string_form(value)
    cut = truncate(text, MAX_MESSAGE_BYTES)
    return cut if cut else "(empty)"


def _text_or_none(value: Any) -> str | None:
    """``str(value)``, or None when its ``__str__`` raises: the field is left out, the event kept."""
    if value is None or isinstance(value, str):
        return value
    try:
        return str(value)
    except Exception:
        return None


def category(value: Any) -> str | None:
    text = _text_or_none(value)
    return (truncate(text, MAX_CATEGORY_BYTES) or None) if text is not None else None


def identifier(value: Any) -> str | None:
    text = _text_or_none(value)
    return (truncate(text, MAX_ID_BYTES) or None) if text is not None else None


_UUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$")


def event_id(value: Any) -> str | None:
    """``value`` when the ingest takes it as an event id (a UUID string), else None."""
    return value if isinstance(value, str) and _UUID.match(value) else None


def event_ts(value: Any) -> str | None:
    """``value`` as an RFC 3339 timestamp with a zone (a string the ingest takes, or an aware
    datetime), else None."""
    if isinstance(value, datetime):
        return timestamp(value) if value.tzinfo is not None else None
    if not isinstance(value, str) or not _RFC3339.match(value):
        return None
    try:  # the shape is right; the date must exist too (no month 13)
        datetime.fromisoformat(value[:19])
    except ValueError:
        return None
    return value


def resource_value(value: Any) -> str:
    return truncate(str(value), MAX_RESOURCE_BYTES)


def name_rule(raw: str | None) -> str | None:
    """An environment or platform name: lowercase letters, digits, ``-`` and ``_``, up to 32."""
    if raw is None:
        return None
    lowered = "".join(
        c if (c.isascii() and (c.isalnum() or c in "-_")) else "-" for c in str(raw).lower()
    )
    name = lowered.strip("-_")[:32]
    return name if name and _NAME_RULE.match(name) else None


def level(value: Any) -> str:
    text = str(value).lower() if value is not None else "info"
    if text == "warn":
        return "warning"
    if text == "fatal":
        return "critical"
    return text if text in LEVEL_RANK else "info"


def timestamp(moment: datetime) -> str:
    moment = moment.astimezone(timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


# --- attributes ---------------------------------------------------------------------------


def attribute_name(key: Any) -> str:
    """Dots would mean nesting and a leading underscore is reserved, so both are rewritten."""
    name = _string_form(key).replace(".", "_").lstrip("_")
    return truncate(name or "key", MAX_NAME_BYTES) or "key"


def attributes(values: Any, redact: frozenset[str] = DEFAULT_REDACT) -> dict[str, Any]:
    if not isinstance(values, Mapping) or not values:
        return {}
    budget = [MAX_ATTRIBUTES - 1]  # keep room for the dropped marker
    dropped = [0]
    result = _object(values, 1, budget, dropped, redact)
    if dropped[0]:
        result[DROPPED_MARKER] = dropped[0]
    return result


def _object(values: Mapping[Any, Any], depth: int, budget: list[int], dropped: list[int],
            redact: frozenset[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    named = sorted(((attribute_name(key), key) for key in values),
                   key=lambda pair: (pair[0] != _string_form(pair[1]), _string_form(pair[1])))
    for name, key in named:
        value = REDACTED if is_secret(key, redact) else values[key]
        if name in out:
            # "a.b" and "a_b" both become a_b: keep the one that needed no rewriting (else the
            # first in order) and count the rest, never overwrite silently.
            dropped[0] += _leaf_count(value, depth)
            continue
        if isinstance(value, Mapping) and depth < MAX_DEPTH:
            child = _object(value, depth + 1, budget, dropped, redact)
            if child:
                out[name] = child
            continue
        if budget[0] <= 0:
            dropped[0] += _leaf_count(value, depth)
            continue
        budget[0] -= 1
        out[name] = _leaf(value, redact)
    return out


def _leaf(value: Any, redact: frozenset[str] = DEFAULT_REDACT) -> Any:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        return truncate(value, MAX_ATTRIBUTE_STRING_BYTES)
    if isinstance(value, int):
        return number(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (list, tuple, set, frozenset)):
        # Attribute arrays hold strings only.
        items = list(value)[:MAX_ARRAY_ITEMS]
        return [truncate(_string_form(_redacted_pair(item, redact), redact), MAX_ATTRIBUTE_STRING_BYTES)
                for item in items]
    # Deeper than MAX_DEPTH, or an object JSON doesn't know: keep it as a string.
    return truncate(_string_form(value, redact), MAX_ATTRIBUTE_STRING_BYTES)


def _leaf_count(value: Any, depth: int) -> int:
    """How many leaves ``value`` would be at ``depth``; past MAX_DEPTH a mapping is one string
    leaf, so a cyclic one ends."""
    if isinstance(value, Mapping) and depth < MAX_DEPTH:
        return sum(_leaf_count(v, depth + 1) for v in value.values())
    return 1


def _string_form(value: Any, redact: frozenset[str] = DEFAULT_REDACT) -> str:
    if isinstance(value, str):
        return clean(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, int):
        return str(number(value))
    if isinstance(value, (Mapping, list, tuple)):
        try:
            return json.dumps(jsonable(value, redact=redact), ensure_ascii=False, separators=(",", ":"))
        except Exception:
            pass
    return _string_form_safe(value)


# --- payload ------------------------------------------------------------------------------


def jsonable(value: Any, depth: int = 0, seen: set[int] | None = None,
             redact: frozenset[str] = DEFAULT_REDACT) -> Any:
    """Any Python value as something ``json.dumps`` can always write as strict JSON."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        return clean(value)
    if isinstance(value, int):
        return number(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if depth >= 32:
        return _string_form_safe(value)
    if isinstance(value, (Mapping, list, tuple, set, frozenset)):
        seen = seen if seen is not None else set()
        marker = id(value)
        if marker in seen:
            return "<cycle>"
        seen.add(marker)
        try:
            if isinstance(value, Mapping):  # dicts, and Headers-like mappings (never their repr)
                return {_string_form(k): REDACTED if is_secret(k, redact) else jsonable(v, depth + 1, seen, redact)
                        for k, v in value.items()}
            return [_pair(v, depth, seen, redact) for v in value]
        finally:
            seen.discard(marker)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", "replace")
    if isinstance(value, datetime):
        return value.isoformat()
    return _string_form_safe(value)


def _redacted_pair(value: Any, redact: frozenset[str]) -> Any:
    # Header lists are (name, value) tuples: [("Authorization", "Bearer ...")].
    if isinstance(value, tuple) and len(value) == 2 and is_secret(value[0], redact):
        return [clean(value[0]), REDACTED]
    return value


def _pair(value: Any, depth: int, seen: set[int], redact: frozenset[str]) -> Any:
    redacted = _redacted_pair(value, redact)
    return redacted if redacted is not value else jsonable(value, depth + 1, seen, redact)


def _string_form_safe(value: Any) -> str:
    try:
        return clean(str(value))
    except Exception:
        return f"<{type(value).__name__}>"


def json_bytes(value: Any) -> int:
    """How many bytes ``value`` takes on the wire (compact JSON, UTF-8)."""
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8", "replace"))


def json_tail(text: str, max_bytes: int) -> str:
    """The end of ``text``, as long as its JSON string (quotes and escapes included) fits ``max_bytes``."""
    return _json_cut(text, max_bytes, keep_tail=True)


def json_head(text: str, max_bytes: int) -> str:
    """The start of ``text``, as long as its JSON string fits ``max_bytes``."""
    return _json_cut(text, max_bytes, keep_tail=False)


def _json_cut(text: str, max_bytes: int, keep_tail: bool) -> str:
    data = text.encode("utf-8", "replace")
    budget = max(0, max_bytes)
    while True:
        if keep_tail:
            cut = data[-budget:].decode("utf-8", "ignore") if budget else ""
        else:
            cut = data[:budget].decode("utf-8", "ignore")
        size = json_bytes(cut)
        if size <= max_bytes or not cut:
            return cut
        # Escapes (newlines, quotes) made it longer than its bytes: shrink by the excess.
        budget -= max(1, size - max_bytes)


def payload(value: Any, max_bytes: int = MAX_PAYLOAD_BYTES, redact: frozenset[str] = DEFAULT_REDACT) -> Any:
    """The payload as JSON, or a summary ``{"lyrelog_truncated": true, "bytes": N, "preview": ...}``."""
    if value is None:
        return None
    shaped = jsonable(value, redact=redact)
    encoded = json.dumps(shaped, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) <= max_bytes:
        return shaped
    return {
        "lyrelog_truncated": True,
        "bytes": len(encoded),
        "preview": encoded[:1024].decode("utf-8", "ignore"),
    }
