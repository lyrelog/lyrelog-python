import json
import math
from datetime import datetime, timezone

from lyrelog import _prepare as prepare


def test_truncate_never_splits_a_character():
    text = "é" * 10  # 2 bytes each
    cut = prepare.truncate(text, 5)
    assert cut == "éé"
    assert len(cut.encode()) <= 5
    assert prepare.truncate("abc", 10) == "abc"
    assert prepare.truncate("a\ud800b", 10) == "a?b"  # a lone surrogate can't break the line


def test_message_limits():
    assert prepare.message("") == "(empty)"
    assert len(prepare.message("x" * 5000).encode()) == 2048
    assert prepare.message(42) == "42"


def test_category_and_ids():
    assert prepare.category("") is None
    assert len(prepare.category("c" * 100).encode()) == 64
    assert len(prepare.identifier("u" * 300).encode()) == 128
    assert prepare.identifier(None) is None


def test_name_rule_normalises_environment_and_platform():
    assert prepare.name_rule("Staging EU") == "staging-eu"
    assert prepare.name_rule("--x--") == "x"
    assert prepare.name_rule("") is None
    assert prepare.name_rule("é") is None
    assert len(prepare.name_rule("a" * 50)) == 32


def test_levels():
    assert prepare.level("WARN") == "warning"
    assert prepare.level("nope") == "info"
    assert prepare.level("critical") == "critical"


def test_timestamp_is_rfc3339_utc_millis():
    moment = datetime(2026, 11, 18, 10, 0, 0, 123456, tzinfo=timezone.utc)
    assert prepare.timestamp(moment) == "2026-11-18T10:00:00.123Z"


def test_attributes_rewrite_names_and_values():
    attrs = prepare.attributes({
        "a.b": 1, "_hidden": "x", "s": "y" * 2000, "f": math.nan, "ok": True,
        "tags": ["a", 1, None] + ["z"] * 100, "obj": object.__new__(object),
    })
    assert attrs["a_b"] == 1
    assert attrs["hidden"] == "x"
    assert len(attrs["s"].encode()) == 1024
    assert attrs["f"] is None
    assert attrs["ok"] is True
    assert attrs["tags"][:3] == ["a", "1", "null"] and len(attrs["tags"]) == 64
    assert isinstance(attrs["obj"], str)


def test_attributes_nest_four_levels_then_stringify():
    attrs = prepare.attributes({"l1": {"l2": {"l3": {"l4": {"l5": 1}}}}})
    assert attrs["l1"]["l2"]["l3"]["l4"] == '{"l5":1}'


def test_attributes_cap_leaves_and_count_the_rest():
    attrs = prepare.attributes({f"k{i:03d}": i for i in range(250)})
    assert attrs[prepare.DROPPED_MARKER] == 51
    assert len(attrs) == 200


def test_payload_passes_small_and_summarises_big():
    assert prepare.payload({"a": [1, 2, math.inf]}) == {"a": [1, 2, None]}
    big = prepare.payload({"blob": "x" * 70_000})
    assert big["lyrelog_truncated"] is True and big["bytes"] > 64 * 1024
    assert len(big["preview"].encode()) <= 1024
    assert prepare.payload(None) is None


def test_payload_handles_cycles_and_odd_types():
    loop = []
    loop.append(loop)
    out = prepare.payload({"loop": loop, "when": datetime(2026, 1, 1), "raw": b"\xff", "set": {1}})
    json.dumps(out, allow_nan=False)
    assert out["loop"] == ["<cycle>"]


def test_colliding_attribute_names_keep_one_and_count_the_rest():
    attrs = prepare.attributes({"a.b": 1, "a_b": 2, "_a_b": 3, "x": {"y": 1}, "x_": 0, "c": {"d.e": 1, "d_e": 2}})
    assert attrs["a_b"] == 2  # the name that needed no rewriting wins
    assert attrs["c"] == {"d_e": 2}
    assert attrs[prepare.DROPPED_MARKER] == 3


def test_dict_and_leaf_with_the_same_name():
    attrs = prepare.attributes({"k": 1, "_k": {"a": 1, "b": 2}})
    assert attrs["k"] == 1
    assert attrs[prepare.DROPPED_MARKER] == 2


def test_a_cyclic_attribute_past_the_budget_is_counted_not_fatal():
    cyclic: dict = {}
    cyclic["self"] = cyclic
    values = {f"a{i:03d}": i for i in range(250)}
    values["zzz"] = cyclic
    shaped = prepare.attributes(values)
    assert shaped[prepare.DROPPED_MARKER] >= 51
