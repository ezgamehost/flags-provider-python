from __future__ import annotations

import json

from openfeature.evaluation_context import EvaluationContext

from ezgh_flags.evaluator import Evaluator, changed_keys

SNAPSHOT = {
    "$schema": "https://flagd.dev/schema/v0/flags.json",
    "metadata": {"environmentId": "e", "snapshotVersion": 4, "compiledAt": "2026-10-12T09:00:00Z"},
    "flags": {
        "beta": {
            "state": "ENABLED",
            "variants": {"true": True, "false": False},
            "defaultVariant": "false",
            "metadata": {"flagVersion": 2, "enabled": True},
            "targeting": {"if": [{"in": [{"var": "targetingKey"}, ["user-1"]]}, "true"]},
        }
    },
}


def test_targeting_key_attribute_promoted() -> None:
    ev = Evaluator(json.dumps(SNAPSHOT))
    r = ev.resolve("boolean", "beta", False, EvaluationContext(attributes={"targetingKey": "user-1"}))
    assert (r.value, r.reason) == (True, "TARGETING_MATCH")
    # An explicit targeting_key wins.
    r = ev.resolve("boolean", "beta", False, EvaluationContext("user-2", {"targetingKey": "user-1"}))
    assert (r.value, r.reason) == (False, "DEFAULT")
    # A non-string attribute isn't a targeting key.
    r = ev.resolve("boolean", "beta", False, EvaluationContext(attributes={"targetingKey": 1}))
    assert r.value is False


def test_version_and_flag_versions() -> None:
    ev = Evaluator(json.dumps(SNAPSHOT))
    assert ev.version == 4
    assert ev.flag_versions() == {"beta": 2}
    assert changed_keys({"a": 1, "b": 2}, {"b": 3, "c": 1}) == ["a", "b", "c"]


def with_segment(version: int, members: list[str]) -> str:
    snapshot = json.loads(json.dumps(SNAPSHOT))
    snapshot["metadata"]["snapshotVersion"] = version
    snapshot["$evaluators"] = {"testers": {"in": [{"var": "targetingKey"}, members]}, "unused": {"==": [1, 1]}}
    snapshot["flags"]["uses-segment"] = {
        "state": "ENABLED",
        "variants": {"true": True, "false": False},
        "defaultVariant": "false",
        "metadata": {"flagVersion": 1, "enabled": True},
        "targeting": {"if": [{"$ref": "testers"}, "true"]},
    }
    return json.dumps(snapshot)


def test_segment_change_names_the_flags_using_it() -> None:
    a = Evaluator(with_segment(1, ["user-1"]))
    b = Evaluator(with_segment(2, ["user-1", "user-2"]))
    assert changed_keys(a.fingerprints(), b.fingerprints()) == ["uses-segment"]
    # A new snapshot version alone changes nothing.
    c = Evaluator(with_segment(3, ["user-1", "user-2"]))
    assert changed_keys(b.fingerprints(), c.fingerprints()) == []
    assert b.resolve("boolean", "uses-segment", False, EvaluationContext("user-2")).value is True


def test_inlined_segment_change_names_the_flag() -> None:
    def snapshot(version: int, members: list[str]) -> str:
        s = json.loads(json.dumps(SNAPSHOT))
        s["metadata"]["snapshotVersion"] = version
        s["flags"]["beta"]["targeting"] = {"if": [{"in": [{"var": "targetingKey"}, members]}, "true"]}
        return json.dumps(s)

    a, b = Evaluator(snapshot(1, ["user-1"])), Evaluator(snapshot(2, ["user-1", "user-2"]))
    assert "$evaluators" not in json.loads(snapshot(1, []))
    assert changed_keys(a.fingerprints(), b.fingerprints()) == ["beta"]
