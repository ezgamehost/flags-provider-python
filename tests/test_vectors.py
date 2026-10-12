"""Every case in flags-stack's conformance vectors (tests/vectors, synced verbatim by
scripts/sync-vectors.sh) through the provider's Evaluator, as protocol/README.md describes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from openfeature.evaluation_context import EvaluationContext

from ezgh_flags.evaluator import Evaluator

VECTORS = Path(__file__).parent / "vectors"


def same(a: Any, b: Any) -> bool:
    """JSON equality: booleans aren't numbers, and 1 equals 1.0."""
    if isinstance(a, bool) or isinstance(b, bool):
        return type(a) is type(b) and a == b
    if isinstance(a, int | float) and isinstance(b, int | float):
        return a == b
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(same(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b, strict=True))
    return type(a) is type(b) and bool(a == b)


def _load() -> list[Any]:
    params: list[Any] = []
    for path in sorted(VECTORS.glob("*.json")):
        vector = json.loads(path.read_text())
        evaluator = Evaluator(json.dumps(vector["snapshot"]))
        for i, case in enumerate(vector["cases"]):
            params.append(pytest.param(evaluator, case, id=f"{vector['name']}-{i}-{case['flag']}"))
    return params


def test_vectors_present() -> None:
    assert len(list(VECTORS.glob("*.json"))) >= 6


@pytest.mark.parametrize(("evaluator", "case"), _load())
def test_vector(evaluator: Evaluator, case: dict[str, Any]) -> None:
    attributes = dict(case["context"])
    targeting_key = attributes.pop("targetingKey", None)
    result = evaluator.resolve(
        case["type"], case["flag"], case["default"], EvaluationContext(targeting_key, attributes)
    )
    expect = case["expect"]
    assert same(result.value, expect["value"]), (result.value, expect["value"])
    if "errorCode" in expect:
        assert result.error_code is not None
        assert str(result.error_code) == expect["errorCode"]
    else:
        assert result.error_code is None, result.error_message
        assert result.variant == expect["variant"]
        assert str(result.reason) == expect["reason"]
        assert same(result.flag_metadata.get("flagVersion"), expect["flagVersion"])
