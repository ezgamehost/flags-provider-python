"""Local evaluation of an EZGH snapshot (flagd flag-definition JSON) with flagd's Python core,
behind EZGH's reason rules (flags-stack protocol/README.md)."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any, Literal

from openfeature.contrib.tools.flagd.core import FlagdCore
from openfeature.evaluation_context import EvaluationContext
from openfeature.exception import ErrorCode, OpenFeatureError
from openfeature.flag_evaluation import FlagResolutionDetails, Reason

__all__ = ["Evaluator", "FlagType", "InvalidSnapshotError", "changed_keys"]

FlagType = Literal["boolean", "string", "integer", "float", "number", "object"]


class InvalidSnapshotError(ValueError):
    """The snapshot isn't flagd flag-definition JSON the core accepts."""


class Evaluator:
    """One immutable snapshot, loaded into its own flagd core.

    A provider builds a new Evaluator for every snapshot and swaps it in whole, so nothing here
    changes after construction.
    """

    def __init__(self, snapshot_json: str | bytes) -> None:
        try:
            data = json.loads(snapshot_json)
        except ValueError as err:
            raise InvalidSnapshotError(f"snapshot isn't JSON: {err}") from err
        if not isinstance(data, dict) or not isinstance(data.get("flags"), dict):
            raise InvalidSnapshotError("snapshot has no flags object")
        metadata = data.get("metadata") or {}
        if not isinstance(metadata, dict):
            raise InvalidSnapshotError("snapshot metadata isn't an object")
        version = metadata.get("snapshotVersion", 0)
        if isinstance(version, bool) or not isinstance(version, int):
            raise InvalidSnapshotError("snapshot metadata.snapshotVersion isn't an integer")
        self._version: int = version
        self._flag_metadata: dict[str, Mapping[str, Any]] = {}
        self._variants: dict[str, Mapping[str, Any]] = {}
        for key, flag in data["flags"].items():
            if not isinstance(flag, dict):
                raise InvalidSnapshotError(f"flag {key!r} isn't an object")
            flag_metadata = flag.get("metadata") or {}
            self._flag_metadata[key] = flag_metadata if isinstance(flag_metadata, dict) else {}
            variants = flag.get("variants") or {}
            self._variants[key] = variants if isinstance(variants, dict) else {}
        self._core = FlagdCore()
        try:
            self._core.set_flags(data)
        except Exception as err:  # the core raises ParseError, KeyError, TypeError, ...
            raise InvalidSnapshotError(f"flagd rejected the snapshot: {err}") from err

    @property
    def version(self) -> int:
        """The snapshot's version (metadata.snapshotVersion)."""
        return self._version

    def flag_versions(self) -> dict[str, int]:
        """Each flag's config version (metadata.flagVersion), by key."""
        out: dict[str, int] = {}
        for key, metadata in self._flag_metadata.items():
            v = metadata.get("flagVersion")
            out[key] = v if isinstance(v, int) and not isinstance(v, bool) else 0
        return out

    def resolve(
        self, flag_type: FlagType, key: str, default: Any, context: EvaluationContext | None = None
    ) -> FlagResolutionDetails[Any]:
        """Evaluates one flag. Errors answer the caller's default with an error code, never raise."""
        resolvers: dict[str, Callable[..., FlagResolutionDetails[Any]]] = {
            "boolean": self._core.resolve_boolean_value,
            "string": self._core.resolve_string_value,
            "integer": self._core.resolve_integer_value,
            "float": self._core.resolve_float_value,
            "number": self._core.resolve_float_value,
            "object": self._core.resolve_object_value,
        }
        resolver = resolvers.get(flag_type)
        if resolver is None:
            return _error(default, ErrorCode.GENERAL, f"unknown flag type {flag_type!r}")
        try:
            result = resolver(key, default, context or EvaluationContext())
        except OpenFeatureError as err:
            return _error(default, err.error_code, err.error_message)
        except Exception as err:  # a core bug must not escape into the caller's code
            return _error(default, ErrorCode.GENERAL, f"evaluation failed: {err}")
        if result.error_code is not None:
            return _error(default, result.error_code, result.error_message)
        # The core's float check lets booleans through (bool is an int in Python) and turns them
        # into 1.0 and 0.0, so look at the variant's JSON value.
        raw = self._variants.get(key, {}).get(result.variant or "")
        if flag_type in ("float", "number") and isinstance(raw, bool):
            return _error(default, ErrorCode.TYPE_MISMATCH, "Expected type float but got bool")
        metadata = self._flag_metadata.get(key, {})
        reason = result.reason
        # PD7: an off config compiles to its off variation with no targeting (STATIC), marked
        # metadata.enabled == false. EZGH reports it as DISABLED.
        if metadata.get("enabled") is False:
            reason = Reason.DISABLED
        flag_metadata: dict[str, int | float | str | bool] = {}
        flag_version = metadata.get("flagVersion")
        if isinstance(flag_version, int) and not isinstance(flag_version, bool):
            flag_metadata["flagVersion"] = flag_version
        return FlagResolutionDetails(
            value=result.value, variant=result.variant, reason=reason, flag_metadata=flag_metadata
        )


def _error(default: Any, code: ErrorCode, message: str | None) -> FlagResolutionDetails[Any]:
    return FlagResolutionDetails(value=default, error_code=code, error_message=message, reason=Reason.ERROR)


def changed_keys(before: Mapping[str, int], after: Mapping[str, int]) -> list[str]:
    """Keys whose flagVersion changed, or that appeared or disappeared, sorted."""
    return sorted(k for k in before.keys() | after.keys() if before.get(k) != after.get(k))
