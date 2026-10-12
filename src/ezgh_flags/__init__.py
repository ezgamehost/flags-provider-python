"""EZGH Feature Flags for OpenFeature: a server provider that evaluates locally with flagd's core."""

from ._version import VERSION
from .evaluator import Evaluator, InvalidSnapshotError, changed_keys
from .provider import DEFAULT_ENDPOINT, EzghProvider

__all__ = ["DEFAULT_ENDPOINT", "VERSION", "Evaluator", "EzghProvider", "InvalidSnapshotError", "changed_keys"]
