"""Portable bounded passive tasks for plugin API v2."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from math import isfinite
from types import MappingProxyType
from typing import cast

MAX_PLUGIN_TASK_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class ScanTask:
    """Detached, immutable work description; execution resources stay in the plugin."""

    payload: Mapping[str, object]

    def __post_init__(self) -> None:
        if not isinstance(self.payload, Mapping):
            raise ValueError("Scan task payload must be a JSON object")
        _validate_task_payload(self.payload)
        object.__setattr__(self, "payload", _freeze_json(self.payload))

    def to_json(self) -> dict[str, object]:
        return cast(dict[str, object], _mutable_json(self.payload))

    @classmethod
    def from_json(cls, value: object) -> ScanTask:
        if not isinstance(value, dict):
            raise ValueError("Scan task payload must be a JSON object")
        return cls(cast(Mapping[str, object], value))


def _validate_task_payload(value: object) -> None:  # noqa: C901
    """Allow only bounded inert values inside trusted ticket task payloads."""

    nodes = 0
    string_bytes = 0

    def visit(item: object, depth: int) -> None:  # noqa: C901
        nonlocal nodes, string_bytes
        nodes += 1
        if nodes > 1_000_000 or depth > 64:
            raise ValueError("Public plugin task payload is too large")
        if item is None or isinstance(item, bool):
            return
        if isinstance(item, int):
            if not -(2**63) <= item < 2**64:
                raise ValueError("Scan task integers must fit signed or unsigned 64 bits")
            return
        if isinstance(item, float):
            if not isfinite(item):
                raise ValueError("Public plugin task payload contains a non-finite number")
            return
        if isinstance(item, str):
            string_bytes += len(item.encode("utf-8"))
            if len(item) > 1_048_576 or string_bytes > MAX_PLUGIN_TASK_BYTES:
                raise ValueError("Public plugin task payload contains oversized strings")
            if any(ord(char) < 0x20 or ord(char) == 0x7F for char in item):
                raise ValueError("Public plugin task payload contains control characters")
            return
        if isinstance(item, Mapping):
            if len(item) > 65_536:
                raise ValueError("Public plugin task payload has too many keys")
            for key, child in item.items():
                if not isinstance(key, str) or not key or len(key) > 256:
                    raise ValueError("Public plugin task payload has invalid keys")
                visit(key, depth + 1)
                visit(child, depth + 1)
            return
        if isinstance(item, (list, tuple)):
            if len(item) > 65_536:
                raise ValueError("Public plugin task payload has too many items")
            for child in item:
                visit(child, depth + 1)
            return
        raise ValueError("Public plugin task payload must be inert JSON-like data")

    visit(value, 0)


def _freeze_json(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze_json(child) for key, child in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(child) for child in value)
    return value


def _mutable_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _mutable_json(child) for key, child in value.items()}
    if isinstance(value, tuple):
        return [_mutable_json(child) for child in value]
    return value
