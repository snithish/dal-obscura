"""Portable bounded passive tasks for plugin API v2."""

from __future__ import annotations

from collections.abc import Mapping
from math import isfinite

MAX_PLUGIN_TASK_BYTES = 16 * 1024 * 1024


def validate_task_payload(value: object) -> None:  # noqa: C901
    """Allow only bounded inert values inside trusted ticket task payloads."""

    nodes = 0
    string_bytes = 0

    def visit(item: object, depth: int) -> None:  # noqa: C901
        nonlocal nodes, string_bytes
        nodes += 1
        if nodes > 1_000_000 or depth > 64:
            raise ValueError("Public plugin task payload is too large")
        if item is None or isinstance(item, (bool, int)):
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
