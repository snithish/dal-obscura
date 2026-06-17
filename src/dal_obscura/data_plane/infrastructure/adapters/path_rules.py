from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class PathRule:
    root: str


class PathRuleEnforcer:
    """Checks storage paths against published allowed storage roots."""

    def __init__(self, rules: Sequence[Mapping[str, object]]) -> None:
        self._rules = [_path_rule(raw) for raw in rules]

    @property
    def enabled(self) -> bool:
        return bool(self._rules)

    def check(self, path: str) -> None:
        if not self._rules:
            return
        normalized = _normalize_path(path)
        if not normalized:
            raise PermissionError("Path is not allowed")
        if any(_path_is_under_root(normalized, rule.root) for rule in self._rules):
            return
        raise PermissionError("Path is not allowed")


def _path_rule(raw: Mapping[str, object]) -> PathRule:
    if "glob" in raw:
        raise ValueError("Path rule glob patterns are no longer supported; use root")
    root = _normalize_path(raw.get("root"))
    if not root:
        raise ValueError("Path rule requires root")
    if any(character in root for character in "*?[]"):
        raise ValueError("Path rule wildcards are not supported")
    return PathRule(root=root)


def _normalize_path(value: object) -> str:
    text = str(value or "").strip()
    if text == "/":
        return text
    return text.rstrip("/")


def _path_is_under_root(path: str, root: str) -> bool:
    return path == root or path.startswith(f"{root}/")
