from __future__ import annotations

import fnmatch
from collections.abc import Mapping, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class PathRule:
    glob: str
    allow: bool


class PathRuleEnforcer:
    """Checks storage paths against ordered published runtime path rules."""

    def __init__(self, rules: Sequence[Mapping[str, object]]) -> None:
        self._rules = [_path_rule(raw) for raw in rules]

    @property
    def enabled(self) -> bool:
        return bool(self._rules)

    def check(self, path: str) -> None:
        if not self._rules:
            return
        normalized = str(path).strip()
        if not normalized:
            raise PermissionError("Path is not allowed")
        for rule in self._rules:
            if fnmatch.fnmatch(normalized, rule.glob):
                if rule.allow:
                    return
                raise PermissionError("Path is not allowed")
        raise PermissionError("Path is not allowed")


def _path_rule(raw: Mapping[str, object]) -> PathRule:
    pattern = str(raw.get("glob") or "").strip()
    if not pattern:
        raise ValueError("Path rule requires glob")
    return PathRule(glob=pattern, allow=bool(raw.get("allow")))
