"""Storage path allowlist enforcement.

Example:
    ```python
    enforcer = PathRuleEnforcer([{"root": "s3://warehouse/"}])
    enforcer.check("s3://warehouse/users/data.parquet")
    ```
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit, urlunsplit


@dataclass(frozen=True)
class PathRule:
    """One allowed storage root.

    Example:
        ```python
        rule = PathRule(root="s3://warehouse")
        ```
    """

    root: str


class PathRuleEnforcer:
    """Checks storage paths against published allowed storage roots.

    Example:
        ```python
        PathRuleEnforcer([{"root": "/data"}]).check("/data/users.parquet")
        ```
    """

    def __init__(self, rules: Sequence[Mapping[str, object]]) -> None:
        self._rules = [_path_rule(raw) for raw in rules]

    @property
    def enabled(self) -> bool:
        return bool(self._rules)

    def check(self, path: str) -> None:
        """Raises `PermissionError` when `path` is outside every configured root."""
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
    if not text:
        return ""
    parsed = urlsplit(text)
    if parsed.scheme:
        if not parsed.netloc or parsed.username or parsed.password:
            raise ValueError("Path roots must not contain credentials or missing authority")
        if parsed.query or parsed.fragment:
            raise ValueError("Path roots must not contain query or fragment components")
        normalized_path = _normalize_uri_path(parsed.path)
        return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), normalized_path, "", ""))
    if "?" in text or "#" in text:
        raise ValueError("Path roots must not contain query or fragment components")
    return str(Path(text).resolve(strict=False))


def _path_is_under_root(path: str, root: str) -> bool:
    root_parts = urlsplit(root)
    path_parts = urlsplit(path)
    if bool(root_parts.scheme) != bool(path_parts.scheme):
        return False
    if root_parts.scheme:
        if (path_parts.scheme.lower(), path_parts.netloc.lower()) != (
            root_parts.scheme.lower(),
            root_parts.netloc.lower(),
        ):
            return False
        root_path = _normalize_uri_path(root_parts.path)
        path_path = _normalize_uri_path(path_parts.path)
        return path_path == root_path or path_path.startswith(f"{root_path}/")
    try:
        Path(path).relative_to(Path(root))
    except ValueError:
        return False
    return True


def _normalize_uri_path(value: str) -> str:
    decoded = unquote(value or "/")
    parts = [part for part in decoded.split("/") if part not in ("", ".")]
    normalized: list[str] = []
    for part in parts:
        if part == "..":
            if normalized:
                normalized.pop()
            else:
                raise ValueError("Path cannot traverse above its URI root")
        else:
            normalized.append(part)
    return "/" + "/".join(normalized) if normalized else "/"
