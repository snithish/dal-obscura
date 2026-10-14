"""Contained local/S3 reads using Arrow's maintained filesystem implementations.

Storage credentials come from the worker's AWS provider chain. Only locations,
never credentials or live filesystems, belong in handles and scan tasks.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import pyarrow.fs as fs


class StorageRoot:
    """One admitted root and its runtime filesystem; member paths stay contained."""

    def __init__(self, uri: object) -> None:
        if not isinstance(uri, str) or not uri or len(uri) > 4096:
            raise ValueError("Invalid storage root")
        self.remote = uri.startswith("s3:")
        if self.remote:
            parsed = urlsplit(uri)
            if (
                parsed.scheme != "s3"
                or not parsed.netloc
                or parsed.username
                or parsed.password
                or parsed.query
                or parsed.fragment
                or ":" in parsed.netloc
            ):
                raise ValueError("Invalid S3 storage root")
            self.path = parsed.netloc + "/" + _object_path(unquote(parsed.path)).strip("/")
            self.path = self.path.rstrip("/")
            self.uri = "s3://" + quote(self.path, safe="/")
            # Endpoint is operator-owned startup configuration, not table metadata.
            # Used for private S3 endpoints and the real S3-compatible test lane.
            endpoint = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL")
            kwargs = {
                "region": os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
                "connect_timeout": 5,
                "request_timeout": 30,
            }
            if endpoint:
                destination = urlsplit(endpoint)
                if (
                    destination.scheme not in {"http", "https"}
                    or not destination.netloc
                    or destination.username
                    or destination.password
                    or destination.query
                    or destination.fragment
                    or destination.path not in {"", "/"}
                ):
                    raise ValueError("Invalid operator S3 endpoint")
                kwargs.update(endpoint_override=destination.netloc, scheme=destination.scheme)
            factory = getattr(fs, "S3FileSystem", None)
            if not callable(factory):
                raise ValueError("This Arrow build does not support S3")
            self.filesystem = factory(**kwargs)
        else:
            if uri.startswith("file:"):
                parsed = urlsplit(uri)
                if parsed.netloc not in {"", "localhost"} or parsed.query or parsed.fragment:
                    raise ValueError("Storage file URI must be local")
                uri = unquote(parsed.path)
            elif "://" in uri:
                raise ValueError("Supported storage schemes are local files and s3")
            candidate = Path(uri).absolute()
            _reject_symlinks(candidate)
            self.path = str(candidate.resolve(strict=True))
            if not Path(self.path).is_dir():
                raise ValueError("Storage root must be a directory")
            self.uri = self.path
            self.filesystem = fs.LocalFileSystem()

    def member(self, value: object, *, encoded: bool = False, exists: bool = True) -> str:
        if not isinstance(value, str) or not value or len(value) > 4096:
            raise ValueError("Invalid storage member path")
        candidate = self._candidate(value, encoded=encoded)
        if self.remote:
            candidate = _object_path(candidate)
            if candidate != self.path and not candidate.startswith(self.path + "/"):
                raise ValueError("Storage member escapes its admitted root")
        else:
            path = Path(candidate)
            _reject_symlinks(path)
            if ".." in path.parts:
                raise ValueError("Storage member escapes its admitted root")
            try:
                candidate = str(path.resolve(strict=exists))
                Path(candidate).relative_to(self.path)
            except (OSError, ValueError) as exc:
                raise ValueError(
                    "Storage member escapes its admitted root or is unavailable"
                ) from exc
        if exists and self.filesystem.get_file_info(candidate).type == fs.FileType.NotFound:
            raise ValueError("Storage member is unavailable")
        return candidate

    def _candidate(self, value: str, *, encoded: bool) -> str:
        if value.startswith(("s3:", "file:")) or "://" in value:
            parsed = urlsplit(value)
            if parsed.query or parsed.fragment or parsed.username or parsed.password:
                raise ValueError("Storage member URI contains credentials, query or fragment")
            if self.remote and parsed.scheme == "s3":
                candidate = parsed.netloc + unquote(parsed.path)
            elif not self.remote and parsed.scheme == "file" and parsed.netloc in {"", "localhost"}:
                candidate = unquote(parsed.path)
            else:
                raise ValueError("Storage member escapes its admitted root")
        else:
            value = unquote(value) if encoded else value
            if self.remote:
                if value.startswith("/"):
                    raise ValueError("Storage member escapes its admitted root")
                candidate = self.path + "/" + value
            else:
                candidate = str(Path(self.path) / value)
        return candidate

    def location(self, member: str) -> str:
        return "s3://" + quote(member, safe="/") if self.remote else member

    def relative(self, member: str) -> str:
        return PurePosixPath(member).relative_to(self.path).as_posix()

    def read(self, member: str, *, limit: int) -> bytes:
        with self.filesystem.open_input_stream(member) as stream:
            raw = stream.read(limit + 1)
        if len(raw) > limit:
            raise ValueError("Storage object exceeds the byte budget")
        return raw


def _reject_symlinks(path: Path) -> None:
    if any(item.is_symlink() for item in (path, *path.parents)):
        raise ValueError("Storage path contains a symlink")


def _object_path(value: str) -> str:
    if "\\" in value or any(part in {".", ".."} for part in value.strip("/").split("/")):
        raise ValueError("Storage object path contains traversal")
    if "//" in value:
        raise ValueError("Storage object path contains empty segments")
    return value.strip("/")


@dataclass(frozen=True)
class PathRule:
    """One allowed storage root."""

    root: str


class PathRuleEnforcer:
    """Checks storage paths against configured allowed storage roots."""

    def __init__(self, rules: Sequence[Mapping[str, object]]) -> None:
        self._rules = [_path_rule(raw) for raw in rules]

    @property
    def roots(self) -> tuple[str, ...]:
        return tuple(rule.root for rule in self._rules)

    @property
    def enabled(self) -> bool:
        return bool(self._rules)

    def check(self, path: str) -> None:
        """Raises `PermissionError` when `path` is outside every configured root."""
        if not self._rules:
            return
        try:
            normalized = _normalize_path(path)
        except ValueError as exc:
            # Returned provider locations are untrusted data. Treat malformed
            # or above-root paths as an authorization denial at this boundary,
            # rather than leaking an internal validation error to callers.
            raise PermissionError("Path is not allowed") from exc
        if not normalized:
            raise PermissionError("Path is not allowed")
        if any(_path_is_under_root(normalized, rule.root) for rule in self._rules):
            return
        raise PermissionError("Path is not allowed")


def _path_rule(raw: Mapping[str, object]) -> PathRule:
    if set(raw) != {"root"}:
        raise ValueError("Path rule contains unsupported fields")
    root = _normalize_path(raw.get("root"))
    if not root:
        raise ValueError("Path rule requires root")
    if any(character in root for character in "*?[]"):
        raise ValueError("Path rule wildcards are not supported")
    return PathRule(root=root)


def _normalize_path(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("Path roots and paths must be strings")
    text = value.strip()
    if text == "/":
        return text
    if not text:
        return ""
    parsed = urlsplit(text)
    if parsed.scheme:
        if parsed.scheme.lower() == "file":
            # Explicit local file URIs are equivalent to their filesystem
            # paths.  A non-empty authority would select a remote host and
            # must never cross the local path boundary.
            if parsed.netloc or parsed.username or parsed.password:
                raise ValueError("File path roots must be local and credential-free")
            if parsed.query or parsed.fragment:
                raise ValueError("Path roots must not contain query or fragment components")
            decoded_path = unquote(parsed.path or "")
            if not decoded_path:
                raise ValueError("Path roots must have a local path")
            return str(Path(decoded_path).resolve(strict=False))
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
        if root_path == "/":
            return True
        return path_path == root_path or path_path.startswith(f"{root_path}/")
    try:
        Path(path).relative_to(Path(root))
    except ValueError:
        return False
    return True


def _normalize_uri_path(value: str) -> str:
    decoded = unquote(value or "/")
    # Providers are allowed to perform their own URI decoding.  Collapse a
    # bounded second encoding layer for path separators/dot segments so
    # ``%252f..%252f`` cannot become traversal after this check succeeds.
    for _ in range(2):
        lowered = decoded.lower()
        if not any(token in lowered for token in ("%2f", "%5c", "%2e")):
            break
        decoded = unquote(decoded)
    decoded = decoded.replace("\\", "/")
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
