"""Contained local/S3 reads using Arrow's maintained filesystem implementations.

Storage credentials come from the worker's AWS provider chain. Only locations,
never credentials or live filesystems, belong in handles and scan tasks.
"""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
from urllib.parse import quote, unquote, urlsplit

import pyarrow.fs as fs


class StorageRoot:
    """One admitted root and its runtime filesystem; member paths stay contained."""

    def __init__(self, uri: object):
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
