"""Contained paths with delta-rs's official object-store filesystem.

The same native provider opens the log, data, and deletion vectors. Worker
configuration and cloud credentials never enter a handle or scan task.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

import pyarrow.fs as fs
from deltalake.fs import DeltaStorageHandler


class DeltaStorage:
    def __init__(self, uri: str):
        parsed = urlsplit(uri)
        if not uri or len(uri) > 4096 or parsed.query or parsed.fragment or parsed.password:
            raise ValueError("Invalid Delta storage root")
        self.remote = bool(parsed.scheme and parsed.scheme != "file")
        if self.remote:
            if not parsed.netloc or (parsed.username and parsed.scheme not in {"abfs", "abfss"}):
                raise ValueError("Invalid Delta storage root")
            _segments(unquote(parsed.path).strip("/"))
            self.uri = uri.rstrip("/")
        else:
            if parsed.netloc not in {"", "localhost"}:
                raise ValueError("Delta file URI must be local")
            path = Path(unquote(parsed.path) if parsed.scheme else uri).absolute()
            _local(path)
            self.uri = str(path.resolve(strict=True))
        self.filesystem = fs.PyFileSystem(DeltaStorageHandler(self.uri, storage_options()))

    def member(self, value: str, *, encoded=False, exists=True) -> str:
        if not isinstance(value, str) or not value or len(value) > 4096:
            raise ValueError("Invalid Delta storage member")
        parsed = urlsplit(value)
        if parsed.scheme or value.startswith("/"):
            if parsed.query or parsed.fragment or parsed.password:
                raise ValueError("Delta member URI contains credentials, query or fragment")
            location = unquote(parsed.path) if parsed.scheme == "file" else value
            if location == self.uri:
                member = ""
            elif location.startswith(self.uri + "/"):
                member = location[len(self.uri) + 1 :]
                member = unquote(member) if self.remote else member
            else:
                raise ValueError("Delta member escapes its admitted root")
        else:
            member = unquote(value) if encoded else value
        _segments(member)
        if not self.remote:
            _local(Path(self.uri) / member)
        if exists and self.filesystem.get_file_info(member).type == fs.FileType.NotFound:
            raise ValueError("Delta member is unavailable in its admitted root")
        return member

    def location(self, member: str) -> str:
        return self.uri + (
            "/" + quote(member, safe="/")
            if self.remote and member
            else "/" + member
            if member
            else ""
        )

    def read(self, member: str, *, limit: int) -> bytes:
        with self.filesystem.open_input_stream(member) as stream:
            value = stream.read(limit + 1)
        if len(value) > limit:
            raise ValueError("Delta storage object exceeds the byte budget")
        return value


def _local(path: Path):
    if any(item.is_symlink() for item in (path, *path.parents)):
        raise ValueError("Delta storage path contains a symlink")


def _segments(value: str):
    if "\\" in value or "//" in value or any(part in {".", ".."} for part in value.split("/")):
        raise ValueError("Delta member escapes its admitted root")


def storage_options() -> dict[str, str]:
    options = json.loads(os.environ.get("DAL_OBSCURA_DELTA_STORAGE_OPTIONS", "{}"))
    if not isinstance(options, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in options.items()
    ):
        raise ValueError("Worker Delta storage options must be a string mapping")
    return options
