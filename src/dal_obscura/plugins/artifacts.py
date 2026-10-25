"""Static installed-artifact inspection without importing plugin factories."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Generator
from contextlib import contextmanager
from importlib import metadata
from pathlib import Path
from typing import BinaryIO

from dal_obscura_plugin_api import PluginDescriptor, PluginKind
from dal_obscura_plugin_api.descriptors import (
    MAX_STATIC_DESCRIPTOR_BYTES,
    STATIC_DESCRIPTOR_FILENAME,
    parse_descriptor,
)

ENTRY_POINT_GROUPS: dict[PluginKind, str] = {
    "catalog": "dal_obscura.catalogs.v3",
    "table_format": "dal_obscura.table_formats.v3",
    "secret_backend": "dal_obscura.secret_backends.v3",
}
_MODULE_PATH = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*\Z")


class PluginAdmissionError(ValueError):
    """Raised when installed plugin metadata is not explicitly admitted."""


PluginLock = tuple[str, str, str, str, str]


def load_static_plugin_descriptor(entry: metadata.EntryPoint) -> PluginDescriptor:
    """Inspect an installed descriptor without importing its package or factory."""
    distribution = entry.dist
    if distribution is None:
        raise PluginAdmissionError("Plugin provenance is unavailable")
    kind = next((kind for kind, group in ENTRY_POINT_GROUPS.items() if group == entry.group), None)
    if kind is None:
        raise PluginAdmissionError("Plugin entry-point group is unsupported")
    raw = _read_static_descriptor(entry)
    try:
        return parse_descriptor(
            raw,
            kind=kind,
            plugin_id=str(entry.name),
            distribution=distribution.name,
            version=distribution.version,
        )
    except ValueError as exc:
        raise PluginAdmissionError(str(exc)) from exc


def descriptor_digest(descriptor: PluginDescriptor) -> str:
    payload = descriptor.to_json()
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def build_plugin_lock(
    kind: PluginKind,
    entry: metadata.EntryPoint,
    descriptor: PluginDescriptor,
) -> PluginLock:
    """Build a descriptor/artifact-pinned lock for a qualified entry point.

    Release tooling should call this against the built distribution, then
    persist the returned five-part tuple in its immutable admission manifest.
    It refuses to generate a lock when provenance or descriptor identity does
    not match the entry point.
    """

    if descriptor.kind != kind or descriptor.plugin_id != str(entry.name):
        raise PluginAdmissionError("Plugin descriptor does not match entry point")
    if entry.dist is None:
        raise PluginAdmissionError("Plugin provenance is unavailable")
    distribution = entry.dist.name
    version = entry.dist.version
    if descriptor.distribution != distribution or descriptor.version != version:
        raise PluginAdmissionError("Plugin descriptor provenance does not match entry point")
    return (
        distribution,
        version,
        descriptor.api_version,
        descriptor_digest(descriptor),
        artifact_digest(entry),
    )


def artifact_digest(entry: metadata.EntryPoint) -> str:
    distribution = entry.dist
    if distribution is None or distribution.files is None:
        raise PluginAdmissionError("Plugin artifact files are unavailable")
    digest = hashlib.sha256()
    try:
        for relative in sorted(distribution.files, key=str):
            # Installers rewrite these files and bytecode for each environment.
            # Hash shipped code and distribution metadata, not local provenance.
            parts = Path(str(relative)).parts
            if "__pycache__" in parts or (
                any(part.endswith(".dist-info") for part in parts)
                and parts[-1]
                in {"RECORD", "INSTALLER", "REQUESTED", "direct_url.json", "uv_cache.json"}
            ):
                continue
            path = Path(str(distribution.locate_file(str(relative))))
            digest.update(str(relative).encode("utf-8"))
            digest.update(b"\0")
            with open_regular_file(path, "Plugin artifact file") as stream:
                while chunk := stream.read(1_048_576):
                    digest.update(chunk)
            digest.update(b"\0")
    except OSError as exc:
        raise PluginAdmissionError("Plugin artifact files are unreadable") from exc
    return digest.hexdigest()


@contextmanager
def open_regular_file(path: Path, label: str, *, immutable: bool = False) -> Generator[BinaryIO]:
    """Reject links and special files before reading, including replacement races."""

    def open_descriptor(filename: str, flags: int) -> int:
        # Nonblocking open prevents a replacement FIFO from waiting for a writer.
        descriptor = os.open(filename, flags | os.O_NONBLOCK | os.O_NOFOLLOW)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode):
                raise PluginAdmissionError(f"{label} must be a regular file")
            if immutable and info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
                raise PluginAdmissionError(f"{label} must not be group- or world-writable")
        except BaseException:
            os.close(descriptor)
            raise
        return descriptor

    try:
        if path.is_symlink():
            raise PluginAdmissionError(f"{label} must not be a symbolic link")
        with open(path, "rb", opener=open_descriptor) as stream:
            yield stream
    except OSError as exc:
        raise PluginAdmissionError(f"{label} is unavailable") from exc


def read_metadata_file(path: Path, limit: int, label: str, *, immutable: bool = False) -> bytes:
    """Read at most the metadata budget plus one byte to detect oversized input."""
    with open_regular_file(path, label, immutable=immutable) as stream:
        if os.fstat(stream.fileno()).st_size > limit:
            raise PluginAdmissionError(f"{label} is too large")
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise PluginAdmissionError(f"{label} is too large")
    return raw


def _read_static_descriptor(entry: metadata.EntryPoint) -> bytes:
    distribution = entry.dist
    if distribution is None or distribution.files is None:
        raise PluginAdmissionError("Plugin artifact files are unavailable")
    module_path = str(entry.value).split(":", 1)[0]
    if not _MODULE_PATH.fullmatch(module_path):
        raise PluginAdmissionError("Plugin entry-point module is invalid")
    package_path = f"{module_path.split('.', 1)[0]}/{STATIC_DESCRIPTOR_FILENAME}"
    # Inspect only advertised wheel metadata and the entry point's package data.
    # Do not call Distribution.read_text: it allocates the whole file first.
    files = [Path(str(relative)) for relative in distribution.files]
    metadata_paths = [
        path
        for path in files
        if len(path.parts) == 2
        and path.parts[0].endswith(".dist-info")
        and path.name == STATIC_DESCRIPTOR_FILENAME
    ]
    if len(metadata_paths) > 1:
        raise PluginAdmissionError("Plugin static descriptor identity is ambiguous")
    candidates = [*metadata_paths, Path(STATIC_DESCRIPTOR_FILENAME), Path(package_path)]
    advertised = set(files)
    for relative in candidates:
        if relative not in advertised:
            continue
        path = Path(str(distribution.locate_file(str(relative))))
        try:
            return read_metadata_file(path, MAX_STATIC_DESCRIPTOR_BYTES, "Plugin static descriptor")
        except PluginAdmissionError as exc:
            raise PluginAdmissionError(f"Plugin static descriptor is unreadable: {exc}") from exc
    raise PluginAdmissionError("Plugin static descriptor is missing")
