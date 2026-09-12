"""Admission-controlled entry-point discovery for trusted plugins."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from importlib import metadata
from pathlib import Path
from threading import RLock
from typing import Any, cast

from dal_obscura.common.plugin_api.contracts import PluginDescriptor, PluginKind

ENTRY_POINT_GROUPS: dict[PluginKind, str] = {
    "catalog": "dal_obscura.catalogs.v1",
    "table_format": "dal_obscura.table_formats.v1",
}
STATIC_DESCRIPTOR_FILENAME = "dal_obscura-plugin.json"
_PLUGIN_ID = re.compile(r"[a-z][a-z0-9_.-]{0,63}\Z")


class PluginAdmissionError(ValueError):
    """Raised when installed plugin metadata is not explicitly admitted."""


FactoryLoader = Callable[[metadata.EntryPoint], object]
BuiltinRegistration = tuple[PluginDescriptor, object]
DescriptorLoader = Callable[[metadata.EntryPoint], PluginDescriptor]
PluginLock = tuple[str, str, str] | tuple[str, str, str, str, str]


class PluginRegistry:
    """Discovers only operator-allowlisted installed entry points."""

    def __init__(
        self,
        *,
        allowlist: Mapping[tuple[PluginKind, str], PluginLock] | None = None,
        entry_points_fn: Callable[[], metadata.EntryPoints] | None = None,
        factory_loader: FactoryLoader | None = None,
        builtins: Mapping[tuple[PluginKind, str], BuiltinRegistration] | None = None,
        descriptor_loader: DescriptorLoader | None = None,
    ) -> None:
        self._allowlist = dict(allowlist or {})
        self._entry_points_fn = entry_points_fn or metadata.entry_points
        self._factory_loader = factory_loader or (lambda entry: entry.load())
        self._descriptor_loader = descriptor_loader
        self._builtins = dict(builtins or {})
        for key, (descriptor, _) in self._builtins.items():
            if key != (descriptor.kind, descriptor.plugin_id):
                raise ValueError("Built-in plugin registration key does not match descriptor")
        self._snapshot: dict[tuple[PluginKind, str], PluginDescriptor] = {}
        self._snapshot_entries: dict[tuple[PluginKind, str], metadata.EntryPoint] = {}
        self._snapshot_builtins: dict[tuple[PluginKind, str], object] = {}
        self._snapshot_lock = RLock()

    def discover(self) -> dict[tuple[PluginKind, str], PluginDescriptor]:
        """Reads entry-point metadata without importing factories."""
        descriptors, _, _ = self._discover_with_entries()
        return descriptors

    def load(self, kind: PluginKind, plugin_id: str) -> object:
        """Loads one admitted factory; never accepts a request import string."""

        self._validate_id(plugin_id)
        key = (kind, plugin_id)
        with self._snapshot_lock:
            entry = self._snapshot_entries.get(key)
            builtin = self._snapshot_builtins.get(key)
            admitted = key in self._snapshot
        if entry is None and not admitted:
            # Preserve the convenient first-use behavior while still making
            # the resulting entry point part of one immutable generation.
            self.reload()
            with self._snapshot_lock:
                entry = self._snapshot_entries.get(key)
                builtin = self._snapshot_builtins.get(key)
                admitted = key in self._snapshot
        if entry is None or not admitted:
            if builtin is not None:
                return builtin
            raise PluginAdmissionError(f"Plugin is not admitted: {kind}:{plugin_id}")
        if builtin is not None:
            return builtin
        return self._factory_loader(entry)

    def reload(self) -> dict[tuple[PluginKind, str], PluginDescriptor]:
        """Builds a complete admission snapshot before atomically swapping it."""

        candidate, entries, builtins = self._discover_with_entries()
        with self._snapshot_lock:
            self._snapshot = dict(candidate)
            self._snapshot_entries = dict(entries)
            self._snapshot_builtins = dict(builtins)
            return dict(self._snapshot)

    def admitted(self) -> dict[tuple[PluginKind, str], PluginDescriptor]:
        """Returns the last valid snapshot without rebuilding or importing factories."""

        with self._snapshot_lock:
            return dict(self._snapshot)

    def _select(self, group: str) -> list[metadata.EntryPoint]:
        points: Any = self._entry_points_fn()
        selected = (
            points.select(group=group)
            if hasattr(points, "select")
            else points.get(group, ())
        )
        return list(cast(Any, selected))

    def _discover_with_entries(
        self,
    ) -> tuple[
        dict[tuple[PluginKind, str], PluginDescriptor],
        dict[tuple[PluginKind, str], metadata.EntryPoint],
        dict[tuple[PluginKind, str], object],
    ]:
        descriptors = {key: descriptor for key, (descriptor, _) in self._builtins.items()}
        entries: dict[tuple[PluginKind, str], metadata.EntryPoint] = {}
        builtins = {key: factory for key, (_, factory) in self._builtins.items()}
        for kind, group in ENTRY_POINT_GROUPS.items():
            for entry in self._select(group):
                plugin_id = str(entry.name)
                self._validate_id(plugin_id)
                key = (kind, plugin_id)
                if key in descriptors:
                    raise PluginAdmissionError(f"Duplicate plugin ID: {kind}:{plugin_id}")
                admitted = self._allowlist.get(key)
                if admitted is None:
                    continue
                if len(admitted) not in (3, 5):
                    raise PluginAdmissionError(f"Invalid plugin lock for {kind}:{plugin_id}")
                distribution, version, api_version, *digests = admitted
                if entry.dist is None:
                    raise PluginAdmissionError(
                        f"Plugin provenance is unavailable for {kind}:{plugin_id}"
                    )
                actual_distribution = entry.dist.name
                actual_version = entry.dist.version
                if (actual_distribution, actual_version) != (distribution, version):
                    raise PluginAdmissionError(f"Plugin lock mismatch for {kind}:{plugin_id}")
                descriptor = (
                    self._descriptor_loader(entry)
                    if self._descriptor_loader is not None
                    else PluginDescriptor(
                        kind=kind,
                        plugin_id=plugin_id,
                        api_version=api_version,
                        config_version=1,
                        distribution=distribution,
                        version=version,
                    )
                )
                if (
                    descriptor.kind != kind
                    or descriptor.plugin_id != plugin_id
                    or descriptor.api_version != api_version
                    or descriptor.distribution != distribution
                    or descriptor.version != version
                ):
                    raise PluginAdmissionError(f"Plugin descriptor mismatch for {kind}:{plugin_id}")
                if digests:
                    descriptor_digest, artifact_digest = digests
                    if _descriptor_digest(descriptor) != descriptor_digest:
                        raise PluginAdmissionError(
                            f"Plugin descriptor digest mismatch for {kind}:{plugin_id}"
                        )
                    if _artifact_digest(entry) != artifact_digest:
                        raise PluginAdmissionError(
                            f"Plugin artifact digest mismatch for {kind}:{plugin_id}"
                        )
                descriptors[key] = descriptor
                entries[key] = entry
        return descriptors, entries, builtins

    @staticmethod
    def _validate_id(plugin_id: str) -> None:
        if not _PLUGIN_ID.fullmatch(plugin_id):
            raise PluginAdmissionError(f"Invalid plugin ID: {plugin_id!r}")


def load_static_plugin_descriptor(entry: metadata.EntryPoint) -> PluginDescriptor:
    """Loads a plugin descriptor from distribution metadata without importing code.

    Qualified wheels may include ``dal_obscura-plugin.json`` at their root.  The
    descriptor is parsed from the installed distribution's metadata and its
    identity is tied to the entry-point group/name and package provenance.  A
    missing or malformed file fails closed; callers that need legacy entry-point
    compatibility can continue to provide an explicit fallback loader.
    """

    distribution = entry.dist
    if distribution is None:
        raise PluginAdmissionError("Plugin provenance is unavailable")
    raw = distribution.read_text(STATIC_DESCRIPTOR_FILENAME)
    if raw is None:
        raise PluginAdmissionError("Plugin static descriptor is missing")
    try:
        payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise PluginAdmissionError("Plugin static descriptor is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise PluginAdmissionError("Plugin static descriptor must be an object")
    expected_kind = next(
        (kind for kind, group in ENTRY_POINT_GROUPS.items() if group == entry.group),
        None,
    )
    if expected_kind is None:
        raise PluginAdmissionError("Plugin entry-point group is unsupported")
    allowed = {
        "kind",
        "plugin_id",
        "api_version",
        "config_version",
        "capabilities",
        "config_schema",
        "display_name",
    }
    if set(payload) - allowed:
        raise PluginAdmissionError("Plugin static descriptor contains unknown fields")
    kind = payload.get("kind")
    plugin_id = payload.get("plugin_id")
    api_version = payload.get("api_version")
    config_version = payload.get("config_version")
    capabilities = payload.get("capabilities", [])
    config_schema = payload.get("config_schema", {})
    display_name = payload.get("display_name", "")
    if kind != expected_kind or plugin_id != str(entry.name):
        raise PluginAdmissionError("Plugin static descriptor identity mismatch")
    if not isinstance(api_version, str) or not isinstance(config_version, int):
        raise PluginAdmissionError("Plugin static descriptor version fields are invalid")
    if not isinstance(capabilities, list) or any(
        not isinstance(item, str) for item in capabilities
    ):
        raise PluginAdmissionError("Plugin static descriptor capabilities are invalid")
    if not isinstance(config_schema, Mapping) or not isinstance(display_name, str):
        raise PluginAdmissionError("Plugin static descriptor fields are invalid")
    capability_values = cast(list[str], capabilities)
    return PluginDescriptor(
        kind=cast(PluginKind, kind),
        plugin_id=plugin_id,
        api_version=api_version,
        config_version=config_version,
        distribution=distribution.name,
        version=distribution.version,
        capabilities=frozenset(capability_values),
        config_schema=config_schema,
        display_name=display_name,
    )


def _descriptor_digest(descriptor: PluginDescriptor) -> str:
    payload = {
        "kind": descriptor.kind,
        "plugin_id": descriptor.plugin_id,
        "api_version": descriptor.api_version,
        "config_version": descriptor.config_version,
        "distribution": descriptor.distribution,
        "version": descriptor.version,
        "capabilities": sorted(descriptor.capabilities),
        "config_schema": descriptor.config_schema,
        "display_name": descriptor.display_name,
    }
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
        _descriptor_digest(descriptor),
        _artifact_digest(entry),
    )


def _artifact_digest(entry: metadata.EntryPoint) -> str:
    distribution = entry.dist
    if distribution is None or distribution.files is None:
        raise PluginAdmissionError("Plugin artifact files are unavailable")
    digest = hashlib.sha256()
    try:
        for relative in sorted(distribution.files, key=str):
            path = Path(str(distribution.locate_file(str(relative))))
            if not path.is_file():
                raise PluginAdmissionError("Plugin artifact file is unavailable")
            digest.update(str(relative).encode("utf-8"))
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
    except OSError as exc:
        raise PluginAdmissionError("Plugin artifact files are unreadable") from exc
    return digest.hexdigest()
