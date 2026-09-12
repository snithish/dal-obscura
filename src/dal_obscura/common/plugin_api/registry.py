"""Admission-controlled entry-point discovery for trusted plugins."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from importlib import metadata
from threading import RLock
from typing import Any, cast

from dal_obscura.common.plugin_api.contracts import PluginDescriptor, PluginKind

ENTRY_POINT_GROUPS: dict[PluginKind, str] = {
    "catalog": "dal_obscura.catalogs.v1",
    "table_format": "dal_obscura.table_formats.v1",
}
_PLUGIN_ID = re.compile(r"[a-z][a-z0-9_.-]{0,63}\Z")


class PluginAdmissionError(ValueError):
    """Raised when installed plugin metadata is not explicitly admitted."""


FactoryLoader = Callable[[metadata.EntryPoint], object]
BuiltinRegistration = tuple[PluginDescriptor, object]


class PluginRegistry:
    """Discovers only operator-allowlisted installed entry points."""

    def __init__(
        self,
        *,
        allowlist: Mapping[tuple[PluginKind, str], tuple[str, str, str]] | None = None,
        entry_points_fn: Callable[[], metadata.EntryPoints] | None = None,
        factory_loader: FactoryLoader | None = None,
        builtins: Mapping[tuple[PluginKind, str], BuiltinRegistration] | None = None,
    ) -> None:
        self._allowlist = dict(allowlist or {})
        self._entry_points_fn = entry_points_fn or metadata.entry_points
        self._factory_loader = factory_loader or (lambda entry: entry.load())
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
                distribution, version, api_version = admitted
                if entry.dist is None:
                    raise PluginAdmissionError(
                        f"Plugin provenance is unavailable for {kind}:{plugin_id}"
                    )
                actual_distribution = entry.dist.name
                actual_version = entry.dist.version
                if (actual_distribution, actual_version) != (distribution, version):
                    raise PluginAdmissionError(f"Plugin lock mismatch for {kind}:{plugin_id}")
                descriptors[key] = PluginDescriptor(
                    kind=kind,
                    plugin_id=plugin_id,
                    api_version=api_version,
                    config_version=1,
                    distribution=distribution,
                    version=version,
                )
                entries[key] = entry
        return descriptors, entries, builtins

    @staticmethod
    def _validate_id(plugin_id: str) -> None:
        if not _PLUGIN_ID.fullmatch(plugin_id):
            raise PluginAdmissionError(f"Invalid plugin ID: {plugin_id!r}")
