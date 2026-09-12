"""Admission-controlled entry-point discovery for trusted plugins."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from importlib import metadata
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


class PluginRegistry:
    """Discovers only operator-allowlisted installed entry points."""

    def __init__(
        self,
        *,
        allowlist: Mapping[tuple[PluginKind, str], tuple[str, str, str]] | None = None,
        entry_points_fn: Callable[[], metadata.EntryPoints] | None = None,
        factory_loader: FactoryLoader | None = None,
    ) -> None:
        self._allowlist = dict(allowlist or {})
        self._entry_points_fn = entry_points_fn or metadata.entry_points
        self._factory_loader = factory_loader or (lambda entry: entry.load())

    def discover(self) -> dict[tuple[PluginKind, str], PluginDescriptor]:
        """Reads entry-point metadata without importing factories."""

        descriptors: dict[tuple[PluginKind, str], PluginDescriptor] = {}
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
                if entry.dist is not None:
                    actual_distribution = entry.dist.name
                    actual_version = entry.dist.version
                    if (actual_distribution, actual_version) != (distribution, version):
                        raise PluginAdmissionError(
                            f"Plugin lock mismatch for {kind}:{plugin_id}"
                        )
                descriptors[key] = PluginDescriptor(
                    kind=kind,
                    plugin_id=plugin_id,
                    api_version=api_version,
                    config_version=1,
                    distribution=distribution,
                    version=version,
                )
        return descriptors

    def load(self, kind: PluginKind, plugin_id: str) -> object:
        """Loads one admitted factory; never accepts a request import string."""

        self._validate_id(plugin_id)
        group = ENTRY_POINT_GROUPS[kind]
        matches = [entry for entry in self._select(group) if entry.name == plugin_id]
        if len(matches) != 1:
            raise PluginAdmissionError(f"Plugin is not uniquely installed: {kind}:{plugin_id}")
        self.discover()
        return self._factory_loader(matches[0])

    def _select(self, group: str) -> list[metadata.EntryPoint]:
        points: Any = self._entry_points_fn()
        selected = (
            points.select(group=group)
            if hasattr(points, "select")
            else points.get(group, ())
        )
        return list(cast(Any, selected))

    @staticmethod
    def _validate_id(plugin_id: str) -> None:
        if not _PLUGIN_ID.fullmatch(plugin_id):
            raise PluginAdmissionError(f"Invalid plugin ID: {plugin_id!r}")
