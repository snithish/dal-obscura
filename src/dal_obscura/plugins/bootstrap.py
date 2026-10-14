"""Compose an admitted registry from an operator lock or local bundled wheels."""

import json
import os
from collections.abc import Mapping
from importlib import metadata

from dal_obscura_plugin_api import PluginKind

from dal_obscura.plugins.registry import (
    ENTRY_POINT_GROUPS,
    PluginAdmissionError,
    PluginLock,
    PluginRegistry,
    build_plugin_lock,
    load_static_plugin_descriptor,
)

_LOCAL_SELECTION = {
    ("catalog", "iceberg.catalog"): "dal-obscura-iceberg",
    ("table_format", "iceberg"): "dal-obscura-iceberg",
    ("secret_backend", "environment"): "dal-obscura-secrets",
    ("secret_backend", "mounted_file"): "dal-obscura-secrets",
}


def create_plugin_registry(
    *,
    allowlist: Mapping[tuple[PluginKind, str], PluginLock] | None = None,
    options: Mapping[str, object] | None = None,
    environment: Mapping[str, str] | None = None,
) -> PluginRegistry:
    if allowlist is None:
        allowlist = {}
        for kind, group in ENTRY_POINT_GROUPS.items():
            for entry in metadata.entry_points().select(group=group):
                key = (kind, str(entry.name))
                if entry.dist is None or entry.dist.name != _LOCAL_SELECTION.get(key):
                    continue
                if key in allowlist:
                    raise PluginAdmissionError(f"Duplicate local plugin: {kind}:{entry.name}")
                allowlist[key] = build_plugin_lock(
                    kind, entry, load_static_plugin_descriptor(entry)
                )
    if options is None:
        source = os.environ if environment is None else environment
        raw = source.get("DAL_OBSCURA_PLUGIN_OPTIONS", "").strip() or "{}"
        options = json.loads(raw)
        if not isinstance(options, dict):
            raise ValueError("DAL_OBSCURA_PLUGIN_OPTIONS must be a JSON object")
    registry = PluginRegistry(allowlist=allowlist, options=options)
    registry.reload()
    return registry
