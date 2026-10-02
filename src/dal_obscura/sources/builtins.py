"""Trusted built-in plugin registrations for the data plane."""

from __future__ import annotations

from collections.abc import Mapping

from dal_obscura_plugin_api import PluginKind

from dal_obscura.sources.iceberg_plugin import IcebergFormatPlugin
from dal_obscura.sources.plugins import PluginLock, PluginRegistry
from dal_obscura.sources.sql_catalog import DESCRIPTOR, SqlCatalog

_ICEBERG_CATALOG_ID = "iceberg.sql"
_ICEBERG_FORMAT_ID = "iceberg"


def create_builtin_plugin_registry(
    *, allowlist: Mapping[tuple[PluginKind, str], PluginLock] | None = None
) -> PluginRegistry:
    """Build and admit the trusted in-tree Iceberg catalog/format pair.

    Both built-in identities are admitted through the registry boundary.
    """

    registry = PluginRegistry(
        allowlist=allowlist,
        builtins={
            ("catalog", _ICEBERG_CATALOG_ID): (DESCRIPTOR, SqlCatalog),
            ("table_format", _ICEBERG_FORMAT_ID): (
                IcebergFormatPlugin.descriptor,
                IcebergFormatPlugin,
            ),
        },
    )
    registry.reload()
    return registry
