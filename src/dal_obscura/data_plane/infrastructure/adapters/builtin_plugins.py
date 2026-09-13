"""Trusted built-in plugin registrations for the data plane."""

from __future__ import annotations

from collections.abc import Mapping

from dal_obscura_plugin_api import PluginDescriptor, PluginKind

from dal_obscura.common.plugin_api import PluginLock, PluginRegistry
from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import IcebergCatalog
from dal_obscura.data_plane.infrastructure.table_formats.iceberg import IcebergTableFormat

_ICEBERG_CATALOG_ID = "iceberg.sql"
_ICEBERG_FORMAT_ID = "iceberg"
_BUILTIN_DISTRIBUTION = "dal-obscura"
_BUILTIN_VERSION = "0.1.0"


def create_builtin_plugin_registry(
    *, allowlist: Mapping[tuple[PluginKind, str], PluginLock] | None = None
) -> PluginRegistry:
    """Build and admit the trusted in-tree Iceberg catalog/format pair.

    The classes are kept behind the registry boundary so future external
    distributions use the same admission path.  Existing constructors and
    pickle-referenced task classes remain unchanged.
    """

    catalog_descriptor = PluginDescriptor(
        kind="catalog",
        plugin_id=_ICEBERG_CATALOG_ID,
        api_version="1",
        config_version=1,
        distribution=_BUILTIN_DISTRIBUTION,
        version=_BUILTIN_VERSION,
        display_name="Iceberg SQL catalog",
        capabilities=frozenset({"nested_schema", "snapshot_reads", "splittable_scan"}),
        output_formats=frozenset({_ICEBERG_FORMAT_ID}),
        handle_versions=frozenset({1}),
        config_schema={
            "fields": [
                {"name": "uri", "type": "string", "required": True, "secret": False},
                {"name": "warehouse", "type": "string", "required": False, "secret": False},
                {"name": "user", "type": "string", "required": False, "secret": False},
                {
                    "name": "password",
                    "type": "secret_reference",
                    "required": False,
                    "secret": True,
                },
            ]
        },
    )
    format_descriptor = PluginDescriptor(
        kind="table_format",
        plugin_id=_ICEBERG_FORMAT_ID,
        api_version="1",
        config_version=1,
        distribution=_BUILTIN_DISTRIBUTION,
        version=_BUILTIN_VERSION,
        display_name="Apache Iceberg",
        capabilities=frozenset({"nested_schema", "snapshot_reads", "splittable_scan"}),
        handle_versions=frozenset({1}),
    )
    registry = PluginRegistry(
        allowlist=allowlist,
        builtins={
            ("catalog", _ICEBERG_CATALOG_ID): (catalog_descriptor, IcebergCatalog),
            ("table_format", _ICEBERG_FORMAT_ID): (format_descriptor, IcebergTableFormat),
        },
    )
    registry.reload()
    return registry
