"""Versioned public contracts for trusted catalog and table-format plugins.

The contracts contain metadata and request-scoped values only.  They do not
perform authorization or expose the control-plane database.
"""

from dal_obscura.common.plugin_api.contracts import (
    CatalogConfig,
    CatalogFactory,
    CatalogPlugin,
    DiscoveryPage,
    ExecutionContext,
    PluginDescriptor,
    PluginError,
    SchemaDescriptor,
    TableFormatFactory,
    TableFormatPlugin,
    TableHandle,
    TableIdentifier,
)
from dal_obscura.common.plugin_api.registry import (
    PluginAdmissionError,
    PluginLock,
    PluginRegistry,
    build_plugin_lock,
    load_static_plugin_descriptor,
)

__all__ = [
    "CatalogConfig",
    "CatalogFactory",
    "CatalogPlugin",
    "DiscoveryPage",
    "ExecutionContext",
    "PluginAdmissionError",
    "PluginDescriptor",
    "PluginError",
    "PluginLock",
    "PluginRegistry",
    "SchemaDescriptor",
    "TableFormatFactory",
    "TableFormatPlugin",
    "TableHandle",
    "TableIdentifier",
    "build_plugin_lock",
    "load_static_plugin_descriptor",
]
