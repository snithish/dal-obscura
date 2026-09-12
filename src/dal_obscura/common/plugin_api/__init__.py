"""Versioned public contracts for trusted catalog and table-format plugins.

The contracts contain metadata and request-scoped values only.  They do not
perform authorization or expose the control-plane database.
"""

from dal_obscura.common.plugin_api.contracts import (
    CatalogConfig,
    CatalogPlugin,
    DiscoveryPage,
    ExecutionContext,
    PluginDescriptor,
    PluginError,
    SchemaDescriptor,
    TableFormatPlugin,
    TableHandle,
    TableIdentifier,
)
from dal_obscura.common.plugin_api.registry import PluginAdmissionError, PluginRegistry

__all__ = [
    "CatalogConfig",
    "CatalogPlugin",
    "DiscoveryPage",
    "ExecutionContext",
    "PluginAdmissionError",
    "PluginDescriptor",
    "PluginError",
    "PluginRegistry",
    "SchemaDescriptor",
    "TableFormatPlugin",
    "TableHandle",
    "TableIdentifier",
]
