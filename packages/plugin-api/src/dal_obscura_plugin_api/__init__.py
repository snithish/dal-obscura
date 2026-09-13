"""Public, dependency-light contracts for Dal Obscura plugins."""

from dal_obscura_plugin_api.contracts import (
    SUPPORTED_CAPABILITIES,
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

__all__ = [
    "SUPPORTED_CAPABILITIES",
    "CatalogConfig",
    "CatalogFactory",
    "CatalogPlugin",
    "DiscoveryPage",
    "ExecutionContext",
    "PluginDescriptor",
    "PluginError",
    "SchemaDescriptor",
    "TableFormatFactory",
    "TableFormatPlugin",
    "TableHandle",
    "TableIdentifier",
]
