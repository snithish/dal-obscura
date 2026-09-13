"""Public, dependency-light contracts for Dal Obscura plugins."""

from dal_obscura_plugin_api.contracts import (
    PLUGIN_API_VERSION,
    SUPPORTED_CAPABILITIES,
    SUPPORTED_PLUGIN_API_VERSIONS,
    SUPPORTED_PLUGIN_CONFIG_VERSIONS,
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
    "PLUGIN_API_VERSION",
    "SUPPORTED_CAPABILITIES",
    "SUPPORTED_PLUGIN_API_VERSIONS",
    "SUPPORTED_PLUGIN_CONFIG_VERSIONS",
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
