"""Public, dependency-light contracts for Dal Obscura plugins."""

from dal_obscura_plugin_api.contracts import (
    PLUGIN_API_VERSION,
    PLUGIN_CONFIG_VERSION,
    SUPPORTED_CAPABILITIES,
    CatalogConfig,
    CatalogFactory,
    CatalogPlugin,
    DiscoveryPage,
    ExecutionContext,
    PluginDescriptor,
    PluginKind,
    SchemaDescriptor,
    TableFormatFactory,
    TableFormatPlugin,
    TableHandle,
    TableIdentifier,
)
from dal_obscura_plugin_api.scan import ScanRequest
from dal_obscura_plugin_api.tasks import ScanTask

__all__ = [
    "PLUGIN_API_VERSION",
    "PLUGIN_CONFIG_VERSION",
    "SUPPORTED_CAPABILITIES",
    "CatalogConfig",
    "CatalogFactory",
    "CatalogPlugin",
    "DiscoveryPage",
    "ExecutionContext",
    "PluginDescriptor",
    "PluginKind",
    "ScanRequest",
    "ScanTask",
    "SchemaDescriptor",
    "TableFormatFactory",
    "TableFormatPlugin",
    "TableHandle",
    "TableIdentifier",
]
