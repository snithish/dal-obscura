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
from dal_obscura_plugin_api.execution import active, checked_iterator
from dal_obscura_plugin_api.options import PluginOptions
from dal_obscura_plugin_api.schema.projection import ScanRequest
from dal_obscura_plugin_api.secrets import SecretBackend, SecretBackendFactory
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
    "PluginOptions",
    "ScanRequest",
    "ScanTask",
    "SchemaDescriptor",
    "SecretBackend",
    "SecretBackendFactory",
    "TableFormatFactory",
    "TableFormatPlugin",
    "TableHandle",
    "TableIdentifier",
    "active",
    "checked_iterator",
]
