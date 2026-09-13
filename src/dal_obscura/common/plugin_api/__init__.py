"""Versioned public contracts for trusted catalog and table-format plugins.

The contracts contain metadata and request-scoped values only.  They do not
perform authorization or expose the control-plane database.
"""

from dal_obscura.common.plugin_api.contracts import (
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
from dal_obscura.common.plugin_api.lockfile import load_plugin_lock_file
from dal_obscura.common.plugin_api.lifecycle import (
    PluginLifecycleError,
    PluginLifecycleState,
    transition_plugin_lifecycle,
)
from dal_obscura.common.plugin_api.registry import (
    PluginAdmissionError,
    PluginLock,
    PluginRegistry,
    build_plugin_lock,
    load_static_plugin_descriptor,
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
    "PluginAdmissionError",
    "PluginDescriptor",
    "PluginError",
    "PluginLock",
    "PluginLifecycleError",
    "PluginLifecycleState",
    "PluginRegistry",
    "SchemaDescriptor",
    "TableFormatFactory",
    "TableFormatPlugin",
    "TableHandle",
    "TableIdentifier",
    "build_plugin_lock",
    "load_plugin_lock_file",
    "load_static_plugin_descriptor",
    "transition_plugin_lifecycle",
]
