"""Versioned public contracts for trusted catalog and table-format plugins.

The contracts contain metadata and request-scoped values only.  They do not
perform authorization or expose the control-plane database.
"""

from dal_obscura.sources.plugins.lifecycle import (
    PluginLifecycleError,
    PluginLifecycleState,
    transition_plugin_lifecycle,
)
from dal_obscura.sources.plugins.lockfile import load_plugin_lock_file
from dal_obscura.sources.plugins.registry import (
    PluginAdmissionError,
    PluginLock,
    PluginRegistry,
    build_plugin_lock,
    load_static_plugin_descriptor,
)

__all__ = [
    "PluginAdmissionError",
    "PluginLifecycleError",
    "PluginLifecycleState",
    "PluginLock",
    "PluginRegistry",
    "build_plugin_lock",
    "load_plugin_lock_file",
    "load_static_plugin_descriptor",
    "transition_plugin_lifecycle",
]
