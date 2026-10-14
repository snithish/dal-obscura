"""Installed plugin admission, artifact locks and lifecycle transitions."""

from dal_obscura.plugins.lifecycle import (
    PluginLifecycleError,
    PluginLifecycleState,
    transition_plugin_lifecycle,
)
from dal_obscura.plugins.lockfile import load_plugin_lock_file
from dal_obscura.plugins.registry import (
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
