"""Installed plugin admission and artifact locks."""

from dal_obscura.kernel.plugins import PluginAdmissionError, PluginLock
from dal_obscura.plugins.artifacts import (
    build_plugin_lock,
    load_static_plugin_descriptor,
)
from dal_obscura.plugins.lockfile import load_plugin_lock_file
from dal_obscura.plugins.registry import PluginRegistry

__all__ = [
    "PluginAdmissionError",
    "PluginLock",
    "PluginRegistry",
    "build_plugin_lock",
    "load_plugin_lock_file",
    "load_static_plugin_descriptor",
]
