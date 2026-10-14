"""Real installed package admission for core routing tests."""

from typing import Any, cast

from dal_obscura.plugins import PluginRegistry
from dal_obscura.plugins.inspection import inspect_distributions


def manifest_registry() -> PluginRegistry:
    document = inspect_distributions(["dal-obscura-manifest-parquet"])
    locks = {
        (row["kind"], row["plugin_id"]): tuple(row["lock"])
        for row in cast(Any, document["lock"])["plugins"]
    }
    registry = PluginRegistry(allowlist=locks)
    registry.reload()
    return registry
