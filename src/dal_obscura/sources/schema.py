"""Schema discovery uses the same admitted source lifecycle as governed reads."""

from collections.abc import Callable
from typing import Any, cast

import pyarrow as pa

from dal_obscura.sources.builtins import create_builtin_plugin_registry
from dal_obscura.sources.plugin_runtime import (
    PublicCatalogFactory,
    PublicPluginCatalogAdapter,
    PublicPluginTableFormat,
    _close_plugin_preserving_error,
)
from dal_obscura.sources.plugins import PluginRegistry


def load_source_schema(
    *,
    asset: dict[str, object],
    catalog: dict[str, object],
    options: dict[str, Any],
    plugin_registry: PluginRegistry | None,
    validate_metadata: Callable[[dict[str, Any]], None],
) -> pa.Schema:
    registry = plugin_registry or create_builtin_plugin_registry()
    adapter = PublicPluginCatalogAdapter(
        str(catalog["name"]),
        options,
        str(catalog["plugin_id"]),
        cast(PublicCatalogFactory, registry.load("catalog", str(catalog["plugin_id"]))),
        lambda plugin_id: registry.load("table_format", plugin_id),
        revision=int(cast(int | str, catalog.get("revision", 0))),
    )
    try:
        source = cast(
            PublicPluginTableFormat,
            adapter.resolve_table(str(asset.get("table_identifier") or asset["name"])),
        )
        if source.format != asset["backend"]:
            raise ValueError("Catalog and table-format plugins do not match")
        validate_metadata(cast(dict[str, Any], source.handle.to_json()["metadata"]))
        return source.get_schema()
    finally:
        _close_plugin_preserving_error(adapter)
