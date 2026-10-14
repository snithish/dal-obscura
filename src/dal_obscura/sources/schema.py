"""Schema discovery uses the same admitted source lifecycle as governed reads."""

from collections.abc import Callable
from typing import cast

import pyarrow as pa
from dal_obscura_plugin_api.execution import close_preserving_error

from dal_obscura.plugins import PluginRegistry
from dal_obscura.sources.adapters import PublicPluginCatalogAdapter


def load_source_schema(
    *,
    asset: dict[str, object],
    catalog: dict[str, object],
    options: dict[str, object],
    plugin_registry: PluginRegistry | None,
    validate_metadata: Callable[[dict[str, object]], None],
) -> pa.Schema:
    if plugin_registry is None:
        raise ValueError("Provider IO requires a startup plugin registry")
    registry = plugin_registry
    adapter = PublicPluginCatalogAdapter(
        str(catalog["name"]),
        options,
        str(catalog["plugin_id"]),
        registry.load("catalog", str(catalog["plugin_id"])),
        lambda plugin_id: registry.load("table_format", plugin_id),
        revision=int(cast(int | str, catalog.get("revision", 0))),
        format_options=registry.format_options,
    )
    try:
        source = adapter.resolve_table(str(asset.get("table_identifier") or asset["name"]))
        if source.format != asset["backend"]:
            raise ValueError("Catalog and table-format plugins do not match")
        validate_metadata(dict(source.handle.metadata))
        return source.get_schema()
    finally:
        close_preserving_error(adapter)
