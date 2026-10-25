from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from threading import RLock

from dal_obscura_plugin_api import ExecutionContext
from dal_obscura_plugin_api.execution import close_all_preserving_error
from dal_obscura_plugin_api.storage import PathRuleEnforcer

from dal_obscura.adapters.providers.adapters import PublicPluginCatalogAdapter
from dal_obscura.application.reads.sources import (
    CatalogTableListing,
    Source,
)
from dal_obscura.plugins import PluginRegistry


@dataclass(frozen=True)
class CatalogConfig:
    """One published catalog instance bound to an admitted SDK factory."""

    name: str
    options: Mapping[str, object] = field(default_factory=dict)
    path_enforcer: PathRuleEnforcer | None = None
    plugin_id: str = "iceberg.catalog"
    revision: int = 0

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("Catalog configuration requires a non-empty logical name")
        if self.revision < 0:
            raise ValueError("Catalog configuration revision cannot be negative")


@dataclass(frozen=True)
class ServiceConfig:
    """Live catalog configuration installed in a data-plane registry."""

    catalogs: dict[str, CatalogConfig]


class CatalogRegistry:
    """Resolves configured catalog targets into executable table formats."""

    def __init__(
        self,
        config: ServiceConfig,
        *,
        plugin_registry: PluginRegistry,
        context: ExecutionContext | None = None,
    ) -> None:
        self._plugin_registry = plugin_registry
        self._closed = False
        self._catalogs: dict[str, PublicPluginCatalogAdapter] = {}
        try:
            for name, catalog_config in config.catalogs.items():
                self._catalogs[name] = _build_catalog(
                    catalog_config, plugin_registry=self._plugin_registry, context=context
                )
        except BaseException:
            close_all_preserving_error(self._catalogs.values())
            raise
        self._swap_lock = RLock()

    def resolve(
        self,
        catalog: str | None,
        target: str,
        *,
        context: ExecutionContext | None = None,
    ) -> Source:
        self._ensure_open()
        if catalog is None:
            raise ValueError("Catalog name is required to resolve a target")
        with self._swap_lock:
            implementation = self._catalogs.get(catalog)
            if implementation is None:
                raise ValueError(f"Unknown catalog: {catalog}")
            return implementation.resolve_table(target, context=context)

    def list_tables(self, catalog_name: str) -> list[CatalogTableListing]:
        self._ensure_open()
        with self._swap_lock:
            implementation = self._catalogs.get(catalog_name)
            if implementation is None:
                raise ValueError(f"Unknown catalog: {catalog_name}")
            return implementation.list_tables()

    def close(self) -> None:
        with self._swap_lock:
            if self._closed:
                return
            self._closed = True
            catalogs = tuple(self._catalogs.values())
            self._catalogs = {}
        close_all_preserving_error(catalogs)

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError("Catalog registry is closed")


def _build_catalog(
    config: CatalogConfig,
    *,
    plugin_registry: PluginRegistry,
    context: ExecutionContext | None = None,
) -> PublicPluginCatalogAdapter:
    factory = plugin_registry.load("catalog", config.plugin_id)
    return PublicPluginCatalogAdapter(
        config.name,
        config.options,
        config.plugin_id,
        factory,
        lambda plugin_id: plugin_registry.load("table_format", plugin_id),
        config.path_enforcer,
        config.revision,
        plugin_registry.format_options,
        context=context,
    )
