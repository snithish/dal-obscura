"""Deliberately invalid provider output for trust-boundary tests."""

from datetime import UTC

from dal_obscura_plugin_api import DiscoveryPage, PluginDescriptor, TableIdentifier

from dal_obscura.sources.plugins import PluginRegistry


class PagedCatalogRegistry(PluginRegistry):
    """A virtual large catalog: only the requested page is constructed."""

    descriptor = PluginDescriptor(
        kind="catalog",
        plugin_id="fixture.catalog",
        api_version="2",
        config_version=1,
        distribution="fixture",
        version="1.0.0",
        output_formats=frozenset({"iceberg"}),
        handle_versions=frozenset({1}),
        capabilities=frozenset({"snapshot_reads"}),
    )

    def __init__(self, *, total=100_000, on_list=None):
        self.total = total
        self.on_list = on_list
        self.requests = []
        self.closed = 0

    def admitted(self):
        from dal_obscura.sources.builtins import create_builtin_plugin_registry

        return {
            **create_builtin_plugin_registry().admitted(),
            ("catalog", "fixture.catalog"): self.descriptor,
        }

    def load(self, kind, plugin_id):
        assert (kind, plugin_id) == ("catalog", "fixture.catalog")
        registry = self

        class Catalog:
            def list_namespaces(self, context, *, namespace=()):
                raise AssertionError("table paging must not enumerate namespaces")

            def list_tables(self, context, *, continuation=None, limit=500):
                if registry.on_list is not None:
                    registry.on_list()
                registry.requests.append((continuation, limit))
                start = int(continuation or 0)
                end = min(start + limit, registry.total)
                return DiscoveryPage(
                    tuple(
                        TableIdentifier(("default",), f"table_{i:06}") for i in range(start, end)
                    ),
                    str(end) if end < registry.total else None,
                )

            def close(self):
                registry.closed += 1

        return lambda config, context: Catalog()


def discover_iceberg_tables(
    catalog_name,
    options,
    *,
    load_catalog_fn,
    max_namespaces=1000,
    max_tables=10000,
    deadline_at=None,
    cancel_check=None,
):
    """Drive the actual SQL SDK plugin with a replaceable provider I/O boundary."""
    from datetime import datetime, timedelta
    from time import monotonic
    from unittest.mock import patch

    from dal_obscura_plugin_api import CatalogConfig, ExecutionContext

    from dal_obscura.sources import iceberg_catalog
    from dal_obscura.sources.plugin_runtime import _close_plugin_preserving_error, _identifier_name

    remaining = 30 if deadline_at is None else deadline_at - monotonic()
    context = ExecutionContext(
        deadline=datetime.now(UTC) + timedelta(seconds=remaining),
        correlation_id="sql-provider-contract",
        cancel_check=cancel_check,
    )
    with (
        patch.object(
            iceberg_catalog,
            "_load_iceberg_catalog",
            lambda name, values: load_catalog_fn(name, **values),
        ),
        patch.object(iceberg_catalog, "DEFAULT_MAX_NAMESPACES", max_namespaces),
        patch.object(iceberg_catalog, "DEFAULT_MAX_TABLES", max_tables),
    ):
        plugin = iceberg_catalog.IcebergCatalog(
            CatalogConfig(
                plugin_id="iceberg.catalog", instance_id=catalog_name, revision=0, options=options
            ),
            context,
        )
        try:
            identifiers = []
            continuation = None
            while True:
                page = plugin.list_tables(context, continuation=continuation)
                identifiers.extend(page.entries)
                if page.continuation is None:
                    return [
                        {
                            "backend": "iceberg",
                            "name": _identifier_name(item),
                            "table_identifier": _identifier_name(item),
                        }
                        for item in identifiers
                    ]
                continuation = page.continuation
        finally:
            _close_plugin_preserving_error(plugin)


def _unchecked_discovery_page(entries: object, continuation: object) -> DiscoveryPage:
    page = object.__new__(DiscoveryPage)
    object.__setattr__(page, "entries", entries)
    object.__setattr__(page, "continuation", continuation)
    return page
