"""Deliberately invalid provider output for trust-boundary tests."""

from dal_obscura_plugin_api import DiscoveryPage, PluginDescriptor, TableIdentifier

from dal_obscura.plugins import PluginRegistry


class PagedCatalogRegistry(PluginRegistry):
    """A virtual large catalog: only the requested page is constructed."""

    descriptor = PluginDescriptor(
        kind="catalog",
        plugin_id="fixture.catalog",
        api_version="3",
        config_version=1,
        distribution="fixture",
        version="1.0.0",
        output_formats=frozenset({"iceberg"}),
        handle_versions=frozenset({1}),
        capabilities=frozenset({"snapshot_reads"}),
    )

    def __init__(self, *, total=100_000, on_list=None):
        super().__init__()
        self.total = total
        self.on_list = on_list
        self.requests = []
        self.closed = 0

    def admitted(self):
        from dal_obscura.plugins.bootstrap import create_plugin_registry

        return {
            **create_plugin_registry().admitted(),
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


def _unchecked_discovery_page(entries: object, continuation: object) -> DiscoveryPage:
    page = object.__new__(DiscoveryPage)
    object.__setattr__(page, "entries", entries)
    object.__setattr__(page, "continuation", continuation)
    return page
