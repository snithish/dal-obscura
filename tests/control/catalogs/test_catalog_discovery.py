from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from uuid import uuid4

import pytest
from dal_obscura_plugin_api import CatalogConfig, DiscoveryPage, PluginDescriptor, TableIdentifier

import dal_obscura.adapters.providers.discovery as discovery
import dal_obscura.adapters.providers.governance as catalog_service
from dal_obscura.adapters.providers.discovery import (
    discover_public_catalog_tables,
)
from dal_obscura.application.governance.values import CatalogDiscovery
from dal_obscura.plugins import PluginRegistry
from tests.support.sources.discovery import (
    PagedCatalogRegistry,
    _unchecked_discovery_page,
)


def test_public_discovery_reads_only_the_requested_page_of_a_large_catalog():
    registry = PagedCatalogRegistry()
    page = discover_public_catalog_tables(
        "analytics",
        "fixture.catalog",
        {},
        plugin_registry=registry,
        continuation="99950",
        limit=25,
    )
    assert page.entries == tuple(
        TableIdentifier(("default",), f"table_{i:06}") for i in range(99950, 99975)
    )
    assert page.continuation == "99975"
    assert registry.requests == [("99950", 25)]
    assert registry.closed == 1


def test_public_catalog_discovery_uses_admitted_plugin_and_closes_it():
    closed = []
    received_revision = []
    listed_namespaces = []

    class PublicCatalog:
        descriptor = PluginDescriptor(
            kind="catalog",
            plugin_id="fixture.catalog",
            api_version="3",
            config_version=1,
            distribution="fixture",
            version="1.0.0",
        )

        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            listed_namespaces.append(True)
            return (("default",),)

        def list_tables(self, context, *, continuation=None, limit):
            del context, limit
            entries = (
                (TableIdentifier(namespace=("default",), name="orders"),)
                if continuation is not None
                else (TableIdentifier(namespace=("default",), name="users"),)
            )
            return DiscoveryPage(entries=entries, continuation=None)

        def close(self):
            closed.append(True)

    class Registry(PluginRegistry):
        def load(self, kind, plugin_id):
            assert (kind, plugin_id) == ("catalog", "fixture.catalog")

            def factory(config, context):
                del context
                received_revision.append(config.revision)
                return PublicCatalog()

            return factory

    tables = discover_public_catalog_tables(
        "analytics",
        "fixture.catalog",
        {"uri": "https://catalog.example"},
        revision=4,
        plugin_registry=Registry(),
    )

    assert tables == DiscoveryPage((TableIdentifier(("default",), "users"),))
    assert received_revision == [4]
    assert listed_namespaces == []
    assert closed == [True]


def test_public_catalog_discovery_releases_capacity_when_close_fails() -> None:
    class FailingCloseCatalog:
        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return ()

        def list_tables(self, context, *, continuation=None, limit):
            del context, continuation, limit
            return DiscoveryPage(entries=(), continuation=None)

        def close(self):
            raise RuntimeError("close failed")

    class HealthyCatalog(FailingCloseCatalog):
        def close(self):
            return None

    class Registry(PluginRegistry):
        def __init__(self) -> None:
            self.calls = 0

        def load(self, kind, plugin_id):
            assert (kind, plugin_id) == ("catalog", "fixture.catalog")

            def factory(config, context):
                del config, context
                self.calls += 1
                return FailingCloseCatalog() if self.calls <= 8 else HealthyCatalog()

            return factory

    registry = Registry()
    for _ in range(8):
        with pytest.raises(RuntimeError, match="close failed"):
            discover_public_catalog_tables(
                "analytics", "fixture.catalog", {}, plugin_registry=registry
            )

    assert discover_public_catalog_tables(
        "analytics", "fixture.catalog", {}, plugin_registry=registry
    ) == DiscoveryPage(())


def test_public_catalog_discovery_rejects_missing_lifecycle_methods() -> None:
    class IncompleteCatalog:
        def list_tables(self, context, *, continuation=None, limit):
            del context, continuation, limit
            return DiscoveryPage(entries=(), continuation=None)

        def close(self):
            return None

    class Registry(PluginRegistry):
        def load(self, kind, plugin_id):
            del kind, plugin_id
            return lambda config, context: IncompleteCatalog()

    with pytest.raises(ValueError, match="required lifecycle"):
        discover_public_catalog_tables(
            "analytics", "fixture.catalog", {}, plugin_registry=Registry()
        )


def test_public_catalog_discovery_rejects_when_process_capacity_is_exhausted():
    capacity = discovery.DEFAULT_MAX_ACTIVE_DISCOVERIES
    entered = Barrier(capacity + 1)
    release = Event()

    def blocked():
        entered.wait(timeout=10)
        assert release.wait(10), "Provider calls were not released"

    registry = PagedCatalogRegistry(total=1, on_list=blocked)
    with ThreadPoolExecutor(max_workers=capacity) as workers:
        futures = [
            workers.submit(
                discover_public_catalog_tables,
                "analytics",
                "fixture.catalog",
                {},
                plugin_registry=registry,
            )
            for _ in range(capacity)
        ]
        try:
            entered.wait(timeout=10)
            with pytest.raises(RuntimeError, match="capacity is exhausted"):
                discover_public_catalog_tables(
                    "analytics", "fixture.catalog", {}, plugin_registry=registry
                )
        finally:
            release.set()
        assert all(future.result(timeout=10).entries for future in futures)
    registry.on_list = None
    assert discover_public_catalog_tables(
        "analytics", "fixture.catalog", {}, plugin_registry=registry
    ).entries


def test_public_catalog_discovery_releases_capacity_after_factory_failure():
    class Registry(PluginRegistry):
        def load(self, kind, plugin_id):
            raise RuntimeError("factory lookup failed")

    for _ in range(discovery.DEFAULT_MAX_ACTIVE_DISCOVERIES + 1):
        with pytest.raises(RuntimeError, match="factory lookup failed"):
            discover_public_catalog_tables(
                "analytics", "fixture.catalog", {}, plugin_registry=Registry()
            )
    assert discover_public_catalog_tables(
        "analytics", "fixture.catalog", {}, plugin_registry=PagedCatalogRegistry(total=1)
    ).entries


def test_public_catalog_discovery_rejects_forged_table_identifiers() -> None:
    class ForgedIdentifier:
        namespace = ("default",)
        name = "users"

    class PublicCatalog:
        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return (("default",),)

        def list_tables(self, context, *, continuation=None, limit):
            del context, continuation, limit
            return _unchecked_discovery_page((ForgedIdentifier(),), None)

        def close(self):
            return None

    class Registry(PluginRegistry):
        def load(self, kind, plugin_id):
            del kind, plugin_id
            return lambda config, context: PublicCatalog()

    with pytest.raises(ValueError, match="invalid table identifier"):
        discover_public_catalog_tables(
            "analytics", "fixture.catalog", {}, plugin_registry=Registry()
        )


def test_public_catalog_discovery_rejects_oversized_page() -> None:
    class PublicCatalog:
        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return ()

        def list_tables(self, context, *, continuation=None, limit):
            del context, continuation, limit
            entries = [TableIdentifier(namespace=("default",), name=str(i)) for i in range(501)]
            return _unchecked_discovery_page(tuple(entries), None)

        def close(self):
            return None

    class Registry(PluginRegistry):
        def load(self, kind, plugin_id):
            del kind, plugin_id
            return lambda config, context: PublicCatalog()

    with pytest.raises(ValueError, match="too many page entries"):
        discover_public_catalog_tables(
            "analytics", "fixture.catalog", {}, plugin_registry=Registry()
        )


def test_workspace_discovery_limits_each_authenticated_session():
    capacity = catalog_service.DEFAULT_MAX_ACTIVE_DISCOVERIES_PER_SESSION
    entered = Barrier(capacity + 1)
    release = Event()
    session_key = "session-" + uuid4().hex

    def blocked():
        entered.wait(timeout=10)
        assert release.wait(10), "Session discoveries were not released"

    registry = PagedCatalogRegistry(total=1, on_list=blocked)
    browser = catalog_service.ProviderCatalogBrowser(plugin_registry=registry, secret_provider=None)
    captured = CatalogDiscovery(uuid4(), CatalogConfig("fixture.catalog", "analytics", 0, {}))
    with ThreadPoolExecutor(max_workers=capacity) as workers:
        futures = [
            workers.submit(browser.discover, captured, session_key=session_key)
            for _ in range(capacity)
        ]
        try:
            entered.wait(timeout=10)
            with pytest.raises(catalog_service.ValidationFailure, match="session capacity"):
                browser.discover(captured, session_key=session_key)
            # Other callers retain their own admission budget.
            registry.on_list = None
            assert browser.discover(captured, session_key="other-" + session_key).entries
        finally:
            release.set()
        assert all(future.result(timeout=10).entries for future in futures)
    assert browser.discover(captured, session_key=session_key).entries


def test_public_discovery_preserves_failure_when_provider_close_also_fails() -> None:
    class Catalog:
        def list_namespaces(self, context, *, namespace=()):
            return ()

        def list_tables(self, context, *, continuation=None, limit):
            raise ValueError("listing failed")

        def close(self):
            raise RuntimeError("cleanup failed")

    class Registry(PluginRegistry):
        def load(self, kind, plugin_id):
            return lambda config, context: Catalog()

    with pytest.raises(ValueError, match="listing failed"):
        discover_public_catalog_tables("example", "fixture.catalog", {}, plugin_registry=Registry())
