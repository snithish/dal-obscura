from __future__ import annotations

import pytest
from dal_obscura_plugin_api import DiscoveryPage, PluginDescriptor, TableIdentifier

import dal_obscura.adapters.providers.discovery as discovery
import dal_obscura.control.catalogs.service as catalog_service
from dal_obscura.adapters.providers.discovery import (
    discover_public_catalog_tables,
)
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


def test_public_catalog_discovery_rejects_when_process_capacity_is_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class OccupiedSlots:
        def acquire(self, *, blocking: bool) -> bool:
            assert blocking is False
            return False

        def release(self) -> None:
            raise AssertionError("an unacquired slot must not be released")

    monkeypatch.setattr(discovery, "_DISCOVERY_SLOTS", OccupiedSlots())

    with pytest.raises(RuntimeError, match="capacity is exhausted"):
        discover_public_catalog_tables(
            "analytics", "fixture.catalog", {}, plugin_registry=PluginRegistry()
        )


def test_public_catalog_discovery_releases_capacity_after_factory_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Slots:
        acquired = 0
        released = 0

        def acquire(self, *, blocking: bool) -> bool:
            assert blocking is False
            self.acquired += 1
            return True

        def release(self) -> None:
            self.released += 1

    slots = Slots()
    monkeypatch.setattr(discovery, "_DISCOVERY_SLOTS", slots)

    class Registry(PluginRegistry):
        def load(self, kind, plugin_id):
            del kind, plugin_id
            raise RuntimeError("factory lookup failed")

    with pytest.raises(RuntimeError, match="factory lookup failed"):
        discover_public_catalog_tables(
            "analytics", "fixture.catalog", {}, plugin_registry=Registry()
        )

    assert (slots.acquired, slots.released) == (1, 1)


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


def test_workspace_discovery_limits_each_authenticated_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(catalog_service, "_SESSION_DISCOVERY_SLOTS", {})
    first = catalog_service._admit_session_discovery("issuer|operator")
    second = catalog_service._admit_session_discovery("issuer|operator")
    first.__enter__()
    second.__enter__()
    try:
        with (
            pytest.raises(catalog_service.ValidationFailure, match="session capacity"),
            catalog_service._admit_session_discovery("issuer|operator"),
        ):
            pass
    finally:
        second.__exit__(None, None, None)
        first.__exit__(None, None, None)
    with catalog_service._admit_session_discovery("issuer|operator"):
        pass


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
