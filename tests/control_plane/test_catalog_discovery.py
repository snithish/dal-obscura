from __future__ import annotations

import pytest
from dal_obscura_plugin_api import DiscoveryPage, PluginDescriptor, TableIdentifier

import dal_obscura.control_plane.application.catalog_service as catalog_service
import dal_obscura.control_plane.infrastructure.catalog_discovery as discovery
from dal_obscura.control_plane.infrastructure.catalog_discovery import (
    discover_iceberg_tables,
    discover_public_catalog_tables,
)


class FakeIcebergCatalog:
    def list_namespaces(self, namespace=()):
        if namespace == ():
            return [("default",), ("prod",)]
        return []

    def list_tables(self, namespace):
        if namespace == ("default",):
            return [("default", "users")]
        if namespace == ("prod",):
            return [("prod", "orders")]
        return []


def test_iceberg_discovery_lists_tables_across_namespaces():
    tables = discover_iceberg_tables(
        "analytics",
        {"type": "sql", "uri": "sqlite:///catalog.db"},
        load_catalog_fn=lambda name, **options: FakeIcebergCatalog(),
    )

    assert tables == [
        {"backend": "iceberg", "name": "default.users", "table_identifier": "default.users"},
        {"backend": "iceberg", "name": "prod.orders", "table_identifier": "prod.orders"},
    ]


def test_public_catalog_discovery_uses_admitted_plugin_and_closes_it():
    closed = []
    received_revision = []
    validated = []
    listed_namespaces = []

    class PublicCatalog:
        descriptor = PluginDescriptor(
            kind="catalog",
            plugin_id="fixture.catalog",
            api_version="1",
            config_version=1,
            distribution="fixture",
            version="1.0.0",
        )

        def validate_config(self, context):
            del context
            validated.append(True)

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

    class Registry:
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

    assert tables == [
        {
            "backend": "fixture.catalog",
            "name": "default.users",
            "table_identifier": "default.users",
        }
    ]
    assert received_revision == [4]
    assert validated == [True]
    assert listed_namespaces == [True]
    assert closed == [True]


def test_public_catalog_discovery_rejects_missing_lifecycle_methods() -> None:
    class IncompleteCatalog:
        def list_tables(self, context, *, continuation=None, limit):
            del context, continuation, limit
            return DiscoveryPage(entries=(), continuation=None)

        def close(self):
            return None

    class Registry:
        def load(self, kind, plugin_id):
            del kind, plugin_id
            return lambda config, context: IncompleteCatalog()

    with pytest.raises(ValueError, match="required lifecycle"):
        discover_public_catalog_tables(
            "analytics",
            "fixture.catalog",
            {},
            plugin_registry=Registry(),
        )


def test_public_catalog_discovery_rejects_forged_table_identifiers() -> None:
    class ForgedIdentifier:
        namespace = ("default",)
        name = "users"

    class PublicCatalog:
        def validate_config(self, context):
            del context

        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return (("default",),)

        def list_tables(self, context, *, continuation=None, limit):
            del context, continuation, limit
            return type("Page", (), {"entries": (ForgedIdentifier(),), "continuation": None})()

        def close(self):
            return None

    class Registry:
        def load(self, kind, plugin_id):
            del kind, plugin_id
            return lambda config, context: PublicCatalog()

    with pytest.raises(ValueError, match="invalid table identifier"):
        discover_public_catalog_tables(
            "analytics",
            "fixture.catalog",
            {},
            plugin_registry=Registry(),
        )


def test_iceberg_discovery_rejects_namespace_explosion():
    with pytest.raises(ValueError, match="namespace limit"):
        discover_iceberg_tables(
            "analytics",
            {},
            load_catalog_fn=lambda name, **options: FakeIcebergCatalog(),
            max_namespaces=2,
        )


def test_iceberg_discovery_rejects_table_explosion():
    with pytest.raises(ValueError, match="table limit"):
        discover_iceberg_tables(
            "analytics",
            {},
            load_catalog_fn=lambda name, **options: FakeIcebergCatalog(),
            max_tables=1,
        )


def test_iceberg_discovery_stops_an_unbounded_provider_page():
    class EndlessCatalog:
        def list_namespaces(self, namespace=()):
            if namespace == ():
                return (("ns", str(index)) for index in range(100_000))
            return ()

        def list_tables(self, namespace):
            return ()

    with pytest.raises(ValueError, match="namespace limit"):
        discover_iceberg_tables(
            "analytics",
            {},
            load_catalog_fn=lambda name, **options: EndlessCatalog(),
            max_namespaces=4,
        )


def test_iceberg_discovery_honors_cancellation_and_deadline():
    class SlowCatalog:
        def list_namespaces(self, namespace=()):
            return (("ns", index) for index in range(100_000))

        def list_tables(self, namespace):
            return ()

    with pytest.raises(RuntimeError, match="cancelled"):
        discover_iceberg_tables(
            "analytics",
            {},
            load_catalog_fn=lambda name, **options: SlowCatalog(),
            cancel_check=lambda: True,
        )

    with pytest.raises(TimeoutError, match="deadline"):
        discover_iceberg_tables(
            "analytics",
            {},
            load_catalog_fn=lambda name, **options: SlowCatalog(),
            deadline_at=0.0,
        )


def test_iceberg_discovery_rejects_malformed_provider_identifier_segments():
    class MalformedCatalog:
        def list_namespaces(self, namespace=()):
            if namespace == ():
                return [("default", 7)]
            return []

        def list_tables(self, namespace):
            del namespace
            return [("default", "users\n")]

    with pytest.raises(ValueError, match="invalid namespace"):
        discover_iceberg_tables(
            "analytics",
            {},
            load_catalog_fn=lambda name, **options: MalformedCatalog(),
        )


def test_iceberg_discovery_rejects_when_process_capacity_is_exhausted(
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
        discover_iceberg_tables("analytics", {})


def test_iceberg_discovery_releases_capacity_after_provider_failure(
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

    with pytest.raises(RuntimeError, match="provider failed"):
        discover_iceberg_tables(
            "analytics",
            {},
            load_catalog_fn=lambda name, **options: (_ for _ in ()).throw(
                RuntimeError("provider failed")
            ),
        )

    assert (slots.acquired, slots.released) == (1, 1)


def test_workspace_discovery_limits_each_authenticated_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(catalog_service, "_SESSION_DISCOVERY_SLOTS", {})
    first = catalog_service._admit_session_discovery("issuer|operator")
    second = catalog_service._admit_session_discovery("issuer|operator")
    first.__enter__()
    second.__enter__()
    try:
        with pytest.raises(
            catalog_service.ValidationFailure, match="session capacity"
        ), catalog_service._admit_session_discovery("issuer|operator"):
            pass
    finally:
        second.__exit__(None, None, None)
        first.__exit__(None, None, None)
    with catalog_service._admit_session_discovery("issuer|operator"):
        pass
