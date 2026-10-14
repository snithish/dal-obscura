from datetime import UTC, datetime, timedelta

import pytest
from dal_obscura_iceberg.catalog import IcebergCatalog
from dal_obscura_plugin_api import CatalogConfig, ExecutionContext, TableIdentifier


def context(seconds=30):
    return ExecutionContext(
        deadline=datetime.now(UTC) + timedelta(seconds=seconds),
        correlation_id="sql-discovery",
    )


def catalog(monkeypatch, provider, operation):
    monkeypatch.setattr("dal_obscura_iceberg.catalog._load_iceberg_catalog", lambda *args: provider)
    return IcebergCatalog(
        CatalogConfig(plugin_id="iceberg.catalog", instance_id="analytics", revision=1), operation
    )


def test_sql_discovery_preserves_literal_names_and_materializes_once_per_operation(monkeypatch):
    calls = []

    class Provider:
        def list_namespaces(self, namespace):
            return [("weird.ns",)] if not namespace else []

        def list_tables(self, namespace):
            calls.append(namespace)
            return [("weird.ns", name) for name in ("a.b", "c", "d")] if namespace else []

    operation = context()
    plugin = catalog(monkeypatch, Provider(), operation)
    first = plugin.list_tables(operation, limit=2)
    second = plugin.list_tables(operation, continuation=first.continuation, limit=2)
    assert first.entries + second.entries == tuple(
        TableIdentifier(namespace=("weird.ns",), name=name) for name in ("a.b", "c", "d")
    )
    assert calls == [(), ("weird.ns",)]
    plugin.list_tables(context(), limit=2)
    assert len(calls) == 4


def test_sql_discovery_stops_namespace_traversal_at_deadline(monkeypatch):
    visits = []
    now = [0.0]
    monkeypatch.setattr("dal_obscura_iceberg.catalog.monotonic", lambda: now[0])
    monkeypatch.setattr("dal_obscura_iceberg.discovery.monotonic", lambda: now[0])

    class Provider:
        def list_namespaces(self, namespace):
            if namespace:
                return []

            def slow_namespaces():
                for index in range(3):
                    now[0] += 31
                    visits.append(index)
                    yield (str(index),)

            return slow_namespaces()

        def list_tables(self, namespace):
            return []

    operation = context()
    plugin = catalog(monkeypatch, Provider(), operation)
    with pytest.raises(TimeoutError, match="deadline"):
        plugin.list_tables(operation)
    assert visits == [0]


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

    from dal_obscura_iceberg import catalog as iceberg_catalog
    from dal_obscura_plugin_api import CatalogConfig, ExecutionContext
    from dal_obscura_plugin_api.execution import close_preserving_error
    from dal_obscura_plugin_api.identifiers import identifier_name

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
                            "name": identifier_name(item),
                            "table_identifier": identifier_name(item),
                        }
                        for item in identifiers
                    ]
                continuation = page.continuation
        finally:
            close_preserving_error(plugin)


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


def test_iceberg_discovery_closes_provider_after_success() -> None:
    closed: list[bool] = []

    class ClosableCatalog(FakeIcebergCatalog):
        def close(self) -> None:
            closed.append(True)

    discover_iceberg_tables(
        "analytics", {}, load_catalog_fn=lambda name, **options: ClosableCatalog()
    )

    assert closed == [True]


def test_iceberg_discovery_closes_provider_after_failure() -> None:
    closed: list[bool] = []

    class ClosableCatalog(FakeIcebergCatalog):
        def list_namespaces(self, namespace=()):
            del namespace
            raise RuntimeError("provider failed")

        def close(self) -> None:
            closed.append(True)

    with pytest.raises(RuntimeError, match="provider failed"):
        discover_iceberg_tables(
            "analytics", {}, load_catalog_fn=lambda name, **options: ClosableCatalog()
        )

    assert closed == [True]


def test_iceberg_discovery_rejects_root_only_list_tables_signature() -> None:
    class RootOnlyCatalog:
        def list_namespaces(self, namespace=()):
            return [] if namespace else [()]

        def list_tables(self):
            return [("users",)]

    with pytest.raises(TypeError):
        discover_iceberg_tables(
            "analytics", {}, load_catalog_fn=lambda name, **options: RootOnlyCatalog()
        )


def test_iceberg_discovery_skips_pyiceberg_empty_root_namespace() -> None:
    class IcebergCatalog:
        def list_namespaces(self, namespace=()):
            return [("retail",)] if not namespace else []

        def list_tables(self, namespace):
            if not namespace:
                raise ValueError("Empty namespace identifier")
            return [("retail", "customer_revenue")]

    assert discover_iceberg_tables(
        "analytics", {}, load_catalog_fn=lambda name, **options: IcebergCatalog()
    ) == [
        {
            "backend": "iceberg",
            "name": "retail.customer_revenue",
            "table_identifier": "retail.customer_revenue",
        }
    ]


def test_iceberg_discovery_does_not_hide_root_table_provider_errors() -> None:
    class FailingCatalog:
        def list_namespaces(self, namespace=()):
            return [] if namespace else [()]

        def list_tables(self, namespace):
            del namespace
            raise RuntimeError("catalog unavailable")

    with pytest.raises(RuntimeError, match="catalog unavailable"):
        discover_iceberg_tables(
            "analytics", {}, load_catalog_fn=lambda name, **options: FailingCatalog()
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

    with pytest.raises(InterruptedError, match="cancelled"):
        discover_iceberg_tables(
            "analytics",
            {},
            load_catalog_fn=lambda name, **options: SlowCatalog(),
            cancel_check=lambda: True,
        )

    with pytest.raises(TimeoutError, match="deadline"):
        discover_iceberg_tables(
            "analytics", {}, load_catalog_fn=lambda name, **options: SlowCatalog(), deadline_at=0.0
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
            "analytics", {}, load_catalog_fn=lambda name, **options: MalformedCatalog()
        )
