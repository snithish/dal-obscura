from datetime import datetime, timedelta, timezone

import pytest
from dal_obscura_plugin_api import CatalogConfig, ExecutionContext, TableIdentifier

from dal_obscura.sources.sql_catalog import SqlCatalog


def context(seconds=30):
    return ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(seconds=seconds),
        correlation_id="sql-discovery",
    )


def catalog(monkeypatch, provider, operation):
    monkeypatch.setattr(
        "dal_obscura.sources.sql_catalog._load_iceberg_catalog", lambda *args: provider
    )
    return SqlCatalog(
        CatalogConfig(plugin_id="iceberg.sql", instance_id="analytics", revision=1), operation
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
    assert first.entries[0] == TableIdentifier(namespace=("weird.ns",), name="a.b")
    assert len(first.entries) + len(second.entries) == 3
    assert calls == [(), ("weird.ns",)]
    plugin.list_tables(context(), limit=2)
    assert len(calls) == 4


def test_sql_discovery_stops_namespace_traversal_at_deadline(monkeypatch):
    visits = []
    now = [0.0]
    monkeypatch.setattr("dal_obscura.sources.sql_catalog.monotonic", lambda: now[0])
    monkeypatch.setattr("dal_obscura.sources.discovery.monotonic", lambda: now[0])

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
