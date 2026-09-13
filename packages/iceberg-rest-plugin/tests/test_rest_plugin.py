from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from dal_obscura_iceberg_rest.catalog import RestCatalog
from dal_obscura_plugin_api import CatalogConfig, ExecutionContext, TableIdentifier


def _context() -> ExecutionContext:
    return ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(minutes=1),
        correlation_id="rest-fixture",
    )


def _config(**options: object) -> CatalogConfig:
    return CatalogConfig(
        plugin_id="iceberg.rest",
        instance_id="fixture",
        revision=1,
        options={"uri": "https://catalog.example/v1", **options},
    )


def test_rest_catalog_rejects_credential_bearing_uri():
    with pytest.raises(ValueError, match="credentials"):
        RestCatalog(
            _config(uri="https://user:pass@catalog.example/v1"),
            _context(),
        )


def test_rest_catalog_rejects_unsupported_options():
    with pytest.raises(ValueError, match="unsupported keys"):
        RestCatalog(_config(script="import os"), _context())


def test_rest_catalog_context_cancellation_is_fail_closed():
    context = ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(minutes=1),
        correlation_id="rest-cancel",
        cancel_check=lambda: True,
    )
    with pytest.raises(ValueError, match="cancelled"):
        RestCatalog(_config(), context)


def test_rest_catalog_paginates_bounded_sorted_identifiers():
    plugin = RestCatalog(_config(), _context())

    class FakeCatalog:
        def list_namespaces(self):
            return [("default",)]

        def list_tables(self, namespace):
            assert namespace == ("default",)
            return [("default", "b"), ("default", "a")]

    plugin._catalog = FakeCatalog()
    first = plugin.list_tables(_context(), limit=1)
    second = plugin.list_tables(_context(), continuation=first.continuation, limit=1)
    assert [item.name for item in first.entries] == ["a"]
    assert [item.name for item in second.entries] == ["b"]
    assert second.continuation is None


def test_rest_catalog_does_not_copy_provider_io_credentials_into_handle():
    plugin = RestCatalog(_config(), _context())

    class FakeSnapshot:
        snapshot_id = 42

    class FakeTable:
        metadata_location = "https://storage.example/metadata/v1.json"
        current_snapshot = FakeSnapshot()
        io = type("IO", (), {"properties": {"s3.access-key-id": "secret"}})()

    class FakeCatalog:
        def load_table(self, identifier):
            assert identifier == ("default", "users")
            return FakeTable()

    plugin._catalog = FakeCatalog()
    handle = plugin.resolve_table(
        TableIdentifier(namespace=("default",), name="users"),
        _context(),
    )
    assert handle.metadata == {"metadata_location": "https://storage.example/metadata/v1.json"}


def test_rest_catalog_bounds_namespace_listing():
    plugin = RestCatalog(_config(), _context())

    class FakeCatalog:
        def list_namespaces(self):
            yield from (("ns", str(index)) for index in range(10_001))

        def list_tables(self, namespace):
            return []

    plugin._catalog = FakeCatalog()
    with pytest.raises(ValueError, match="namespaces"):
        plugin.list_tables(_context(), limit=10)
