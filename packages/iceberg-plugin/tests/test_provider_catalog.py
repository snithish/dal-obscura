"""Provider configuration and bounded catalog discovery through the public SDK."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest
from dal_obscura_iceberg import catalog as iceberg_catalog
from dal_obscura_iceberg.catalog import IcebergCatalog
from dal_obscura_plugin_api import CatalogConfig, ExecutionContext, TableIdentifier


def _context():
    return ExecutionContext(datetime.now(UTC) + timedelta(minutes=1), "catalog")


def _catalog(*, name, options):
    return IcebergCatalog(CatalogConfig("iceberg.catalog", name, 1, options), _context())


def test_iceberg_catalog_uses_provider_catalog_name_from_options(monkeypatch):
    loaded: dict[str, object] = {}

    class FakePyIcebergCatalog:
        def load_table(self, table_identifier: str) -> object:
            loaded["table_identifier"] = table_identifier
            return FakePyIcebergTable()

    def fake_load_catalog(catalog_name: str, options: dict[str, Any]) -> object:
        loaded["catalog_name"] = catalog_name
        loaded["options"] = options
        return FakePyIcebergCatalog()

    monkeypatch.setattr(iceberg_catalog, "_load_iceberg_catalog", fake_load_catalog)

    catalog = _catalog(
        name="analytics",
        options={"provider_catalog_name": "prod_glue", "type": "glue"},
    )

    descriptor = catalog.resolve_table(TableIdentifier(("default",), "users"), _context())

    assert loaded == {
        "catalog_name": "prod_glue",
        "options": {"type": "glue"},
        "table_identifier": ("default", "users"),
    }
    assert descriptor.catalog_instance_id == "analytics"
    assert descriptor.identifier == TableIdentifier(namespace=("default",), name="users")


def test_iceberg_catalog_rejects_unsupported_catalog_option() -> None:
    with pytest.raises(ValueError, match="Unsupported catalog option: catalog_name"):
        _catalog(name="analytics", options={"catalog_name": "old-provider-name"})


@pytest.mark.parametrize("bad_namespace", [("prod", 7), "prod..staging", ""])
def test_iceberg_registry_rejects_malformed_provider_namespaces(monkeypatch, bad_namespace):
    class MalformedCatalog:
        def list_namespaces(self, namespace):
            del namespace
            return [bad_namespace]

        def list_tables(self, namespace):
            del namespace
            return []

    monkeypatch.setattr(
        iceberg_catalog,
        "_load_iceberg_catalog",
        lambda catalog_name, options: MalformedCatalog(),
    )
    catalog = _catalog(name="analytics", options={})

    with pytest.raises(ValueError, match="invalid namespace"):
        catalog.list_tables(_context())


@pytest.mark.parametrize("bad_identifier", [("prod", 7), "prod..orders", "orders\n"])
def test_iceberg_registry_rejects_malformed_provider_table_identifiers(monkeypatch, bad_identifier):
    class MalformedCatalog:
        def list_namespaces(self, namespace):
            del namespace
            return [()]

        def list_tables(self, namespace):
            del namespace
            return [bad_identifier]

    monkeypatch.setattr(
        iceberg_catalog,
        "_load_iceberg_catalog",
        lambda catalog_name, options: MalformedCatalog(),
    )
    catalog = _catalog(name="analytics", options={})

    with pytest.raises(ValueError, match="invalid table identifier"):
        catalog.list_tables(_context())


def test_iceberg_registry_bounds_unbounded_namespace_providers(monkeypatch):
    class EndlessCatalog:
        def list_namespaces(self, namespace):
            del namespace
            return (("namespace", str(index)) for index in range(100_000))

        def list_tables(self, namespace):
            del namespace
            return []

    monkeypatch.setattr(
        iceberg_catalog,
        "_load_iceberg_catalog",
        lambda catalog_name, options: EndlessCatalog(),
    )
    catalog = _catalog(name="analytics", options={})

    with pytest.raises(ValueError, match=r"namespace limit"):
        catalog.list_tables(_context())


def test_iceberg_registry_bounds_unbounded_table_providers(monkeypatch):
    class EndlessCatalog:
        def list_namespaces(self, namespace):
            del namespace
            return [()]

        def list_tables(self, namespace):
            del namespace
            return (("users", str(index)) for index in range(100_000))

    monkeypatch.setattr(
        iceberg_catalog,
        "_load_iceberg_catalog",
        lambda catalog_name, options: EndlessCatalog(),
    )
    catalog = _catalog(name="analytics", options={})

    with pytest.raises(ValueError, match="table limit"):
        catalog.list_tables(_context())


class FakePyIcebergTable:
    metadata = SimpleNamespace(current_snapshot_id=None)
    metadata_location = "s3://warehouse/default/users/metadata.json"

    class io:
        properties: ClassVar[dict[str, str]] = {"warehouse": "s3://warehouse"}


def test_catalog_handle_never_copies_provider_credentials_or_redirect_properties(monkeypatch):
    class Table:
        metadata = SimpleNamespace(current_snapshot_id=None)
        metadata_location = "s3://analytics-demo/warehouse/metadata.json"

        class io:
            properties: ClassVar[dict[str, str]] = {
                "warehouse": "/outside/warehouse",
                "s3.secret-access-key": "private-value",
            }

    class Catalog:
        def load_table(self, identifier):
            return Table()

    monkeypatch.setattr(iceberg_catalog, "_load_iceberg_catalog", lambda *args: Catalog())
    source = _catalog(name="analytics", options={}).resolve_table(
        TableIdentifier(("default",), "users"), _context()
    )
    assert dict(source.metadata) == {"metadata_location": Table.metadata_location}
