from __future__ import annotations

import textwrap
from unittest.mock import MagicMock

import pytest

from dal_obscura.domain.catalog.ports import TableFormat
from dal_obscura.infrastructure.adapters.catalog_registry import DynamicCatalogRegistry
from dal_obscura.infrastructure.adapters.service_config import load_catalog_config


@pytest.fixture(autouse=True)
def mock_pyiceberg_catalog(monkeypatch):
    mock_catalog = MagicMock()
    mock_table = MagicMock()
    mock_table.metadata_location = "s3://fake/metadata.json"
    mock_table.io.properties = {"s3.region": "us-east-1"}
    mock_catalog.load_table.return_value = mock_table

    mock_load_catalog = MagicMock(return_value=mock_catalog)
    monkeypatch.setattr("pyiceberg.catalog.load_catalog", mock_load_catalog)
    return mock_load_catalog


def _build_registry(config_path):
    return DynamicCatalogRegistry(load_catalog_config(config_path))


def test_load_catalog_config_parses_catalog_types_and_targets(tmp_path):
    config_path = tmp_path / "service.yaml"
    config_path.write_text(
        textwrap.dedent(
            """
            catalogs:
              glue_prod:
                module: dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog
                options:
                  type: glue
                  warehouse: s3://warehouse
              static_alias:
                module: dal_obscura.infrastructure.adapters.catalog_registry.StaticCatalog
                options:
                  type: glue
                  warehouse: s3://warehouse
                targets:
                  users_alias:
                    backend: iceberg
                    table: analytics.users
            """
        )
    )

    config = load_catalog_config(config_path)

    assert set(config.catalogs) == {"glue_prod", "static_alias"}
    assert (
        config.catalogs["glue_prod"].module
        == "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog"
    )
    assert config.catalogs["static_alias"].targets["users_alias"].backend == "iceberg"
    assert config.catalogs["static_alias"].targets["users_alias"].table == "analytics.users"


def test_builtin_catalog_types_resolve_through_registry(tmp_path):
    config_path = tmp_path / "service.yaml"
    config_path.write_text(
        textwrap.dedent(
            """
            catalogs:
              sql_cat:
                module: dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog
                options: {type: sql}
              glue_cat:
                module: dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog
                options: {type: glue}
              hive_cat:
                module: dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog
                options: {type: hive}
              unity_cat:
                module: dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog
                options: {type: unity}
              polaris_cat:
                module: dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog
                options: {type: polaris}
            """
        )
    )

    catalog_registry = _build_registry(config_path)
    for catalog_name in ("sql_cat", "glue_cat", "hive_cat", "unity_cat", "polaris_cat"):
        table_format = catalog_registry.describe(catalog_name, "db.users")
        assert isinstance(table_format, TableFormat)
        assert table_format.format == "iceberg"
        assert table_format.table_name == "db.users"


def test_catalog_registry_rejects_non_iceberg_target_backends(tmp_path):
    config_path = tmp_path / "service.yaml"
    config_path.write_text(
        textwrap.dedent(
            """
            catalogs:
              mixed:
                module: dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog
                options:
                  type: sql
                  warehouse: s3://warehouse
                targets:
                  users_csv:
                    backend: duckdb_file
            """
        )
    )

    with pytest.raises(ValueError, match="Unsupported backend"):
        _build_registry(config_path)


def test_catalog_config_rejects_unknown_target_fields(tmp_path):
    config_path = tmp_path / "service.yaml"
    config_path.write_text(
        textwrap.dedent(
            """
            catalogs:
              mixed:
                module: dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog
                targets:
                  users:
                    format: iceberg
            """
        )
    )

    with pytest.raises(ValueError, match="format"):
        load_catalog_config(config_path)


def test_catalog_registry_requires_catalog_name(tmp_path):
    config_path = tmp_path / "empty.yaml"
    config_path.write_text("{}")

    catalog_registry = _build_registry(config_path)
    with pytest.raises(ValueError, match="Catalog name is required"):
        catalog_registry.describe(None, "users")


def test_iceberg_catalog_uses_exact_target_overrides_not_wildcards(
    tmp_path, mock_pyiceberg_catalog
):
    config_path = tmp_path / "service.yaml"
    config_path.write_text(
        textwrap.dedent(
            """
            catalogs:
              exact_only:
                module: dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog
                options: {type: sql}
                targets:
                  db.*:
                    table: analytics.override_users
            """
        )
    )

    catalog_registry = _build_registry(config_path)
    catalog_registry.describe("exact_only", "db.users")

    mock_catalog = mock_pyiceberg_catalog.return_value
    assert mock_catalog.load_table.call_args_list[-1].args[0] == "db.users"


def test_iceberg_catalog_is_instantiated_once_and_reused(tmp_path, mock_pyiceberg_catalog):
    config_path = tmp_path / "service.yaml"
    config_path.write_text(
        textwrap.dedent(
            """
            catalogs:
              sql_cat:
                module: dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog
                options: {type: sql}
            """
        )
    )

    catalog_registry = _build_registry(config_path)
    catalog_registry.describe("sql_cat", "db.users")
    catalog_registry.describe("sql_cat", "db.orders")

    assert mock_pyiceberg_catalog.call_count == 1
