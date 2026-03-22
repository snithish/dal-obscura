from __future__ import annotations

import textwrap
from unittest.mock import MagicMock

import pytest

from dal_obscura.domain.format_handler.ports import FormatHandler, InputPartition, Plan, ScanTask
from dal_obscura.infrastructure.adapters.catalog_registry import DynamicCatalogRegistry
from dal_obscura.infrastructure.adapters.format_registry import DynamicFormatRegistry
from dal_obscura.infrastructure.adapters.service_config import load_service_config


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


def _build_registries(config_path):
    catalog_registry = DynamicCatalogRegistry(load_service_config(config_path))
    format_registry = DynamicFormatRegistry()
    return catalog_registry, format_registry


def test_load_service_config_parses_catalog_types_and_targets(tmp_path):
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
              local_files:
                module: dal_obscura.infrastructure.adapters.catalog_registry.StaticCatalog
                targets:
                  users_csv:
                    backend: duckdb_file
                    format: csv
                    paths: ["./data/*.csv"]
                    options:
                      sample_rows: 111
                      sample_files: 2
            paths:
              - glob: "/landing/**/*.csv"
                options:
                  sample_rows: 333
                  sample_files: 4
            """
        )
    )

    config = load_service_config(config_path)

    assert set(config.catalogs) == {"glue_prod", "local_files"}
    assert (
        config.catalogs["glue_prod"].module
        == "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog"
    )
    assert config.catalogs["local_files"].targets["users_csv"].backend == "duckdb_file"
    assert config.catalogs["local_files"].targets["users_csv"].options["sample_rows"] == 111
    assert config.paths[0].glob == "/landing/**/*.csv"


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

    catalog_registry, format_registry = _build_registries(config_path)
    for catalog_name in ("sql_cat", "glue_cat", "hive_cat", "unity_cat", "polaris_cat"):
        resolved = catalog_registry.describe(catalog_name, "db.users")
        assert getattr(resolved, "format", None) == "iceberg"
        assert resolved.table_name == "db.users"
        handler = format_registry.get_handler(resolved.format)
        assert handler.supported_format == "iceberg"


def test_catalog_registry_can_mix_format_families_within_one_catalog(tmp_path):
    csv_path = tmp_path / "users.csv"
    csv_path.write_text("id,name\n1,a\n")
    config_path = tmp_path / "service.yaml"
    config_path.write_text(
        textwrap.dedent(
            f"""
            catalogs:
              mixed:
                module: dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog
                options:
                  type: sql
                  warehouse: s3://warehouse
                targets:
                  users_csv:
                    backend: duckdb_file
                    format: csv
                    paths: ["{csv_path}"]
            """
        )
    )

    catalog_registry, _format_registry = _build_registries(config_path)
    iceberg_target = catalog_registry.describe("mixed", "db.users")
    file_target = catalog_registry.describe("mixed", "users_csv")

    assert getattr(iceberg_target, "format", None) == "iceberg"
    assert getattr(file_target, "format", None) == "duckdb_file"
    assert file_target.paths == (str(csv_path),)


def test_catalog_registry_infers_raw_path_format_and_prefers_most_specific_overlay(tmp_path):
    raw_dir = tmp_path / "landing" / "special"
    raw_dir.mkdir(parents=True)
    csv_path = raw_dir / "users.csv"
    csv_path.write_text("id,name\n1,a\n")
    target = str(raw_dir / "*.csv")
    config_path = tmp_path / "service.yaml"
    config_path.write_text(
        textwrap.dedent(
            f"""
            paths:
              - glob: "{tmp_path}/landing/**/*.csv"
                options:
                  sample_rows: 10
                  sample_files: 1
              - glob: "{target}"
                options:
                  sample_rows: 500
                  sample_files: 3
            """
        )
    )

    catalog_registry, _ = _build_registries(config_path)
    resolved = catalog_registry.describe(None, target)

    assert resolved.format == "duckdb_file"
    assert resolved.catalog_name == ""
    assert resolved.table_name == target
    assert resolved.file_format == "csv"
    assert resolved.paths == (str(csv_path),)
    assert resolved.options == {"sample_rows": 500, "sample_files": 3}


def test_catalog_registry_rejects_mixed_raw_path_formats(tmp_path):
    data_dir = tmp_path / "landing"
    data_dir.mkdir()
    (data_dir / "users.csv").write_text("id,name\n1,a\n")
    (data_dir / "users.json").write_text('{"id":1,"name":"a"}\n')
    config_path = tmp_path / "empty.yaml"
    config_path.write_text("{}")

    catalog_registry, _ = _build_registries(config_path)
    with pytest.raises(ValueError):
        catalog_registry.describe(None, str(data_dir / "users.*"))


def test_runtime_supports_dynamic_format_registration():
    class CustomFormatHandler(FormatHandler):
        @property
        def supported_format(self):
            return "custom_fmt"

        def get_schema(self, table):
            return "schema"

        def plan(self, table, request, max_tickets):
            class StubPartition(InputPartition):
                pass

            return Plan(
                schema="schema",
                tasks=[ScanTask(format="custom_fmt", schema="schema", partition=StubPartition())],
            )

        def execute(self, partition):
            return "schema", []

    format_registry = DynamicFormatRegistry()
    format_registry.register_handler("custom_fmt", CustomFormatHandler())

    handler = format_registry.get_handler("custom_fmt")
    assert handler.supported_format == "custom_fmt"


def test_format_registry_rejects_handler_without_methods():
    class InvalidHandler:
        def execute(self, payload):
            pass

    format_registry = DynamicFormatRegistry()
    with pytest.raises(TypeError, match="plan"):
        format_registry.register_handler("broken", InvalidHandler())  # type: ignore[arg-type]
