import textwrap
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, cast

import pytest

from dal_obscura.domain.query_planning.models import (
    BackendDescriptor,
    BackendReference,
    BoundBackendTarget,
    DatasetSelector,
    Plan,
    ReadPayload,
    ReadSpec,
)
from dal_obscura.infrastructure.adapters.backend_registry import DynamicBackendRegistry
from dal_obscura.infrastructure.adapters.catalog_registry import DynamicCatalogRegistry
from dal_obscura.infrastructure.adapters.file_backend import FileTableDescriptor
from dal_obscura.infrastructure.adapters.iceberg_backend import IcebergTableDescriptor
from dal_obscura.infrastructure.adapters.implementation_base import (
    BackendImplementation,
    CatalogImplementation,
)
from dal_obscura.infrastructure.adapters.service_config import load_service_config


def _build_registries(config_path):
    catalog_registry = DynamicCatalogRegistry(load_service_config(config_path))
    backend_registry = DynamicBackendRegistry()
    return catalog_registry, backend_registry


def test_load_service_config_parses_catalog_types_and_targets(tmp_path):
    config_path = tmp_path / "service.yaml"
    config_path.write_text(
        textwrap.dedent(
            """
            catalogs:
              glue_prod:
                type: glue
                options:
                  warehouse: s3://warehouse
              local_files:
                type: static
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
    assert config.catalogs["glue_prod"].type == "glue"
    assert config.catalogs["local_files"].targets["users_csv"].backend == "duckdb_file"
    assert config.catalogs["local_files"].targets["users_csv"].options["sample_rows"] == 111
    assert config.paths[0].glob == "/landing/**/*.csv"


def test_builtin_catalog_types_resolve_through_registry(tmp_path):
    config_path = tmp_path / "service.yaml"
    config_path.write_text(
        textwrap.dedent(
            """
            catalogs:
              sql_cat: {type: sql}
              glue_cat: {type: glue}
              hive_cat: {type: hive}
              unity_cat: {type: unity}
              polaris_cat: {type: polaris}
            """
        )
    )

    catalog_registry, backend_registry = _build_registries(config_path)
    for catalog_name in ("sql_cat", "glue_cat", "hive_cat", "unity_cat", "polaris_cat"):
        descriptor = catalog_registry.describe(catalog_name, "db.users")
        assert isinstance(descriptor, IcebergTableDescriptor)
        assert descriptor.backend_id == "iceberg"
        assert descriptor.table_identifier == "db.users"
        target = backend_registry.bind_descriptor(descriptor)
        assert target.backend.backend_id == "iceberg"
        assert target.backend.generation == backend_registry.current_generation


def test_catalog_registry_can_mix_backend_families_within_one_catalog(tmp_path):
    csv_path = tmp_path / "users.csv"
    csv_path.write_text("id,name\n1,a\n")
    config_path = tmp_path / "service.yaml"
    config_path.write_text(
        textwrap.dedent(
            f"""
            catalogs:
              mixed:
                type: sql
                options:
                  warehouse: s3://warehouse
                targets:
                  users_csv:
                    backend: duckdb_file
                    format: csv
                    paths: ["{csv_path}"]
            """
        )
    )

    catalog_registry, backend_registry = _build_registries(config_path)
    iceberg_target = catalog_registry.describe("mixed", "db.users")
    file_target = catalog_registry.describe("mixed", "users_csv")

    assert isinstance(iceberg_target, IcebergTableDescriptor)
    assert isinstance(file_target, FileTableDescriptor)
    assert backend_registry.bind_descriptor(iceberg_target).backend.backend_id == "iceberg"
    assert backend_registry.bind_descriptor(file_target).backend.backend_id == "duckdb_file"
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

    catalog_registry, backend_registry = _build_registries(config_path)
    resolved = catalog_registry.describe(None, target)

    assert isinstance(resolved, FileTableDescriptor)
    assert backend_registry.bind_descriptor(resolved).backend.backend_id == "duckdb_file"
    assert resolved.dataset_identity.catalog is None
    assert resolved.format == "csv"
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


def test_runtime_supports_dynamic_catalog_and_backend_registration(tmp_path):
    @dataclass(frozen=True)
    class CustomDescriptor:
        dataset_identity: DatasetSelector
        backend_id: str = field(init=False, default="custom_backend")

    @dataclass(frozen=True)
    class CustomBinding:
        backend_id: str = field(init=False, default="custom_backend")

    class CustomCatalog(CatalogImplementation):
        def resolve(self, catalog_name: str, catalog_config: Any, target: str) -> BackendDescriptor:
            selector = DatasetSelector(catalog=catalog_name, target=target)
            return CustomDescriptor(dataset_identity=selector)

    class CustomBackend(BackendImplementation):
        def bind(self, descriptor: BackendDescriptor) -> CustomBinding:
            return CustomBinding()

        def get_schema(self, bound_target: BoundBackendTarget) -> str:
            return "schema"

        def plan(
            self, bound_target: BoundBackendTarget, columns: Iterable[str], max_tickets: int
        ) -> Plan:
            return Plan(schema="schema", tasks=[ReadPayload(payload=b"payload")])

        def read_spec(self, read_payload: bytes) -> ReadSpec:
            return ReadSpec(
                dataset=DatasetSelector(catalog="custom_cat", target="anything"),
                columns=[],
                schema="schema",
            )

        def read_stream(self, read_payload: bytes) -> list[object]:
            return []

    config_path = tmp_path / "service.yaml"
    config_path.write_text(
        textwrap.dedent(
            """
            catalogs:
              custom_cat:
                type: custom
            """
        )
    )

    catalog_registry, backend_registry = _build_registries(config_path)
    catalog_registry.register_catalog_type("custom", CustomCatalog())
    backend_registry.register_backend("custom_backend", CustomBackend())
    generation = backend_registry.reload()

    descriptor = catalog_registry.describe("custom_cat", "anything")
    resolved = backend_registry.bind_descriptor(descriptor)

    assert resolved.backend == BackendReference(backend_id="custom_backend", generation=generation)
    assert backend_registry.schema_for(resolved) == "schema"


def test_runtime_generation_binding_fails_closed_after_unload(tmp_path):
    config_v1 = tmp_path / "service-v1.yaml"
    config_v1.write_text(
        textwrap.dedent(
            """
            catalogs:
              analytics:
                type: sql
            """
        )
    )
    config_v2 = tmp_path / "service-v2.yaml"
    config_v2.write_text(
        textwrap.dedent(
            """
            catalogs:
              analytics:
                type: static
                targets:
                  users_csv:
                    backend: duckdb_file
                    format: csv
                    paths: ["/tmp/does-not-exist.csv"]
            """
        )
    )

    catalog_registry = DynamicCatalogRegistry(load_service_config(config_v1))
    backend_registry = DynamicBackendRegistry()
    old_generation = backend_registry.current_generation
    old_target = backend_registry.bind_descriptor(
        catalog_registry.describe("analytics", "db.users")
    )

    catalog_registry.reload(load_service_config(config_v2))
    backend_registry.reload()
    backend_registry.unload_generation(old_generation)

    with pytest.raises(ValueError):
        backend_registry.read_spec_for(old_target.backend, b"payload")


def test_backend_registry_rejects_backend_without_read_stream():
    class InvalidBackend:
        def bind(self, descriptor: BackendDescriptor):
            return object()

        def get_schema(self, bound_target):
            return "schema"

        def plan(self, bound_target, columns, max_tickets):
            return "plan"

        def read_spec(self, read_payload):
            return "spec"

    backend_registry = DynamicBackendRegistry()
    backend_registry.register_backend("broken", cast(Any, InvalidBackend()))

    with pytest.raises(TypeError, match="read_stream"):
        backend_registry.reload()
