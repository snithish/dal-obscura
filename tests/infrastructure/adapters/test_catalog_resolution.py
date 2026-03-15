import textwrap

import pytest

from dal_obscura.domain.query_planning import BackendReference
from dal_obscura.infrastructure.adapters import (
    DynamicRegistryRuntime,
    load_service_config,
)


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

    runtime = DynamicRegistryRuntime(load_service_config(config_path))
    for catalog_name in ("sql_cat", "glue_cat", "hive_cat", "unity_cat", "polaris_cat"):
        target = runtime.resolve(catalog_name, "db.users")
        assert target.backend.backend_id == "iceberg"
        assert target.backend.generation == runtime.current_generation
        assert target.handle["table_identifier"] == "db.users"


def test_catalog_resolver_can_mix_backend_families_within_one_catalog(tmp_path):
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

    runtime = DynamicRegistryRuntime(load_service_config(config_path))
    iceberg_target = runtime.resolve("mixed", "db.users")
    file_target = runtime.resolve("mixed", "users_csv")

    assert iceberg_target.backend.backend_id == "iceberg"
    assert file_target.backend.backend_id == "duckdb_file"
    assert file_target.handle["paths"] == [str(csv_path)]


def test_catalog_resolver_infers_raw_path_format_and_prefers_most_specific_overlay(tmp_path):
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

    runtime = DynamicRegistryRuntime(load_service_config(config_path))
    resolved = runtime.resolve(None, target)

    assert resolved.backend.backend_id == "duckdb_file"
    assert resolved.dataset_identity.catalog is None
    assert resolved.handle["format"] == "csv"
    assert resolved.handle["paths"] == [str(csv_path)]
    assert resolved.handle["options"] == {"sample_rows": 500, "sample_files": 3}


def test_catalog_resolver_rejects_mixed_raw_path_formats(tmp_path):
    data_dir = tmp_path / "landing"
    data_dir.mkdir()
    (data_dir / "users.csv").write_text("id,name\n1,a\n")
    (data_dir / "users.json").write_text('{"id":1,"name":"a"}\n')
    config_path = tmp_path / "empty.yaml"
    config_path.write_text("{}")

    runtime = DynamicRegistryRuntime(load_service_config(config_path))
    with pytest.raises(ValueError):
        runtime.resolve(None, str(data_dir / "users.*"))


def test_runtime_supports_dynamic_catalog_and_backend_registration(tmp_path):
    class CustomCatalog:
        def resolve(self, generation, catalog_name, catalog_config, target):
            from dal_obscura.domain.query_planning import (
                BackendReference,
                DatasetSelector,
                ResolvedBackendTarget,
            )

            selector = DatasetSelector(catalog=catalog_name, target=target)
            return ResolvedBackendTarget(
                dataset_identity=selector,
                backend=BackendReference(backend_id="custom_backend", generation=generation),
                handle={"custom": True},
            )

    class CustomBackend:
        def get_schema(self, target):
            return "schema"

        def plan(self, target, columns, max_tickets):
            return "plan"

        def read_spec(self, read_payload):
            return "spec"

        def read_stream(self, read_payload):
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

    runtime = DynamicRegistryRuntime(load_service_config(config_path))
    runtime.register_catalog_type("custom", CustomCatalog())
    runtime.register_backend("custom_backend", CustomBackend())
    generation = runtime.reload(load_service_config(config_path))

    resolved = runtime.resolve("custom_cat", "anything")

    assert resolved.backend == BackendReference(backend_id="custom_backend", generation=generation)
    assert runtime.get_schema(resolved) == "schema"


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

    runtime = DynamicRegistryRuntime(load_service_config(config_v1))
    old_generation = runtime.current_generation
    old_target = runtime.resolve("analytics", "db.users")

    runtime.reload(load_service_config(config_v2))
    runtime.unload_generation(old_generation)

    with pytest.raises(ValueError):
        runtime.read_spec(old_target.backend, b"payload")
