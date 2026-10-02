from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest
from dal_obscura_manifest_parquet.catalog import CATALOG_DESCRIPTOR, manifest_factory
from dal_obscura_manifest_parquet.format import FORMAT_DESCRIPTOR, parquet_factory
from dal_obscura_plugin_api import DiscoveryPage, ExecutionContext, TableHandle, TableIdentifier

from dal_obscura.common.plugin_api import PluginRegistry
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    CatalogRegistry,
    ServiceConfig,
)
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer
from dal_obscura.data_plane.infrastructure.adapters.public_plugin_adapter import (
    PublicPluginCatalogAdapter,
    PublicPluginPartition,
    PublicPluginTableFormat,
)
from tests.support.discovery import _unchecked_discovery_page
from tests.support.public_plugins import _contract_format, _fixture


def test_catalog_registry_routes_public_manifest_plugin_through_governed_port(
    tmp_path: Path,
) -> None:
    root, table = _fixture(tmp_path)
    registry = PluginRegistry(
        builtins=cast(
            Any,
            {
                ("catalog", "manifest"): (CATALOG_DESCRIPTOR, manifest_factory),
                ("table_format", "parquet.dataset"): (FORMAT_DESCRIPTOR, parquet_factory),
            },
        )
    )
    registry.reload()
    catalogs = CatalogRegistry(
        ServiceConfig(
            catalogs={
                "datasets": CatalogConfig(
                    name="datasets",
                    type=cast(Any, "iceberg"),
                    plugin_id="manifest",
                    revision=17,
                    options={"root": str(root), "manifest_path": "manifest.json"},
                )
            }
        ),
        plugin_registry=registry,
    )

    table_format = catalogs.resolve("datasets", "default.users")

    assert isinstance(table_format, PublicPluginTableFormat)
    assert table_format.get_schema() == table.schema
    plan = table_format.plan(PlanRequest(target="default.users", columns=["*"]), max_tickets=4)
    assert len(plan.tasks) == 2
    partition = cast(PublicPluginPartition, plan.tasks[0].partition)
    assert partition.handle.catalog_revision == 17
    serialized = pickle.dumps(plan.tasks[0])
    restored = pickle.loads(serialized)
    output_schema, batches = restored.table_format.execute(restored.partition)
    assert output_schema == table.schema
    assert pa.Table.from_batches(list(batches)).to_pylist() == [{"id": 1}]

    projected = table_format.plan(
        PlanRequest(target="default.users", columns=["id"]), max_tickets=4
    )
    projected_schema, projected_batches = projected.tasks[0].table_format.execute(
        projected.tasks[0].partition
    )
    assert projected_schema.names == ["id"]
    assert pa.Table.from_batches(list(projected_batches)).to_pylist() == [{"id": 1}]


def test_public_catalog_enforces_handle_metadata_destination(tmp_path: Path) -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")
    handle = TableHandle(
        catalog_plugin_id="manifest",
        catalog_instance_id="analytics",
        catalog_revision=1,
        identifier=identifier,
        format_plugin_id="parquet.dataset",
        handle_version=1,
        metadata={"metadata_location": "s3://untrusted/metadata.json"},
    )

    class Catalog:
        descriptor = CATALOG_DESCRIPTOR

        def close(self):
            return None

        def validate_config(self, context):
            del context

        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return (("default",),)

        def list_tables(self, context: ExecutionContext, *, continuation: str | None, limit: int):
            return DiscoveryPage((identifier,))

        def resolve_table(self, value: TableIdentifier, context: ExecutionContext):
            assert value == identifier
            return handle

    adapter = PublicPluginCatalogAdapter(
        "analytics",
        {},
        "manifest",
        lambda config, context: Catalog(),
        lambda plugin_id: object(),
        PathRuleEnforcer([{"root": str(tmp_path)}]),
        revision=1,
    )
    with pytest.raises(PermissionError, match="not allowed"):
        adapter.resolve_table("default.users")


def test_public_catalog_adapter_rejects_forged_handle_identity() -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")
    forged = TableHandle(
        catalog_plugin_id="other.catalog",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=identifier,
        format_plugin_id="parquet.dataset",
        handle_version=1,
    )

    class Catalog:
        descriptor = CATALOG_DESCRIPTOR

        def validate_config(self, context):
            del context

        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return ()

        def list_tables(self, context, *, continuation=None, limit):
            del context, continuation, limit
            return DiscoveryPage(())

        def resolve_table(self, value, context):
            del value, context
            return forged

        def close(self):
            return None

    adapter = PublicPluginCatalogAdapter(
        "fixture",
        {},
        "manifest",
        lambda config, context: Catalog(),
        lambda plugin_id: object(),
        revision=1,
    )
    with pytest.raises(ValueError, match="mismatched table handle identity"):
        adapter.resolve_table("default.users")


def test_public_catalog_adapter_closes_catalog_and_rejects_reuse() -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")
    closed = []

    class ClosableCatalog:
        descriptor = CATALOG_DESCRIPTOR

        def validate_config(self, context):
            del context

        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return (("default",),)

        def list_tables(self, context, *, continuation, limit):
            del context, continuation, limit
            return DiscoveryPage((identifier,))

        def resolve_table(self, value, context):
            del value, context
            raise AssertionError("resolve should not run after close")

        def close(self):
            closed.append(True)

    adapter = PublicPluginCatalogAdapter(
        "fixture",
        {},
        "manifest",
        lambda config, context: ClosableCatalog(),
        lambda plugin_id: object(),
    )
    adapter.close()
    adapter.close()

    assert closed == [True]
    with pytest.raises(ValueError, match="closed"):
        adapter.list_tables()


def test_public_catalog_adapter_rejects_missing_lifecycle_methods() -> None:
    class IncompleteCatalog:
        descriptor = CATALOG_DESCRIPTOR

        def list_tables(self, context, *, continuation, limit):
            del context, continuation, limit
            return DiscoveryPage(())

        def resolve_table(self, value, context):
            del value, context
            raise AssertionError("resolve should not run")

        def close(self):
            return None

    with pytest.raises(ValueError, match="invalid plugin"):
        PublicPluginCatalogAdapter(
            "fixture",
            {},
            "manifest",
            lambda config, context: IncompleteCatalog(),
            lambda plugin_id: object(),
        )


@pytest.mark.parametrize("malformed_token", [["unhashable"], "bad\n token", "x" * 4_097])
def test_public_catalog_adapter_rejects_malformed_continuation(malformed_token) -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")

    class Catalog:
        descriptor = CATALOG_DESCRIPTOR

        def validate_config(self, context):
            del context

        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return ()

        def list_tables(self, context, *, continuation=None, limit):
            del context, continuation, limit
            return _unchecked_discovery_page((identifier,), malformed_token)

        def resolve_table(self, value, context):
            del value, context
            raise AssertionError("resolve should not run")

        def close(self):
            return None

    adapter = PublicPluginCatalogAdapter(
        "fixture", {}, "manifest", lambda config, context: Catalog(), lambda plugin_id: object()
    )
    with pytest.raises(ValueError, match="continuation token"):
        adapter.list_tables()


def test_public_catalog_adapter_rejects_oversized_page() -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")

    class Catalog:
        descriptor = CATALOG_DESCRIPTOR

        def validate_config(self, context):
            del context

        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return ()

        def list_tables(self, context, *, continuation=None, limit):
            del context, continuation, limit
            entries = tuple(identifier for _ in range(501))
            return _unchecked_discovery_page(entries, None)

        def resolve_table(self, value, context):
            del value, context
            raise AssertionError("resolve should not run")

        def close(self):
            return None

    adapter = PublicPluginCatalogAdapter(
        "fixture", {}, "manifest", lambda config, context: Catalog(), lambda plugin_id: object()
    )
    with pytest.raises(ValueError, match="too many page entries"):
        adapter.list_tables()


def test_catalog_identifiers_round_trip_without_dotted_name_collisions():
    from dal_obscura.data_plane.infrastructure.adapters.public_plugin_adapter import (
        _identifier_name,
        _table_identifier,
    )

    identifiers = [
        TableIdentifier(namespace=("a.b",), name="c"),
        TableIdentifier(namespace=("a",), name="b.c"),
    ]
    names = [_identifier_name(identifier) for identifier in identifiers]
    assert len(set(names)) == 2
    assert [_table_identifier(name) for name in names] == identifiers


def test_manifest_read_enforces_policy_filter_before_masking(tmp_path):
    import hashlib

    from dal_obscura.common.access_control.models import AccessDecision, MaskRule
    from tests.application.access_flow.helpers import (
        AUTHORIZATION_HEADER,
        _build_end_to_end_access_flow,
    )

    root, _ = _fixture(tmp_path)
    catalog = PublicPluginCatalogAdapter(
        "fixture",
        {"root": str(root), "manifest_path": str(root / "manifest.json")},
        "manifest",
        manifest_factory,
        lambda _: parquet_factory,
    )
    try:
        table = catalog.resolve_table("default.users")
        planner, fetch = _build_end_to_end_access_flow(
            table,
            AccessDecision(
                allowed_columns=["id"],
                masks={"id": MaskRule(type="hash")},
                row_filter="id > 1",
                policy_version=1,
            ),
        )
        plan = planner.execute(
            PlanRequest(catalog="fixture", target="default.users", columns=["id"]),
            AUTHORIZATION_HEADER,
        )
        assert len(plan.ticket_tokens) == 1
        result = fetch.execute(plan.ticket_tokens[0], AUTHORIZATION_HEADER)
        assert pa.Table.from_batches(result.result_batches).to_pylist() == [
            {"id": hashlib.sha256(b"2").hexdigest()}
        ]
    finally:
        catalog.close()


def test_schema_must_match_catalog_pinned_snapshot():
    from dataclasses import replace

    schema = pa.schema([pa.field("id", pa.int64())])
    table, _ = _contract_format(schema)
    table = replace(table, handle=replace(table.handle, snapshot_id="pinned"))
    with pytest.raises(ValueError, match="snapshot"):
        table.get_schema()
    with pytest.raises(ValueError, match="snapshot"):
        table.plan(PlanRequest(target="default.users", columns=["id"]), 1)
