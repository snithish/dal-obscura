from __future__ import annotations

import json
from uuid import uuid4

import pyarrow as pa
import pytest
from dal_obscura_plugin_api.schema.bounds import MAX_SCHEMA_DEPTH, MAX_SCHEMA_NODES
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, StructType

from dal_obscura.adapters.providers.governance import ProviderAssetSchemaLoader
from dal_obscura.application.governance.errors import ValidationFailure
from dal_obscura.kernel.identity.actor import ControlPlaneActor
from tests.control.assets.schema_cases import (
    _FakeCatalog,
    _FakeStore,
    _FakeTable,
    _nested_schema,
    discover_schema,
)
from tests.support.sources.schema_catalog import schema_registry


def test_get_asset_schema_reports_scope_encoding_budget(monkeypatch) -> None:
    schema = pa.schema([pa.field("界" * 400_000, pa.string())])
    monkeypatch.setattr(
        "dal_obscura.adapters.providers.governance.ProviderAssetSchemaLoader.load_asset_schema",
        lambda *args, **kwargs: schema,
    )
    asset_id = uuid4()
    with pytest.raises(ValidationFailure, match="Schema encoding exceeds"):
        discover_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
        )


def test_get_asset_schema_rejects_path_amplification_before_building_all_nodes(monkeypatch) -> None:
    from dal_obscura.application.governance.assets import schema as schema_service

    schema = pa.schema(
        [
            pa.field(
                "ancestor_" + "x" * 100_000,
                pa.struct([pa.field(f"child_{index}", pa.string()) for index in range(100)]),
            )
        ]
    )
    monkeypatch.setattr(
        ProviderAssetSchemaLoader, "load_asset_schema", lambda *args, **kwargs: schema
    )
    visited = []
    original = schema_service.numeric_field_id

    def record_field(field, names, scope):
        visited.append(field.name)
        return original(field, names, scope)

    monkeypatch.setattr(schema_service, "numeric_field_id", record_field)
    asset_id = uuid4()
    with pytest.raises(ValidationFailure, match="Schema response exceeds the 8388608-byte budget"):
        discover_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
        )
    assert 1 < len(visited) < 101


def test_get_asset_schema_preserves_long_names_within_response_budget(monkeypatch) -> None:
    from dal_obscura.interfaces.http.routes.asset_schemas import AssetSchemaResponse

    name = '界."' * 4096
    schema = pa.schema([pa.field(name, pa.struct([pa.field("leaf", pa.string())]))])
    monkeypatch.setattr(
        ProviderAssetSchemaLoader, "load_asset_schema", lambda *args, **kwargs: schema
    )
    asset_id = uuid4()
    result = discover_schema(
        _FakeStore(asset_id),  # type: ignore[arg-type]
        asset_id,
        ControlPlaneActor.for_platform_admin("admin"),
    )
    encoded = AssetSchemaResponse.model_validate(result).model_dump_json().encode("utf-8")
    assert len(encoded) <= 8 * 1024 * 1024
    node = json.loads(encoded)["fields"][0]
    assert node["name"] == name
    assert node["children"][0]["path"]["segments"][0]["name"] == name
    assert node["children"][0]["human_path"] == f"[{json.dumps(name, ensure_ascii=False)}].leaf"


def test_schema_response_budget_matches_serialized_http_contract(monkeypatch) -> None:
    from dal_obscura.application.governance.assets import schema as schema_service
    from dal_obscura.interfaces.http.routes.asset_schemas import AssetSchemaResponse

    schema = _nested_schema().as_arrow()
    monkeypatch.setattr(
        ProviderAssetSchemaLoader, "load_asset_schema", lambda *args, **kwargs: schema
    )
    asset_id = uuid4()

    def discover():
        return discover_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
        )

    expected_bytes = len(AssetSchemaResponse.model_validate(discover()).model_dump_json().encode())
    budget = schema_service._SchemaResponseBudget(remaining=expected_bytes)
    monkeypatch.setattr(schema_service, "_SchemaResponseBudget", lambda: budget)
    assert (
        len(AssetSchemaResponse.model_validate(discover()).model_dump_json().encode())
        == expected_bytes
    )
    assert budget.remaining == 0
    budget.remaining = expected_bytes - 1
    with pytest.raises(ValidationFailure, match="Schema response exceeds"):
        discover()


def test_get_asset_schema_rejects_excessive_node_count() -> None:
    asset_id = uuid4()
    schema = Schema(
        *(
            NestedField(field_id=index, name=f"field_{index}", field_type=StringType())
            for index in range(1, MAX_SCHEMA_NODES + 2)
        )
    )

    with pytest.raises(ValidationFailure, match="Schema discovery failed") as caught:
        discover_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            plugin_registry=schema_registry(lambda *_args, **_: _FakeCatalog(_FakeTable(schema))),
        )
    assert "node" in str(caught.value.__cause__)


def test_get_asset_schema_rejects_excessive_nesting_depth() -> None:
    asset_id = uuid4()
    nested: object = StringType()
    for index in range(MAX_SCHEMA_DEPTH + 1, 0, -1):
        nested = StructType(NestedField(field_id=index, name=f"level_{index}", field_type=nested))
    schema = Schema(NestedField(field_id=MAX_SCHEMA_DEPTH + 2, name="root", field_type=nested))

    with pytest.raises(ValidationFailure, match="Schema discovery failed") as caught:
        discover_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            plugin_registry=schema_registry(lambda *_args, **_: _FakeCatalog(_FakeTable(schema))),
        )
    assert "depth" in str(caught.value.__cause__)
