from __future__ import annotations

import json
from uuid import UUID, uuid4

import pyarrow as pa
import pytest
from dal_obscura_plugin_api.schema.bounds import MAX_SCHEMA_DEPTH, MAX_SCHEMA_NODES
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, StructType

from dal_obscura.application.governance.assets.schema import capture_asset_schema, get_asset_schema
from dal_obscura.application.governance.errors import ValidationFailure
from dal_obscura.kernel.identity.actor import ControlPlaneActor
from tests.control.assets.schema_cases import (
    _FakeCatalog,
    _FakeStore,
    _FakeTable,
    discover_schema,
)
from tests.support.sources.schema_catalog import schema_registry


@pytest.fixture
def captured_schema(asset_commands, admitted_plugins):
    commands, work_factory, actor = asset_commands
    asset = commands.upsert_workspace_asset(
        "analytics", "events", "iceberg", "default.events", {}, actor=actor, correlation_id=None
    )
    with work_factory() as work:
        return capture_asset_schema(
            work, UUID(asset["id"]), actor, plugin_registry=admitted_plugins
        )


def test_get_asset_schema_reports_scope_encoding_budget(captured_schema) -> None:
    schema = pa.schema([pa.field("界" * 400_000, pa.string())])
    with pytest.raises(ValidationFailure, match="Schema encoding exceeds"):
        get_asset_schema(captured_schema, schema)


def test_get_asset_schema_rejects_path_amplification(captured_schema) -> None:
    schema = pa.schema(
        [
            pa.field(
                "ancestor_" + "x" * 100_000,
                pa.struct([pa.field(f"child_{index}", pa.string()) for index in range(100)]),
            )
        ]
    )
    with pytest.raises(ValidationFailure, match="Schema response exceeds the 8388608-byte budget"):
        get_asset_schema(captured_schema, schema)


def test_get_asset_schema_preserves_long_names_within_response_budget(captured_schema) -> None:
    from dal_obscura.interfaces.http.routes.asset_schemas import AssetSchemaResponse

    name = '界."' * 4096
    schema = pa.schema([pa.field(name, pa.struct([pa.field("leaf", pa.string())]))])
    result = get_asset_schema(captured_schema, schema)
    encoded = AssetSchemaResponse.model_validate(result).model_dump_json().encode("utf-8")
    assert len(encoded) <= 8 * 1024 * 1024
    node = json.loads(encoded)["fields"][0]
    assert node["name"] == name
    assert node["children"][0]["path"]["segments"][0]["name"] == name
    assert node["children"][0]["human_path"] == f"[{json.dumps(name, ensure_ascii=False)}].leaf"


def test_schema_response_budget_bounds_serialized_http_contract(captured_schema) -> None:
    from dal_obscura.interfaces.http.routes.asset_schemas import AssetSchemaResponse

    # Modest source schemas can amplify substantially through repeated paths in the response.
    name = "ancestor_" + "x" * 100_000
    schema = pa.schema(
        [
            pa.field(
                name, pa.struct([pa.field(f"child_{index}", pa.string()) for index in range(30)])
            )
        ]
    )
    result = get_asset_schema(captured_schema, schema)
    encoded = AssetSchemaResponse.model_validate(result).model_dump_json().encode("utf-8")
    assert 6 * 1024 * 1024 < len(encoded) <= 8 * 1024 * 1024
    assert len(json.loads(encoded)["fields"][0]["children"]) == 30
    expanded = pa.schema(
        [
            pa.field(
                name, pa.struct([pa.field(f"child_{index}", pa.string()) for index in range(60)])
            )
        ]
    )
    with pytest.raises(ValidationFailure, match="Schema response exceeds"):
        get_asset_schema(captured_schema, expanded)


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
            _FakeStore(asset_id),
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
            _FakeStore(asset_id),
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            plugin_registry=schema_registry(lambda *_args, **_: _FakeCatalog(_FakeTable(schema))),
        )
    assert "depth" in str(caught.value.__cause__)
