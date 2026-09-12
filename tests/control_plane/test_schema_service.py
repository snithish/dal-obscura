from __future__ import annotations

from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    ListType,
    MapType,
    NestedField,
    StringType,
    StructType,
)

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.control_plane.application.schema_service import (
    MAX_SCHEMA_DEPTH,
    MAX_SCHEMA_NODES,
    get_asset_schema,
    schema_fingerprint,
)


class _FakeTable:
    def __init__(self, schema: Schema) -> None:
        self._schema = schema

    def schema(self) -> Schema:
        return self._schema


class _FakeCatalog:
    def __init__(self, table: _FakeTable) -> None:
        self._table = table
        self.loaded_identifier: str | None = None

    def load_table(self, identifier: str) -> _FakeTable:
        self.loaded_identifier = identifier
        return self._table


class _FakeStore:
    def __init__(self, asset_id: UUID) -> None:
        self.asset_id = asset_id

    def get_workspace_asset(self, asset_id: UUID) -> dict[str, object]:
        assert asset_id == self.asset_id
        return {
            "catalog": "analytics",
            "name": "default.events",
            "table_identifier": "default.events",
        }

    def get_default_workspace_context(self) -> object:
        return object()

    def get_workspace_catalog(self, context: object, name: str) -> dict[str, object]:
        assert name == "analytics"
        return {
            "name": "analytics",
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        }

    def list_asset_owners(self, asset_id: UUID) -> list[str]:
        return []

    def list_asset_grants(self, asset_id: UUID) -> list[dict[str, str]]:
        return []


def _nested_schema() -> Schema:
    return Schema(
        NestedField(
            field_id=1,
            name="profile",
            field_type=StructType(
                NestedField(field_id=2, name="email", field_type=StringType()),
                NestedField(
                    field_id=3,
                    name="labels",
                    field_type=ListType(
                        element_id=4,
                        element_type=StringType(),
                        element_required=False,
                    ),
                ),
            ),
        ),
        NestedField(
            field_id=5,
            name="attributes",
            field_type=MapType(
                key_id=6,
                key_type=StringType(),
                value_id=7,
                value_type=IntegerType(),
                value_required=False,
            ),
        ),
    )


def test_get_asset_schema_returns_typed_nested_paths() -> None:
    asset_id = uuid4()
    store = _FakeStore(asset_id)
    catalog = _FakeCatalog(_FakeTable(_nested_schema()))
    calls: list[tuple[str, dict[str, Any]]] = []

    def load_catalog(name: str, **options: Any) -> _FakeCatalog:
        calls.append((name, options))
        return catalog

    result = get_asset_schema(
        store,  # type: ignore[arg-type]
        asset_id,
        ControlPlaneActor.for_platform_admin("admin"),
        load_catalog_fn=load_catalog,
    )

    assert calls == [("analytics", {"type": "sql", "uri": "sqlite:///catalog.db"})]
    assert catalog.loaded_identifier == "default.events"
    assert result["schema_version"] == 1
    assert isinstance(result["schema_fingerprint"], str)
    assert len(cast(str, result["schema_fingerprint"])) == 64
    fields = cast(list[dict[str, object]], result["fields"])
    profile = fields[0]
    assert profile["kind"] == "struct"
    assert profile["path"] == {
        "version": 1,
        "segments": [{"kind": "field", "name": "profile", "field_id": 1}],
    }
    children = cast(list[dict[str, object]], profile["children"])
    assert children[0]["human_path"] == "profile.email"
    assert children[1]["human_path"] == "profile.labels"
    labels_element = cast(list[dict[str, object]], children[1]["children"])[0]
    assert labels_element["human_path"] == "profile.labels.$element"
    attributes = fields[1]
    assert attributes["kind"] == "map"
    attribute_children = cast(list[dict[str, object]], attributes["children"])
    assert [child["human_path"] for child in attribute_children] == [
        "attributes.$key",
        "attributes.$value",
    ]


def test_schema_fingerprint_includes_collection_ids_and_matches_arrow_normalization() -> None:
    original = Schema(
        NestedField(
            field_id=1,
            name="items",
            field_type=ListType(
                element_id=2,
                element_type=StructType(
                    NestedField(field_id=3, name="value", field_type=StringType())
                ),
            ),
        )
    )
    changed_element = Schema(
        NestedField(
            field_id=1,
            name="items",
            field_type=ListType(
                element_id=99,
                element_type=StructType(
                    NestedField(field_id=3, name="value", field_type=StringType())
                ),
            ),
        )
    )

    assert schema_fingerprint(original) == schema_fingerprint(original.as_arrow())
    assert schema_fingerprint(original) != schema_fingerprint(changed_element)


def test_get_asset_schema_requires_read_capability() -> None:
    asset_id = uuid4()
    with pytest.raises(AuthorizationFailure):
        get_asset_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor("outsider", ()),
            load_catalog_fn=lambda **_: pytest.fail("catalog must not be loaded"),
        )


def test_get_asset_schema_redacts_catalog_provider_errors() -> None:
    asset_id = uuid4()

    def failing_catalog(**_: object):
        raise ValueError("failed https://catalog-user:catalog-password@catalog.example")

    with pytest.raises(ValidationFailure, match="Schema discovery failed") as failure:
        get_asset_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            load_catalog_fn=failing_catalog,
        )

    assert "catalog-password" not in str(failure.value)


def test_get_asset_schema_rejects_excessive_node_count() -> None:
    asset_id = uuid4()
    schema = Schema(
        *(
            NestedField(field_id=index, name=f"field_{index}", field_type=StringType())
            for index in range(1, MAX_SCHEMA_NODES + 2)
        )
    )

    with pytest.raises(ValidationFailure, match="field-node limit"):
        get_asset_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            load_catalog_fn=lambda *_args, **_: _FakeCatalog(_FakeTable(schema)),
        )


def test_get_asset_schema_rejects_excessive_nesting_depth() -> None:
    asset_id = uuid4()
    nested: object = StringType()
    for index in range(MAX_SCHEMA_DEPTH + 1, 0, -1):
        nested = StructType(
            NestedField(
                field_id=index,
                name=f"level_{index}",
                field_type=nested,  # type: ignore[arg-type]
            )
        )
    schema = Schema(
        NestedField(
            field_id=MAX_SCHEMA_DEPTH + 2,
            name="root",
            field_type=nested,  # type: ignore[arg-type]
        )
    )

    with pytest.raises(ValidationFailure, match="nesting-depth limit"):
        get_asset_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            load_catalog_fn=lambda *_args, **_: _FakeCatalog(_FakeTable(schema)),
        )
