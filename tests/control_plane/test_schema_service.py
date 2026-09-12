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
from dal_obscura.control_plane.application.errors import AuthorizationFailure
from dal_obscura.control_plane.application.schema_service import get_asset_schema


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


def test_get_asset_schema_requires_read_capability() -> None:
    asset_id = uuid4()
    with pytest.raises(AuthorizationFailure):
        get_asset_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor("outsider", ()),
            load_catalog_fn=lambda **_: pytest.fail("catalog must not be loaded"),
        )
