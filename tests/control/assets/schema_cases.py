"""Fixture construction shared by adjacent behavioral suites."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock
from uuid import UUID

from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, ListType, MapType, NestedField, StringType, StructType

from dal_obscura.adapters.providers.governance import ProviderAssetSchemaLoader
from dal_obscura.application.governance.assets.schema import (
    capture_asset_schema,
    get_asset_schema,
)
from dal_obscura.kernel.identity.actor import ControlPlaneActor


def discover_schema(
    store: _FakeStore, asset_id: UUID, actor: ControlPlaneActor, **options: Any
) -> dict[str, object]:
    captured = capture_asset_schema(
        Mock(assets=store, catalogs=store, workspace=store),
        asset_id,
        actor,
        egress_allowlist=options.get("egress_allowlist", ()),
        plugin_registry=options.get("plugin_registry"),
    )
    return get_asset_schema(
        captured, ProviderAssetSchemaLoader(**options).load_asset_schema(captured)
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
            "id": str(asset_id),
            "revision": 0,
            "backend": "iceberg",
            "catalog": "analytics",
            "name": "default.events",
            "table_identifier": "default.events",
        }

    def get_asset_binding(self, asset_id: UUID) -> dict[str, object]:
        return self.get_workspace_asset(asset_id)

    def get_workspace(self) -> object:
        return object()

    def get_workspace_catalog(self, name: str) -> dict[str, object]:
        assert name == "analytics"
        return {
            "name": "analytics",
            "id": "00000000-0000-4000-8000-000000000002",
            "plugin_id": "iceberg.catalog",
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
                        element_id=4, element_type=StringType(), element_required=False
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
