"""Live nested Iceberg schema discovery for governed assets."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import Any, cast
from uuid import UUID

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import ListType, MapType, NestedField, StructType

from dal_obscura.common.query_planning.field_paths import (
    FieldPath,
    FieldPathSegment,
    FieldSegment,
    ListElementSegment,
    MapKeySegment,
    MapValueSegment,
)
from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.application.policy_service import ensure_asset_capability
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore

CatalogLoader = Callable[..., Any]

# The response is intentionally bounded even though Iceberg itself permits
# substantially larger schemas.  This protects the control plane from
# recursively materializing an untrusted catalog response in one request.
MAX_SCHEMA_NODES = 10_000
MAX_SCHEMA_DEPTH = 64


def get_asset_schema(
    store: PublicationStore,
    asset_id: UUID,
    actor: ControlPlaneActor,
    *,
    load_catalog_fn: CatalogLoader | None = None,
) -> dict[str, object]:
    ensure_asset_capability(store, asset_id, actor, "read")
    schema = load_asset_iceberg_schema(
        store,
        asset_id,
        actor,
        load_catalog_fn=load_catalog_fn,
    )
    _validate_schema_bounds(schema)
    asset = store.get_workspace_asset(asset_id)
    return {
        "asset_id": str(asset_id),
        "catalog": asset["catalog"],
        "target": asset["name"],
        "schema_version": 1,
        "schema_fingerprint": schema_fingerprint(schema),
        "fields": [
            _field_node(field, (FieldSegment(field.name, field.field_id),))
            for field in schema.fields
        ],
    }


def load_asset_iceberg_schema(
    store: PublicationStore,
    asset_id: UUID,
    actor: ControlPlaneActor,
    *,
    load_catalog_fn: CatalogLoader | None = None,
) -> Schema:
    """Loads the authoritative Iceberg schema without reading table rows."""

    ensure_asset_capability(store, asset_id, actor, "read")
    asset = store.get_workspace_asset(asset_id)
    context = store.get_default_workspace_context()
    if context is None:
        raise LookupError("No workspace has been configured")
    catalog = store.get_workspace_catalog(
        context,
        str(asset["catalog"]),
    )
    options = cast(dict[str, Any], catalog["options"])
    table_identifier = str(asset["table_identifier"])
    loader = load_catalog if load_catalog_fn is None else load_catalog_fn
    try:
        table = loader(str(catalog["name"]), **options).load_table(table_identifier)
    except Exception as exc:
        # Catalog/provider failures can contain URIs, credentials, and internal
        # paths. Keep those details outside the control-plane response.
        raise ValidationFailure("Schema discovery failed") from exc
    schema = table.schema()
    if not isinstance(schema, Schema):
        raise TypeError("Iceberg catalog returned an invalid schema")
    return schema


def schema_fingerprint(schema: object) -> str:
    """Returns a stable digest for an authoritative Iceberg or Arrow schema."""

    return hashlib.sha256(str(schema).encode("utf-8")).hexdigest()


def _validate_schema_bounds(schema: Schema) -> None:
    nodes = 0
    max_depth = 0

    def visit(field: NestedField, depth: int) -> None:
        nonlocal nodes, max_depth
        nodes += 1
        max_depth = max(max_depth, depth)
        if nodes > MAX_SCHEMA_NODES:
            raise ValidationFailure(
                f"Iceberg schema exceeds the {MAX_SCHEMA_NODES} field-node limit"
            )
        if depth > MAX_SCHEMA_DEPTH:
            raise ValidationFailure(
                f"Iceberg schema exceeds the {MAX_SCHEMA_DEPTH} nesting-depth limit"
            )
        field_type = field.field_type
        if isinstance(field_type, StructType):
            for child in field_type.fields:
                visit(child, depth + 1)
        elif isinstance(field_type, ListType):
            visit(field_type.element_field, depth + 1)
        elif isinstance(field_type, MapType):
            visit(field_type.key_field, depth + 1)
            visit(field_type.value_field, depth + 1)

    for field in schema.fields:
        visit(field, 1)


def _field_node(field: NestedField, path: tuple[FieldPathSegment, ...]) -> dict[str, object]:
    field_type = field.field_type
    if isinstance(field_type, StructType):
        kind = "struct"
    elif isinstance(field_type, ListType):
        kind = "list"
    elif isinstance(field_type, MapType):
        kind = "map"
    else:
        kind = "scalar"
    node: dict[str, object] = {
        "field_id": field.field_id,
        "name": field.name,
        "path": FieldPath(path).to_wire(),
        "human_path": FieldPath(path).to_human(),
        "type": str(field_type),
        "nullable": not field.required,
        "kind": kind,
    }
    if isinstance(field_type, StructType):
        node["children"] = [
            _field_node(child, (*path, FieldSegment(child.name, child.field_id)))
            for child in field_type.fields
        ]
    elif isinstance(field_type, ListType):
        element = field_type.element_field
        node["children"] = [_field_node(element, (*path, ListElementSegment()))]
    elif isinstance(field_type, MapType):
        node["children"] = [
            _field_node(field_type.key_field, (*path, MapKeySegment())),
            _field_node(field_type.value_field, (*path, MapValueSegment())),
        ]
    return node
