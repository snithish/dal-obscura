"""Live nested Iceberg schema discovery for governed assets."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from typing import Any, cast
from uuid import UUID

import pyarrow as pa
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
from dal_obscura.common.schema_bounds import (
    MAX_SCHEMA_DEPTH,
    MAX_SCHEMA_ENCODING_BYTES,
    MAX_SCHEMA_NODES,
    validate_arrow_schema_bounds,
)
from dal_obscura.common.schema_identity import schema_has_stable_ids, schema_scope_digest
from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.catalog_service import validate_catalog_options
from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.application.policy_service import ensure_asset_capability
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore
from dal_obscura.data_plane.infrastructure.adapters.secret_providers import (
    EnvSecretProvider,
    resolve_secret_refs,
)

CatalogLoader = Callable[..., Any]
ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)

def get_asset_schema(
    store: PublicationStore,
    asset_id: UUID,
    actor: ControlPlaneActor,
    *,
    load_catalog_fn: CatalogLoader | None = None,
    egress_allowlist: tuple[str, ...] = (),
    plugin_registry: Any | None = None,
) -> dict[str, object]:
    ensure_asset_capability(store, asset_id, actor, "read")
    schema = load_asset_iceberg_schema(
        store,
        asset_id,
        actor,
        load_catalog_fn=load_catalog_fn,
        egress_allowlist=egress_allowlist,
        plugin_registry=plugin_registry,
    )
    arrow_schema = schema.as_arrow() if isinstance(schema, Schema) else schema
    _validate_arrow_schema_bounds(arrow_schema)
    asset = store.get_workspace_asset(asset_id)
    return {
        "asset_id": str(asset_id),
        "catalog": asset["catalog"],
        "target": asset["name"],
        "schema_version": 1,
        "schema_fingerprint": schema_fingerprint(arrow_schema),
        "stable_field_ids": schema_has_stable_ids(arrow_schema),
        "fields": (
            [
                _field_node(field, (FieldSegment(field.name, field.field_id),))
                for field in schema.fields
            ]
            if isinstance(schema, Schema)
            else [
                _arrow_field_node(field, (field.name,), schema_scope_digest(arrow_schema))
                for field in arrow_schema
            ]
        ),
    }


def load_asset_iceberg_schema(
    store: PublicationStore,
    asset_id: UUID,
    actor: ControlPlaneActor,
    *,
    load_catalog_fn: CatalogLoader | None = None,
    egress_allowlist: tuple[str, ...] = (),
    plugin_registry: Any | None = None,
) -> Schema | pa.Schema:
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
    validate_catalog_options(options, egress_allowlist=egress_allowlist)
    options = cast(
        dict[str, Any],
        resolve_secret_refs(options, provider=EnvSecretProvider()),
    )
    if plugin_registry is not None and str(catalog["module"]) != ICEBERG_CATALOG_MODULE:
        return _load_public_plugin_schema(
            asset=asset,
            catalog=catalog,
            options=options,
            plugin_registry=plugin_registry,
        )
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
    _validate_schema_bounds(schema)
    return schema


def _load_public_plugin_schema(
    *,
    asset: dict[str, object],
    catalog: dict[str, object],
    options: dict[str, Any],
    plugin_registry: Any,
) -> pa.Schema:
    """Resolve one admitted catalog/format pair without importing request paths."""

    from datetime import datetime, timedelta, timezone

    from dal_obscura_plugin_api import (
        CatalogConfig,
        ExecutionContext,
        TableIdentifier,
    )

    catalog_plugin_id = str(catalog["module"])
    format_plugin_id = str(asset["backend"])
    context = ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(seconds=30),
        correlation_id=f"schema-{asset['id']}",
    )
    catalog_plugin: Any | None = None
    format_plugin: Any | None = None
    try:
        catalog_factory = plugin_registry.load("catalog", catalog_plugin_id)
        format_factory = plugin_registry.load("table_format", format_plugin_id)
        if not callable(catalog_factory) or not callable(format_factory):
            raise ValidationFailure("Admitted schema plugin is unavailable")
        catalog_plugin = catalog_factory(
            CatalogConfig(
                plugin_id=catalog_plugin_id,
                instance_id=str(catalog["name"]),
                revision=int(cast(int | str, catalog.get("revision", 0))),
                options=options,
            ),
            context,
        )
        _validate_plugin_descriptor(catalog_plugin, "catalog", catalog_plugin_id)
        _require_plugin_methods(
            catalog_plugin,
            ("validate_config", "list_namespaces", "resolve_table", "close"),
            "catalog",
        )
        catalog_plugin.validate_config(context)
        target = str(asset.get("table_identifier") or asset["name"])
        identifier = _public_table_identifier(target, TableIdentifier)
        handle = catalog_plugin.resolve_table(identifier, context)
        from dal_obscura_plugin_api import SchemaDescriptor, TableHandle

        if not isinstance(handle, TableHandle):
            raise ValidationFailure("Catalog plugin returned an invalid table handle")
        if handle.format_plugin_id != format_plugin_id:
            raise ValidationFailure("Catalog and table-format plugins do not match")
        format_plugin = format_factory(handle, context)
        _validate_plugin_descriptor(format_plugin, "table_format", format_plugin_id)
        _require_plugin_methods(format_plugin, ("schema", "close"), "table-format")
        descriptor = format_plugin.schema(handle, context)
        if not isinstance(descriptor, SchemaDescriptor):
            raise ValidationFailure("Table-format plugin returned an invalid schema")
        _validate_arrow_schema_bounds(descriptor.arrow_schema)
        return descriptor.arrow_schema
    except ValidationFailure:
        raise
    except Exception as exc:
        raise ValidationFailure("Schema discovery failed") from exc
    finally:
        if format_plugin is not None:
            _close_plugin(format_plugin)
        if catalog_plugin is not None:
            _close_plugin(catalog_plugin)


def _require_plugin_methods(plugin: object, names: tuple[str, ...], kind: str) -> None:
    if not all(callable(getattr(plugin, name, None)) for name in names):
        raise ValidationFailure(f"Admitted {kind} plugin has an incomplete lifecycle")


def _validate_plugin_descriptor(plugin: object, kind: str, plugin_id: str) -> None:
    from dal_obscura_plugin_api import PluginDescriptor

    descriptor = getattr(plugin, "descriptor", None)
    if not isinstance(descriptor, PluginDescriptor):
        raise ValidationFailure(f"Admitted {kind} plugin has an invalid descriptor")
    if descriptor.kind != kind or descriptor.plugin_id != plugin_id:
        raise ValidationFailure(f"Admitted {kind} plugin identity does not match its binding")


def _close_plugin(plugin: object) -> None:
    close = getattr(plugin, "close", None)
    if callable(close):
        close()


def _public_table_identifier(target: str, identifier_type: Any) -> Any:
    parts = tuple(target.split("."))
    if not parts or any(not part for part in parts):
        raise ValidationFailure("Asset table identifier is invalid")
    return identifier_type(namespace=parts[:-1], name=parts[-1])


def schema_fingerprint(schema: object) -> str:
    """Returns a stable digest for an authoritative Iceberg or Arrow schema.

    Iceberg schemas are normalized through their Arrow representation so API,
    evaluation, and review use one encoding.  Field and collection IDs are
    carried in Arrow metadata and included in the canonical bytes; ``repr`` is
    intentionally never used as schema identity.
    """

    arrow_schema = schema.as_arrow() if isinstance(schema, Schema) else schema
    if not isinstance(arrow_schema, pa.Schema):
        raise TypeError("schema_fingerprint expects an Iceberg or Arrow schema")
    _validate_arrow_schema_bounds(arrow_schema)
    encoded = json.dumps(
        _canonical_arrow_schema(arrow_schema),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    if len(encoded) > MAX_SCHEMA_ENCODING_BYTES:
        raise ValidationFailure(
            f"Schema encoding exceeds the {MAX_SCHEMA_ENCODING_BYTES}-byte limit"
        )
    return hashlib.sha256(encoded).hexdigest()


def _canonical_arrow_schema(schema: pa.Schema) -> dict[str, object]:
    return {
        "encoding": 1,
        "metadata": _canonical_metadata(schema.metadata),
        "fields": [_canonical_arrow_field(field) for field in schema],
    }


def _canonical_arrow_field(field: pa.Field) -> dict[str, object]:
    return {
        "name": field.name,
        "nullable": field.nullable,
        "metadata": _canonical_metadata(field.metadata),
        "type": _canonical_arrow_type(field.type),
    }


def _canonical_arrow_type(data_type: pa.DataType) -> object:
    value: dict[str, object] = {"id": str(data_type)}
    if pa.types.is_struct(data_type):
        value["children"] = [_canonical_arrow_field(field) for field in data_type]
    elif pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        value["value_field"] = _canonical_arrow_field(data_type.value_field)
    elif pa.types.is_map(data_type):
        value["key_field"] = _canonical_arrow_field(data_type.key_field)
        value["item_field"] = _canonical_arrow_field(data_type.item_field)
    elif pa.types.is_fixed_size_list(data_type):
        value["list_size"] = data_type.list_size
        value["value_field"] = _canonical_arrow_field(data_type.value_field)
    return value


def _canonical_metadata(metadata: dict[bytes, bytes] | None) -> list[list[str]]:
    if not metadata:
        return []
    return sorted(
        [
            [
                key.decode("utf-8", "backslashreplace"),
                value.decode("utf-8", "backslashreplace"),
            ]
            for key, value in metadata.items()
        ]
    )


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


def _validate_arrow_schema_bounds(schema: pa.Schema) -> None:
    try:
        validate_arrow_schema_bounds(
            schema,
            max_nodes=MAX_SCHEMA_NODES,
            max_depth=MAX_SCHEMA_DEPTH,
            max_encoding_bytes=MAX_SCHEMA_ENCODING_BYTES,
        )
    except ValueError as exc:
        raise ValidationFailure(str(exc)) from exc


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


def _arrow_field_node(
    field: pa.Field,
    path: tuple[str, ...],
    scope_digest: str,
) -> dict[str, object]:
    field_id = _arrow_field_id(field, path, scope_digest)
    field_type = field.type
    if pa.types.is_struct(field_type):
        kind = "struct"
    elif pa.types.is_list(field_type) or pa.types.is_large_list(field_type):
        kind = "list"
    elif pa.types.is_map(field_type):
        kind = "map"
    elif pa.types.is_fixed_size_list(field_type):
        kind = "list"
    else:
        kind = "scalar"
    node: dict[str, object] = {
        "field_id": field_id,
        "name": field.name,
        "path": _arrow_field_path(path, field_id),
        "human_path": ".".join(path),
        "type": str(field_type),
        "nullable": field.nullable,
        "kind": kind,
    }
    if pa.types.is_struct(field_type):
        node["children"] = [
            _arrow_field_node(child, (*path, child.name), scope_digest) for child in field_type
        ]
    elif (
        pa.types.is_list(field_type)
        or pa.types.is_large_list(field_type)
        or pa.types.is_fixed_size_list(field_type)
    ):
        node["children"] = [
            _arrow_field_node(field_type.value_field, (*path, "$element"), scope_digest)
        ]
    elif pa.types.is_map(field_type):
        node["children"] = [
            _arrow_field_node(field_type.key_field, (*path, "$key"), scope_digest),
            _arrow_field_node(field_type.item_field, (*path, "$value"), scope_digest),
        ]
    return node


def _arrow_field_path(path: tuple[str, ...], field_id: int) -> dict[str, object]:
    segments: list[FieldPathSegment] = []
    for index, part in enumerate(path):
        if part == "$element":
            segments.append(ListElementSegment())
        elif part == "$key":
            segments.append(MapKeySegment())
        elif part == "$value":
            segments.append(MapValueSegment())
        else:
            segments.append(FieldSegment(part, field_id if index == len(path) - 1 else None))
    return FieldPath(tuple(segments)).to_wire()


def _arrow_field_id(field: pa.Field, path: tuple[str, ...], scope_digest: str) -> int:
    metadata = field.metadata or {}
    for key in (b"PARQUET:field_id", b"iceberg.field.id"):
        raw = metadata.get(key)
        if raw is not None:
            try:
                return int(raw.decode("utf-8"))
            except (TypeError, ValueError):
                break
    digest = hashlib.sha256("\x1f".join((scope_digest, *path)).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False)
