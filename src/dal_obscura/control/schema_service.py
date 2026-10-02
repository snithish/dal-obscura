"""Live nested source schema discovery for governed assets."""

from __future__ import annotations

import hashlib
import json
from typing import Any, cast
from uuid import UUID

import pyarrow as pa
from pyiceberg.schema import Schema
from sqlalchemy.orm import Session

from dal_obscura.control.access import ControlPlaneActor
from dal_obscura.control.catalog_service import (
    validate_admitted_catalog_options,
    validate_catalog_options,
)
from dal_obscura.control.errors import ValidationFailure
from dal_obscura.control.policy_service import ensure_asset_capability
from dal_obscura.policy.mask_types import SUPPORTED_MASK_TYPES
from dal_obscura.policy.paths import (
    FieldPath,
    FieldPathSegment,
    FieldSegment,
    ListElementSegment,
    MapKeySegment,
)
from dal_obscura.policy.schema_bounds import (
    MAX_SCHEMA_DEPTH,
    MAX_SCHEMA_ENCODING_BYTES,
    MAX_SCHEMA_NODES,
    validate_arrow_schema_bounds,
)
from dal_obscura.policy.schema_identity import (
    numeric_field_id,
    schema_has_stable_ids,
    schema_scope_digest,
    schema_shape,
)
from dal_obscura.policy.schema_index import field_children
from dal_obscura.sources.schema import load_source_schema
from dal_obscura.sources.secrets import (
    EnvSecretProvider,
    SecretProvider,
    resolve_secret_refs,
)
from dal_obscura.storage import assets as _db_assets
from dal_obscura.storage import catalogs as _db_catalogs
from dal_obscura.storage import workspace as _db_workspace


def get_asset_schema(
    store: Session,
    asset_id: UUID,
    actor: ControlPlaneActor,
    *,
    egress_allowlist: tuple[str, ...] = (),
    plugin_registry: Any | None = None,
    secret_provider: SecretProvider | None = None,
) -> dict[str, object]:
    ensure_asset_capability(store, asset_id, actor, "read")
    schema = load_asset_schema(
        store,
        asset_id,
        actor,
        egress_allowlist=egress_allowlist,
        plugin_registry=plugin_registry,
        secret_provider=secret_provider,
    )
    arrow_schema = schema
    _validate_arrow_schema_bounds(arrow_schema)
    asset = _db_assets.get_workspace_asset(store, asset_id)
    scope_digest = schema_scope_digest(arrow_schema)
    return {
        "asset_id": str(asset_id),
        "catalog": asset["catalog"],
        "target": asset["name"],
        "schema_version": 1,
        "schema_fingerprint": schema_fingerprint(arrow_schema),
        "stable_field_ids": schema_has_stable_ids(arrow_schema),
        "supported_masks": list(SUPPORTED_MASK_TYPES),
        "fields": [
            _arrow_field_node(field, (FieldSegment(field.name),), scope_digest)
            for field in arrow_schema
        ],
    }


def load_asset_schema(
    store: Session,
    asset_id: UUID,
    actor: ControlPlaneActor,
    *,
    egress_allowlist: tuple[str, ...] = (),
    plugin_registry: Any | None = None,
    secret_provider: SecretProvider | None = None,
) -> pa.Schema:
    """Loads the authoritative admitted schema without reading table rows."""

    ensure_asset_capability(store, asset_id, actor, "read")
    asset = _db_assets.get_workspace_asset(store, asset_id)
    context = _db_workspace.get_workspace(store)
    if context is None:
        raise LookupError("No workspace has been configured")
    catalog = _db_catalogs.get_workspace_catalog(store, str(asset["catalog"]))
    options = cast(dict[str, Any], catalog["options"])
    validate_admitted_catalog_options(str(catalog["plugin_id"]), options, plugin_registry)
    validate_catalog_options(options, egress_allowlist=egress_allowlist)
    options = cast(
        dict[str, Any],
        resolve_secret_refs(
            options,
            provider=secret_provider or EnvSecretProvider(),
            expected_scope=f"catalog:{catalog['name']}",
        ),
    )
    try:
        return load_source_schema(
            asset=asset,
            catalog=catalog,
            options=options,
            plugin_registry=plugin_registry,
            validate_metadata=lambda metadata: validate_catalog_options(
                metadata, egress_allowlist=egress_allowlist
            ),
        )
    except ValidationFailure:
        raise
    except Exception as exc:
        # Provider errors can contain credentials and internal paths.
        raise ValidationFailure("Schema discovery failed") from exc


def schema_fingerprint(schema: object) -> str:
    """Returns a stable digest for an authoritative Iceberg or Arrow schema.

    Iceberg schemas are normalized through their Arrow representation so API,
    evaluation, and schema admission use one encoding. Field and collection IDs are
    carried in Arrow metadata and included in the canonical bytes; ``repr`` is
    intentionally never used as schema identity.
    """

    arrow_schema = schema.as_arrow() if isinstance(schema, Schema) else schema
    if not isinstance(arrow_schema, pa.Schema):
        raise TypeError("schema_fingerprint expects an Iceberg or Arrow schema")
    _validate_arrow_schema_bounds(arrow_schema)
    encoded = json.dumps(
        {"encoding": 1, **schema_shape(arrow_schema)},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    if len(encoded) > MAX_SCHEMA_ENCODING_BYTES:
        raise ValidationFailure(
            f"Schema encoding exceeds the {MAX_SCHEMA_ENCODING_BYTES}-byte limit"
        )
    return hashlib.sha256(encoded).hexdigest()


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


def _arrow_field_node(
    field: pa.Field,
    path: tuple[FieldPathSegment, ...],
    scope_digest: str,
) -> dict[str, object]:
    names = tuple(
        segment.name
        if isinstance(segment, FieldSegment)
        else "$element"
        if isinstance(segment, ListElementSegment)
        else "$key"
        if isinstance(segment, MapKeySegment)
        else "$value"
        for segment in path
    )
    field_id = numeric_field_id(field, names, scope_digest)
    bound_path = (
        (*path[:-1], FieldSegment(field.name, field_id))
        if isinstance(path[-1], FieldSegment)
        else path
    )
    field_type = field.type
    kind = (
        "struct"
        if pa.types.is_struct(field_type)
        else "map"
        if pa.types.is_map(field_type)
        else "list"
        if pa.types.is_list(field_type)
        or pa.types.is_large_list(field_type)
        or pa.types.is_fixed_size_list(field_type)
        else "scalar"
    )
    node: dict[str, object] = {
        "field_id": field_id,
        "name": field.name,
        "path": FieldPath(bound_path).to_wire(),
        "human_path": FieldPath(bound_path).to_human(),
        "type": str(field_type),
        "nullable": field.nullable,
        "kind": kind,
    }
    children = tuple(field_children(field))
    if children:
        node["children"] = [
            _arrow_field_node(child, (*bound_path, segment), scope_digest)
            for child, segment in children
        ]
    return node
