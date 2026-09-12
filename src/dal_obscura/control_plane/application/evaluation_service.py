"""Bounded, source-free policy evaluation for the governance UI."""

from __future__ import annotations

import hashlib
import json
from typing import Any, cast
from uuid import UUID

import pyarrow as pa

from dal_obscura.common.access_control.filters import deserialize_row_filter
from dal_obscura.common.access_control.models import MaskRule
from dal_obscura.control_plane.application import policy_service, schema_service
from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore
from dal_obscura.data_plane.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)

MAX_SYNTHETIC_ROWS = 100
EVALUATOR_VERSION = "duckdb-synthetic-v1"


def evaluate_asset_policy(
    store: PublicationStore,
    asset_id: UUID,
    actor: ControlPlaneActor,
    *,
    principal: str,
    groups: list[str],
    claims: dict[str, object],
    rows: list[dict[str, object]],
) -> dict[str, object]:
    """Evaluates a policy over bounded synthetic rows and returns evidence."""

    if len(rows) > MAX_SYNTHETIC_ROWS:
        raise ValidationFailure(f"Synthetic evaluation accepts at most {MAX_SYNTHETIC_ROWS} rows")
    arrow_schema = schema_service.load_asset_iceberg_schema(store, asset_id, actor).as_arrow()
    requested_columns = _leaf_paths(arrow_schema)
    preview = policy_service.preview_asset_policy(
        store,
        asset_id,
        principal=principal,
        groups=groups,
        claims=claims,
        actor=actor,
        requested_columns=requested_columns,
    )
    draft = store.get_asset_policy_draft(asset_id=asset_id, author_principal=actor.principal)
    revision = 0 if draft is None else int(cast(int | str, draft["revision"]))
    schema_digest = schema_service.schema_fingerprint(arrow_schema)
    evidence = {
        "draft_revision": revision,
        "draft_content_hash": None if draft is None else draft["content_hash"],
        "schema_fingerprint": schema_digest,
        "persona_fingerprint": _fingerprint(
            json.dumps(
                {"principal": principal, "groups": groups, "claims": claims},
                sort_keys=True,
                default=str,
            )
        ),
        "evaluator_version": EVALUATOR_VERSION,
    }
    if preview["decision"] != "allow":
        return {
            "status": "completed",
            "decision": "deny",
            "allowed_columns": [],
            "masks": [],
            "row_filter": None,
            "input_rows": len(rows),
            "output_rows": 0,
            "schema": str(arrow_schema),
            "rows": [],
            "evidence": evidence,
        }

    synthetic_rows = rows or [_sample_row(arrow_schema)]
    batches = pa.Table.from_pylist(synthetic_rows, schema=arrow_schema).to_batches()
    raw_rules = store.list_policy_rules(asset_id)
    if draft is not None:
        raw_rules = cast(list[dict[str, object]], draft["rules"])
    mask_values: dict[str, object | None] = {}
    for raw_rule in raw_rules:
        for column, raw_mask in cast(dict[str, object], raw_rule.get("masks", {})).items():
            if isinstance(raw_mask, dict):
                mask_values.setdefault(column, cast(dict[str, Any], raw_mask).get("value"))
    masks = {
        str(item["column"]): MaskRule(
            type=str(item["type"]),
            value=mask_values.get(str(item["column"])),
        )
        for item in cast(list[dict[str, object]], preview["masks"])
    }
    row_filter_sql = cast(str | None, preview["row_filter"])
    row_filter = deserialize_row_filter(row_filter_sql) if row_filter_sql else None
    try:
        transformed = list(
            DuckDBRowTransformAdapter(DefaultMaskingAdapter()).apply_filters_and_masks_stream(
                batches,
                cast(list[str], preview["visible_columns"]),
                row_filter,
                masks,
            )
        )
    except Exception as exc:
        # Evaluator errors can include synthetic values or provider internals;
        # return a stable message at the browser boundary.
        raise ValidationFailure("Synthetic evaluation failed") from exc
    output = pa.Table.from_batches(transformed) if transformed else pa.table({})
    return {
        "status": "completed",
        "decision": "allow",
        "allowed_columns": preview["visible_columns"],
        "masks": preview["masks"],
        "row_filter": preview["row_filter"],
        "input_rows": len(synthetic_rows),
        "output_rows": output.num_rows,
        "schema": str(output.schema),
        "rows": output.to_pylist(),
        "evidence": evidence,
    }


def _leaf_paths(schema: pa.Schema) -> list[str]:
    paths: list[str] = []
    for field in schema:
        if pa.types.is_struct(field.type):
            paths.extend(_nested_leaf_paths(field.name, field.type))
        else:
            paths.append(field.name)
    return paths


def _nested_leaf_paths(prefix: str, data_type: pa.DataType) -> list[str]:
    if pa.types.is_struct(data_type):
        paths: list[str] = []
        for field in data_type:
            child = f"{prefix}.{field.name}"
            paths.extend(_nested_leaf_paths(child, field.type))
        return paths
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return _nested_leaf_paths(f"{prefix}.$element", data_type.value_type)
    if pa.types.is_map(data_type):
        return [
            *_nested_leaf_paths(f"{prefix}.$key", data_type.key_type),
            *_nested_leaf_paths(f"{prefix}.$value", data_type.item_type),
        ]
    return [prefix]


def _sample_row(schema: pa.Schema) -> dict[str, object]:
    return {field.name: _sample_value(field.name, field.type) for field in schema}


def _sample_value(name: str, data_type: pa.DataType) -> object:
    if pa.types.is_struct(data_type):
        return {field.name: _sample_value(field.name, field.type) for field in data_type}
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return [_sample_value(name, data_type.value_type)]
    if pa.types.is_map(data_type):
        return {"synthetic": _sample_value(name, data_type.item_type)}
    if pa.types.is_boolean(data_type):
        return True
    if pa.types.is_integer(data_type) or pa.types.is_floating(data_type):
        return 1
    if pa.types.is_date(data_type) or pa.types.is_timestamp(data_type):
        return None
    if "email" in name.lower():
        return "synthetic@example.com"
    if name.lower() == "region":
        return "us"
    return "synthetic"


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
