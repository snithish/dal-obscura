"""Bounded, source-free policy evaluation for the governance UI."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import cast
from uuid import UUID

import pyarrow as pa

from dal_obscura.common.access_control.filters import deserialize_row_filter
from dal_obscura.common.access_control.models import MaskRule
from dal_obscura.common.query_planning.field_paths import (
    FieldPath,
    FieldSegment,
    ListElementSegment,
    MapKeySegment,
    MapValueSegment,
)
from dal_obscura.control_plane.application import policy_service, schema_service
from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore
from dal_obscura.data_plane.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)

MAX_SYNTHETIC_ROWS = 100
MAX_SYNTHETIC_BYTES = 2 * 1024 * 1024
EVALUATOR_VERSION = "duckdb-synthetic-v1"


def evaluate_asset_policy(
    store: PublicationStore,
    asset_id: UUID,
    actor: ControlPlaneActor,
    *,
    principal: str,
    groups: list[str],
    claims: dict[str, object],
    rows: list[dict[str, object]] | None,
    egress_allowlist: tuple[str, ...] = (),
) -> dict[str, object]:
    """Evaluates a policy over bounded synthetic rows and returns evidence."""

    _validate_synthetic_rows(rows)
    supplied_row_count = 0 if rows is None else len(rows)
    arrow_schema = schema_service.load_asset_iceberg_schema(
        store,
        asset_id,
        actor,
        egress_allowlist=egress_allowlist,
    ).as_arrow()
    requested_columns = _leaf_paths(arrow_schema)
    preview = policy_service.preview_asset_policy(
        store,
        asset_id,
        principal=principal,
        groups=groups,
        claims=claims,
        actor=actor,
        requested_columns=requested_columns,
        include_mask_values=True,
    )
    draft = store.get_asset_policy_draft(asset_id=asset_id, author_principal=actor.identity_key())
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
                "input_rows": supplied_row_count,
            "output_rows": 0,
            "schema": str(arrow_schema),
            "rows": [],
            "evidence": evidence,
        }

    synthetic_rows = [_sample_row(arrow_schema)] if rows is None else rows
    evidence["fixture_fingerprint"] = _fingerprint(
        json.dumps(
            {"schema": schema_digest, "rows": synthetic_rows},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
    )
    try:
        batches = pa.Table.from_pylist(synthetic_rows, schema=arrow_schema).to_batches()
    except Exception as exc:
        # User-supplied synthetic values can contain arbitrary JSON types. Keep
        # Arrow's detailed type/schema errors out of the browser response.
        raise ValidationFailure("Synthetic evaluation rows are invalid") from exc
    masks = {
        str(item["column"]): MaskRule(
            type=str(item["type"]),
            value=item.get("value"),
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
    output_schema = DefaultMaskingAdapter().masked_schema(
        arrow_schema,
        cast(list[str], preview["visible_columns"]),
        masks,
    )
    output = (
        pa.Table.from_batches(transformed, schema=output_schema)
        if transformed
        else pa.Table.from_pylist([], schema=output_schema)
    )
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
        paths.extend(_nested_leaf_paths((FieldSegment(field.name, _field_id(field)),), field.type))
    return paths


def _nested_leaf_paths(
    prefix: tuple[FieldSegment | ListElementSegment | MapKeySegment | MapValueSegment, ...],
    data_type: pa.DataType,
) -> list[str]:
    if pa.types.is_struct(data_type):
        paths: list[str] = []
        for field in data_type:
            child = (*prefix, FieldSegment(field.name, _field_id(field)))
            paths.extend(_nested_leaf_paths(child, field.type))
        return paths
    if (
        pa.types.is_list(data_type)
        or pa.types.is_large_list(data_type)
        or pa.types.is_fixed_size_list(data_type)
    ):
        return _nested_leaf_paths((*prefix, ListElementSegment()), data_type.value_type)
    if pa.types.is_map(data_type):
        return [
            *_nested_leaf_paths((*prefix, MapKeySegment()), data_type.key_type),
            *_nested_leaf_paths((*prefix, MapValueSegment()), data_type.item_type),
        ]
    return [FieldPath(prefix).to_human()]


def _field_id(field: pa.Field) -> int | None:
    metadata = field.metadata or {}
    raw_id = metadata.get(b"PARQUET:field_id") or metadata.get(b"iceberg.field.id")
    if raw_id is None:
        return None
    try:
        return int(raw_id)
    except (TypeError, ValueError):
        return None


def _sample_row(schema: pa.Schema) -> dict[str, object]:
    return {field.name: _sample_value(field.name, field.type) for field in schema}


def _sample_value(name: str, data_type: pa.DataType) -> object:  # noqa: C901
    if pa.types.is_struct(data_type):
        return {field.name: _sample_value(field.name, field.type) for field in data_type}
    if pa.types.is_fixed_size_list(data_type):
        return [_sample_value(name, data_type.value_type) for _ in range(data_type.list_size)]
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return [_sample_value(name, data_type.value_type)]
    if pa.types.is_map(data_type):
        return {
            _sample_value(name, data_type.key_type): _sample_value(name, data_type.item_type)
        }
    if pa.types.is_boolean(data_type):
        return True
    if pa.types.is_integer(data_type):
        return 1
    if pa.types.is_floating(data_type):
        return 1.0
    if pa.types.is_decimal(data_type):
        return Decimal("1").scaleb(-data_type.scale)
    if pa.types.is_date32(data_type) or pa.types.is_date64(data_type):
        return date(2024, 1, 2)
    if pa.types.is_timestamp(data_type):
        value = datetime(2024, 1, 2, 3, 4, 5)
        return value.replace(tzinfo=timezone.utc) if data_type.tz else value
    if pa.types.is_time32(data_type) or pa.types.is_time64(data_type):
        return time(3, 4, 5)
    if pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type):
        return b"synthetic"
    if pa.types.is_fixed_size_binary(data_type):
        return b"x" * data_type.byte_width
    if "email" in name.lower():
        return "synthetic@example.com"
    if name.lower() == "region":
        return "us"
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return "synthetic"
    raise ValidationFailure(f"Synthetic evaluation does not support Arrow type {data_type}")


def _validate_synthetic_rows(rows: list[dict[str, object]] | None) -> None:
    if rows is None:
        return
    if len(rows) > MAX_SYNTHETIC_ROWS:
        raise ValidationFailure(f"Synthetic evaluation accepts at most {MAX_SYNTHETIC_ROWS} rows")
    try:
        encoded = json.dumps(
            rows,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValidationFailure("Synthetic evaluation rows are invalid") from exc
    if len(encoded) > MAX_SYNTHETIC_BYTES:
        raise ValidationFailure(
            f"Synthetic evaluation accepts at most {MAX_SYNTHETIC_BYTES} encoded bytes"
        )


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
