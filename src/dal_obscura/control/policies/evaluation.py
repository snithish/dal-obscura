"""Bounded, source-free policy evaluation for the governance UI."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import cast
from uuid import UUID

import pyarrow as pa
from dal_obscura_plugin_api.schema.index import SchemaIndex
from sqlalchemy.orm import Session

from dal_obscura.adapters.duckdb.transform import (
    DuckDBRowTransformAdapter,
)
from dal_obscura.adapters.sql import workspace as _db_workspace
from dal_obscura.application.governance.errors import (
    ConfigurationConflictError,
    ValidationFailure,
)
from dal_obscura.application.reads.ports import ProjectionCompiler
from dal_obscura.control.assets import schema as schema_service
from dal_obscura.control.policies import service as policy_service
from dal_obscura.control.workspace.attributes import provider_mapper
from dal_obscura.kernel.identity.actor import ControlPlaneActor
from dal_obscura.kernel.identity.principal import Principal
from dal_obscura.kernel.policy.filters import deserialize_row_filter
from dal_obscura.kernel.policy.models import AssetPolicy, MaskRule
from dal_obscura.plugins import PluginRegistry

MAX_SYNTHETIC_ROWS = 100
MAX_SYNTHETIC_BYTES = 2 * 1024 * 1024
EVALUATOR_VERSION = "duckdb-synthetic-v1"


@dataclass(frozen=True)
class PolicyEvaluation:
    """Captured policy, mapped subject and detached bounded synthetic inputs."""

    source: schema_service.AssetSchemaDiscovery
    policy: AssetPolicy
    principal: Principal
    claims_json: str
    rows_json: str | None
    provider_ordinal: int | None
    provider_revision: int | None


def capture_asset_evaluation(
    store: Session,
    asset_id: UUID,
    actor: ControlPlaneActor,
    *,
    principal: str,
    groups: list[str],
    claims: dict[str, object],
    rows: list[dict[str, object]] | None,
    provider_ordinal: int | None = None,
    egress_allowlist: tuple[str, ...] = (),
    plugin_registry: PluginRegistry | None = None,
) -> PolicyEvaluation:
    source = schema_service.capture_asset_schema(
        store,
        asset_id,
        actor,
        egress_allowlist=egress_allowlist,
        plugin_registry=plugin_registry,
    )
    rows_json = _encode_synthetic_rows(rows)
    try:
        claims_json = json.dumps(claims, sort_keys=True, allow_nan=False)
        if json.loads(claims_json) != claims:
            raise ValidationFailure("Synthetic identity claims are invalid")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ValidationFailure("Synthetic identity claims are invalid") from exc
    provider_revision = None
    if provider_ordinal is not None:
        mapper, provider = provider_mapper(store, provider_ordinal)
        try:
            subject = mapper.map_claims(claims)
        except PermissionError as exc:
            raise ValidationFailure(str(exc)) from exc
        provider_revision = int(cast(int | str, provider["revision"]))
        claims_json = json.dumps(dict(subject.attributes), sort_keys=True, allow_nan=False)
    else:
        subject = policy_service.preview_principal(principal, groups, claims)
    return PolicyEvaluation(
        source=source,
        policy=policy_service.capture_asset_policy(store, asset_id),
        principal=subject,
        claims_json=claims_json,
        rows_json=rows_json,
        provider_ordinal=provider_ordinal,
        provider_revision=provider_revision,
    )


def ensure_asset_evaluation_current(
    store: Session, captured: PolicyEvaluation, actor: ControlPlaneActor
) -> None:
    asset = schema_service.ensure_asset_schema_current(store, captured.source, actor)
    revision = int(cast(int | str, asset["policy_revision"]))
    if revision != captured.policy.version:
        raise ConfigurationConflictError(
            "Policy changed during evaluation; run evaluation again", current_revision=revision
        )
    if captured.provider_revision is not None:
        revision = _db_workspace.get_auth_provider_revision(store)
        if revision != captured.provider_revision:
            raise ConfigurationConflictError(
                "Identity mapping changed during evaluation; run evaluation again",
                current_revision=revision,
            )


def evaluate_asset_policy(
    captured: PolicyEvaluation, arrow_schema: pa.Schema, *, compiler: ProjectionCompiler
) -> dict[str, object]:
    """Evaluate a captured policy over bounded synthetic rows without database IO."""
    rows = json.loads(captured.rows_json) if captured.rows_json is not None else None
    claims = json.loads(captured.claims_json)
    principal = captured.principal.id
    groups = list(captured.principal.groups)
    supplied_row_count = 0 if rows is None else len(rows)
    preview = policy_service.preview_asset_policy(
        captured.policy,
        captured.principal,
        requested_columns=SchemaIndex(arrow_schema).expand(["*"]),
        include_mask_values=True,
    )
    schema_digest = schema_service.schema_fingerprint(arrow_schema)
    mapping_evidence: dict[str, object] = {"mode": "internal_attributes"}
    if captured.provider_ordinal is not None:
        mapping_evidence = {
            "mode": "provider_claims",
            "provider_ordinal": captured.provider_ordinal,
            "provider_revision": captured.provider_revision,
        }
    evidence = {
        "policy_revision": captured.policy.version,
        "schema_fingerprint": schema_digest,
        "persona_fingerprint": _fingerprint(
            json.dumps(
                {"principal": principal, "groups": groups, "claims": claims},
                sort_keys=True,
                default=str,
            )
        ),
        "evaluator_version": EVALUATOR_VERSION,
        "identity_mapping": mapping_evidence,
        "identity": {"principal": principal, "groups": groups, "attributes": claims},
        "conditions": preview.get("conditions", []),
    }
    if preview["decision"] != "allow":
        return {
            "status": "completed",
            "policy_revision": evidence["policy_revision"],
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
        projection = compiler.compile(
            arrow_schema, cast(list[str], preview["visible_columns"]), masks
        )
        transformed = list(
            DuckDBRowTransformAdapter().apply_filters_and_masks_stream(
                batches,
                projection,
                row_filter,
            )
        )
    except Exception as exc:
        # Evaluator errors can include synthetic values or provider internals;
        # return a stable message at the browser boundary.
        raise ValidationFailure("Synthetic evaluation failed") from exc
    output_schema = projection.output_schema
    output = (
        pa.Table.from_batches(transformed, schema=output_schema)
        if transformed
        else pa.Table.from_pylist([], schema=output_schema)
    )
    return {
        "status": "completed",
        "policy_revision": evidence["policy_revision"],
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
        return {_sample_value(name, data_type.key_type): _sample_value(name, data_type.item_type)}
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
        return value.replace(tzinfo=UTC) if data_type.tz else value
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


def _encode_synthetic_rows(rows: list[dict[str, object]] | None) -> str | None:
    if rows is None:
        return None
    if len(rows) > MAX_SYNTHETIC_ROWS:
        raise ValidationFailure(f"Synthetic evaluation accepts at most {MAX_SYNTHETIC_ROWS} rows")
    try:
        encoded = json.dumps(
            rows,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ValidationFailure("Synthetic evaluation rows are invalid") from exc
    if len(encoded.encode("utf-8")) > MAX_SYNTHETIC_BYTES:
        raise ValidationFailure(
            f"Synthetic evaluation accepts at most {MAX_SYNTHETIC_BYTES} encoded bytes"
        )

    try:
        # JSON snapshots must not change map keys or collection kinds.
        if json.loads(encoded) != rows:
            raise ValidationFailure("Synthetic evaluation rows are invalid")
    except RecursionError as exc:
        raise ValidationFailure("Synthetic evaluation rows are invalid") from exc
    return encoded


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
