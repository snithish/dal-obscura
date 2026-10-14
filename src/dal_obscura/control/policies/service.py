"""Workspace asset-policy service functions."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal, cast
from uuid import UUID

from sqlalchemy.orm import Session

from dal_obscura.control.access import ControlPlaneActor
from dal_obscura.control.assets.access import ensure_asset_capability, ensure_asset_owner
from dal_obscura.control.policies.compiler import compile_policy_rule_payloads
from dal_obscura.policy.attribute_conditions import attributes_match
from dal_obscura.policy.models import (
    AccessRule,
    AssetPolicy,
    MaskRule,
    Principal,
    PrincipalConditionValue,
)
from dal_obscura.policy.policy_resolution import resolve_access
from dal_obscura.storage import assets as _db_assets
from dal_obscura.storage import audit as _db_audit
from dal_obscura.storage import policies as _db_policies


def list_policy_rules(
    store: Session,
    asset_id: UUID,
    *,
    actor: ControlPlaneActor | None = None,
) -> list[dict[str, object]]:
    """Lists ordered policy rules for one asset."""

    if actor is not None:
        ensure_asset_capability(store, asset_id, actor, "read")
    return _db_policies.list_policy_rules(store, asset_id)


def replace_policy_rules(
    store: Session,
    asset_id: UUID,
    rules: list[dict[str, Any]],
    *,
    actor: ControlPlaneActor,
    expected_revision: int | None = None,
    revoke_existing_tokens: bool = False,
) -> dict[str, int]:
    """Validates and replaces policy rules for one asset."""

    _db_assets.lock_asset_for_update(store, asset_id)
    ensure_asset_capability(store, asset_id, actor, "edit")
    schema_fields = _db_assets.list_asset_schema_fields(store, asset_id)
    compiled_rules = compile_policy_rule_payloads(rules, schema_fields)
    if expected_revision is None:
        current_revision = _db_assets.get_workspace_asset(store, asset_id)["policy_revision"]
        if not isinstance(current_revision, int):
            raise RuntimeError("Asset policy revision is unavailable")
        expected_revision = current_revision
    if revoke_existing_tokens:
        ensure_asset_owner(store, asset_id, actor)
    revision = _db_policies.replace_policy_rules(
        store,
        asset_id=asset_id,
        rules=compiled_rules,
        expected_revision=expected_revision,
    )
    revoked_count = (
        _db_policies.revoke_asset_tickets(store, asset_id=asset_id) if revoke_existing_tokens else 0
    )
    _db_audit.record_asset_audit_event(
        store,
        asset_id=asset_id,
        actor_principal=actor.identity_key(),
        action="asset.policy.replace",
        details={"policy_revision": revision, "revoked_token_count": revoked_count},
    )
    return {"policy_revision": revision, "revoked_token_count": revoked_count}


def revoke_asset_tickets(
    store: Session,
    asset_id: UUID,
    *,
    actor: ControlPlaneActor,
) -> dict[str, int]:
    """Revokes all unexpired asset tokens; only owners and platform admins may do so."""

    _db_assets.lock_asset_for_update(store, asset_id)
    ensure_asset_owner(store, asset_id, actor)
    revoked_count = _db_policies.revoke_asset_tickets(store, asset_id=asset_id)
    _db_audit.record_asset_audit_event(
        store,
        asset_id=asset_id,
        actor_principal=actor.identity_key(),
        action="asset.tokens.revoke",
        details={"revoked_token_count": revoked_count},
    )
    return {"revoked_token_count": revoked_count}


def capture_asset_policy(store: Session, asset_id: UUID) -> AssetPolicy:
    """Read an immutable policy after the caller has authorized its asset binding."""
    asset = _db_assets.get_asset_binding(store, asset_id)
    return AssetPolicy(
        version=int(cast(int | str, asset["policy_revision"])),
        catalog=str(asset["catalog"]),
        target=str(asset["name"]),
        rules=[
            _compiled_rule_from_response(rule)
            for rule in _db_policies.list_policy_rules(store, asset_id)
        ],
    )


def preview_asset_policy(
    snapshot: AssetPolicy,
    principal: Principal,
    *,
    requested_columns: list[str],
    include_mask_values: bool = False,
) -> dict[str, object]:
    """Evaluate one captured policy without a database session."""
    policy = snapshot.to_policy()
    conditions = [
        {
            "rule_ordinal": rule.ordinal,
            "key": key,
            "expected": expected,
            "actual": principal.attributes.get(key),
            "matched": attributes_match(principal.attributes, {key: expected}),
            "missing": key not in principal.attributes,
        }
        for rule in snapshot.rules
        for key, expected in (rule.when or {}).items()
    ]
    matched_ordinal = _first_matching_rule_ordinal(snapshot.rules, principal)
    try:
        visible_columns, masks, row_filter = resolve_access(
            policy,
            principal,
            snapshot.target,
            snapshot.catalog,
            requested_columns,
        )
    except PermissionError:
        return {
            "conditions": conditions,
            "decision": "deny",
            "matched_ordinal": matched_ordinal,
            "reason": _deny_preview_reason(matched_ordinal),
            "visible_columns": [],
            "masks": [],
            "row_filter": None,
        }
    mask_payload = [
        {
            "column": column,
            "type": mask.type,
            **({"value": mask.value} if include_mask_values else {}),
        }
        for column, mask in sorted(masks.items())
    ]
    return {
        "conditions": conditions,
        "decision": "allow",
        "matched_ordinal": matched_ordinal,
        "reason": _allow_preview_reason(matched_ordinal),
        "visible_columns": visible_columns,
        "masks": mask_payload,
        "row_filter": row_filter,
    }


def _compiled_rule_from_response(raw: dict[str, object]) -> AccessRule:
    masks = cast(dict[str, object], raw.get("masks", {}))

    def compile_mask(value: object) -> MaskRule:
        raw_mask = cast(dict[str, object], value)
        return MaskRule(
            type=str(raw_mask.get("type")),
            value=raw_mask.get("value"),
            exempt_principals=tuple(
                str(item) for item in _object_list(raw_mask.get("exempt_principals"))
            ),
        )

    return AccessRule(
        ordinal=int(cast(int | str, raw.get("ordinal", 0))),
        principals=[str(item) for item in _object_list(raw.get("principals"))],
        columns=[str(item) for item in _object_list(raw.get("columns"))],
        masks={
            str(column): compile_mask(value)
            for column, value in masks.items()
            if isinstance(value, dict) and cast(dict[str, object], value).get("type")
        },
        row_filter=cast(str | None, raw.get("row_filter")),
        effect=cast(Literal["allow", "allow_all"], str(raw.get("effect", "allow"))),
        when=cast(dict[str, PrincipalConditionValue], raw.get("when", {})),
        name=str(raw.get("name", "")),
        description=str(raw.get("description", "")),
    )


def preview_principal(
    principal: str, groups: Sequence[str], claims: dict[str, object]
) -> Principal:
    """Normalize synthetic internal attributes through the policy preview boundary."""
    from dal_obscura.control.errors import ValidationFailure

    if any(isinstance(value, dict | list) for value in claims.values()):
        raise ValidationFailure(
            "Internal attributes must be scalar values; use provider claims for nested input"
        )
    attributes = {
        str(key): str(value).strip()
        for key, value in claims.items()
        if value is not None and str(value).strip()
    }
    return Principal(id=principal, groups=groups, attributes=attributes)


def _object_list(value: object) -> list[object]:
    return list(value) if isinstance(value, list) else []


def _first_matching_rule_ordinal(
    rules: Sequence[AccessRule],
    principal: Principal,
) -> int | None:
    principal_tokens = set(principal.tokens())
    for rule in rules:
        if "*" not in rule.principals and not principal_tokens.intersection(rule.principals):
            continue
        if attributes_match(principal.attributes, rule.when):
            return rule.ordinal
    return None


def _allow_preview_reason(matched_ordinal: int | None) -> str:
    if matched_ordinal is None:
        return "Policy allowed access."
    return f"Rule {matched_ordinal} matched."


def _deny_preview_reason(matched_ordinal: int | None) -> str:
    if matched_ordinal is None:
        return "No rule matched."
    return f"Rule {matched_ordinal} denied access."
