"""Workspace asset-policy service functions."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal, cast
from uuid import UUID

from dal_obscura.application.governance.ports import (
    GovernanceUnitOfWork,
)
from dal_obscura.kernel.identity.principal import Principal
from dal_obscura.kernel.policy.attribute_conditions import attributes_match
from dal_obscura.kernel.policy.models import (
    AccessRule,
    AssetPolicy,
    MaskRule,
    PrincipalConditionValue,
)
from dal_obscura.kernel.policy.policy_resolution import resolve_access


def capture_asset_policy(store: GovernanceUnitOfWork, asset_id: UUID) -> AssetPolicy:
    """Read an immutable policy after the caller has authorized its asset binding."""
    asset = store.assets.get_asset_binding(asset_id)
    return AssetPolicy(
        version=int(cast(int | str, asset["policy_revision"])),
        catalog=str(asset["catalog"]),
        target=str(asset["name"]),
        rules=[
            _compiled_rule_from_response(rule)
            for rule in store.policies.list_policy_rules(asset_id)
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
    from dal_obscura.application.governance.errors import ValidationFailure

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
