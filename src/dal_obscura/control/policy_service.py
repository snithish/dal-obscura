"""Workspace asset-policy service functions.

Example:
    ```python
    rules = list_policy_rules(store, asset_id)
    ```
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal, cast
from uuid import UUID

from sqlalchemy.orm import Session

from dal_obscura.control.access import ControlPlaneActor
from dal_obscura.control.errors import AuthorizationFailure
from dal_obscura.control.policy_compiler import compile_policy_rule_payloads
from dal_obscura.policy.attribute_conditions import attributes_match
from dal_obscura.policy.models import (
    AccessRule,
    AssetPolicy,
    MaskRule,
    Principal,
    PrincipalConditionValue,
)
from dal_obscura.policy.paths import parse_field_path
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
    """Lists ordered policy rules for one asset.

    Example:
        ```python
        rules = list_policy_rules(store, asset_id)
        ```
    """

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
    """Validates and replaces policy rules for one asset.

    Example:
        ```python
        replace_policy_rules(store, asset_id, rules, actor=actor)
        ```
    """

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


def preview_asset_policy(
    store: Session,
    asset_id: UUID,
    *,
    principal: str,
    groups: list[str],
    claims: dict[str, object],
    actor: ControlPlaneActor | None = None,
    requested_columns: list[str] | None = None,
    include_mask_values: bool = False,
) -> dict[str, object]:
    """Evaluates current live policy rules for a preview principal.

    Example:
        ```python
        preview = preview_asset_policy(
            store,
            asset_id,
            principal="alice",
            groups=["analytics"],
            claims={},
        )
        ```
    """

    if actor is not None:
        ensure_asset_capability(store, asset_id, actor, "read")
    asset = _db_assets.get_workspace_asset(store, asset_id)
    raw_rules = _db_policies.list_policy_rules(store, asset_id)
    compiled = _compiled_policy_from_response(asset, raw_rules)
    policy = compiled.to_policy()
    rules = policy.datasets[0].rules
    preview_principal = Principal(
        id=principal,
        groups=groups,
        attributes=_principal_attributes(claims),
    )
    conditions = [
        {
            "rule_ordinal": raw["ordinal"],
            "key": key,
            "expected": expected,
            "actual": preview_principal.attributes.get(key),
            "matched": _preview_conditions_match({key: expected}, preview_principal.attributes),
            "missing": key not in preview_principal.attributes,
        }
        for raw in raw_rules
        for key, expected in cast(dict[str, PrincipalConditionValue], raw.get("when", {})).items()
    ]
    requested = requested_columns or _preview_columns(asset, rules)
    matched_ordinal = _first_matching_rule_ordinal(raw_rules, preview_principal)
    try:
        visible_columns, masks, row_filter = resolve_access(
            policy,
            preview_principal,
            str(asset["name"]),
            str(asset["catalog"]),
            requested,
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


def ensure_policy_editor(
    store: Session,
    asset_id: UUID,
    actor: ControlPlaneActor,
) -> None:
    """Requires the actor to be a platform admin or asset owner.

    Example:
        ```python
        ensure_policy_editor(store, asset_id, actor)
        ```
    """

    ensure_asset_capability(store, asset_id, actor, "edit")


def ensure_asset_reader(
    store: Session,
    asset_id: UUID,
    actor: ControlPlaneActor,
) -> None:
    """Requires an actor to be a platform admin or an owner of the asset."""

    ensure_asset_capability(store, asset_id, actor, "read")


def ensure_asset_capability(
    store: Session,
    asset_id: UUID,
    actor: ControlPlaneActor,
    capability: str,
) -> None:
    """Requires an actor to hold an asset capability or be its owner."""

    if actor.platform_admin:
        return
    principals = actor.owner_principals()
    owners = set(_db_assets.list_asset_owners(store, asset_id))
    if owners.intersection(principals) and capability in {"read", "edit"}:
        return
    if any(
        grant["principal"] in principals and grant["capability"] == capability
        for grant in _db_assets.list_asset_grants(store, asset_id)
    ):
        return
    raise AuthorizationFailure(
        "Only platform admins or asset owners with the required capability may access this asset; "
        f"the authenticated actor lacks asset capability {capability!r}."
    )


def ensure_asset_owner(
    store: Session,
    asset_id: UUID,
    actor: ControlPlaneActor,
) -> None:
    """Requires explicit asset ownership or platform-admin authority."""

    if actor.platform_admin:
        return
    if set(_db_assets.list_asset_owners(store, asset_id)).intersection(actor.owner_principals()):
        return
    raise AuthorizationFailure("Only platform admins or asset owners may revoke asset tokens.")


def _compiled_policy_from_response(
    asset: dict[str, object],
    raw_rules: list[dict[str, object]],
) -> AssetPolicy:
    return AssetPolicy(
        version=0,
        catalog=str(asset["catalog"]),
        target=str(asset["name"]),
        rules=[_compiled_rule_from_response(rule) for rule in raw_rules],
    )


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


def _access_rule_from_response(raw: dict[str, object]) -> AccessRule:
    return _compiled_rule_from_response(raw)


def _principal_attributes(claims: dict[str, object]) -> dict[str, str]:
    from dal_obscura.control.errors import ValidationFailure

    if any(isinstance(value, dict | list) for value in claims.values()):
        raise ValidationFailure(
            "Internal attributes must be scalar values; use provider claims for nested input"
        )
    return {
        str(key): str(value).strip()
        for key, value in claims.items()
        if value is not None and str(value).strip()
    }


def _object_list(value: object) -> list[object]:
    return list(value) if isinstance(value, list) else []


def _preview_columns(asset: dict[str, object], rules: Sequence[AccessRule]) -> list[str]:
    schema_fields = cast(list[dict[str, object]], asset.get("schema_fields", []))
    schema_columns = [str(field["name"]) for field in schema_fields if str(field.get("name", ""))]
    if schema_columns:
        paths = {name: parse_field_path(name).segments for name in schema_columns}
        return [
            name
            for name, path in paths.items()
            if not any(
                len(other) > len(path) and other[: len(path)] == path for other in paths.values()
            )
        ]
    columns: list[str] = []
    seen: set[str] = set()
    for rule in rules:
        for column in rule.columns:
            if column == "*" or column in seen:
                continue
            columns.append(column)
            seen.add(column)
    return columns or ["*"]


def _first_matching_rule_ordinal(
    raw_rules: list[dict[str, object]],
    principal: Principal,
) -> int | None:
    principal_tokens = set(principal.tokens())
    for raw_rule in raw_rules:
        rule = _access_rule_from_response(raw_rule)
        if "*" not in rule.principals and not principal_tokens.intersection(rule.principals):
            continue
        if _preview_conditions_match(rule.when, principal.attributes):
            return int(cast(int | str, raw_rule["ordinal"]))
    return None


def _preview_conditions_match(
    conditions: Mapping[str, PrincipalConditionValue] | None,
    attributes: Mapping[str, str],
) -> bool:
    return attributes_match(attributes, conditions)


def _allow_preview_reason(matched_ordinal: int | None) -> str:
    if matched_ordinal is None:
        return "Policy allowed access."
    return f"Rule {matched_ordinal} matched."


def _deny_preview_reason(matched_ordinal: int | None) -> str:
    if matched_ordinal is None:
        return "No rule matched."
    return f"Rule {matched_ordinal} denied access."
