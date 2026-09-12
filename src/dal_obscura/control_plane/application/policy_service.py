"""Workspace asset-policy service functions.

Example:
    ```python
    rules = list_policy_rules(store, asset_id)
    ```
"""

from __future__ import annotations

from typing import Any, Literal, cast
from uuid import UUID

from dal_obscura.common.access_control.compiled_policy import (
    CompiledMaskRule,
    CompiledPolicy,
    CompiledPolicyRule,
)
from dal_obscura.common.access_control.models import (
    AccessRule,
    Principal,
    PrincipalConditionValue,
)
from dal_obscura.common.access_control.policy_resolution import resolve_access
from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.compiler import validate_policy_rule_payloads
from dal_obscura.control_plane.application.errors import AuthorizationFailure
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore


def list_policy_rules(
    store: PublicationStore,
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
    return store.list_policy_rules(asset_id)


def replace_policy_rules(
    store: PublicationStore,
    asset_id: UUID,
    rules: list[dict[str, Any]],
    *,
    actor: ControlPlaneActor,
) -> None:
    """Validates and replaces policy rules for one asset.

    Example:
        ```python
        replace_policy_rules(store, asset_id, rules, actor=actor)
        ```
    """

    validate_policy_rule_payloads(rules)
    # Serialize legacy shared-rule edits with publication verification.  This
    # prevents a request already waiting on the asset lock from racing a review
    # candidate at the activation boundary.
    store.lock_asset_for_publication(asset_id)
    ensure_asset_capability(store, asset_id, actor, "edit")
    store.replace_policy_rules(asset_id=asset_id, rules=rules)


def preview_asset_policy(
    store: PublicationStore,
    asset_id: UUID,
    *,
    principal: str,
    groups: list[str],
    claims: dict[str, object],
    actor: ControlPlaneActor | None = None,
    requested_columns: list[str] | None = None,
    include_mask_values: bool = False,
) -> dict[str, object]:
    """Evaluates draft policy rules for a preview principal.

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
    asset = store.get_workspace_asset(asset_id)
    raw_rules = store.list_policy_rules(asset_id)
    if actor is not None:
        draft = store.get_asset_policy_draft(
            asset_id=asset_id,
            author_principal=actor.identity_key(),
        )
        if draft is not None:
            raw_rules = cast(list[dict[str, object]], draft["rules"])
    compiled = _compiled_policy_from_response(asset, raw_rules)
    policy = compiled.to_policy()
    rules = policy.datasets[0].rules
    preview_principal = Principal(
        id=principal,
        groups=groups,
        attributes=_principal_attributes(claims),
    )
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
        "decision": "allow",
        "matched_ordinal": matched_ordinal,
        "reason": _allow_preview_reason(matched_ordinal),
        "visible_columns": visible_columns,
        "masks": mask_payload,
        "row_filter": row_filter,
    }


def ensure_policy_editor(
    store: PublicationStore,
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
    store: PublicationStore,
    asset_id: UUID,
    actor: ControlPlaneActor,
) -> None:
    """Requires an actor to be a platform admin or an owner of the asset."""

    ensure_asset_capability(store, asset_id, actor, "read")


def ensure_asset_capability(
    store: PublicationStore,
    asset_id: UUID,
    actor: ControlPlaneActor,
    capability: str,
) -> None:
    """Requires an actor to hold an asset capability or be its owner."""

    if actor.platform_admin:
        return
    principals = actor.owner_principals()
    owners = set(store.list_asset_owners(asset_id))
    if owners.intersection(principals) and capability in {"read", "edit"}:
        return
    if any(
        grant["principal"] in principals and grant["capability"] == capability
        for grant in store.list_asset_grants(asset_id)
    ):
        return
    raise AuthorizationFailure(
        "Only platform admins or asset owners with the required capability may access this asset; "
        f"the authenticated actor lacks asset capability {capability!r}."
    )


def _compiled_policy_from_response(
    asset: dict[str, object],
    raw_rules: list[dict[str, object]],
) -> CompiledPolicy:
    return CompiledPolicy(
        version=0,
        catalog=str(asset["catalog"]),
        target=str(asset["name"]),
        rules=[_compiled_rule_from_response(rule) for rule in raw_rules],
    )


def _compiled_rule_from_response(raw: dict[str, object]) -> CompiledPolicyRule:
    masks = cast(dict[str, object], raw.get("masks", {}))
    return CompiledPolicyRule(
        ordinal=int(cast(int | str, raw.get("ordinal", 0))),
        principals=[str(item) for item in _object_list(raw.get("principals"))],
        columns=[str(item) for item in _object_list(raw.get("columns"))],
        masks={
            str(column): CompiledMaskRule(
                type=str(cast(dict[str, object], value).get("type")),
                value=cast(dict[str, object], value).get("value"),
            )
            for column, value in masks.items()
            if isinstance(value, dict) and cast(dict[str, object], value).get("type")
        },
        row_filter=cast(str | None, raw.get("row_filter")),
        effect=cast(Literal["allow", "deny"], str(raw.get("effect", "allow"))),
        when=cast(dict[str, PrincipalConditionValue], raw.get("when", {})),
    )


def _access_rule_from_response(raw: dict[str, object]) -> AccessRule:
    return _compiled_rule_from_response(raw).to_access_rule()


def _principal_attributes(claims: dict[str, object]) -> dict[str, str]:
    return {str(key): str(value) for key, value in claims.items()}


def _object_list(value: object) -> list[object]:
    return list(value) if isinstance(value, list) else []


def _preview_columns(asset: dict[str, object], rules: list[AccessRule]) -> list[str]:
    schema_fields = cast(list[dict[str, object]], asset.get("schema_fields", []))
    schema_columns = [str(field["name"]) for field in schema_fields if str(field.get("name", ""))]
    if schema_columns:
        return schema_columns
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
        if not principal_tokens.intersection(rule.principals):
            continue
        if _preview_conditions_match(rule.when, principal.attributes):
            return int(cast(int | str, raw_rule["ordinal"]))
    return None


def _preview_conditions_match(
    conditions: dict[str, PrincipalConditionValue] | None,
    attributes: dict[str, str],
) -> bool:
    if not conditions:
        return True
    for key, expected in conditions.items():
        actual = attributes.get(key)
        if actual is None:
            return False
        if isinstance(expected, list):
            if actual not in {str(item) for item in expected}:
                return False
            continue
        if actual != str(expected):
            return False
    return True


def _allow_preview_reason(matched_ordinal: int | None) -> str:
    if matched_ordinal is None:
        return "Policy allowed access."
    return f"Rule {matched_ordinal} matched."


def _deny_preview_reason(matched_ordinal: int | None) -> str:
    if matched_ordinal is None:
        return "No rule matched."
    return f"Rule {matched_ordinal} denied access."
