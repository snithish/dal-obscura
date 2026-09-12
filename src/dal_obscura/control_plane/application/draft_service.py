"""Revisioned policy-draft application service."""

from __future__ import annotations

import hashlib
import json
from typing import Any, cast
from uuid import UUID

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.compiler import validate_policy_rule_payloads
from dal_obscura.control_plane.application.policy_service import ensure_asset_capability
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore


def get_policy_draft(
    store: PublicationStore,
    asset_id: UUID,
    actor: ControlPlaneActor,
) -> dict[str, object]:
    ensure_asset_capability(store, asset_id, actor, "read")
    existing = store.get_asset_policy_draft(
        asset_id=asset_id,
        author_principal=actor.principal,
    )
    if existing is not None:
        return existing
    return {
        "id": None,
        "asset_id": str(asset_id),
        "author_principal": actor.principal,
        "revision": 0,
        "base_policy_version": 0,
        "rules": store.list_policy_rules(asset_id),
        "content_hash": _content_hash(store.list_policy_rules(asset_id)),
        "created_at": None,
        "updated_at": None,
    }


def save_policy_draft(
    store: PublicationStore,
    asset_id: UUID,
    actor: ControlPlaneActor,
    *,
    expected_revision: int,
    rules: list[dict[str, Any]],
) -> dict[str, object]:
    ensure_asset_capability(store, asset_id, actor, "edit")
    validate_policy_rule_payloads(rules)
    asset = store.get_workspace_asset(asset_id)
    return store.save_asset_policy_draft(
        asset_id=asset_id,
        author_principal=actor.principal,
        expected_revision=expected_revision,
        rules=rules,
        content_hash=_content_hash(rules),
        base_policy_version=_base_policy_version(asset),
    )


def restore_policy_version(
    store: PublicationStore,
    asset_id: UUID,
    actor: ControlPlaneActor,
    *,
    policy_version: int,
    expected_revision: int,
) -> dict[str, object]:
    """Copies immutable history into a new revisioned draft."""

    ensure_asset_capability(store, asset_id, actor, "edit")
    historical = store.get_published_asset_policy(
        asset_id=asset_id,
        policy_version=policy_version,
    )
    rules = cast(list[dict[str, Any]], historical["rules"])
    validate_policy_rule_payloads(rules)
    return store.save_asset_policy_draft(
        asset_id=asset_id,
        author_principal=actor.principal,
        expected_revision=expected_revision,
        rules=rules,
        content_hash=_content_hash(rules),
        base_policy_version=policy_version,
    )


def _content_hash(rules: list[dict[str, object]]) -> str:
    encoded = json.dumps(rules, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def _base_policy_version(asset: dict[str, object]) -> int:
    raw = asset.get("policy_version", 0)
    if isinstance(raw, bool) or not isinstance(raw, (int, str)):
        return 0
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0
