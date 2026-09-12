"""Server-authoritative review evidence for policy publication."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import cast
from uuid import UUID

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.application.policy_service import ensure_asset_capability
from dal_obscura.control_plane.application.schema_service import (
    load_asset_iceberg_schema,
    schema_fingerprint,
)
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore

REVIEW_TTL_SECONDS = 600


def issue_review_token(
    store: PublicationStore,
    asset_id: UUID,
    actor: ControlPlaneActor,
    evaluation: dict[str, object],
    *,
    secret: str,
    now: int | None = None,
    require_saved_draft: bool = False,
) -> dict[str, object]:
    """Signs completed evaluation evidence for one exact draft generation."""

    ensure_asset_capability(store, asset_id, actor, "publish")
    if evaluation.get("status") != "completed":
        raise ValidationFailure("Only a completed evaluation can be reviewed.")
    if evaluation.get("decision") != "allow" and not _explicit_deny_all_draft(
        store,
        asset_id,
        actor,
    ):
        raise ValidationFailure(
            "Only an allowed evaluation or explicit deny-all draft can be reviewed."
        )
    draft = store.get_asset_policy_draft(
        asset_id=asset_id,
        author_principal=actor.identity_key(),
    )
    if require_saved_draft and draft is None:
        raise ValidationFailure(
            "Save an explicit policy draft before requesting server review."
        )
    revision = 0 if draft is None else int(cast(int | str, draft["revision"]))
    content_hash = None if draft is None else str(draft["content_hash"])
    shared_rules_hash = (
        _rules_hash(store.list_policy_rules(asset_id)) if draft is None else None
    )
    active_publication_id = _active_publication_id(store, asset_id)
    admitted_schema_hash = _admitted_schema_hash(store, asset_id)
    issued_at = int(time.time() if now is None else now)
    payload: dict[str, object] = {
        "asset_id": str(asset_id),
        "actor": actor.identity_key(),
        "draft_revision": revision,
        "draft_content_hash": content_hash,
        "shared_rules_hash": shared_rules_hash,
        "active_publication_id": active_publication_id,
        "admitted_schema_hash": admitted_schema_hash,
        "evidence": cast(dict[str, object], evaluation.get("evidence", {})),
        "issued_at": issued_at,
        "expires_at": issued_at + REVIEW_TTL_SECONDS,
    }
    token = _encode_signed(payload, secret)
    return {
        **evaluation,
        "review_token": token,
        "review_expires_at": payload["expires_at"],
    }


def verify_review_token(
    store: PublicationStore,
    asset_id: UUID,
    actor: ControlPlaneActor,
    token: str,
    *,
    secret: str,
    now: int | None = None,
) -> None:
    """Rejects stale, replayed-for-another-scope, or forged review evidence."""

    ensure_asset_capability(store, asset_id, actor, "publish")
    payload = _decode_signed(token, secret)
    current_time = int(time.time() if now is None else now)
    expires_at = payload.get("expires_at", 0)
    if not isinstance(expires_at, (int, str)) or int(expires_at) <= current_time:
        raise ValidationFailure("Policy review has expired; run the evaluation again.")
    if payload.get("asset_id") != str(asset_id) or payload.get("actor") != actor.identity_key():
        raise ValidationFailure("Policy review is bound to another actor or asset.")
    evidence_raw = payload.get("evidence")
    if not isinstance(evidence_raw, dict):
        raise ValidationFailure("Policy review evidence is invalid.")
    evidence = cast(dict[str, object], evidence_raw)
    if evidence.get("evaluator_version") != "duckdb-synthetic-v1":
        raise ValidationFailure("Policy review evidence is invalid.")
    recorded_schema_fingerprint = evidence.get("schema_fingerprint")
    if not isinstance(recorded_schema_fingerprint, str) or not recorded_schema_fingerprint:
        raise ValidationFailure("Policy review evidence is invalid.")
    current_schema = load_asset_iceberg_schema(store, asset_id, actor)
    if not hmac.compare_digest(
        recorded_schema_fingerprint,
        schema_fingerprint(current_schema.as_arrow()),
    ):
        raise ValidationFailure("Iceberg schema changed after review; review again.")
    if payload.get("admitted_schema_hash") != _admitted_schema_hash(store, asset_id):
        raise ValidationFailure("Admitted schema fields changed after review; review again.")
    draft = store.get_asset_policy_draft(
        asset_id=asset_id,
        author_principal=actor.identity_key(),
    )
    revision = 0 if draft is None else int(cast(int | str, draft["revision"]))
    content_hash = None if draft is None else str(draft["content_hash"])
    if (
        payload.get("draft_revision") != revision
        or payload.get("draft_content_hash") != content_hash
    ):
        raise ValidationFailure("Policy draft changed after review; evaluate the current draft.")
    if payload.get("shared_rules_hash") != (
        _rules_hash(store.list_policy_rules(asset_id)) if draft is None else None
    ):
        raise ValidationFailure("Policy rules changed after review; evaluate the current draft.")
    if payload.get("active_publication_id") != _active_publication_id(store, asset_id):
        raise ValidationFailure("Active publication changed after review; review again.")


def _active_publication_id(store: PublicationStore, asset_id: UUID) -> str | None:
    context = store.get_asset_workspace_context(asset_id)
    try:
        active = store.get_active_publication(context.cell_id)
    except LookupError:
        return None
    return str(active.publication_id)


def _explicit_deny_all_draft(
    store: PublicationStore,
    asset_id: UUID,
    actor: ControlPlaneActor,
) -> bool:
    """Returns true only when this actor saved an intentional empty draft."""

    draft = store.get_asset_policy_draft(
        asset_id=asset_id,
        author_principal=actor.identity_key(),
    )
    return draft is not None and not cast(list[object], draft.get("rules", []))


def _rules_hash(rules: list[dict[str, object]]) -> str:
    encoded = json.dumps(rules, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def _admitted_schema_hash(store: PublicationStore, asset_id: UUID) -> str | None:
    """Hashes persisted field identities so reviews cannot expand on drift."""

    list_fields = getattr(store, "list_asset_schema_fields", None)
    if list_fields is None:
        return None
    fields = list_fields(asset_id)
    if not fields:
        return None
    encoded = json.dumps(fields, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def _encode_signed(payload: dict[str, object], secret: str) -> str:
    body = _b64(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())
    signature = hmac.new(secret.encode(), body.encode(), hashlib.sha256).digest()
    return f"{body}.{_b64(signature)}"


def _decode_signed(token: str, secret: str) -> dict[str, object]:
    try:
        body, signature = token.split(".", 1)
        expected = hmac.new(secret.encode(), body.encode(), hashlib.sha256).digest()
        actual = _decode_b64(signature)
        if not hmac.compare_digest(expected, actual):
            raise ValueError
        payload = json.loads(_decode_b64(body).decode())
    except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
        raise ValidationFailure("Policy review token is invalid.") from None
    if not isinstance(payload, dict):
        raise ValidationFailure("Policy review token is invalid.")
    return cast(dict[str, object], payload)


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def _decode_b64(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
