"""Safe attribute discovery and synthetic mapping over existing provider configuration."""

from __future__ import annotations

from typing import Any, cast
from uuid import UUID

from sqlalchemy.orm import Session

from dal_obscura.control import identity_attributes, policy_service
from dal_obscura.control.access import ControlPlaneActor
from dal_obscura.control.errors import ValidationFailure
from dal_obscura.identity.claims import PrincipalClaimMapper
from dal_obscura.policy.models import Principal
from dal_obscura.storage import workspace as _db_workspace


def attribute_catalog(store: Session) -> list[dict[str, object]]:
    """Expose mapped attributes, not authentication secrets or user identities."""
    result: list[dict[str, object]] = []
    for provider in _db_workspace.list_auth_providers(store):
        if not provider["enabled"]:
            continue
        args = cast(dict[str, Any], provider["args"])
        definitions = args.get("attribute_definitions", {})
        result.append(
            {
                "ordinal": provider["ordinal"],
                "issuer": args.get("issuer", ""),
                "revision": provider["revision"],
                "attributes": [
                    {
                        "key": key,
                        "claim_path": path,
                        "label": definitions.get(key, {}).get("label") or key,
                        "description": definitions.get(key, {}).get("description", ""),
                        "allowed_values": definitions.get(key, {}).get("allowed_values", []),
                    }
                    for key, path in args.get("attribute_claims", {}).items()
                ],
            }
        )
    return result


def provider_mapper(store: Session, ordinal: int) -> tuple[PrincipalClaimMapper, dict[str, object]]:
    """Resolve only an enabled configured provider; never fetch an IdP for previews."""
    for provider in _db_workspace.list_auth_providers(store):
        if provider["ordinal"] == ordinal and provider["enabled"]:
            args = cast(dict[str, Any], provider["args"])
            return PrincipalClaimMapper(
                subject_claim=args.get("subject_claim", "sub"),
                group_claims=args.get("group_claims", []),
                attribute_claims=args.get("attribute_claims", {}),
                attribute_definitions=args.get("attribute_definitions", {}),
            ), provider
    raise ValidationFailure("Choose an enabled identity provider")


def map_preview_identity(store: Session, ordinal: int, claims: dict[str, object]) -> Principal:
    """Use runtime mapping for a synthetic identity, without authenticating it."""
    mapper, _ = provider_mapper(store, ordinal)
    try:
        return mapper.map_claims(claims)
    except PermissionError as exc:
        raise ValidationFailure(str(exc)) from exc


def preview_attributes(
    store: Session,
    ordinal: int,
    claims: dict[str, object],
    provider_args: dict[str, Any] | None = None,
) -> dict[str, object]:
    """Preview saved or unsaved admin mappings without saving or network requests."""
    if provider_args is None:
        mapper, _ = provider_mapper(store, ordinal)
    else:
        from dal_obscura.control.auth_provider_validation import (
            OIDC_IDENTITY_MODULE,
            validate_auth_provider_payloads,
        )

        validate_auth_provider_payloads(
            [{"ordinal": ordinal, "module": OIDC_IDENTITY_MODULE, "args": provider_args}]
        )
        mapper = PrincipalClaimMapper(
            attribute_claims=provider_args.get("attribute_claims"),
            attribute_definitions=provider_args.get("attribute_definitions"),
        )
    try:
        attributes = mapper.map_attributes(claims)
    except PermissionError as exc:
        raise ValidationFailure(str(exc)) from exc
    return {"attributes": attributes}


def list_attribute_catalog(
    store: Session, asset_id: UUID, actor: ControlPlaneActor
) -> list[dict[str, object]]:
    policy_service.ensure_asset_reader(store, asset_id, actor)
    return identity_attributes.attribute_catalog(store)
