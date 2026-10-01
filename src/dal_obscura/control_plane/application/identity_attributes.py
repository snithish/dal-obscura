"""Safe attribute discovery and synthetic mapping over existing provider configuration."""

from __future__ import annotations

from typing import Any, cast

from dal_obscura.common.access_control.identity_claims import PrincipalClaimMapper
from dal_obscura.common.access_control.models import Principal
from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.infrastructure.repositories import ConfigStore


def attribute_catalog(store: ConfigStore) -> list[dict[str, object]]:
    """Expose mapped attributes, not authentication secrets or user identities."""
    result: list[dict[str, object]] = []
    for provider in store.list_auth_providers():
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


def provider_mapper(
    store: ConfigStore, ordinal: int
) -> tuple[PrincipalClaimMapper, dict[str, object]]:
    """Resolve only an enabled configured provider; never fetch an IdP for previews."""
    for provider in store.list_auth_providers():
        if provider["ordinal"] == ordinal and provider["enabled"]:
            args = cast(dict[str, Any], provider["args"])
            return PrincipalClaimMapper(
                subject_claim=args.get("subject_claim", "sub"),
                group_claims=args.get("group_claims", []),
                attribute_claims=args.get("attribute_claims", {}),
                attribute_definitions=args.get("attribute_definitions", {}),
            ), provider
    raise ValidationFailure("Choose an enabled identity provider")


def map_preview_identity(store: ConfigStore, ordinal: int, claims: dict[str, object]) -> Principal:
    """Use runtime mapping for a synthetic identity, without authenticating it."""
    mapper, _ = provider_mapper(store, ordinal)
    try:
        return mapper.map_claims(claims)
    except PermissionError as exc:
        raise ValidationFailure(str(exc)) from exc


def preview_attributes(
    store: ConfigStore,
    ordinal: int,
    claims: dict[str, object],
    provider_args: dict[str, Any] | None = None,
) -> dict[str, object]:
    """Preview saved or unsaved admin mappings without saving or network requests."""
    if provider_args is None:
        mapper, _ = provider_mapper(store, ordinal)
    else:
        from dal_obscura.control_plane.application.auth_provider_validation import (
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
