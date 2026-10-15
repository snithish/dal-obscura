"""Safe attribute discovery and synthetic mapping over existing provider configuration."""

from __future__ import annotations

from typing import Any, cast
from uuid import UUID

from dal_obscura.application.governance.assets import access as asset_access
from dal_obscura.application.governance.errors import ValidationFailure
from dal_obscura.application.governance.ports import GovernanceUnitOfWork, PreviewIdentityMapper
from dal_obscura.kernel.identity.actor import ControlPlaneActor


def attribute_catalog(store: GovernanceUnitOfWork) -> list[dict[str, object]]:
    """Expose mapped attributes, not authentication secrets or user identities."""
    result: list[dict[str, object]] = []
    for provider in store.workspace.list_auth_providers():
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


def preview_attributes(
    store: GovernanceUnitOfWork,
    ordinal: int,
    claims: dict[str, object],
    provider_args: dict[str, Any] | None = None,
    *,
    identity_mapper: PreviewIdentityMapper,
) -> dict[str, object]:
    """Preview saved or unsaved admin mappings without saving or network requests."""
    if provider_args is None:
        provider = next(
            (
                item
                for item in store.workspace.list_auth_providers()
                if item["ordinal"] == ordinal and item["enabled"]
            ),
            None,
        )
        if provider is None:
            raise ValidationFailure("Choose an enabled identity provider")
        provider_args = cast(dict[str, Any], provider["args"])
    else:
        from dal_obscura.application.governance.workspace.auth_providers import (
            OIDC_IDENTITY_MODULE,
            validate_auth_provider_payloads,
        )

        validate_auth_provider_payloads(
            [{"ordinal": ordinal, "module": OIDC_IDENTITY_MODULE, "args": provider_args}]
        )

    try:
        attributes = identity_mapper.map_attributes(provider_args, claims)
    except PermissionError as exc:
        raise ValidationFailure(str(exc)) from exc
    return {"attributes": attributes}


def list_attribute_catalog(
    store: GovernanceUnitOfWork, asset_id: UUID, actor: ControlPlaneActor
) -> list[dict[str, object]]:
    asset_access.ensure_asset_reader(store.assets, asset_id, actor)
    return attribute_catalog(store)
