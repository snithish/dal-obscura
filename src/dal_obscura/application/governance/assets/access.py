"""Administrative asset access and ownership authorization."""

from __future__ import annotations

from uuid import UUID

from dal_obscura.application.governance.errors import AuthorizationFailure
from dal_obscura.application.governance.ports import AssetsRepository
from dal_obscura.kernel.identity.actor import ControlPlaneActor


def ensure_asset_reader(
    store: AssetsRepository,
    asset_id: UUID,
    actor: ControlPlaneActor,
) -> None:
    """Requires an actor to be a platform admin or an owner of the asset."""

    ensure_asset_capability(store, asset_id, actor, "read")


def ensure_asset_capability(
    store: AssetsRepository,
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


def ensure_asset_owner(
    store: AssetsRepository,
    asset_id: UUID,
    actor: ControlPlaneActor,
) -> None:
    """Requires explicit asset ownership or platform-admin authority."""

    if actor.platform_admin:
        return
    if set(store.list_asset_owners(asset_id)).intersection(actor.owner_principals()):
        return
    raise AuthorizationFailure("Only platform admins or asset owners may revoke asset tokens.")
