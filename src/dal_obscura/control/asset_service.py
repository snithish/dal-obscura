"""Workspace asset service functions.

Example:
    ```python
    assets = list_workspace_assets(store)
    ```
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy.orm import Session

from dal_obscura.control.access import ControlPlaneActor
from dal_obscura.control.catalog_service import validate_descriptor_options
from dal_obscura.control.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.control.policy_service import ensure_asset_capability
from dal_obscura.sources.discovery import ICEBERG_CATALOG_ID
from dal_obscura.sources.plugins import PluginRegistry
from dal_obscura.storage import assets as _db_assets
from dal_obscura.storage import audit as _db_audit
from dal_obscura.storage import catalogs as _db_catalogs
from dal_obscura.storage import workspace as _db_workspace

_ASSET_CAPABILITIES = ("read", "edit", "grant")


def list_workspace_assets(
    store: Session,
    actor: ControlPlaneActor | None = None,
) -> list[dict[str, object]]:
    """Lists governed assets in the default workspace.

    Example:
        ```python
        assets = list_workspace_assets(store)
        ```
    """

    context = _db_workspace.get_workspace(store)
    if context is None:
        return []
    if actor is None or actor.platform_admin:
        return _db_assets.list_workspace_assets(store)
    return _db_assets.list_workspace_assets_for_principals(store, actor.owner_principals())


def list_workspace_assets_page(
    store: Session,
    actor: ControlPlaneActor,
    *,
    limit: int,
    cursor: str | None = None,
    search: str | None = None,
) -> dict[str, object]:
    """Returns a bounded, cursor-paginated asset inventory for one actor."""

    context = _db_workspace.get_workspace(store)
    if context is None:
        return {"items": [], "next_cursor": None}
    principals = None if actor.platform_admin else actor.owner_principals()
    try:
        page = _db_assets.list_workspace_assets_page(
            store, limit=limit, cursor=cursor, search=search, principals=principals
        )
    except ValueError as exc:
        raise ValidationFailure(str(exc)) from exc
    return {"items": page.items, "next_cursor": page.next_cursor}


def get_workspace_asset(
    store: Session,
    asset_id: UUID,
    actor: ControlPlaneActor | None = None,
) -> dict[str, object]:
    """Returns one governed asset by id.

    Example:
        ```python
        asset = get_workspace_asset(store, asset_id)
        ```
    """

    asset = _db_assets.get_workspace_asset(store, asset_id)
    if actor is not None:
        ensure_asset_capability(store, asset_id, actor, "read")
    return asset


def get_asset_access(
    store: Session,
    asset_id: UUID,
    actor: ControlPlaneActor,
) -> dict[str, object]:
    """Returns the actor's effective asset capabilities and explanations.

    Capability resolution stays server-side so the UI cannot infer authority
    from role labels or stale grant data. Reading this contract requires the
    asset's ``read`` capability, just like the other asset metadata routes.
    """

    ensure_asset_capability(store, asset_id, actor, "read")
    principals = actor.owner_principals()
    owners = set(_db_assets.list_asset_owners(store, asset_id))
    is_owner = bool(owners.intersection(principals))
    explanations: dict[str, list[str]] = {capability: [] for capability in _ASSET_CAPABILITIES}
    if actor.platform_admin:
        for capability in _ASSET_CAPABILITIES:
            explanations[capability].append("Platform administrator")
    else:
        if is_owner:
            explanations["read"].append("Asset owner")
            explanations["edit"].append("Asset owner")
        for grant in _db_assets.list_asset_grants(store, asset_id):
            if grant["principal"] in principals and grant["capability"] in explanations:
                explanations[grant["capability"]].append(f"Delegated to {grant['principal']}")
    return {
        "asset_id": str(asset_id),
        "principal": actor.principal,
        "issuer": actor.issuer or None,
        "can_revoke_tokens": actor.platform_admin or is_owner,
        "capabilities": [
            {
                "capability": capability,
                "allowed": bool(explanations[capability]),
                "reasons": sorted(set(explanations[capability])),
            }
            for capability in _ASSET_CAPABILITIES
        ],
    }


def upsert_workspace_asset(
    store: Session,
    catalog: str,
    target: str,
    backend: str,
    table_identifier: str | None,
    options: dict[str, Any],
    expected_revision: int | None = None,
    plugin_registry: PluginRegistry | None = None,
) -> dict[str, str]:
    """Creates or updates a governed asset binding.

    Example:
        ```python
        result = upsert_workspace_asset(store, "analytics", "orders", "iceberg", None, {})
        ```
    """

    _required_workspace(store)
    catalog_record = _db_catalogs.get_workspace_catalog(store, catalog)
    catalog_plugin_id = str(catalog_record["plugin_id"])
    if backend != "iceberg" or catalog_plugin_id != ICEBERG_CATALOG_ID:
        if plugin_registry is None:
            raise ValidationFailure("Plugin pair is not admitted")
        # Plugin admission is frozen at startup; never discover or import
        # newly installed code while handling an asset mutation.
        admitted = plugin_registry.admitted()
        catalog_descriptor = admitted.get(("catalog", catalog_plugin_id))
        format_descriptor = admitted.get(("table_format", backend))
        if catalog_descriptor is None or format_descriptor is None:
            raise ValidationFailure("Plugin pair is not admitted")
        if backend not in catalog_descriptor.output_formats:
            raise ValidationFailure("Catalog does not declare this table format")
        if not catalog_descriptor.handle_versions.intersection(format_descriptor.handle_versions):
            raise ValidationFailure("Catalog and table-format handle versions do not overlap")
        if not catalog_descriptor.capabilities.intersection(format_descriptor.capabilities):
            raise ValidationFailure("Catalog and table-format capabilities do not overlap")
        validate_descriptor_options(format_descriptor, options, kind="Table-format")
    asset_id = _db_assets.upsert_asset(
        store,
        catalog=catalog,
        target=target,
        backend=backend,
        table_identifier=table_identifier,
        options=options,
        expected_revision=expected_revision,
    )
    return {"id": str(asset_id), "catalog": catalog, "target": target}


def replace_asset_owners(
    store: Session,
    asset_id: UUID,
    owners: list[str],
    expected_revision: int | None = None,
    actor: ControlPlaneActor | None = None,
) -> list[str]:
    """Replaces owners for one governed asset.

    Example:
        ```python
        owners = replace_asset_owners(store, asset_id, ["alice", "group:analytics"])
        ```
    """

    if actor is not None and not actor.platform_admin:
        raise AuthorizationFailure("Only platform admins may replace asset owners.")
    # Serialize owner changes with access grants and policy updates.
    _db_assets.lock_asset_for_update(store, asset_id)
    existing_owners = _db_assets.list_asset_owners(store, asset_id)
    normalized = [owner.strip() for owner in owners if owner.strip()]
    if existing_owners and not normalized:
        raise ValidationFailure(
            "Cannot remove the last owner without assigning an asset replacement "
            "in the same request."
        )
    if expected_revision is None:
        normalized = _db_assets.replace_asset_owners(store, asset_id=asset_id, owners=owners)
    else:
        normalized = _db_assets.replace_asset_owners(
            store, asset_id=asset_id, owners=owners, expected_revision=expected_revision
        )
    if actor is not None:
        _db_audit.record_asset_audit_event(
            store,
            asset_id=asset_id,
            actor_principal=actor.identity_key(),
            action="asset.owners.replace",
            details={"owner_count": len(normalized)},
        )
    return normalized


def list_asset_grants(store: Session, asset_id: UUID) -> list[dict[str, str]]:
    return _db_assets.list_asset_grants(store, asset_id)


def replace_asset_grants(
    store: Session,
    asset_id: UUID,
    grants: list[dict[str, str]],
    expected_revision: int | None = None,
    actor: ControlPlaneActor | None = None,
) -> list[dict[str, str]]:
    allowed = {"read", "edit", "grant"}
    if any(str(grant.get("capability")) not in allowed for grant in grants):
        raise ValidationFailure("Unsupported asset capability")
    # Serialize grant authorization and replacement against concurrent changes.
    _db_assets.lock_asset_for_update(store, asset_id)
    if actor is not None:
        ensure_asset_capability(store, asset_id, actor, "grant")
    if expected_revision is None:
        normalized = _db_assets.replace_asset_grants(store, asset_id=asset_id, grants=grants)
    else:
        normalized = _db_assets.replace_asset_grants(
            store, asset_id=asset_id, grants=grants, expected_revision=expected_revision
        )
    if actor is not None:
        _db_audit.record_asset_audit_event(
            store,
            asset_id=asset_id,
            actor_principal=actor.identity_key(),
            action="asset.grants.replace",
            details={"grant_count": len(normalized)},
        )
    return normalized


def replace_asset_schema_fields(
    store: Session,
    asset_id: UUID,
    fields: list[dict[str, Any]],
    expected_revision: int | None = None,
) -> list[dict[str, object]]:
    """Replaces the schema-field metadata for one governed asset.

    Example:
        ```python
        fields = replace_asset_schema_fields(store, asset_id, [{"name": "id"}])
        ```
    """

    # Serialize schema replacement against concurrent asset updates.
    _db_assets.lock_asset_for_update(store, asset_id)
    try:
        if expected_revision is None:
            return _db_assets.replace_asset_schema_fields(store, asset_id=asset_id, fields=fields)
        return _db_assets.replace_asset_schema_fields(
            store, asset_id=asset_id, fields=fields, expected_revision=expected_revision
        )
    except ValueError as exc:
        # Repository normalization errors are caller input failures. Keep
        # malformed identities out of the generic 500 boundary.
        raise ValidationFailure(str(exc)) from exc


def _required_workspace(store: Session):
    context = _db_workspace.get_workspace(store)
    if context is None:
        raise LookupError("No workspace has been configured")
    return context
