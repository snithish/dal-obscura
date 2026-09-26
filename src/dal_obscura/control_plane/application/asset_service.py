"""Workspace asset service functions.

Example:
    ```python
    assets = list_workspace_assets(store)
    ```
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from dal_obscura.common.plugin_api import PluginRegistry
from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.catalog_service import (
    validate_descriptor_options,
)
from dal_obscura.control_plane.application.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.control_plane.application.policy_service import ensure_asset_capability
from dal_obscura.control_plane.infrastructure.catalog_discovery import ICEBERG_CATALOG_ID
from dal_obscura.control_plane.infrastructure.repositories import ConfigStore

_ASSET_CAPABILITIES = ("read", "edit", "grant")


def list_workspace_assets(
    store: ConfigStore,
    actor: ControlPlaneActor | None = None,
) -> list[dict[str, object]]:
    """Lists governed assets in the default workspace.

    Example:
        ```python
        assets = list_workspace_assets(store)
        ```
    """

    context = store.get_default_workspace_context()
    if context is None:
        return []
    if actor is None or actor.platform_admin:
        return store.list_workspace_assets(context)
    return store.list_workspace_assets_for_principals(context, actor.owner_principals())


def list_workspace_assets_page(
    store: ConfigStore,
    actor: ControlPlaneActor,
    *,
    limit: int,
    cursor: str | None = None,
    search: str | None = None,
) -> dict[str, object]:
    """Returns a bounded, cursor-paginated asset inventory for one actor."""

    context = store.get_default_workspace_context()
    if context is None:
        return {"items": [], "next_cursor": None}
    principals = None if actor.platform_admin else actor.owner_principals()
    try:
        page = store.list_workspace_assets_page(
            context,
            limit=limit,
            cursor=cursor,
            search=search,
            principals=principals,
        )
    except ValueError as exc:
        raise ValidationFailure(str(exc)) from exc
    return {"items": page.items, "next_cursor": page.next_cursor}


def get_workspace_asset(
    store: ConfigStore,
    asset_id: UUID,
    actor: ControlPlaneActor | None = None,
) -> dict[str, object]:
    """Returns one governed asset by id.

    Example:
        ```python
        asset = get_workspace_asset(store, asset_id)
        ```
    """

    asset = store.get_workspace_asset(asset_id)
    if actor is not None:
        ensure_asset_capability(store, asset_id, actor, "read")
    return asset


def get_asset_access(
    store: ConfigStore,
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
    owners = set(store.list_asset_owners(asset_id))
    is_owner = bool(owners.intersection(principals))
    explanations: dict[str, list[str]] = {capability: [] for capability in _ASSET_CAPABILITIES}
    if actor.platform_admin:
        for capability in _ASSET_CAPABILITIES:
            explanations[capability].append("Platform administrator")
    else:
        if is_owner:
            explanations["read"].append("Asset owner")
            explanations["edit"].append("Asset owner")
        for grant in store.list_asset_grants(asset_id):
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
    store: ConfigStore,
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

    context = _required_workspace_context(store)
    catalog_record = store.get_workspace_catalog(context, catalog)
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
        validate_descriptor_options(
            format_descriptor,
            options,
            kind="Table-format",
        )
    asset_id = store.upsert_asset(
        cell_id=context.cell_id,
        tenant_id=context.tenant_id,
        catalog=catalog,
        target=target,
        backend=backend,
        table_identifier=table_identifier,
        options=options,
        expected_revision=expected_revision,
    )
    return {"id": str(asset_id), "catalog": catalog, "target": target}


def replace_asset_owners(
    store: ConfigStore,
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
    store.lock_asset_for_update(asset_id)
    existing_owners = store.list_asset_owners(asset_id)
    normalized = [owner.strip() for owner in owners if owner.strip()]
    if existing_owners and not normalized:
        raise ValidationFailure(
            "Cannot remove the last owner without assigning an asset replacement "
            "in the same request."
        )
    if expected_revision is None:
        normalized = store.replace_asset_owners(asset_id=asset_id, owners=owners)
    else:
        normalized = store.replace_asset_owners(
            asset_id=asset_id,
            owners=owners,
            expected_revision=expected_revision,
        )
    if actor is not None:
        store.record_asset_audit_event(
            asset_id=asset_id,
            actor_principal=actor.identity_key(),
            action="asset.owners.replace",
            details={"owner_count": len(normalized)},
        )
    return normalized


def list_asset_grants(store: ConfigStore, asset_id: UUID) -> list[dict[str, str]]:
    return store.list_asset_grants(asset_id)


def replace_asset_grants(
    store: ConfigStore,
    asset_id: UUID,
    grants: list[dict[str, str]],
    expected_revision: int | None = None,
    actor: ControlPlaneActor | None = None,
) -> list[dict[str, str]]:
    allowed = {"read", "edit", "grant"}
    if any(str(grant.get("capability")) not in allowed for grant in grants):
        raise ValidationFailure("Unsupported asset capability")
    # Serialize grant authorization and replacement against concurrent changes.
    store.lock_asset_for_update(asset_id)
    if actor is not None:
        ensure_asset_capability(store, asset_id, actor, "grant")
    if expected_revision is None:
        normalized = store.replace_asset_grants(asset_id=asset_id, grants=grants)
    else:
        normalized = store.replace_asset_grants(
            asset_id=asset_id,
            grants=grants,
            expected_revision=expected_revision,
        )
    if actor is not None:
        store.record_asset_audit_event(
            asset_id=asset_id,
            actor_principal=actor.identity_key(),
            action="asset.grants.replace",
            details={"grant_count": len(normalized)},
        )
    return normalized


def replace_asset_schema_fields(
    store: ConfigStore,
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
    store.lock_asset_for_update(asset_id)
    try:
        if expected_revision is None:
            return store.replace_asset_schema_fields(asset_id=asset_id, fields=fields)
        return store.replace_asset_schema_fields(
            asset_id=asset_id,
            fields=fields,
            expected_revision=expected_revision,
        )
    except ValueError as exc:
        # Repository normalization errors are caller input failures. Keep
        # malformed identities out of the generic 500 boundary.
        raise ValidationFailure(str(exc)) from exc


def _required_workspace_context(store: ConfigStore):
    context = store.get_default_workspace_context()
    if context is None:
        raise LookupError("No workspace has been configured")
    return context
