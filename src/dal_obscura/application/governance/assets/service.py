"""Workspace asset service functions."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from dal_obscura.application.governance.assets import schema as schema_service
from dal_obscura.application.governance.assets.access import ensure_asset_capability
from dal_obscura.application.governance.catalogs.service import (
    ICEBERG_CATALOG_ID,
    validate_descriptor_options,
)
from dal_obscura.application.governance.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.application.governance.ports import (
    AssetSchemaLoader,
    GovernancePluginRegistry,
    GovernanceUnitOfWork,
    GovernanceUnitOfWorkFactory,
)
from dal_obscura.kernel.identity.actor import ControlPlaneActor

_ASSET_CAPABILITIES = ("read", "edit", "grant")


def list_workspace_assets(
    store: GovernanceUnitOfWork,
    actor: ControlPlaneActor | None = None,
) -> list[dict[str, object]]:
    """Lists governed assets in the default workspace."""

    context = store.workspace.get_workspace()
    if context is None:
        return []
    if actor is None or actor.platform_admin:
        return store.assets.list_workspace_assets()
    return store.assets.list_workspace_assets_for_principals(actor.owner_principals())


def list_workspace_assets_page(
    store: GovernanceUnitOfWork,
    actor: ControlPlaneActor,
    *,
    limit: int,
    cursor: str | None = None,
    search: str | None = None,
) -> dict[str, object]:
    """Returns a bounded, cursor-paginated asset inventory for one actor."""

    context = store.workspace.get_workspace()
    if context is None:
        return {"items": [], "next_cursor": None}
    principals = None if actor.platform_admin else actor.owner_principals()
    try:
        page = store.assets.list_workspace_assets_page(
            limit=limit, cursor=cursor, search=search, principals=principals
        )
    except ValueError as exc:
        raise ValidationFailure(str(exc)) from exc
    return {"items": page.items, "next_cursor": page.next_cursor}


def get_workspace_asset(
    store: GovernanceUnitOfWork,
    asset_id: UUID,
    actor: ControlPlaneActor | None = None,
) -> dict[str, object]:
    """Returns one governed asset by id."""

    asset = store.assets.get_workspace_asset(asset_id)
    if actor is not None:
        ensure_asset_capability(store.assets, asset_id, actor, "read")
    return asset


def get_asset_access(
    store: GovernanceUnitOfWork,
    asset_id: UUID,
    actor: ControlPlaneActor,
) -> dict[str, object]:
    """Returns the actor's effective asset capabilities and explanations.

    Capability resolution stays server-side so the UI cannot infer authority
    from role labels or stale grant data. Reading this contract requires the
    asset's ``read`` capability, just like the other asset metadata routes.
    """

    ensure_asset_capability(store.assets, asset_id, actor, "read")
    principals = actor.owner_principals()
    owners = set(store.assets.list_asset_owners(asset_id))
    is_owner = bool(owners.intersection(principals))
    explanations: dict[str, list[str]] = {capability: [] for capability in _ASSET_CAPABILITIES}
    if actor.platform_admin:
        for capability in _ASSET_CAPABILITIES:
            explanations[capability].append("Platform administrator")
    else:
        if is_owner:
            explanations["read"].append("Asset owner")
            explanations["edit"].append("Asset owner")
        for grant in store.assets.list_asset_grants(asset_id):
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
    store: GovernanceUnitOfWork,
    catalog: str,
    target: str,
    backend: str,
    table_identifier: str | None,
    options: dict[str, Any],
    expected_revision: int | None = None,
    plugin_registry: GovernancePluginRegistry | None = None,
) -> dict[str, str]:
    """Creates or updates a governed asset binding."""

    _required_workspace(store)
    catalog_record = store.catalogs.get_workspace_catalog(catalog)
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
    asset_id = store.assets.upsert_asset(
        catalog=catalog,
        target=target,
        backend=backend,
        table_identifier=table_identifier,
        options=options,
        expected_revision=expected_revision,
    )
    return {"id": str(asset_id), "catalog": catalog, "target": target}


def replace_asset_owners(
    store: GovernanceUnitOfWork,
    asset_id: UUID,
    owners: list[str],
    expected_revision: int | None = None,
    actor: ControlPlaneActor | None = None,
    *,
    correlation_id: str | None = None,
) -> list[str]:
    """Replaces owners for one governed asset."""

    if actor is not None and not actor.platform_admin:
        raise AuthorizationFailure("Only platform admins may replace asset owners.")
    # Serialize owner changes with access grants and policy updates.
    store.assets.lock_asset_for_update(asset_id)
    existing_owners = store.assets.list_asset_owners(asset_id)
    normalized = [owner.strip() for owner in owners if owner.strip()]
    if existing_owners and not normalized:
        raise ValidationFailure(
            "Cannot remove the last owner without assigning an asset replacement "
            "in the same request."
        )
    if expected_revision is None:
        normalized = store.assets.replace_asset_owners(asset_id=asset_id, owners=owners)
    else:
        normalized = store.assets.replace_asset_owners(
            asset_id=asset_id, owners=owners, expected_revision=expected_revision
        )
    if actor is not None:
        store.audit.record_asset_audit_event(
            asset_id=asset_id,
            actor_principal=actor.identity_key(),
            action="asset.owners.replace",
            details={"owner_count": len(normalized)},
            correlation_id=correlation_id,
        )
    return normalized


def list_asset_grants(store: GovernanceUnitOfWork, asset_id: UUID) -> list[dict[str, str]]:
    return store.assets.list_asset_grants(asset_id)


def replace_asset_grants(
    store: GovernanceUnitOfWork,
    asset_id: UUID,
    grants: list[dict[str, str]],
    expected_revision: int | None = None,
    actor: ControlPlaneActor | None = None,
    *,
    correlation_id: str | None = None,
) -> list[dict[str, str]]:
    allowed = {"read", "edit", "grant"}
    if any(str(grant.get("capability")) not in allowed for grant in grants):
        raise ValidationFailure("Unsupported asset capability")
    # Serialize grant authorization and replacement against concurrent changes.
    store.assets.lock_asset_for_update(asset_id)
    if actor is not None:
        ensure_asset_capability(store.assets, asset_id, actor, "grant")
    if expected_revision is None:
        normalized = store.assets.replace_asset_grants(asset_id=asset_id, grants=grants)
    else:
        normalized = store.assets.replace_asset_grants(
            asset_id=asset_id, grants=grants, expected_revision=expected_revision
        )
    if actor is not None:
        store.audit.record_asset_audit_event(
            asset_id=asset_id,
            actor_principal=actor.identity_key(),
            action="asset.grants.replace",
            details={"grant_count": len(normalized)},
            correlation_id=correlation_id,
        )
    return normalized


def replace_asset_schema_fields(
    store: GovernanceUnitOfWork,
    asset_id: UUID,
    fields: list[dict[str, Any]],
    expected_revision: int | None = None,
) -> list[dict[str, object]]:
    """Replaces the schema-field metadata for one governed asset."""

    # Serialize schema replacement against concurrent asset updates.
    store.assets.lock_asset_for_update(asset_id)
    try:
        if expected_revision is None:
            return store.assets.replace_asset_schema_fields(asset_id=asset_id, fields=fields)
        return store.assets.replace_asset_schema_fields(
            asset_id=asset_id, fields=fields, expected_revision=expected_revision
        )
    except ValueError as exc:
        # Repository normalization errors are caller input failures. Keep
        # malformed identities out of the generic 500 boundary.
        raise ValidationFailure(str(exc)) from exc


def _required_workspace(store: GovernanceUnitOfWork) -> None:
    if store.workspace.get_workspace() is None:
        raise LookupError("No workspace has been configured")


class AssetsCommands:
    def __init__(
        self,
        work_factory: GovernanceUnitOfWorkFactory,
        *,
        schema_loader: AssetSchemaLoader,
        plugin_registry: GovernancePluginRegistry | None,
        egress_allowlist: tuple[str, ...] = (),
    ) -> None:
        self._work_factory = work_factory
        self._schema_loader = schema_loader
        self._plugin_registry = plugin_registry
        self._egress_allowlist = egress_allowlist

    def list_workspace_assets(
        self, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> list[dict[str, object]]:
        with self._work_factory() as work:
            return list_workspace_assets(work, actor)

    def list_workspace_assets_page(
        self,
        *,
        limit: int,
        cursor: str | None = None,
        search: str | None = None,
        actor: ControlPlaneActor,
        correlation_id: str | None,
    ) -> dict[str, object]:
        with self._work_factory() as work:
            return list_workspace_assets_page(
                work, actor, limit=limit, cursor=cursor, search=search
            )

    def get_workspace_asset(
        self, asset_id: UUID, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> dict[str, object]:
        with self._work_factory() as work:
            return get_workspace_asset(work, asset_id, actor)

    def get_asset_access(
        self, asset_id: UUID, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> dict[str, object]:
        with self._work_factory() as work:
            return get_asset_access(work, asset_id, actor)

    def get_asset_schema(
        self, asset_id: UUID, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> dict[str, object]:
        with self._work_factory() as work:
            captured = schema_service.capture_asset_schema(
                work,
                asset_id,
                actor,
                egress_allowlist=self._egress_allowlist,
                plugin_registry=self._plugin_registry,
            )
        schema = self._schema_loader.load_asset_schema(captured)
        payload = schema_service.get_asset_schema(captured, schema)
        with self._work_factory() as work:
            schema_service.ensure_asset_schema_current(work, captured, actor)
        return payload

    def upsert_workspace_asset(
        self,
        catalog: str,
        target: str,
        backend: str,
        table_identifier: str | None,
        options: dict[str, Any],
        expected_revision: int | None = None,
        *,
        actor: ControlPlaneActor,
        correlation_id: str | None,
    ) -> dict[str, str]:
        _require_admin(actor)
        with self._work_factory() as work:
            result = upsert_workspace_asset(
                work,
                catalog,
                target,
                backend,
                table_identifier,
                options,
                expected_revision,
                self._plugin_registry,
            )
            work.commit()
            return result

    def replace_asset_owners(
        self,
        asset_id: UUID,
        owners: list[str],
        expected_revision: int | None = None,
        *,
        actor: ControlPlaneActor,
        correlation_id: str | None,
    ) -> list[str]:
        with self._work_factory() as work:
            result = replace_asset_owners(
                work, asset_id, owners, expected_revision, actor, correlation_id=correlation_id
            )
            work.commit()
            return result

    def list_asset_grants(
        self, asset_id: UUID, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> list[dict[str, str]]:
        with self._work_factory() as work:
            ensure_asset_capability(work.assets, asset_id, actor, "grant")
            return list_asset_grants(work, asset_id)

    def replace_asset_grants(
        self,
        asset_id: UUID,
        grants: list[dict[str, str]],
        expected_revision: int | None = None,
        *,
        actor: ControlPlaneActor,
        correlation_id: str | None,
    ) -> dict[str, object]:
        with self._work_factory() as work:
            work.assets.lock_asset_for_update(asset_id)
            ensure_asset_capability(work.assets, asset_id, actor, "grant")
            if not actor.platform_admin:
                existing_grants = {
                    (item["principal"], item["capability"])
                    for item in list_asset_grants(work, asset_id)
                }
                if any(
                    item["capability"] == "grant"
                    and (item["principal"], item["capability"]) not in existing_grants
                    for item in grants
                ):
                    raise AuthorizationFailure(
                        "Only platform admins may delegate grant-management capability."
                    )
                for grant in grants:
                    if grant["principal"] not in actor.owner_principals():
                        continue
                    try:
                        ensure_asset_capability(work.assets, asset_id, actor, grant["capability"])
                    except AuthorizationFailure as exc:
                        raise AuthorizationFailure(
                            "Cannot grant yourself a capability you do not already hold."
                        ) from exc
            result = replace_asset_grants(
                work, asset_id, grants, expected_revision, actor, correlation_id=correlation_id
            )
            work.commit()
            return {"asset_id": str(asset_id), "grants": result}

    def replace_asset_schema_fields(
        self,
        asset_id: UUID,
        fields: list[dict[str, Any]],
        expected_revision: int | None = None,
        *,
        actor: ControlPlaneActor,
        correlation_id: str | None,
    ) -> list[dict[str, object]]:
        _require_admin(actor)
        with self._work_factory() as work:
            result = replace_asset_schema_fields(work, asset_id, fields, expected_revision)
            work.commit()
            return result


def _require_admin(actor: ControlPlaneActor) -> None:
    if not actor.platform_admin:
        raise AuthorizationFailure("Platform admin required")
