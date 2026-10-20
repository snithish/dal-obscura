"""Asset commands own authorization, transactions and audit records."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from dal_obscura.application.governance.assets import schema as schema_service
from dal_obscura.application.governance.assets.access import ensure_asset_capability
from dal_obscura.application.governance.catalogs.validation import (
    ICEBERG_CATALOG_ID,
    validate_descriptor_options,
)
from dal_obscura.application.governance.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.application.governance.ports import (
    AssetSchemaLoader,
    GovernancePluginRegistry,
    GovernanceUnitOfWorkFactory,
)
from dal_obscura.application.governance.workspace.service import required_workspace
from dal_obscura.kernel.identity.actor import ControlPlaneActor

_ASSET_CAPABILITIES = ("read", "edit", "grant")


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
            context = work.workspace.get_workspace()
            if context is None:
                return []
            if actor.platform_admin:
                return work.assets.list_workspace_assets()
            return work.assets.list_workspace_assets_for_principals(actor.owner_principals())

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
            context = work.workspace.get_workspace()
            if context is None:
                return {"items": [], "next_cursor": None}
            principals = None if actor.platform_admin else actor.owner_principals()
            try:
                page = work.assets.list_workspace_assets_page(
                    limit=limit, cursor=cursor, search=search, principals=principals
                )
            except ValueError as exc:
                raise ValidationFailure(str(exc)) from exc
            return {"items": page.items, "next_cursor": page.next_cursor}

    def get_workspace_asset(
        self, asset_id: UUID, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> dict[str, object]:
        with self._work_factory() as work:
            asset = work.assets.get_workspace_asset(asset_id)
            ensure_asset_capability(work.assets, asset_id, actor, "read")
            return asset

    def get_asset_access(
        self, asset_id: UUID, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> dict[str, object]:
        """Resolve capabilities server-side; role labels cannot establish authority."""
        with self._work_factory() as work:
            ensure_asset_capability(work.assets, asset_id, actor, "read")
            principals = actor.owner_principals()
            owners = set(work.assets.list_asset_owners(asset_id))
            is_owner = bool(owners.intersection(principals))
            explanations: dict[str, list[str]] = {
                capability: [] for capability in _ASSET_CAPABILITIES
            }
            if actor.platform_admin:
                for capability in _ASSET_CAPABILITIES:
                    explanations[capability].append("Platform administrator")
            else:
                if is_owner:
                    explanations["read"].append("Asset owner")
                    explanations["edit"].append("Asset owner")
                for grant in work.assets.list_asset_grants(asset_id):
                    if grant["principal"] in principals and grant["capability"] in explanations:
                        explanations[grant["capability"]].append(
                            f"Delegated to {grant['principal']}"
                        )
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
            required_workspace(work)
            catalog_record = work.catalogs.get_workspace_catalog(catalog)
            catalog_plugin_id = str(catalog_record["plugin_id"])
            if backend != "iceberg" or catalog_plugin_id != ICEBERG_CATALOG_ID:
                if self._plugin_registry is None:
                    raise ValidationFailure("Plugin pair is not admitted")
                # Plugin admission is frozen at startup; never discover or import
                # newly installed code while handling an asset mutation.
                admitted = self._plugin_registry.admitted()
                catalog_descriptor = admitted.get(("catalog", catalog_plugin_id))
                format_descriptor = admitted.get(("table_format", backend))
                if catalog_descriptor is None or format_descriptor is None:
                    raise ValidationFailure("Plugin pair is not admitted")
                if backend not in catalog_descriptor.output_formats:
                    raise ValidationFailure("Catalog does not declare this table format")
                if not catalog_descriptor.handle_versions.intersection(
                    format_descriptor.handle_versions
                ):
                    raise ValidationFailure(
                        "Catalog and table-format handle versions do not overlap"
                    )
                if not catalog_descriptor.capabilities.intersection(format_descriptor.capabilities):
                    raise ValidationFailure("Catalog and table-format capabilities do not overlap")
                validate_descriptor_options(format_descriptor, options, kind="Table-format")
            asset_id = work.assets.upsert_asset(
                catalog=catalog,
                target=target,
                backend=backend,
                table_identifier=table_identifier,
                options=options,
                expected_revision=expected_revision,
            )
            work.commit()
            return {"id": str(asset_id), "catalog": catalog, "target": target}

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
            if not actor.platform_admin:
                raise AuthorizationFailure("Only platform admins may replace asset owners.")
            # Serialize owner changes with access grants and policy updates.
            work.assets.lock_asset_for_update(asset_id)
            existing_owners = work.assets.list_asset_owners(asset_id)
            normalized = [owner.strip() for owner in owners if owner.strip()]
            if existing_owners and not normalized:
                raise ValidationFailure(
                    "Cannot remove the last owner without assigning an asset replacement "
                    "in the same request."
                )
            normalized = work.assets.replace_asset_owners(
                asset_id=asset_id, owners=owners, expected_revision=expected_revision
            )
            work.audit.record_asset_audit_event(
                asset_id=asset_id,
                actor_principal=actor.identity_key(),
                action="asset.owners.replace",
                details={"owner_count": len(normalized)},
                correlation_id=correlation_id,
            )
            work.commit()
            return normalized

    def list_asset_grants(
        self, asset_id: UUID, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> list[dict[str, str]]:
        with self._work_factory() as work:
            ensure_asset_capability(work.assets, asset_id, actor, "grant")
            return work.assets.list_asset_grants(asset_id)

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
                    for item in work.assets.list_asset_grants(asset_id)
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
            if any(str(grant.get("capability")) not in _ASSET_CAPABILITIES for grant in grants):
                raise ValidationFailure("Unsupported asset capability")
            result = work.assets.replace_asset_grants(
                asset_id=asset_id, grants=grants, expected_revision=expected_revision
            )
            work.audit.record_asset_audit_event(
                asset_id=asset_id,
                actor_principal=actor.identity_key(),
                action="asset.grants.replace",
                details={"grant_count": len(result)},
                correlation_id=correlation_id,
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
            work.assets.lock_asset_for_update(asset_id)
            try:
                result = work.assets.replace_asset_schema_fields(
                    asset_id=asset_id, fields=fields, expected_revision=expected_revision
                )
            except ValueError as exc:
                # Repository normalization errors are caller input failures. Keep
                # malformed identities out of the generic 500 boundary.
                raise ValidationFailure(str(exc)) from exc
            work.commit()
            return result


def _require_admin(actor: ControlPlaneActor) -> None:
    if not actor.platform_admin:
        raise AuthorizationFailure("Platform admin required")
