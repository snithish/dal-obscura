from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.orm import Session

from dal_obscura.common.plugin_api import PluginRegistry
from dal_obscura.control_plane.application import (
    asset_service,
    audit_service,
    catalog_service,
    draft_service,
    evaluation_service,
    policy_service,
    policy_version_service,
    review_service,
    schema_service,
    workspace_service,
)
from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import AuthorizationFailure
from dal_obscura.control_plane.infrastructure.catalog_discovery import discover_catalog_tables
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore


class ProvisioningService:
    """Application service used by the FastAPI control-plane routes."""

    def __init__(
        self,
        session: Session,
        *,
        review_secret: str = "",
        require_review: bool = False,
        catalog_egress_allowlist: tuple[str, ...] = (),
        plugin_registry: PluginRegistry | None = None,
    ) -> None:
        self._store = PublicationStore(session)
        self._review_secret = review_secret
        self._require_review = require_review
        self._catalog_egress_allowlist = catalog_egress_allowlist
        self._plugin_registry = plugin_registry

    def create_tenant(self, slug: str, display_name: str) -> dict[str, str]:
        tenant_id = uuid4()
        self._store.create_tenant(tenant_id=tenant_id, slug=slug, display_name=display_name)
        return {"id": str(tenant_id), "slug": slug, "display_name": display_name}

    def create_cell(self, name: str, region: str) -> dict[str, str]:
        cell_id = uuid4()
        self._store.create_cell(cell_id=cell_id, name=name, region=region)
        return {"id": str(cell_id), "name": name, "region": region}

    def create_cell_for_tenant(
        self,
        tenant_id: UUID,
        name: str,
        region: str,
        shard_key: str,
    ) -> dict[str, str]:
        cell = self.create_cell(name=name, region=region)
        self.assign_tenant(UUID(cell["id"]), tenant_id, shard_key)
        return cell

    def list_tenants(self) -> list[dict[str, str]]:
        return self._store.list_tenants()

    def list_cells(self) -> list[dict[str, str]]:
        return self._store.list_cells()

    def list_cells_for_tenant(self, tenant_id: UUID) -> list[dict[str, str]]:
        return self._store.list_cells_for_tenant(tenant_id)

    def list_cell_tenant_assignments(self) -> list[dict[str, str]]:
        return self._store.list_cell_tenant_assignments()

    def get_runtime_settings(self, cell_id: UUID) -> dict[str, object] | None:
        return self._store.get_runtime_settings(cell_id)

    def list_catalogs(self, cell_id: UUID) -> list[dict[str, object]]:
        return self._store.list_catalogs(cell_id)

    def list_assets(self, cell_id: UUID) -> list[dict[str, object]]:
        return self._store.list_assets(cell_id)

    def list_policy_rules(
        self,
        asset_id: UUID,
        *,
        actor: ControlPlaneActor | None = None,
    ) -> list[dict[str, object]]:
        return policy_service.list_policy_rules(self._store, asset_id, actor=actor)

    def list_auth_providers(self, cell_id: UUID) -> list[dict[str, object]]:
        return self._store.list_auth_providers(cell_id)

    def get_cell_draft(self, cell_id: UUID) -> dict[str, object]:
        return self._store.get_cell_draft(cell_id)

    def list_publications(self, cell_id: UUID) -> list[dict[str, object]]:
        return self._store.list_publications(cell_id)

    def get_active_publication_summary(self, cell_id: UUID) -> dict[str, str]:
        return self._store.get_active_publication_summary(cell_id)

    def get_workspace_summary(
        self,
        actor: ControlPlaneActor | None = None,
    ) -> dict[str, object]:
        return workspace_service.get_workspace_summary(self._store, actor)

    def get_workspace_runtime_settings(self) -> dict[str, object] | None:
        return workspace_service.get_workspace_runtime_settings(self._store)

    def get_workspace_observations(
        self,
        actor: ControlPlaneActor,
    ) -> dict[str, object]:
        return workspace_service.get_workspace_observations(self._store, actor)

    def list_workspace_auth_providers(self) -> list[dict[str, object]]:
        return workspace_service.list_workspace_auth_providers(self._store)

    def list_workspace_publications(self) -> list[dict[str, object]]:
        return policy_version_service.list_workspace_publications(self._store)

    def list_policy_version_history(
        self,
        *,
        actor: ControlPlaneActor | None = None,
    ) -> list[dict[str, object]]:
        return policy_version_service.list_policy_version_history(self._store, actor=actor)

    def list_policy_version_history_page(
        self,
        *,
        actor: ControlPlaneActor,
        limit: int,
        cursor: str | None = None,
    ) -> dict[str, object]:
        return policy_version_service.list_policy_version_history_page(
            self._store,
            actor=actor,
            limit=limit,
            cursor=cursor,
        )

    def list_audit_events(
        self,
        *,
        actor: ControlPlaneActor,
        asset_id: UUID | None = None,
        limit: int = 100,
    ) -> list[dict[str, object]]:
        return audit_service.list_audit_events(
            self._store,
            actor=actor,
            asset_id=asset_id,
            limit=limit,
        )

    def list_audit_events_page(
        self,
        *,
        actor: ControlPlaneActor,
        asset_id: UUID | None = None,
        actor_filter: str | None = None,
        action: str | None = None,
        resource_type: str | None = None,
        outcome: str | None = None,
        correlation_id: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> dict[str, object]:
        return audit_service.list_audit_events_page(
            self._store,
            actor=actor,
            asset_id=asset_id,
            actor_filter=actor_filter,
            action=action,
            resource_type=resource_type,
            outcome=outcome,
            correlation_id=correlation_id,
            created_after=created_after,
            created_before=created_before,
            limit=limit,
            cursor=cursor,
        )

    def list_asset_policy_version_history(
        self,
        asset_id: UUID,
        *,
        actor: ControlPlaneActor,
    ) -> list[dict[str, object]]:
        return policy_version_service.list_asset_policy_version_history(
            self._store,
            asset_id,
            actor=actor,
        )

    def get_asset_policy_version(
        self,
        asset_id: UUID,
        policy_version: int,
        *,
        actor: ControlPlaneActor,
    ) -> dict[str, object]:
        return policy_version_service.get_asset_policy_version(
            self._store,
            asset_id,
            policy_version,
            actor=actor,
        )

    def restore_policy_version(
        self,
        asset_id: UUID,
        policy_version: int,
        *,
        actor: ControlPlaneActor,
        expected_revision: int,
    ) -> dict[str, object]:
        return draft_service.restore_policy_version(
            self._store,
            asset_id,
            actor,
            policy_version=policy_version,
            expected_revision=expected_revision,
        )

    def list_workspace_catalogs(self) -> list[dict[str, object]]:
        return catalog_service.list_workspace_catalogs(self._store)

    def discover_workspace_catalog_tables(
        self,
        name: str,
        *,
        actor: ControlPlaneActor | None = None,
    ) -> dict[str, object]:
        return catalog_service.discover_workspace_catalog_tables(
            self._store,
            name,
            discover=discover_catalog_tables,
            egress_allowlist=self._catalog_egress_allowlist,
            session_key=actor.identity_key() if actor is not None else None,
            plugin_registry=self._plugin_registry,
        )

    def diagnose_workspace_catalog(
        self,
        name: str,
        *,
        actor: ControlPlaneActor | None = None,
    ) -> dict[str, object]:
        return catalog_service.diagnose_workspace_catalog(
            self._store,
            name,
            discover=discover_catalog_tables,
            egress_allowlist=self._catalog_egress_allowlist,
            session_key=actor.identity_key() if actor is not None else None,
            plugin_registry=self._plugin_registry,
        )

    def list_workspace_assets(
        self,
        actor: ControlPlaneActor | None = None,
    ) -> list[dict[str, object]]:
        return asset_service.list_workspace_assets(self._store, actor)

    def list_workspace_assets_page(
        self,
        actor: ControlPlaneActor,
        *,
        limit: int,
        cursor: str | None = None,
        search: str | None = None,
    ) -> dict[str, object]:
        return asset_service.list_workspace_assets_page(
            self._store,
            actor,
            limit=limit,
            cursor=cursor,
            search=search,
        )

    def get_workspace_asset(
        self,
        asset_id: UUID,
        actor: ControlPlaneActor | None = None,
    ) -> dict[str, object]:
        return asset_service.get_workspace_asset(self._store, asset_id, actor)

    def get_asset_schema(
        self,
        asset_id: UUID,
        actor: ControlPlaneActor,
    ) -> dict[str, object]:
        return schema_service.get_asset_schema(
            self._store,
            asset_id,
            actor,
            egress_allowlist=self._catalog_egress_allowlist,
            plugin_registry=self._plugin_registry,
        )

    def get_workspace_draft(self) -> dict[str, object]:
        return workspace_service.get_workspace_draft(self._store)

    def create_workspace_publication(
        self,
        actor: ControlPlaneActor | None = None,
    ) -> dict[str, object]:
        return policy_version_service.create_workspace_publication(
            self._store,
            self.create_publication,
            actor_principal="system" if actor is None else actor.identity_key(),
            plugin_registry=self._plugin_registry,
        )

    def create_asset_policy_version(
        self,
        asset_id: UUID,
        *,
        actor: ControlPlaneActor,
        expected_draft_revision: int | None = None,
        expected_publication_id: UUID | None = None,
        review_token: str | None = None,
        idempotency_key: str | None = None,
        draft_id: UUID | None = None,
    ) -> dict[str, object]:
        return policy_version_service.create_asset_policy_version(
            self._store,
            asset_id,
            actor=actor,
            create_publication=self.create_publication,
            activate_publication=self.activate_publication,
            plugin_registry=self._plugin_registry,
            expected_draft_revision=expected_draft_revision,
            expected_publication_id=expected_publication_id,
            review_token=review_token,
            catalog_egress_allowlist=self._catalog_egress_allowlist,
            idempotency_key=idempotency_key,
            require_review=self._require_review,
            review_secret=self._review_secret,
            draft_id=draft_id,
        )

    def get_publication_operation(
        self,
        asset_id: UUID,
        idempotency_key: str,
        *,
        actor: ControlPlaneActor,
    ) -> dict[str, object]:
        return policy_version_service.get_publication_operation(
            self._store,
            asset_id,
            idempotency_key,
            actor=actor,
        )

    def activate_workspace_publication(
        self,
        publication_id: UUID,
        *,
        expected_publication_id: UUID | None = None,
        actor: ControlPlaneActor | None = None,
    ) -> dict[str, str]:
        return workspace_service.activate_workspace_publication(
            self._store,
            self.activate_publication,
            publication_id,
            expected_publication_id=expected_publication_id,
            actor_principal="system" if actor is None else actor.identity_key(),
        )

    def assign_tenant(self, cell_id: UUID, tenant_id: UUID, shard_key: str) -> None:
        self._store.assign_tenant_to_cell(
            cell_id=cell_id,
            tenant_id=tenant_id,
            shard_key=shard_key,
        )

    def upsert_runtime_settings(
        self,
        cell_id: UUID,
        ttl: int,
        max_tickets: int,
        max_ticket_exchanges: int,
    ) -> None:
        self._store.upsert_runtime_settings(
            cell_id=cell_id,
            ticket_ttl_seconds=ttl,
            max_tickets=max_tickets,
            max_ticket_exchanges=max_ticket_exchanges,
        )

    def upsert_workspace_runtime_settings(
        self,
        ttl: int,
        max_tickets: int,
        max_ticket_exchanges: int,
        expected_revision: int | None = None,
        actor: ControlPlaneActor | None = None,
    ) -> dict[str, object]:
        return workspace_service.upsert_workspace_runtime_settings(
            self._store,
            ttl=ttl,
            max_tickets=max_tickets,
            max_ticket_exchanges=max_ticket_exchanges,
            expected_revision=expected_revision,
            actor_principal="system" if actor is None else actor.identity_key(),
        )

    def upsert_catalog(
        self,
        cell_id: UUID,
        tenant_id: UUID,
        name: str,
        module: str,
        options: dict[str, Any],
    ) -> dict[str, str]:
        catalog_id = self._store.upsert_catalog(
            cell_id=cell_id,
            tenant_id=tenant_id,
            name=name,
            module=module,
            options=options,
        )
        return {"id": str(catalog_id), "name": name}

    def upsert_workspace_catalog(
        self,
        name: str,
        module: str,
        options: dict[str, Any],
        expected_revision: int | None = None,
        actor: ControlPlaneActor | None = None,
    ) -> dict[str, str]:
        return catalog_service.upsert_workspace_catalog(
            self._store,
            name=name,
            module=module,
            options=options,
            expected_revision=expected_revision,
            egress_allowlist=self._catalog_egress_allowlist,
            actor_principal="system" if actor is None else actor.identity_key(),
            plugin_registry=self._plugin_registry,
        )

    def upsert_asset(
        self,
        cell_id: UUID,
        tenant_id: UUID,
        catalog: str,
        target: str,
        backend: str,
        table_identifier: str | None,
        options: dict[str, Any],
    ) -> dict[str, str]:
        asset_id = self._store.upsert_asset(
            cell_id=cell_id,
            tenant_id=tenant_id,
            catalog=catalog,
            target=target,
            backend=backend,
            table_identifier=table_identifier,
            options=options,
        )
        return {"id": str(asset_id), "catalog": catalog, "target": target}

    def upsert_workspace_asset(
        self,
        catalog: str,
        target: str,
        backend: str,
        table_identifier: str | None,
        options: dict[str, Any],
        expected_revision: int | None = None,
    ) -> dict[str, str]:
        return asset_service.upsert_workspace_asset(
            self._store,
            catalog=catalog,
            target=target,
            backend=backend,
            table_identifier=table_identifier,
            options=options,
            expected_revision=expected_revision,
            plugin_registry=self._plugin_registry,
        )

    def replace_policy_rules(
        self,
        asset_id: UUID,
        rules: list[dict[str, Any]],
        *,
        actor: ControlPlaneActor,
    ) -> None:
        policy_service.replace_policy_rules(
            self._store,
            asset_id,
            rules,
            actor=actor,
        )

    def replace_asset_owners(
        self,
        asset_id: UUID,
        owners: list[str],
        expected_revision: int | None = None,
        actor: ControlPlaneActor | None = None,
    ) -> list[str]:
        return asset_service.replace_asset_owners(
            self._store,
            asset_id,
            owners,
            expected_revision=expected_revision,
            actor=actor,
        )

    def list_asset_grants(self, asset_id: UUID) -> list[dict[str, str]]:
        return asset_service.list_asset_grants(self._store, asset_id)

    def lock_asset_for_publication(self, asset_id: UUID) -> None:
        """Locks an asset before authorization that participates in a mutation.

        Grant-manager authorization must observe the same asset generation that
        the subsequent replacement writes.  Exposing the repository lock through
        the application service keeps that ordering out of the route adapter.
        """

        self._store.lock_asset_for_publication(asset_id)

    def get_policy_draft(self, asset_id: UUID, actor: ControlPlaneActor) -> dict[str, object]:
        return draft_service.get_policy_draft(self._store, asset_id, actor)

    def get_policy_draft_by_id(
        self, asset_id: UUID, draft_id: UUID, actor: ControlPlaneActor
    ) -> dict[str, object]:
        return draft_service.get_policy_draft_by_id(self._store, asset_id, draft_id, actor)

    def save_policy_draft(
        self,
        asset_id: UUID,
        actor: ControlPlaneActor,
        *,
        expected_revision: int,
        rules: list[dict[str, Any]],
    ) -> dict[str, object]:
        return draft_service.save_policy_draft(
            self._store,
            asset_id,
            actor,
            expected_revision=expected_revision,
            rules=rules,
        )

    def replace_asset_grants(
        self,
        asset_id: UUID,
        grants: list[dict[str, str]],
        expected_revision: int | None = None,
        actor: ControlPlaneActor | None = None,
    ) -> list[dict[str, str]]:
        return asset_service.replace_asset_grants(
            self._store,
            asset_id,
            grants,
            expected_revision=expected_revision,
            actor=actor,
        )

    def ensure_asset_capability(
        self,
        asset_id: UUID,
        actor: ControlPlaneActor,
        capability: str,
    ) -> None:
        policy_service.ensure_asset_capability(self._store, asset_id, actor, capability)

    def replace_asset_schema_fields(
        self,
        asset_id: UUID,
        fields: list[dict[str, Any]],
        expected_revision: int | None = None,
    ) -> list[dict[str, object]]:
        return asset_service.replace_asset_schema_fields(
            self._store,
            asset_id,
            fields,
            expected_revision=expected_revision,
        )

    def preview_asset_policy(
        self,
        asset_id: UUID,
        *,
        principal: str,
        groups: list[str],
        claims: dict[str, object],
        actor: ControlPlaneActor | None = None,
        requested_columns: list[str] | None = None,
        draft_id: UUID | None = None,
    ) -> dict[str, object]:
        return policy_service.preview_asset_policy(
            self._store,
            asset_id,
            principal=principal,
            groups=groups,
            claims=claims,
            actor=actor,
            requested_columns=requested_columns,
            draft_id=draft_id,
        )

    def evaluate_asset_policy(
        self,
        asset_id: UUID,
        actor: ControlPlaneActor,
        *,
        principal: str,
        groups: list[str],
        claims: dict[str, object],
        rows: list[dict[str, object]] | None,
        draft_id: UUID | None = None,
        draft_revision: int | None = None,
    ) -> dict[str, object]:
        return evaluation_service.evaluate_asset_policy(
            self._store,
            asset_id,
            actor,
            principal=principal,
            groups=groups,
            claims=claims,
            rows=rows,
            egress_allowlist=self._catalog_egress_allowlist,
            plugin_registry=self._plugin_registry,
            draft_id=draft_id,
            draft_revision=draft_revision,
        )

    def review_asset_policy(
        self,
        asset_id: UUID,
        actor: ControlPlaneActor,
        *,
        principal: str,
        groups: list[str],
        claims: dict[str, object],
        rows: list[dict[str, object]] | None,
        draft_id: UUID | None = None,
        draft_revision: int | None = None,
    ) -> dict[str, object]:
        evaluation = evaluation_service.evaluate_asset_policy(
            self._store,
            asset_id,
            actor,
            principal=principal,
            groups=groups,
            claims=claims,
            rows=rows,
            egress_allowlist=self._catalog_egress_allowlist,
            plugin_registry=self._plugin_registry,
            draft_id=draft_id,
            draft_revision=draft_revision,
        )
        try:
            return review_service.issue_review_token(
                self._store,
                asset_id,
                actor,
                evaluation,
                secret=self._review_secret,
                require_saved_draft=self._require_review,
                egress_allowlist=self._catalog_egress_allowlist,
                draft_id=draft_id,
            )
        except AuthorizationFailure:
            return evaluation

    def replace_auth_providers(self, cell_id: UUID, providers: list[dict[str, Any]]) -> None:
        self._store.replace_auth_providers(cell_id=cell_id, providers=providers)

    def replace_workspace_auth_providers(
        self,
        providers: list[dict[str, Any]],
        expected_revision: int | None = None,
        actor: ControlPlaneActor | None = None,
    ) -> list[dict[str, object]]:
        return workspace_service.replace_workspace_auth_providers(
            self._store,
            providers,
            expected_revision=expected_revision,
            actor_principal="system" if actor is None else actor.identity_key(),
        )

    def workspace_auth_provider_revision(self) -> int:
        return workspace_service.workspace_auth_provider_revision(self._store)

    def create_publication(
        self,
        cell_id: UUID,
        *,
        plugin_registry: PluginRegistry | None = None,
    ) -> dict[str, object]:
        return policy_version_service.create_publication(
            self._store,
            cell_id,
            plugin_registry=self._plugin_registry if plugin_registry is None else plugin_registry,
        )

    def activate_publication(
        self,
        cell_id: UUID,
        publication_id: UUID,
        *,
        expected_publication_id: UUID | None = None,
        actor_principal: str = "system",
        audit_workspace: bool = False,
    ) -> dict[str, str]:
        return policy_version_service.activate_publication(
            self._store,
            cell_id,
            publication_id,
            expected_publication_id=expected_publication_id,
            actor_principal=actor_principal,
            audit_workspace=audit_workspace,
        )

    def _required_workspace_context(self):
        return workspace_service.required_workspace_context(self._store)

    def _ensure_policy_editor(self, asset_id: UUID, actor: ControlPlaneActor) -> None:
        policy_service.ensure_policy_editor(self._store, asset_id, actor)

    def _validate_publish_readiness(self, draft) -> None:
        policy_version_service._validate_publish_readiness(self._store, draft)
