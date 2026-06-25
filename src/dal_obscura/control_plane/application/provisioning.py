from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.orm import Session

from dal_obscura.control_plane.application import (
    asset_service,
    catalog_service,
    policy_service,
    policy_version_service,
    workspace_service,
)
from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.infrastructure.catalog_discovery import discover_catalog_tables
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore


class ProvisioningService:
    """Application service used by the FastAPI control-plane routes."""

    def __init__(self, session: Session) -> None:
        self._store = PublicationStore(session)

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

    def list_policy_rules(self, asset_id: UUID) -> list[dict[str, object]]:
        return self._store.list_policy_rules(asset_id)

    def list_auth_providers(self, cell_id: UUID) -> list[dict[str, object]]:
        return self._store.list_auth_providers(cell_id)

    def get_cell_draft(self, cell_id: UUID) -> dict[str, object]:
        return self._store.get_cell_draft(cell_id)

    def list_publications(self, cell_id: UUID) -> list[dict[str, object]]:
        return self._store.list_publications(cell_id)

    def get_active_publication_summary(self, cell_id: UUID) -> dict[str, str]:
        return self._store.get_active_publication_summary(cell_id)

    def get_workspace_summary(self) -> dict[str, object]:
        return workspace_service.get_workspace_summary(self._store)

    def get_workspace_runtime_settings(self) -> dict[str, object] | None:
        return workspace_service.get_workspace_runtime_settings(self._store)

    def list_workspace_auth_providers(self) -> list[dict[str, object]]:
        return workspace_service.list_workspace_auth_providers(self._store)

    def list_workspace_publications(self) -> list[dict[str, object]]:
        return policy_version_service.list_workspace_publications(self._store)

    def list_workspace_catalogs(self) -> list[dict[str, object]]:
        return catalog_service.list_workspace_catalogs(self._store)

    def discover_workspace_catalog_tables(self, name: str) -> dict[str, object]:
        return catalog_service.discover_workspace_catalog_tables(
            self._store,
            name,
            discover=discover_catalog_tables,
        )

    def list_workspace_assets(self) -> list[dict[str, object]]:
        return asset_service.list_workspace_assets(self._store)

    def get_workspace_asset(self, asset_id: UUID) -> dict[str, object]:
        return asset_service.get_workspace_asset(self._store, asset_id)

    def get_workspace_draft(self) -> dict[str, object]:
        return workspace_service.get_workspace_draft(self._store)

    def create_workspace_publication(self) -> dict[str, object]:
        return policy_version_service.create_workspace_publication(
            self._store,
            self.create_publication,
        )

    def create_asset_policy_version(
        self,
        asset_id: UUID,
        *,
        actor: ControlPlaneActor,
    ) -> dict[str, object]:
        return policy_version_service.create_asset_policy_version(
            self._store,
            asset_id,
            actor=actor,
            create_publication=self.create_publication,
            activate_publication=self.activate_publication,
        )

    def activate_workspace_publication(self, publication_id: UUID) -> dict[str, str]:
        return workspace_service.activate_workspace_publication(
            self._store,
            self.activate_publication,
            publication_id,
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
    ) -> None:
        workspace_service.upsert_workspace_runtime_settings(
            self._store,
            ttl=ttl,
            max_tickets=max_tickets,
            max_ticket_exchanges=max_ticket_exchanges,
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
    ) -> dict[str, str]:
        return catalog_service.upsert_workspace_catalog(
            self._store,
            name=name,
            module=module,
            options=options,
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
    ) -> dict[str, str]:
        return asset_service.upsert_workspace_asset(
            self._store,
            catalog=catalog,
            target=target,
            backend=backend,
            table_identifier=table_identifier,
            options=options,
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

    def replace_asset_owners(self, asset_id: UUID, owners: list[str]) -> list[str]:
        return asset_service.replace_asset_owners(self._store, asset_id, owners)

    def replace_asset_schema_fields(
        self,
        asset_id: UUID,
        fields: list[dict[str, Any]],
    ) -> list[dict[str, object]]:
        return asset_service.replace_asset_schema_fields(self._store, asset_id, fields)

    def preview_asset_policy(
        self,
        asset_id: UUID,
        *,
        principal: str,
        groups: list[str],
        claims: dict[str, object],
    ) -> dict[str, object]:
        return policy_service.preview_asset_policy(
            self._store,
            asset_id,
            principal=principal,
            groups=groups,
            claims=claims,
        )

    def replace_auth_providers(self, cell_id: UUID, providers: list[dict[str, Any]]) -> None:
        self._store.replace_auth_providers(cell_id=cell_id, providers=providers)

    def replace_workspace_auth_providers(self, providers: list[dict[str, Any]]) -> None:
        workspace_service.replace_workspace_auth_providers(self._store, providers)

    def create_publication(self, cell_id: UUID) -> dict[str, object]:
        return policy_version_service.create_publication(self._store, cell_id)

    def activate_publication(self, cell_id: UUID, publication_id: UUID) -> dict[str, str]:
        return policy_version_service.activate_publication(
            self._store,
            cell_id,
            publication_id,
        )

    def _required_workspace_context(self):
        return workspace_service.required_workspace_context(self._store)

    def _ensure_policy_editor(self, asset_id: UUID, actor: ControlPlaneActor) -> None:
        policy_service.ensure_policy_editor(self._store, asset_id, actor)

    def _validate_publish_readiness(self, draft) -> None:
        policy_version_service._validate_publish_readiness(self._store, draft)
