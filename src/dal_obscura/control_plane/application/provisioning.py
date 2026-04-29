from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.orm import Session

from dal_obscura.control_plane.application.compiler import PublicationCompiler
from dal_obscura.control_plane.domain.models import CompiledPublication
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
        path_rules: list[dict[str, Any]],
    ) -> None:
        self._store.upsert_runtime_settings(
            cell_id=cell_id,
            ticket_ttl_seconds=ttl,
            max_tickets=max_tickets,
            path_rules=path_rules,
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

    def replace_policy_rules(self, asset_id: UUID, rules: list[dict[str, Any]]) -> None:
        self._store.replace_policy_rules(asset_id=asset_id, rules=rules)

    def replace_auth_providers(self, cell_id: UUID, providers: list[dict[str, Any]]) -> None:
        self._store.replace_auth_providers(cell_id=cell_id, providers=providers)

    def create_publication(self, cell_id: UUID) -> dict[str, object]:
        draft = self._store.load_publish_draft(cell_id)
        compiled = PublicationCompiler().compile(draft)
        publication_id = uuid4()
        self._store.insert_compiled_publication(
            publication_id=publication_id,
            compiled=compiled,
        )
        return _publication_response(publication_id, compiled)

    def activate_publication(self, cell_id: UUID, publication_id: UUID) -> dict[str, str]:
        self._store.activate_publication(cell_id=cell_id, publication_id=publication_id)
        return {"cell_id": str(cell_id), "publication_id": str(publication_id)}


def _publication_response(publication_id: UUID, compiled: CompiledPublication) -> dict[str, object]:
    return {
        "publication_id": str(publication_id),
        "cell_id": str(compiled.cell_id),
        "asset_count": len(compiled.assets),
        "catalog_count": len(compiled.catalogs),
        "manifest_hash": compiled.manifest_hash,
    }
