from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from dal_obscura.control_plane.infrastructure.orm import (
    ActivePublicationRecord,
    CellRecord,
    CellTenantRecord,
    ConfigPublicationRecord,
    PublishedAssetRecord,
    TenantRecord,
)


@dataclass(frozen=True)
class ActivePublication:
    cell_id: UUID
    publication_id: UUID


@dataclass(frozen=True)
class PublishedAsset:
    publication_id: UUID
    tenant_id: UUID
    catalog: str
    target: str
    backend: str
    compiled_config: dict[str, Any]
    policy_version: int


class PublicationStore:
    def __init__(self, session: Session) -> None:
        self._session = session

    def create_cell(self, *, cell_id: UUID, name: str, region: str) -> None:
        self._session.add(CellRecord(id=cell_id, name=name, region=region, status="active"))
        self._session.flush()

    def create_tenant(self, *, tenant_id: UUID, slug: str, display_name: str) -> None:
        self._session.add(
            TenantRecord(id=tenant_id, slug=slug, display_name=display_name, status="active")
        )
        self._session.flush()

    def assign_tenant_to_cell(self, *, cell_id: UUID, tenant_id: UUID, shard_key: str) -> None:
        self._session.add(
            CellTenantRecord(cell_id=cell_id, tenant_id=tenant_id, shard_key=shard_key)
        )
        self._session.flush()

    def insert_publication(
        self, *, cell_id: UUID, publication_id: UUID, manifest_hash: str
    ) -> None:
        self._session.add(
            ConfigPublicationRecord(
                id=publication_id,
                cell_id=cell_id,
                schema_version=1,
                status="published",
                manifest_hash=manifest_hash,
            )
        )
        self._session.flush()

    def activate_publication(self, *, cell_id: UUID, publication_id: UUID) -> None:
        existing = self._session.get(ActivePublicationRecord, cell_id)
        if existing is None:
            self._session.add(
                ActivePublicationRecord(cell_id=cell_id, publication_id=publication_id)
            )
        else:
            existing.publication_id = publication_id
        self._session.flush()

    def get_active_publication(self, cell_id: UUID) -> ActivePublication:
        record = self._session.get(ActivePublicationRecord, cell_id)
        if record is None:
            raise LookupError(f"No active publication for cell {cell_id}")
        return ActivePublication(cell_id=record.cell_id, publication_id=record.publication_id)

    def insert_published_asset(
        self,
        *,
        publication_id: UUID,
        tenant_id: UUID,
        catalog: str,
        target: str,
        backend: str,
        compiled_config: dict[str, Any],
        policy_version: int,
    ) -> None:
        self._session.add(
            PublishedAssetRecord(
                publication_id=publication_id,
                tenant_id=tenant_id,
                catalog=catalog,
                target=target,
                backend=backend,
                compiled_config_json=compiled_config,
                policy_version=policy_version,
            )
        )
        self._session.flush()

    def get_published_asset(
        self,
        *,
        publication_id: UUID,
        tenant_id: UUID,
        catalog: str,
        target: str,
    ) -> PublishedAsset:
        record = self._session.scalar(
            select(PublishedAssetRecord).where(
                PublishedAssetRecord.publication_id == publication_id,
                PublishedAssetRecord.tenant_id == tenant_id,
                PublishedAssetRecord.catalog == catalog,
                PublishedAssetRecord.target == target,
            )
        )
        if record is None:
            raise LookupError(f"No published asset for {catalog}/{target}")
        return PublishedAsset(
            publication_id=record.publication_id,
            tenant_id=record.tenant_id,
            catalog=record.catalog,
            target=record.target,
            backend=record.backend,
            compiled_config=dict(record.compiled_config_json),
            policy_version=record.policy_version,
        )
