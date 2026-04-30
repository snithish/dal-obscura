from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, cast
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from dal_obscura.common.config_store.orm import (
    ActivePublicationRecord,
    AssetRecord,
    AuthProviderRecord,
    CatalogRecord,
    CellRecord,
    CellRuntimeSettingsRecord,
    CellTenantRecord,
    ConfigPublicationRecord,
    PolicyRuleRecord,
    PublishedAssetRecord,
    PublishedCatalogRecord,
    PublishedCellRuntimeRecord,
    TenantRecord,
)
from dal_obscura.control_plane.domain.models import (
    AssetDraft,
    AuthProviderDraft,
    CatalogDraft,
    CellRuntimeDraft,
    CompiledPublication,
    PolicyRuleDraft,
    PublishDraft,
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

    def upsert_runtime_settings(
        self,
        *,
        cell_id: UUID,
        ticket_ttl_seconds: int,
        max_tickets: int,
        path_rules: list[dict[str, Any]],
    ) -> None:
        existing = self._session.get(CellRuntimeSettingsRecord, cell_id)
        if existing is None:
            self._session.add(
                CellRuntimeSettingsRecord(
                    cell_id=cell_id,
                    ticket_ttl_seconds=ticket_ttl_seconds,
                    max_tickets=max_tickets,
                    path_rules_json=path_rules,
                )
            )
        else:
            existing.ticket_ttl_seconds = ticket_ttl_seconds
            existing.max_tickets = max_tickets
            existing.path_rules_json = path_rules
        self._session.flush()

    def upsert_catalog(
        self,
        *,
        cell_id: UUID,
        tenant_id: UUID,
        name: str,
        module: str,
        options: dict[str, Any],
    ) -> UUID:
        existing = self._session.scalar(
            select(CatalogRecord).where(
                CatalogRecord.cell_id == cell_id,
                CatalogRecord.tenant_id == tenant_id,
                CatalogRecord.name == name,
            )
        )
        if existing is None:
            catalog_id = uuid4()
            self._session.add(
                CatalogRecord(
                    id=catalog_id,
                    cell_id=cell_id,
                    tenant_id=tenant_id,
                    name=name,
                    module=module,
                    options_json=options,
                )
            )
        else:
            catalog_id = existing.id
            existing.module = module
            existing.options_json = options
        self._session.flush()
        return catalog_id

    def upsert_asset(
        self,
        *,
        cell_id: UUID,
        tenant_id: UUID,
        catalog: str,
        target: str,
        backend: str,
        table_identifier: str | None,
        options: dict[str, Any],
    ) -> UUID:
        catalog_record = self._catalog_by_name(cell_id=cell_id, tenant_id=tenant_id, name=catalog)
        existing = self._session.scalar(
            select(AssetRecord).where(
                AssetRecord.cell_id == cell_id,
                AssetRecord.tenant_id == tenant_id,
                AssetRecord.catalog_id == catalog_record.id,
                AssetRecord.target == target,
            )
        )
        if existing is None:
            asset_id = uuid4()
            self._session.add(
                AssetRecord(
                    id=asset_id,
                    cell_id=cell_id,
                    tenant_id=tenant_id,
                    catalog_id=catalog_record.id,
                    target=target,
                    backend=backend,
                    table_identifier=table_identifier,
                    options_json=options,
                )
            )
        else:
            asset_id = existing.id
            existing.backend = backend
            existing.table_identifier = table_identifier
            existing.options_json = options
        self._session.flush()
        return asset_id

    def replace_policy_rules(self, *, asset_id: UUID, rules: list[dict[str, Any]]) -> None:
        for record in self._session.scalars(
            select(PolicyRuleRecord).where(PolicyRuleRecord.asset_id == asset_id)
        ):
            self._session.delete(record)
        for raw in rules:
            self._session.add(
                PolicyRuleRecord(
                    id=uuid4(),
                    asset_id=asset_id,
                    ordinal=int(raw["ordinal"]),
                    effect=str(raw.get("effect", "allow")),
                    principals_json=list(raw.get("principals", [])),
                    when_json=dict(raw.get("when", {})),
                    columns_json=list(raw.get("columns", [])),
                    masks_json=dict(raw.get("masks", {})),
                    row_filter_sql=raw.get("row_filter"),
                )
            )
        self._session.flush()

    def replace_auth_providers(self, *, cell_id: UUID, providers: list[dict[str, Any]]) -> None:
        for record in self._session.scalars(
            select(AuthProviderRecord).where(AuthProviderRecord.cell_id == cell_id)
        ):
            self._session.delete(record)
        for raw in providers:
            self._session.add(
                AuthProviderRecord(
                    id=uuid4(),
                    cell_id=cell_id,
                    ordinal=int(raw["ordinal"]),
                    module=str(raw["module"]),
                    args_json=dict(raw.get("args", {})),
                    enabled=bool(raw.get("enabled", True)),
                )
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

    def load_publish_draft(self, cell_id: UUID) -> PublishDraft:
        runtime_record = self._session.get(CellRuntimeSettingsRecord, cell_id)
        if runtime_record is None:
            raise LookupError(f"No runtime settings for cell {cell_id}")

        tenants = [
            item.tenant_id
            for item in self._session.scalars(
                select(CellTenantRecord).where(CellTenantRecord.cell_id == cell_id)
            )
        ]
        auth_providers = [
            AuthProviderDraft(
                ordinal=item.ordinal,
                module=item.module,
                args=dict(item.args_json),
                enabled=item.enabled,
            )
            for item in self._session.scalars(
                select(AuthProviderRecord)
                .where(AuthProviderRecord.cell_id == cell_id)
                .order_by(AuthProviderRecord.ordinal)
            )
        ]
        catalog_records = list(
            self._session.scalars(select(CatalogRecord).where(CatalogRecord.cell_id == cell_id))
        )
        catalog_by_id = {item.id: item for item in catalog_records}
        catalogs = [
            CatalogDraft(
                id=item.id,
                cell_id=item.cell_id,
                tenant_id=item.tenant_id,
                name=item.name,
                module=item.module,
                options=dict(item.options_json),
            )
            for item in catalog_records
        ]
        assets = []
        for asset in self._session.scalars(
            select(AssetRecord).where(AssetRecord.cell_id == cell_id)
        ):
            catalog = catalog_by_id[asset.catalog_id]
            rules = [
                PolicyRuleDraft(
                    ordinal=rule.ordinal,
                    effect=cast(Literal["allow", "deny"], rule.effect),
                    principals=list(rule.principals_json),
                    when=cast(dict[str, str | list[str]], dict(rule.when_json)),
                    columns=list(rule.columns_json),
                    masks=dict(rule.masks_json),
                    row_filter=rule.row_filter_sql,
                )
                for rule in self._session.scalars(
                    select(PolicyRuleRecord)
                    .where(PolicyRuleRecord.asset_id == asset.id)
                    .order_by(PolicyRuleRecord.ordinal)
                )
            ]
            assets.append(
                AssetDraft(
                    id=asset.id,
                    cell_id=asset.cell_id,
                    tenant_id=asset.tenant_id,
                    catalog_id=asset.catalog_id,
                    catalog_name=catalog.name,
                    target=asset.target,
                    backend=asset.backend,
                    table_identifier=asset.table_identifier,
                    options=dict(asset.options_json),
                    rules=rules,
                )
            )

        return PublishDraft(
            cell_id=cell_id,
            tenants=tenants,
            runtime=CellRuntimeDraft(
                ticket_ttl_seconds=runtime_record.ticket_ttl_seconds,
                max_tickets=runtime_record.max_tickets,
                path_rules=list(runtime_record.path_rules_json),
            ),
            auth_providers=auth_providers,
            catalogs=catalogs,
            assets=assets,
        )

    def insert_compiled_publication(
        self,
        *,
        publication_id: UUID,
        compiled: CompiledPublication,
    ) -> None:
        self.insert_publication(
            cell_id=compiled.cell_id,
            publication_id=publication_id,
            manifest_hash=compiled.manifest_hash,
        )
        self._session.add(
            PublishedCellRuntimeRecord(
                publication_id=publication_id,
                auth_chain_json=compiled.runtime.auth_chain,
                ticket_json=compiled.runtime.ticket,
                path_rules_json=compiled.runtime.path_rules,
            )
        )
        for catalog in compiled.catalogs:
            self._session.add(
                PublishedCatalogRecord(
                    publication_id=publication_id,
                    tenant_id=catalog.tenant_id,
                    catalog=catalog.catalog,
                    config_json=catalog.config,
                )
            )
        for asset in compiled.assets:
            self.insert_published_asset(
                publication_id=publication_id,
                tenant_id=asset.tenant_id,
                catalog=asset.catalog,
                target=asset.target,
                backend=asset.backend,
                compiled_config=asset.compiled_config,
                policy_version=asset.policy_version,
            )
        self._session.flush()

    def _catalog_by_name(self, *, cell_id: UUID, tenant_id: UUID, name: str) -> CatalogRecord:
        catalog = self._session.scalar(
            select(CatalogRecord).where(
                CatalogRecord.cell_id == cell_id,
                CatalogRecord.tenant_id == tenant_id,
                CatalogRecord.name == name,
            )
        )
        if catalog is None:
            raise LookupError(f"No catalog {name!r} for tenant {tenant_id}")
        return catalog
