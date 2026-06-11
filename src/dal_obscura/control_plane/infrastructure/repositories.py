from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, cast
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from dal_obscura.common.config_store.orm import (
    ActivePublicationRecord,
    AssetOwnerRecord,
    AssetRecord,
    AssetSchemaFieldRecord,
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
    CompiledAsset,
    CompiledCatalog,
    CompiledPublication,
    CompiledRuntime,
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


@dataclass(frozen=True)
class WorkspaceContext:
    cell_id: UUID
    tenant_id: UUID


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

    def list_tenants(self) -> list[dict[str, str]]:
        return [
            {
                "id": str(record.id),
                "slug": record.slug,
                "display_name": record.display_name,
                "status": record.status,
            }
            for record in self._session.scalars(select(TenantRecord).order_by(TenantRecord.slug))
        ]

    def list_cells(self) -> list[dict[str, str]]:
        return [
            {
                "id": str(record.id),
                "name": record.name,
                "region": record.region,
                "status": record.status,
            }
            for record in self._session.scalars(select(CellRecord).order_by(CellRecord.name))
        ]

    def list_cells_for_tenant(self, tenant_id: UUID) -> list[dict[str, str]]:
        return [
            {
                "id": str(cell.id),
                "name": cell.name,
                "region": cell.region,
                "status": cell.status,
                "shard_key": assignment.shard_key,
            }
            for cell, assignment in self._session.execute(
                select(CellRecord, CellTenantRecord)
                .join(CellTenantRecord, CellTenantRecord.cell_id == CellRecord.id)
                .where(CellTenantRecord.tenant_id == tenant_id)
                .order_by(CellRecord.name)
            )
        ]

    def list_cell_tenant_assignments(self) -> list[dict[str, str]]:
        return [
            {
                "cell_id": str(record.cell_id),
                "tenant_id": str(record.tenant_id),
                "shard_key": record.shard_key,
            }
            for record in self._session.scalars(
                select(CellTenantRecord).order_by(
                    CellTenantRecord.cell_id,
                    CellTenantRecord.tenant_id,
                )
            )
        ]

    def get_default_workspace_context(self) -> WorkspaceContext | None:
        row = self._session.execute(
            select(CellRecord, TenantRecord)
            .join(CellTenantRecord, CellTenantRecord.cell_id == CellRecord.id)
            .join(TenantRecord, TenantRecord.id == CellTenantRecord.tenant_id)
            .order_by(TenantRecord.slug, CellRecord.name)
            .limit(1)
        ).first()
        if row is None:
            return None
        cell, tenant = row
        return WorkspaceContext(cell_id=cell.id, tenant_id=tenant.id)

    def ensure_default_workspace_context(self) -> WorkspaceContext:
        existing = self.get_default_workspace_context()
        if existing is not None:
            return existing

        tenant_id = uuid4()
        cell_id = uuid4()
        self._session.add(
            TenantRecord(
                id=tenant_id,
                slug="default",
                display_name="Default workspace",
                status="active",
            )
        )
        self._session.add(
            CellRecord(
                id=cell_id,
                name="default",
                region="local",
                status="active",
            )
        )
        self._session.flush()
        self._session.add(
            CellTenantRecord(cell_id=cell_id, tenant_id=tenant_id, shard_key="default")
        )
        self._session.flush()
        return WorkspaceContext(cell_id=cell_id, tenant_id=tenant_id)

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
        max_ticket_exchanges: int,
        path_rules: list[dict[str, Any]],
    ) -> None:
        existing = self._session.get(CellRuntimeSettingsRecord, cell_id)
        if existing is None:
            self._session.add(
                CellRuntimeSettingsRecord(
                    cell_id=cell_id,
                    ticket_ttl_seconds=ticket_ttl_seconds,
                    max_tickets=max_tickets,
                    max_ticket_exchanges=max_ticket_exchanges,
                    path_rules_json=path_rules,
                )
            )
        else:
            existing.ticket_ttl_seconds = ticket_ttl_seconds
            existing.max_tickets = max_tickets
            existing.max_ticket_exchanges = max_ticket_exchanges
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
        self._session.flush()
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

    def replace_asset_owners(self, *, asset_id: UUID, owners: list[str]) -> list[str]:
        asset = self._session.get(AssetRecord, asset_id)
        if asset is None:
            raise LookupError(f"No asset {asset_id}")
        normalized = _normalize_principals(owners)
        for record in self._session.scalars(
            select(AssetOwnerRecord).where(AssetOwnerRecord.asset_id == asset_id)
        ):
            self._session.delete(record)
        self._session.flush()
        for ordinal, principal in enumerate(normalized, start=1):
            self._session.add(
                AssetOwnerRecord(
                    id=uuid4(),
                    asset_id=asset_id,
                    ordinal=ordinal,
                    principal=principal,
                )
            )
        self._session.flush()
        return normalized

    def replace_asset_schema_fields(
        self,
        *,
        asset_id: UUID,
        fields: list[dict[str, Any]],
    ) -> list[dict[str, object]]:
        asset = self._session.get(AssetRecord, asset_id)
        if asset is None:
            raise LookupError(f"No asset {asset_id}")
        normalized = _normalize_schema_fields(fields)
        for record in self._session.scalars(
            select(AssetSchemaFieldRecord).where(AssetSchemaFieldRecord.asset_id == asset_id)
        ):
            self._session.delete(record)
        self._session.flush()
        for ordinal, field in enumerate(normalized, start=1):
            self._session.add(
                AssetSchemaFieldRecord(
                    id=uuid4(),
                    asset_id=asset_id,
                    ordinal=ordinal,
                    name=str(field["name"]),
                    type=str(field["type"]),
                    nullable=bool(field["nullable"]),
                )
            )
        self._session.flush()
        return normalized

    def replace_auth_providers(self, *, cell_id: UUID, providers: list[dict[str, Any]]) -> None:
        for record in self._session.scalars(
            select(AuthProviderRecord).where(AuthProviderRecord.cell_id == cell_id)
        ):
            self._session.delete(record)
        self._session.flush()
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
        publication = self._session.get(ConfigPublicationRecord, publication_id)
        if publication is None or publication.cell_id != cell_id:
            raise LookupError(f"No publication {publication_id} for cell {cell_id}")
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

    def get_cell(self, cell_id: UUID) -> dict[str, str]:
        record = self._session.get(CellRecord, cell_id)
        if record is None:
            raise LookupError(f"No cell {cell_id}")
        return {
            "id": str(record.id),
            "name": record.name,
            "region": record.region,
            "status": record.status,
        }

    def get_runtime_settings(self, cell_id: UUID) -> dict[str, object] | None:
        record = self._session.get(CellRuntimeSettingsRecord, cell_id)
        if record is None:
            return None
        return {
            "cell_id": str(record.cell_id),
            "ticket_ttl_seconds": record.ticket_ttl_seconds,
            "max_tickets": record.max_tickets,
            "max_ticket_exchanges": record.max_ticket_exchanges,
            "path_rules": list(record.path_rules_json),
        }

    def list_catalogs(self, cell_id: UUID) -> list[dict[str, object]]:
        return [
            {
                "id": str(record.id),
                "cell_id": str(record.cell_id),
                "tenant_id": str(record.tenant_id),
                "name": record.name,
                "module": record.module,
                "options": dict(record.options_json),
            }
            for record in self._session.scalars(
                select(CatalogRecord)
                .where(CatalogRecord.cell_id == cell_id)
                .order_by(CatalogRecord.name)
            )
        ]

    def list_workspace_catalogs(self, context: WorkspaceContext) -> list[dict[str, object]]:
        assets_by_catalog: dict[UUID, int] = {}
        for asset in self._session.scalars(
            select(AssetRecord).where(
                AssetRecord.cell_id == context.cell_id,
                AssetRecord.tenant_id == context.tenant_id,
            )
        ):
            assets_by_catalog[asset.catalog_id] = assets_by_catalog.get(asset.catalog_id, 0) + 1
        return [
            {
                "id": str(record.id),
                "name": record.name,
                "module": record.module,
                "options": dict(record.options_json),
                "status": "configured",
                "discovered_table_count": 0,
                "governed_asset_count": assets_by_catalog.get(record.id, 0),
            }
            for record in self._session.scalars(
                select(CatalogRecord)
                .where(
                    CatalogRecord.cell_id == context.cell_id,
                    CatalogRecord.tenant_id == context.tenant_id,
                )
                .order_by(CatalogRecord.name)
            )
        ]

    def get_workspace_catalog(self, context: WorkspaceContext, name: str) -> dict[str, object]:
        record = self._catalog_by_name(
            cell_id=context.cell_id,
            tenant_id=context.tenant_id,
            name=name,
        )
        return {
            "id": str(record.id),
            "name": record.name,
            "module": record.module,
            "options": dict(record.options_json),
        }

    def list_assets(self, cell_id: UUID) -> list[dict[str, object]]:
        catalog_records = list(
            self._session.scalars(select(CatalogRecord).where(CatalogRecord.cell_id == cell_id))
        )
        catalog_by_id = {record.id: record for record in catalog_records}
        assets = []
        for record in self._session.scalars(
            select(AssetRecord).where(AssetRecord.cell_id == cell_id).order_by(AssetRecord.target)
        ):
            catalog = catalog_by_id[record.catalog_id]
            assets.append(
                {
                    "id": str(record.id),
                    "cell_id": str(record.cell_id),
                    "tenant_id": str(record.tenant_id),
                    "catalog_id": str(record.catalog_id),
                    "catalog": catalog.name,
                    "target": record.target,
                    "backend": record.backend,
                    "table_identifier": record.table_identifier,
                    "options": dict(record.options_json),
                }
            )
        return assets

    def list_workspace_assets(self, context: WorkspaceContext) -> list[dict[str, object]]:
        catalog_records = list(
            self._session.scalars(
                select(CatalogRecord).where(
                    CatalogRecord.cell_id == context.cell_id,
                    CatalogRecord.tenant_id == context.tenant_id,
                )
            )
        )
        catalog_by_id = {record.id: record for record in catalog_records}
        return [
            self._workspace_asset_row(record, catalog_by_id[record.catalog_id])
            for record in self._session.scalars(
                select(AssetRecord)
                .where(
                    AssetRecord.cell_id == context.cell_id,
                    AssetRecord.tenant_id == context.tenant_id,
                )
                .order_by(AssetRecord.target)
            )
        ]

    def get_workspace_asset(self, asset_id: UUID) -> dict[str, object]:
        record = self._session.get(AssetRecord, asset_id)
        if record is None:
            raise LookupError(f"No asset {asset_id}")
        catalog = self._session.get(CatalogRecord, record.catalog_id)
        if catalog is None:
            raise LookupError(f"No catalog {record.catalog_id}")
        return {
            **self._workspace_asset_row(record, catalog),
            "options": dict(record.options_json),
            "schema_fields": self.list_asset_schema_fields(asset_id),
            "policy_rules": self.list_policy_rules(asset_id),
        }

    def list_asset_owners(self, asset_id: UUID) -> list[str]:
        return [
            record.principal
            for record in self._session.scalars(
                select(AssetOwnerRecord)
                .where(AssetOwnerRecord.asset_id == asset_id)
                .order_by(AssetOwnerRecord.ordinal)
            )
        ]

    def list_asset_schema_fields(self, asset_id: UUID) -> list[dict[str, object]]:
        return [
            {
                "name": record.name,
                "type": record.type,
                "nullable": record.nullable,
            }
            for record in self._session.scalars(
                select(AssetSchemaFieldRecord)
                .where(AssetSchemaFieldRecord.asset_id == asset_id)
                .order_by(AssetSchemaFieldRecord.ordinal)
            )
        ]

    def list_policy_rules(self, asset_id: UUID) -> list[dict[str, object]]:
        return [
            {
                "id": str(record.id),
                "asset_id": str(record.asset_id),
                "ordinal": record.ordinal,
                "effect": record.effect,
                "principals": list(record.principals_json),
                "when": dict(record.when_json),
                "columns": list(record.columns_json),
                "masks": dict(record.masks_json),
                "row_filter": record.row_filter_sql,
            }
            for record in self._session.scalars(
                select(PolicyRuleRecord)
                .where(PolicyRuleRecord.asset_id == asset_id)
                .order_by(PolicyRuleRecord.ordinal)
            )
        ]

    def list_auth_providers(self, cell_id: UUID) -> list[dict[str, object]]:
        return [
            {
                "id": str(record.id),
                "cell_id": str(record.cell_id),
                "ordinal": record.ordinal,
                "module": record.module,
                "args": dict(record.args_json),
                "enabled": record.enabled,
            }
            for record in self._session.scalars(
                select(AuthProviderRecord)
                .where(AuthProviderRecord.cell_id == cell_id)
                .order_by(AuthProviderRecord.ordinal)
            )
        ]

    def get_cell_draft(self, cell_id: UUID) -> dict[str, object]:
        cell = self.get_cell(cell_id)
        assignments = [
            item for item in self.list_cell_tenant_assignments() if item["cell_id"] == str(cell_id)
        ]
        assets = []
        for asset in self.list_assets(cell_id):
            rules = self.list_policy_rules(UUID(str(asset["id"])))
            assets.append({**asset, "policy_rules": rules})
        return {
            "cell": cell,
            "assignments": assignments,
            "runtime_settings": self.get_runtime_settings(cell_id),
            "catalogs": self.list_catalogs(cell_id),
            "assets": assets,
            "auth_providers": self.list_auth_providers(cell_id),
        }

    def list_publications(self, cell_id: UUID) -> list[dict[str, object]]:
        active = self._session.get(ActivePublicationRecord, cell_id)
        active_publication_id = active.publication_id if active is not None else None
        return [
            {
                "id": str(record.id),
                "cell_id": str(record.cell_id),
                "schema_version": record.schema_version,
                "status": record.status,
                "manifest_hash": record.manifest_hash,
                "active": record.id == active_publication_id,
            }
            for record in self._session.scalars(
                select(ConfigPublicationRecord)
                .where(ConfigPublicationRecord.cell_id == cell_id)
                .order_by(ConfigPublicationRecord.created_at)
            )
        ]

    def get_workspace_summary(self, context: WorkspaceContext | None) -> dict[str, object]:
        if context is None:
            return _empty_workspace_summary()
        catalogs = self.list_workspace_catalogs(context)
        assets = self.list_workspace_assets(context)
        missing_policy_count = sum(1 for asset in assets if asset["policy_status"] == "missing")
        active: dict[str, str] | None
        try:
            active_publication = self.get_active_publication_summary(context.cell_id)
            active = {
                "publication_id": active_publication["publication_id"],
                "manifest_hash": active_publication["manifest_hash"],
                "status": active_publication["status"],
            }
        except LookupError:
            active = None
        enabled_auth_provider_count = sum(
            1 for provider in self.list_auth_providers(context.cell_id) if provider["enabled"]
        )
        return {
            "catalog_count": len(catalogs),
            "asset_count": len(assets),
            "unowned_asset_count": sum(1 for asset in assets if asset["owner_count"] == 0),
            "missing_policy_count": missing_policy_count,
            "draft_change_count": len(assets),
            "runtime_configured": self.get_runtime_settings(context.cell_id) is not None,
            "enabled_auth_provider_count": enabled_auth_provider_count,
            "active_publication": active,
        }

    def get_workspace_draft(self, context: WorkspaceContext) -> dict[str, object]:
        catalogs = self.list_workspace_catalogs(context)
        assets = self.list_workspace_assets(context)
        return {
            "catalog_count": len(catalogs),
            "asset_count": len(assets),
            "catalogs": catalogs,
            "assets": assets,
        }

    def get_active_publication_summary(self, cell_id: UUID) -> dict[str, str]:
        active = self._session.get(ActivePublicationRecord, cell_id)
        if active is None:
            raise LookupError(f"No active publication for cell {cell_id}")
        publication = self._session.get(ConfigPublicationRecord, active.publication_id)
        if publication is None:
            raise LookupError(f"No publication {active.publication_id}")
        return {
            "cell_id": str(active.cell_id),
            "publication_id": str(active.publication_id),
            "manifest_hash": publication.manifest_hash,
            "status": publication.status,
        }

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
                max_ticket_exchanges=runtime_record.max_ticket_exchanges,
                path_rules=list(runtime_record.path_rules_json),
            ),
            auth_providers=auth_providers,
            catalogs=catalogs,
            assets=assets,
        )

    def load_asset_publish_draft(self, asset_id: UUID) -> tuple[AssetDraft, CatalogDraft]:
        asset = self._session.get(AssetRecord, asset_id)
        if asset is None:
            raise LookupError(f"No asset {asset_id}")
        catalog = self._session.get(CatalogRecord, asset.catalog_id)
        if catalog is None:
            raise LookupError(f"No catalog {asset.catalog_id}")
        catalog_draft = CatalogDraft(
            id=catalog.id,
            cell_id=catalog.cell_id,
            tenant_id=catalog.tenant_id,
            name=catalog.name,
            module=catalog.module,
            options=dict(catalog.options_json),
        )
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
        return (
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
            ),
            catalog_draft,
        )

    def load_active_compiled_publication(self, cell_id: UUID) -> CompiledPublication:
        active = self._session.get(ActivePublicationRecord, cell_id)
        if active is None:
            raise LookupError(f"No active publication for cell {cell_id}")
        publication = self._session.get(ConfigPublicationRecord, active.publication_id)
        if publication is None:
            raise LookupError(f"No publication {active.publication_id}")
        runtime = self._session.get(PublishedCellRuntimeRecord, active.publication_id)
        if runtime is None:
            raise LookupError(f"No published runtime for publication {active.publication_id}")
        catalogs = [
            CompiledCatalog(
                tenant_id=record.tenant_id,
                catalog=record.catalog,
                config=dict(record.config_json),
            )
            for record in self._session.scalars(
                select(PublishedCatalogRecord).where(
                    PublishedCatalogRecord.publication_id == active.publication_id
                )
            )
        ]
        assets = [
            CompiledAsset(
                tenant_id=record.tenant_id,
                catalog=record.catalog,
                target=record.target,
                backend=record.backend,
                compiled_config=dict(record.compiled_config_json),
                policy_version=record.policy_version,
            )
            for record in self._session.scalars(
                select(PublishedAssetRecord).where(
                    PublishedAssetRecord.publication_id == active.publication_id
                )
            )
        ]
        return CompiledPublication(
            cell_id=cell_id,
            runtime=CompiledRuntime(
                auth_chain=dict(runtime.auth_chain_json),
                ticket=dict(runtime.ticket_json),
                path_rules=list(runtime.path_rules_json),
            ),
            catalogs=catalogs,
            assets=assets,
            manifest_hash=publication.manifest_hash,
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

    def _workspace_asset_row(
        self,
        record: AssetRecord,
        catalog: CatalogRecord,
    ) -> dict[str, object]:
        policy_rules = self.list_policy_rules(record.id)
        owners = self.list_asset_owners(record.id)
        return {
            "id": str(record.id),
            "name": record.target,
            "catalog": catalog.name,
            "backend": record.backend,
            "table_identifier": record.table_identifier,
            "owner_count": len(owners),
            "owners": owners,
            "policy_status": "configured" if policy_rules else "missing",
            "draft_status": "draft",
        }


def _empty_workspace_summary() -> dict[str, object]:
    return {
        "catalog_count": 0,
        "asset_count": 0,
        "unowned_asset_count": 0,
        "missing_policy_count": 0,
        "draft_change_count": 0,
        "runtime_configured": False,
        "enabled_auth_provider_count": 0,
        "active_publication": None,
    }


def _normalize_principals(principals: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for principal in principals:
        value = principal.strip()
        if value and value not in seen:
            normalized.append(value)
            seen.add(value)
    return normalized


def _normalize_schema_fields(fields: list[dict[str, Any]]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    seen: set[str] = set()
    for field in fields:
        name = str(field.get("name", "")).strip()
        if not name or name in seen:
            continue
        normalized.append(
            {
                "name": name,
                "type": str(field.get("type", "string")).strip() or "string",
                "nullable": bool(field.get("nullable", True)),
            }
        )
        seen.add(name)
    return normalized
