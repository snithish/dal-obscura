from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Literal, cast
from uuid import UUID, uuid4

from sqlalchemy import and_, delete, func, or_, select, tuple_, update
from sqlalchemy.orm import Session

from dal_obscura.common.config_store.orm import (
    ActivePublicationRecord,
    ActivePublishedAssetRecord,
    AssetGrantRecord,
    AssetOwnerRecord,
    AssetPolicyDraftRecord,
    AssetRecord,
    AssetSchemaFieldRecord,
    AuditEventRecord,
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
    utcnow,
)
from dal_obscura.control_plane.application.errors import PublicationConflictError
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
    """Active publication pointer for one data-plane cell.

    Example:
        ```python
        active = store.active_publication(cell_id)
        ```
    """

    cell_id: UUID
    publication_id: UUID


@dataclass(frozen=True)
class PublishedAsset:
    """Published asset view returned by repository read paths.

    Example:
        ```python
        asset = store.published_asset(publication_id, tenant_id, "analytics", "orders")
        ```
    """

    publication_id: UUID
    tenant_id: UUID
    catalog: str
    target: str
    backend: str
    compiled_config: dict[str, Any]
    policy_version: int


@dataclass(frozen=True)
class WorkspaceContext:
    """Internal cell and tenant identifiers for the default workspace.

    Example:
        ```python
        context = store.ensure_default_workspace_context()
        ```
    """

    cell_id: UUID
    tenant_id: UUID


@dataclass(frozen=True)
class AssetPage:
    """Cursor-paginated asset listing.

    Example:
        ```python
        page = store.list_assets(cell_id=context.cell_id, tenant_id=context.tenant_id)
        ```
    """

    items: list[dict[str, object]]
    next_cursor: str | None


class PublicationStore:
    """Repository for draft configuration, publication, and activation records.

    Example:
        ```python
        with Session(engine) as session:
            store = PublicationStore(session)
            context = store.ensure_default_workspace_context()
        ```
    """

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

    def ensure_publication_context(self, *, cell_id: UUID, tenant_id: UUID) -> None:
        """Creates the manifest-selected cell and tenant when publishing the first generation."""

        if self._session.get(CellRecord, cell_id) is None:
            self._session.add(
                CellRecord(
                    id=cell_id,
                    name=f"operator-{cell_id}",
                    region="operator",
                    status="active",
                )
            )
        if self._session.get(TenantRecord, tenant_id) is None:
            self._session.add(
                TenantRecord(
                    id=tenant_id,
                    slug=f"operator-{tenant_id}",
                    display_name="Operator-managed tenant",
                    status="active",
                )
            )
        self._session.flush()
        assignment = self._session.get(
            CellTenantRecord,
            {"cell_id": cell_id, "tenant_id": tenant_id},
        )
        if assignment is None:
            self._session.add(
                CellTenantRecord(cell_id=cell_id, tenant_id=tenant_id, shard_key="operator")
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
        max_ticket_exchanges: int,
    ) -> None:
        existing = self._session.get(CellRuntimeSettingsRecord, cell_id)
        if existing is None:
            self._session.add(
                CellRuntimeSettingsRecord(
                    cell_id=cell_id,
                    ticket_ttl_seconds=ticket_ttl_seconds,
                    max_tickets=max_tickets,
                    max_ticket_exchanges=max_ticket_exchanges,
                    path_rules_json=[],
                )
            )
        else:
            existing.ticket_ttl_seconds = ticket_ttl_seconds
            existing.max_tickets = max_tickets
            existing.max_ticket_exchanges = max_ticket_exchanges
            existing.path_rules_json = []
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
        normalized_rules = [_normalize_policy_rule(raw) for raw in rules]
        for record in self._session.scalars(
            select(PolicyRuleRecord).where(PolicyRuleRecord.asset_id == asset_id)
        ):
            self._session.delete(record)
        self._session.flush()
        for raw in normalized_rules:
            self._session.add(
                PolicyRuleRecord(
                    id=uuid4(),
                    asset_id=asset_id,
                    ordinal=int(raw["ordinal"]),
                    effect=raw["effect"],
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
        self._replace_active_assets(cell_id=cell_id, publication_id=publication_id)
        self._session.flush()

    def activate_publication_if_current(
        self,
        *,
        cell_id: UUID,
        publication_id: UUID,
        expected_publication_id: UUID,
    ) -> None:
        """Atomically activates a publication only when the expected generation remains active."""

        publication = self._session.get(ConfigPublicationRecord, publication_id)
        if publication is None or publication.cell_id != cell_id:
            raise LookupError(f"No publication {publication_id} for cell {cell_id}")
        result = self._session.execute(
            update(ActivePublicationRecord)
            .where(
                ActivePublicationRecord.cell_id == cell_id,
                ActivePublicationRecord.publication_id == expected_publication_id,
            )
            .values(publication_id=publication_id)
        )
        if getattr(result, "rowcount", None) != 1:
            raise PublicationConflictError(
                "active generation changed; reread status before publishing"
            )
        self._replace_active_assets(cell_id=cell_id, publication_id=publication_id)
        self._session.flush()

    def activate_initial_publication(self, *, cell_id: UUID, publication_id: UUID) -> None:
        """Activates the first publication only when no generation is active."""

        publication = self._session.get(ConfigPublicationRecord, publication_id)
        if publication is None or publication.cell_id != cell_id:
            raise LookupError(f"No publication {publication_id} for cell {cell_id}")
        if self._session.get(ActivePublicationRecord, cell_id) is not None:
            raise PublicationConflictError(
                "active generation already exists; reread status before publishing"
            )
        self._session.add(ActivePublicationRecord(cell_id=cell_id, publication_id=publication_id))
        self._replace_active_assets(cell_id=cell_id, publication_id=publication_id)
        self._session.flush()

    def _replace_active_assets(self, *, cell_id: UUID, publication_id: UUID) -> None:
        self._session.execute(
            delete(ActivePublishedAssetRecord).where(ActivePublishedAssetRecord.cell_id == cell_id)
        )
        for asset in self._session.scalars(
            select(PublishedAssetRecord).where(
                PublishedAssetRecord.publication_id == publication_id
            )
        ):
            self.activate_published_asset(
                cell_id=cell_id,
                tenant_id=asset.tenant_id,
                catalog=asset.catalog,
                target=asset.target,
                publication_id=publication_id,
            )

    def activate_published_asset(
        self,
        *,
        cell_id: UUID,
        tenant_id: UUID,
        catalog: str,
        target: str,
        publication_id: UUID,
    ) -> None:
        existing = self._session.get(
            ActivePublishedAssetRecord,
            {
                "cell_id": cell_id,
                "tenant_id": tenant_id,
                "catalog": catalog,
                "target": target,
            },
        )
        if existing is None:
            self._session.add(
                ActivePublishedAssetRecord(
                    cell_id=cell_id,
                    tenant_id=tenant_id,
                    catalog=catalog,
                    target=target,
                    publication_id=publication_id,
                )
            )
        else:
            existing.publication_id = publication_id

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
        for row in self._session.execute(
            select(AssetRecord.catalog_id, func.count())
            .where(
                AssetRecord.cell_id == context.cell_id,
                AssetRecord.tenant_id == context.tenant_id,
            )
            .group_by(AssetRecord.catalog_id)
        ):
            assets_by_catalog[row[0]] = row[1]
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
        records = list(
            self._session.scalars(
                select(AssetRecord)
                .where(
                    AssetRecord.cell_id == context.cell_id,
                    AssetRecord.tenant_id == context.tenant_id,
                )
                .order_by(AssetRecord.target, AssetRecord.id)
            )
        )
        return self._workspace_asset_rows(records)

    def list_workspace_assets_for_principals(
        self,
        context: WorkspaceContext,
        principals: set[str],
    ) -> list[dict[str, object]]:
        """Lists only assets owned by one of the supplied actor principals."""

        if not principals:
            return []
        records = list(
            self._session.scalars(
                select(AssetRecord)
                .outerjoin(AssetOwnerRecord, AssetOwnerRecord.asset_id == AssetRecord.id)
                .outerjoin(AssetGrantRecord, AssetGrantRecord.asset_id == AssetRecord.id)
                .where(
                    AssetRecord.cell_id == context.cell_id,
                    AssetRecord.tenant_id == context.tenant_id,
                    or_(
                        AssetOwnerRecord.principal.in_(principals),
                        and_(
                            AssetGrantRecord.principal.in_(principals),
                            AssetGrantRecord.capability == "read",
                        ),
                    ),
                )
                .distinct()
                .order_by(AssetRecord.target, AssetRecord.id)
            )
        )
        return self._workspace_asset_rows(records)

    def list_workspace_assets_page(
        self,
        context: WorkspaceContext,
        *,
        limit: int,
        cursor: str | None = None,
        search: str | None = None,
    ) -> AssetPage:
        after = _decode_asset_cursor(cursor) if cursor else None
        query = select(AssetRecord).where(
            AssetRecord.cell_id == context.cell_id,
            AssetRecord.tenant_id == context.tenant_id,
        )
        if search:
            pattern = f"%{search.strip()}%"
            query = query.where(
                or_(
                    AssetRecord.target.ilike(pattern),
                    AssetRecord.table_identifier.ilike(pattern),
                    AssetRecord.backend.ilike(pattern),
                )
            )
        if after is not None:
            query = query.where(tuple_(AssetRecord.target, AssetRecord.id) > after)
        records = list(
            self._session.scalars(
                query.order_by(AssetRecord.target, AssetRecord.id).limit(limit + 1)
            )
        )
        next_cursor = None
        if len(records) > limit:
            records = records[:limit]
            next_cursor = _encode_asset_cursor(records[-1])
        return AssetPage(items=self._workspace_asset_rows(records), next_cursor=next_cursor)

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

    def get_asset_workspace_context(self, asset_id: UUID) -> WorkspaceContext:
        """Returns the internal workspace scope for one asset."""

        record = self._session.get(AssetRecord, asset_id)
        if record is None:
            raise LookupError(f"No asset {asset_id}")
        return WorkspaceContext(cell_id=record.cell_id, tenant_id=record.tenant_id)

    def list_asset_owners(self, asset_id: UUID) -> list[str]:
        return [
            record.principal
            for record in self._session.scalars(
                select(AssetOwnerRecord)
                .where(AssetOwnerRecord.asset_id == asset_id)
                .order_by(AssetOwnerRecord.ordinal)
            )
        ]

    def list_asset_grants(self, asset_id: UUID) -> list[dict[str, str]]:
        return [
            {
                "id": str(record.id),
                "asset_id": str(record.asset_id),
                "principal": record.principal,
                "capability": record.capability,
            }
            for record in self._session.scalars(
                select(AssetGrantRecord)
                .where(AssetGrantRecord.asset_id == asset_id)
                .order_by(AssetGrantRecord.principal, AssetGrantRecord.capability)
            )
        ]

    def replace_asset_grants(
        self,
        *,
        asset_id: UUID,
        grants: list[dict[str, str]],
    ) -> list[dict[str, str]]:
        if self._session.get(AssetRecord, asset_id) is None:
            raise LookupError(f"No asset {asset_id}")
        for record in self._session.scalars(
            select(AssetGrantRecord).where(AssetGrantRecord.asset_id == asset_id)
        ):
            self._session.delete(record)
        self._session.flush()
        normalized: list[dict[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for raw in grants:
            principal = str(raw["principal"]).strip()
            capability = str(raw["capability"]).strip()
            key = (principal, capability)
            if not principal or not capability or key in seen:
                continue
            seen.add(key)
            self._session.add(
                AssetGrantRecord(
                    id=uuid4(),
                    asset_id=asset_id,
                    principal=principal,
                    capability=capability,
                )
            )
            normalized.append({"principal": principal, "capability": capability})
        self._session.flush()
        return normalized

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

    def get_asset_policy_draft(
        self,
        *,
        asset_id: UUID,
        author_principal: str,
    ) -> dict[str, object] | None:
        record = self._session.scalar(
            select(AssetPolicyDraftRecord).where(
                AssetPolicyDraftRecord.asset_id == asset_id,
                AssetPolicyDraftRecord.author_principal == author_principal,
                AssetPolicyDraftRecord.discarded_at.is_(None),
            )
        )
        if record is None:
            return None
        return {
            "id": str(record.id),
            "asset_id": str(record.asset_id),
            "author_principal": record.author_principal,
            "revision": record.revision,
            "base_policy_version": record.base_policy_version,
            "rules": [dict(rule) for rule in record.rules_json],
            "content_hash": record.content_hash,
            "created_at": _isoformat(record.created_at),
            "updated_at": _isoformat(record.updated_at),
        }

    def save_asset_policy_draft(
        self,
        *,
        asset_id: UUID,
        author_principal: str,
        expected_revision: int,
        rules: list[dict[str, object]],
        content_hash: str,
        base_policy_version: int,
    ) -> dict[str, object]:
        record = self._session.scalar(
            select(AssetPolicyDraftRecord).where(
                AssetPolicyDraftRecord.asset_id == asset_id,
                AssetPolicyDraftRecord.author_principal == author_principal,
                AssetPolicyDraftRecord.discarded_at.is_(None),
            )
        )
        current_revision = 0 if record is None else record.revision
        if current_revision != expected_revision:
            raise PublicationConflictError(
                "Policy draft revision changed "
                f"(expected {expected_revision}, current {current_revision})."
            )
        now = utcnow()
        if record is None:
            record = AssetPolicyDraftRecord(
                id=uuid4(),
                asset_id=asset_id,
                author_principal=author_principal,
                revision=1,
                base_policy_version=base_policy_version,
                rules_json=[dict(rule) for rule in rules],
                content_hash=content_hash,
                created_at=now,
                updated_at=now,
            )
            self._session.add(record)
        else:
            record.revision += 1
            record.rules_json = [dict(rule) for rule in rules]
            record.content_hash = content_hash
            record.updated_at = now
        self._session.flush()
        return {
            "id": str(record.id),
            "asset_id": str(record.asset_id),
            "author_principal": record.author_principal,
            "revision": record.revision,
            "base_policy_version": record.base_policy_version,
            "rules": [dict(rule) for rule in record.rules_json],
            "content_hash": record.content_hash,
            "created_at": _isoformat(record.created_at),
            "updated_at": _isoformat(record.updated_at),
        }

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

    def list_policy_version_history(self, context: WorkspaceContext) -> list[dict[str, object]]:
        active = self._session.get(ActivePublicationRecord, context.cell_id)
        active_publication_id = active.publication_id if active is not None else None
        rows = self._session.execute(
            select(
                PublishedAssetRecord,
                ConfigPublicationRecord,
                AssetRecord,
            )
            .join(
                ConfigPublicationRecord,
                ConfigPublicationRecord.id == PublishedAssetRecord.publication_id,
            )
            .join(
                CatalogRecord,
                CatalogRecord.cell_id == ConfigPublicationRecord.cell_id,
            )
            .join(
                AssetRecord,
                AssetRecord.catalog_id == CatalogRecord.id,
            )
            .where(
                ConfigPublicationRecord.cell_id == context.cell_id,
                PublishedAssetRecord.tenant_id == context.tenant_id,
                CatalogRecord.tenant_id == context.tenant_id,
                CatalogRecord.name == PublishedAssetRecord.catalog,
                AssetRecord.tenant_id == context.tenant_id,
                AssetRecord.target == PublishedAssetRecord.target,
            )
            .order_by(ConfigPublicationRecord.created_at, PublishedAssetRecord.target)
        )
        return [
            {
                "asset_id": str(asset.id),
                "asset_name": asset.target,
                "catalog": published.catalog,
                "target": published.target,
                "policy_version": published.policy_version,
                "active": published.publication_id == active_publication_id,
                "created_at": _isoformat(publication.created_at),
            }
            for published, publication, asset in rows
        ]

    def get_published_asset_policy(
        self,
        *,
        asset_id: UUID,
        policy_version: int,
    ) -> dict[str, object]:
        """Returns the immutable policy body for one asset version."""

        asset = self._session.get(AssetRecord, asset_id)
        if asset is None:
            raise LookupError(f"No asset {asset_id}")
        catalog = self._session.get(CatalogRecord, asset.catalog_id)
        if catalog is None:
            raise LookupError(f"No catalog {asset.catalog_id}")
        record = self._session.scalar(
            select(PublishedAssetRecord)
            .join(
                ConfigPublicationRecord,
                ConfigPublicationRecord.id == PublishedAssetRecord.publication_id,
            )
            .where(
                PublishedAssetRecord.tenant_id == asset.tenant_id,
                PublishedAssetRecord.catalog == catalog.name,
                PublishedAssetRecord.target == asset.target,
                PublishedAssetRecord.policy_version == policy_version,
            )
            .order_by(ConfigPublicationRecord.created_at.desc())
        )
        if record is None:
            raise LookupError(f"No published policy version {policy_version} for asset {asset_id}")
        config = cast(dict[str, object], record.compiled_config_json)
        policy = cast(dict[str, object], config.get("policy", {}))
        return {
            "asset_id": str(asset_id),
            "policy_version": record.policy_version,
            "rules": cast(list[dict[str, object]], policy.get("rules", [])),
            "compiled_config": config,
        }

    def record_asset_audit_event(
        self,
        *,
        asset_id: UUID,
        actor_principal: str,
        action: str,
        outcome: str = "success",
        details: dict[str, object] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Records a safe asset-scoped event in the current transaction."""

        asset = self._session.get(AssetRecord, asset_id)
        if asset is None:
            raise LookupError(f"No asset {asset_id}")
        self._session.add(
            AuditEventRecord(
                id=uuid4(),
                cell_id=asset.cell_id,
                tenant_id=asset.tenant_id,
                actor_principal=actor_principal,
                action=action,
                resource_type="asset",
                resource_id=str(asset_id),
                outcome=outcome,
                details_json=dict(details or {}),
                correlation_id=correlation_id,
            )
        )
        self._session.flush()

    def list_audit_events(
        self,
        context: WorkspaceContext,
        *,
        asset_ids: set[str] | None = None,
        limit: int = 100,
    ) -> list[dict[str, object]]:
        """Returns bounded, tenant-scoped audit events with safe details."""

        bounded_limit = max(1, min(limit, 200))
        query = select(AuditEventRecord).where(
            AuditEventRecord.cell_id == context.cell_id,
            AuditEventRecord.tenant_id == context.tenant_id,
        )
        if asset_ids is not None:
            if not asset_ids:
                return []
            query = query.where(
                AuditEventRecord.resource_type == "asset",
                AuditEventRecord.resource_id.in_(asset_ids),
            )
        records = self._session.scalars(
            query.order_by(AuditEventRecord.created_at.desc()).limit(bounded_limit)
        )
        return [
            {
                "id": str(record.id),
                "actor": record.actor_principal,
                "action": record.action,
                "resource_type": record.resource_type,
                "resource_id": record.resource_id,
                "outcome": record.outcome,
                "details": dict(record.details_json),
                "correlation_id": record.correlation_id,
                "created_at": _isoformat(record.created_at),
            }
            for record in records
        ]

    def get_workspace_summary(self, context: WorkspaceContext | None) -> dict[str, object]:
        if context is None:
            return _empty_workspace_summary()
        catalogs = self.list_workspace_catalogs(context)
        assets = self.list_workspace_assets(context)
        missing_policy_count = sum(1 for asset in assets if asset["policy_status"] == "missing")
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
                    effect=_normalize_policy_rule_effect(rule.effect),
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
            ),
            auth_providers=auth_providers,
            catalogs=catalogs,
            assets=assets,
        )

    def load_asset_publish_draft(
        self,
        asset_id: UUID,
        *,
        author_principal: str | None = None,
    ) -> tuple[AssetDraft, CatalogDraft]:
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
                effect=_normalize_policy_rule_effect(rule.effect),
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
        if author_principal is not None:
            personal_draft = self._session.scalar(
                select(AssetPolicyDraftRecord).where(
                    AssetPolicyDraftRecord.asset_id == asset_id,
                    AssetPolicyDraftRecord.author_principal == author_principal,
                    AssetPolicyDraftRecord.discarded_at.is_(None),
                )
            )
            if personal_draft is not None:
                rules = [
                    PolicyRuleDraft(
                        ordinal=int(raw.get("ordinal", 0)),
                        effect="allow",
                        principals=[str(item) for item in raw.get("principals", [])],
                        when=cast(dict[str, str | list[str]], dict(raw.get("when", {}))),
                        columns=[str(item) for item in raw.get("columns", [])],
                        masks=dict(raw.get("masks", {})),
                        row_filter=cast(str | None, raw.get("row_filter")),
                    )
                    for raw in personal_draft.rules_json
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

    def load_active_compiled_publication_config(self, cell_id: UUID) -> CompiledPublication:
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
        assets: list[CompiledAsset] = []
        for active_asset in self._session.scalars(
            select(ActivePublishedAssetRecord).where(ActivePublishedAssetRecord.cell_id == cell_id)
        ):
            record = self._session.get(
                PublishedAssetRecord,
                {
                    "publication_id": active_asset.publication_id,
                    "tenant_id": active_asset.tenant_id,
                    "catalog": active_asset.catalog,
                    "target": active_asset.target,
                },
            )
            if record is None:
                raise LookupError(
                    f"No published asset {active_asset.catalog}/{active_asset.target}"
                )
            assets.append(
                CompiledAsset(
                    tenant_id=record.tenant_id,
                    catalog=record.catalog,
                    target=record.target,
                    backend=record.backend,
                    compiled_config=dict(record.compiled_config_json),
                    policy_version=record.policy_version,
                )
            )
        return CompiledPublication(
            cell_id=cell_id,
            runtime=CompiledRuntime(
                auth_chain=dict(runtime.auth_chain_json),
                ticket=dict(runtime.ticket_json),
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
                path_rules_json=[],
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
        return self._workspace_asset_rows([record], catalog_by_id={catalog.id: catalog})[0]

    def _workspace_asset_rows(
        self,
        records: list[AssetRecord],
        *,
        catalog_by_id: dict[UUID, CatalogRecord] | None = None,
    ) -> list[dict[str, object]]:
        if not records:
            return []
        if catalog_by_id is None:
            catalog_ids = {record.catalog_id for record in records}
            catalog_by_id = {
                record.id: record
                for record in self._session.scalars(
                    select(CatalogRecord).where(CatalogRecord.id.in_(catalog_ids))
                )
            }
        asset_ids = [record.id for record in records]
        owners_by_asset: dict[UUID, list[str]] = {asset_id: [] for asset_id in asset_ids}
        for asset_id, principal in self._session.execute(
            select(AssetOwnerRecord.asset_id, AssetOwnerRecord.principal)
            .where(AssetOwnerRecord.asset_id.in_(asset_ids))
            .order_by(AssetOwnerRecord.asset_id, AssetOwnerRecord.ordinal)
        ):
            owners_by_asset.setdefault(asset_id, []).append(principal)
        assets_with_rules = {
            asset_id
            for (asset_id,) in self._session.execute(
                select(PolicyRuleRecord.asset_id)
                .where(PolicyRuleRecord.asset_id.in_(asset_ids))
                .group_by(PolicyRuleRecord.asset_id)
            )
        }
        rows = []
        for record in records:
            catalog = catalog_by_id[record.catalog_id]
            owners = owners_by_asset.get(record.id, [])
            rows.append(
                {
                    "id": str(record.id),
                    "name": record.target,
                    "catalog": catalog.name,
                    "backend": record.backend,
                    "table_identifier": record.table_identifier,
                    "owner_count": len(owners),
                    "owners": owners,
                    "policy_status": "configured" if record.id in assets_with_rules else "missing",
                    "draft_status": "draft",
                }
            )
        return rows


def _encode_asset_cursor(record: AssetRecord) -> str:
    raw = json.dumps({"target": record.target, "id": str(record.id)}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def _decode_asset_cursor(value: str) -> tuple[str, UUID]:
    try:
        padded = value + "=" * (-len(value) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        data = json.loads(raw)
        return str(data["target"]), UUID(str(data["id"]))
    except Exception as exc:
        raise ValueError("Invalid asset cursor") from exc


def _empty_workspace_summary() -> dict[str, object]:
    return {
        "catalog_count": 0,
        "asset_count": 0,
        "unowned_asset_count": 0,
        "missing_policy_count": 0,
        "draft_change_count": 0,
        "runtime_configured": False,
        "enabled_auth_provider_count": 0,
    }


def _isoformat(value) -> str:
    return value.isoformat().replace("+00:00", "Z")


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


def _normalize_policy_rule(raw: dict[str, Any]) -> dict[str, Any]:
    effect = _normalize_policy_rule_effect(str(raw.get("effect", "allow")))
    return {**raw, "effect": effect}


def _normalize_policy_rule_effect(effect: str) -> Literal["allow"]:
    if effect != "allow":
        raise ValueError("Policy rules are explicit grants")
    return "allow"
