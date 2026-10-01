from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, cast
from uuid import UUID, uuid4

from sqlalchemy import String, and_, exists, func, or_, select, tuple_, update
from sqlalchemy import cast as sql_cast
from sqlalchemy.orm import Session

from dal_obscura.common.config_store.orm import (
    AssetGrantRecord,
    AssetOwnerRecord,
    AssetRecord,
    AssetSchemaFieldRecord,
    AuditEventRecord,
    AuthProviderRecord,
    CatalogRecord,
    DataPlaneTicketRecord,
    PolicyRuleRecord,
    RuntimeSettingsRecord,
    WorkspaceRecord,
    utcnow,
)
from dal_obscura.common.schema_identity import canonical_provider_field_id
from dal_obscura.control_plane.application.errors import (
    ConfigurationConflictError,
    RevisionPreconditionRequired,
)
from dal_obscura.control_plane.infrastructure.request_context import current_request_id


@dataclass(frozen=True)
class AssetPage:
    """Cursor-paginated asset listing.

    Example:
        ```python
        page = store.list_assets()
        ```
    """

    items: list[dict[str, object]]
    next_cursor: str | None


@dataclass(frozen=True)
class AuditEventPage:
    """Cursor-paginated audit events."""

    items: list[dict[str, object]]
    next_cursor: str | None


class ConfigStore:
    """Repository for canonical live configuration and audit records.

    Example:
        ```python
        with Session(engine) as session:
            store = ConfigStore(session)
            context = store.ensure_default_workspace_context()
        ```
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    def get_workspace(self) -> WorkspaceRecord | None:
        """Return the deployment's singleton configuration record."""
        return self._session.get(WorkspaceRecord, 1)

    def ensure_workspace(self) -> WorkspaceRecord:
        """Initialize the deployment configuration if not yet configured."""
        workspace = self.get_workspace()
        if workspace is None:
            workspace = WorkspaceRecord(id=1)
            self._session.add(workspace)
            self._session.flush()
        return workspace

    def upsert_runtime_settings(
        self,
        *,
        ticket_ttl_seconds: int,
        max_tickets: int,
        max_ticket_exchanges: int,
        path_rules: list[dict[str, Any]] | None = None,
        expected_revision: int | None = None,
    ) -> None:
        self.ensure_workspace()
        existing = self._session.scalar(
            select(RuntimeSettingsRecord)
            .where(RuntimeSettingsRecord.id == 1)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if existing is None:
            if expected_revision not in (None, 0):
                raise ConfigurationConflictError(
                    "Runtime settings revision changed "
                    f"(expected {expected_revision}, current 0); reread before writing."
                )
            self._session.add(
                RuntimeSettingsRecord(
                    revision=1,
                    ticket_ttl_seconds=ticket_ttl_seconds,
                    max_tickets=max_tickets,
                    max_ticket_exchanges=max_ticket_exchanges,
                    path_rules_json=[dict(rule) for rule in (path_rules or [])],
                    id=1,
                )
            )
        else:
            if expected_revision is None:
                raise RevisionPreconditionRequired(
                    "Runtime settings revision is required for updates "
                    f"(current {existing.revision}); reread before writing."
                )
            if existing.revision != expected_revision:
                raise ConfigurationConflictError(
                    "Runtime settings revision changed "
                    f"(expected {expected_revision}, current {existing.revision}); "
                    "reread before writing."
                )
            changed = (
                existing.ticket_ttl_seconds != ticket_ttl_seconds
                or existing.max_tickets != max_tickets
                or existing.max_ticket_exchanges != max_ticket_exchanges
                or existing.path_rules_json != list(path_rules or [])
            )
            existing.ticket_ttl_seconds = ticket_ttl_seconds
            existing.max_tickets = max_tickets
            existing.max_ticket_exchanges = max_ticket_exchanges
            existing.path_rules_json = [dict(rule) for rule in (path_rules or [])]
            if changed:
                existing.revision += 1
        self._session.flush()

    def upsert_catalog(
        self,
        *,
        name: str,
        plugin_id: str,
        options: dict[str, Any],
        expected_revision: int | None = None,
    ) -> UUID:
        existing = self._session.scalar(
            select(CatalogRecord)
            .where(CatalogRecord.name == name)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if existing is None:
            if expected_revision not in (None, 0):
                raise ConfigurationConflictError(
                    "Catalog revision changed (expected "
                    f"{expected_revision}, current 0); reread before writing."
                )
            catalog_id = uuid4()
            self._session.add(
                CatalogRecord(id=catalog_id, name=name, plugin_id=plugin_id, options_json=options)
            )
        else:
            if expected_revision is None:
                raise RevisionPreconditionRequired(
                    "Catalog revision is required for updates "
                    f"(current {existing.revision}); reread before writing."
                )
            if existing.revision != expected_revision:
                raise ConfigurationConflictError(
                    "Catalog revision changed (expected "
                    f"{expected_revision}, current {existing.revision}); reread before writing."
                )
            catalog_id = existing.id
            if existing.plugin_id != plugin_id or existing.options_json != options:
                existing.plugin_id = plugin_id
                existing.options_json = options
                existing.revision += 1
        self._session.flush()
        return catalog_id

    def upsert_asset(
        self,
        *,
        catalog: str,
        target: str,
        backend: str,
        table_identifier: str | None,
        options: dict[str, Any],
        expected_revision: int | None = None,
    ) -> UUID:
        catalog_record = self._catalog_by_name(name=catalog)
        existing = self._session.scalar(
            select(AssetRecord)
            .where(AssetRecord.catalog_id == catalog_record.id, AssetRecord.target == target)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if existing is None:
            _assert_new_asset_revision(expected_revision)
            asset_id = uuid4()
            self._session.add(
                AssetRecord(
                    id=asset_id,
                    catalog_id=catalog_record.id,
                    target=target,
                    backend=backend,
                    table_identifier=table_identifier,
                    options_json=options,
                )
            )
        else:
            _assert_asset_revision(existing, expected_revision)
            asset_id = existing.id
            existing.backend = backend
            existing.table_identifier = table_identifier
            existing.options_json = options
            existing.revision += 1
        self._session.flush()
        return asset_id

    def replace_policy_rules(
        self,
        *,
        asset_id: UUID,
        rules: list[dict[str, Any]],
        expected_revision: int,
    ) -> int:
        asset = self._locked_asset(asset_id)
        if asset is None:
            raise LookupError(f"No asset {asset_id}")
        if asset.policy_revision != expected_revision:
            raise ConfigurationConflictError(
                "Policy revision changed "
                f"(expected {expected_revision}, current {asset.policy_revision}); "
                "reread before writing."
            )
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
                    name=raw.get("name", ""),
                    description=raw.get("description", ""),
                    principals_json=list(raw.get("principals", [])),
                    when_json=dict(raw.get("when", {})),
                    columns_json=list(raw.get("columns", [])),
                    masks_json=dict(raw.get("masks", {})),
                    row_filter_sql=raw.get("row_filter"),
                )
            )
        self._session.flush()
        asset.policy_revision += 1
        self._session.flush()
        return asset.policy_revision

    def revoke_asset_tickets(self, *, asset_id: UUID) -> int:
        """Revokes every unexpired token issued for an asset for this asset."""

        asset = self._locked_asset(asset_id)
        if asset is None:
            raise LookupError(f"No asset {asset_id}")
        result = self._session.execute(
            update(DataPlaneTicketRecord)
            .where(DataPlaneTicketRecord.revoked_at.is_(None))
            .where(DataPlaneTicketRecord.expires_at >= int(utcnow().timestamp()))
            .where(DataPlaneTicketRecord.asset_id == asset_id)
            .values(revoked_at=utcnow())
        )
        return int(getattr(result, "rowcount", 0) or 0)

    def replace_asset_owners(
        self,
        *,
        asset_id: UUID,
        owners: list[str],
        expected_revision: int | None = None,
    ) -> list[str]:
        asset = self._locked_asset(asset_id)
        if asset is None:
            raise LookupError(f"No asset {asset_id}")
        _assert_asset_revision(asset, expected_revision)
        normalized = _normalize_principals(owners)
        for record in self._session.scalars(
            select(AssetOwnerRecord).where(AssetOwnerRecord.asset_id == asset_id)
        ):
            self._session.delete(record)
        self._session.flush()
        for ordinal, principal in enumerate(normalized, start=1):
            self._session.add(
                AssetOwnerRecord(
                    id=uuid4(), asset_id=asset_id, ordinal=ordinal, principal=principal
                )
            )
        self._session.flush()
        asset.revision += 1
        self._session.flush()
        return normalized

    def replace_asset_schema_fields(
        self,
        *,
        asset_id: UUID,
        fields: list[dict[str, Any]],
        expected_revision: int | None = None,
    ) -> list[dict[str, object]]:
        asset = self._locked_asset(asset_id)
        if asset is None:
            raise LookupError(f"No asset {asset_id}")
        _assert_asset_revision(asset, expected_revision)
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
                    field_id=str(field["field_id"]),
                    path_json=list(cast(list[str], field["path"])),
                    type=str(field["type"]),
                    nullable=bool(field["nullable"]),
                )
            )
        self._session.flush()
        asset.revision += 1
        self._session.flush()
        return normalized

    def replace_auth_providers(
        self,
        *,
        providers: list[dict[str, Any]],
        expected_revision: int | None = None,
    ) -> None:
        self.lock_auth_providers_for_update()
        workspace = self._session.get(WorkspaceRecord, 1)
        if workspace is None:
            raise LookupError("Workspace is not configured")
        existing = list(self._session.scalars(select(AuthProviderRecord)))
        current_revision = workspace.auth_provider_revision
        if (existing or current_revision > 0) and expected_revision is None:
            raise RevisionPreconditionRequired(
                "Authentication provider revision is required for updates "
                f"(current {current_revision}); reread before writing."
            )
        if (existing or current_revision > 0) and expected_revision != current_revision:
            raise ConfigurationConflictError(
                "Authentication provider revision changed "
                f"(expected {expected_revision}, current {current_revision}); "
                "reread before writing."
            )
        existing_args = {record.ordinal: dict(record.args_json) for record in existing}
        new_revision = 0 if not existing and current_revision == 0 else current_revision + 1
        for record in existing:
            self._session.delete(record)
        self._session.flush()
        for raw in providers:
            self._session.add(
                AuthProviderRecord(
                    id=uuid4(),
                    ordinal=int(raw["ordinal"]),
                    module=str(raw["module"]),
                    args_json=_preserve_redacted_args(
                        dict(raw.get("args", {})), existing_args.get(int(raw["ordinal"]))
                    ),
                    enabled=bool(raw.get("enabled", True)),
                    revision=new_revision,
                )
            )
        self._session.flush()
        workspace.auth_provider_revision = new_revision
        self._session.flush()

    def get_auth_provider_revision(self) -> int:
        workspace = self.get_workspace()
        return 0 if workspace is None else workspace.auth_provider_revision

    def lock_auth_providers_for_update(self) -> None:
        """Serialize authentication-provider collection edits, including first creation."""
        self.ensure_workspace()
        record = self._session.scalar(
            select(WorkspaceRecord)
            .where(WorkspaceRecord.id == 1)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if record is None:
            raise LookupError("Workspace is not configured")

    def get_runtime_settings(
        self,
    ) -> dict[str, object] | None:
        record = self._session.get(RuntimeSettingsRecord, 1)
        if record is None:
            return None
        return {
            "ticket_ttl_seconds": record.ticket_ttl_seconds,
            "max_tickets": record.max_tickets,
            "max_ticket_exchanges": record.max_ticket_exchanges,
            "path_rules": [dict(rule) for rule in record.path_rules_json],
            "revision": record.revision,
        }

    def list_catalogs(
        self,
    ) -> list[dict[str, object]]:
        return [
            {
                "id": str(record.id),
                "name": record.name,
                "plugin_id": record.plugin_id,
                "options": dict(record.options_json),
                "revision": record.revision,
            }
            for record in self._session.scalars(select(CatalogRecord).order_by(CatalogRecord.name))
        ]

    def list_workspace_catalogs(
        self,
    ) -> list[dict[str, object]]:
        assets_by_catalog: dict[UUID, int] = {}
        for row in self._session.execute(
            select(AssetRecord.catalog_id, func.count()).group_by(AssetRecord.catalog_id)
        ):
            assets_by_catalog[row[0]] = row[1]
        return [
            {
                "id": str(record.id),
                "name": record.name,
                "plugin_id": record.plugin_id,
                "options": dict(record.options_json),
                "status": "configured",
                "revision": record.revision,
                "discovered_table_count": 0,
                "governed_asset_count": assets_by_catalog.get(record.id, 0),
            }
            for record in self._session.scalars(select(CatalogRecord).order_by(CatalogRecord.name))
        ]

    def get_workspace_catalog(self, name: str) -> dict[str, object]:
        record = self._catalog_by_name(name=name)
        return {
            "id": str(record.id),
            "name": record.name,
            "plugin_id": record.plugin_id,
            "options": dict(record.options_json),
            "revision": record.revision,
        }

    def list_assets(
        self,
    ) -> list[dict[str, object]]:
        catalog_records = list(self._session.scalars(select(CatalogRecord)))
        catalog_by_id = {record.id: record for record in catalog_records}
        assets = []
        for record in self._session.scalars(select(AssetRecord).order_by(AssetRecord.target)):
            catalog = catalog_by_id[record.catalog_id]
            assets.append(
                {
                    "id": str(record.id),
                    "catalog_id": str(record.catalog_id),
                    "catalog": catalog.name,
                    "target": record.target,
                    "backend": record.backend,
                    "table_identifier": record.table_identifier,
                    "options": dict(record.options_json),
                }
            )
        return assets

    def list_workspace_assets(
        self,
    ) -> list[dict[str, object]]:
        records = list(
            self._session.scalars(select(AssetRecord).order_by(AssetRecord.target, AssetRecord.id))
        )
        return self._workspace_asset_rows(records)

    def list_workspace_assets_for_principals(
        self,
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
                    or_(
                        AssetOwnerRecord.principal.in_(principals),
                        and_(
                            AssetGrantRecord.principal.in_(principals),
                            AssetGrantRecord.capability == "read",
                        ),
                    )
                )
                .distinct()
                .order_by(AssetRecord.target, AssetRecord.id)
            )
        )
        return self._workspace_asset_rows(records)

    def list_workspace_assets_page(
        self,
        *,
        limit: int,
        cursor: str | None = None,
        search: str | None = None,
        principals: set[str] | None = None,
    ) -> AssetPage:
        if limit <= 0:
            raise ValueError("Asset page limit must be positive")
        normalized_search = search.strip() if search else ""
        after = _decode_asset_cursor(cursor, expected_search=normalized_search) if cursor else None
        query = select(AssetRecord)
        if principals is not None:
            if not principals:
                return AssetPage(items=[], next_cursor=None)
            query = query.outerjoin(AssetOwnerRecord, AssetOwnerRecord.asset_id == AssetRecord.id)
            query = query.outerjoin(AssetGrantRecord, AssetGrantRecord.asset_id == AssetRecord.id)
            query = query.where(
                or_(
                    AssetOwnerRecord.principal.in_(principals),
                    and_(
                        AssetGrantRecord.principal.in_(principals),
                        AssetGrantRecord.capability == "read",
                    ),
                )
            ).distinct()
        if normalized_search:
            escaped_search = _escape_like(normalized_search)
            pattern = f"%{escaped_search}%"
            query = query.where(
                or_(
                    AssetRecord.target.ilike(pattern, escape="\\"),
                    AssetRecord.table_identifier.ilike(pattern, escape="\\"),
                    AssetRecord.backend.ilike(pattern, escape="\\"),
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
            next_cursor = _encode_asset_cursor(records[-1], search=normalized_search)
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
            "revision": record.revision,
            "policy_revision": record.policy_revision,
            "options": dict(record.options_json),
            "schema_fields": self.list_asset_schema_fields(asset_id),
            "policy_rules": self.list_policy_rules(asset_id),
        }

    def lock_asset_for_update(self, asset_id: UUID) -> None:
        """Serializes live asset mutations for the duration of the transaction."""

        record = self._session.scalar(
            select(AssetRecord)
            .where(AssetRecord.id == asset_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if record is None:
            raise LookupError(f"No asset {asset_id}")

    def _locked_asset(self, asset_id: UUID) -> AssetRecord | None:
        return self._session.scalar(
            select(AssetRecord)
            .where(AssetRecord.id == asset_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )

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
        expected_revision: int | None = None,
    ) -> list[dict[str, str]]:
        asset = self._locked_asset(asset_id)
        if asset is None:
            raise LookupError(f"No asset {asset_id}")
        _assert_asset_revision(asset, expected_revision)
        for record in self._session.scalars(
            select(AssetGrantRecord).where(AssetGrantRecord.asset_id == asset_id)
        ):
            self._session.delete(record)
        self._session.flush()
        asset.revision += 1
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
                    id=uuid4(), asset_id=asset_id, principal=principal, capability=capability
                )
            )
            normalized.append({"principal": principal, "capability": capability})
        self._session.flush()
        return normalized

    def list_asset_schema_fields(self, asset_id: UUID) -> list[dict[str, object]]:
        return [
            {
                "name": record.name,
                "field_id": record.field_id,
                "path": list(record.path_json),
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
                "name": record.name,
                "description": record.description,
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

    def list_auth_providers(
        self,
    ) -> list[dict[str, object]]:
        return [
            {
                "id": str(record.id),
                "ordinal": record.ordinal,
                "module": record.module,
                "args": dict(record.args_json),
                "enabled": record.enabled,
                "revision": record.revision,
            }
            for record in self._session.scalars(
                select(AuthProviderRecord).order_by(AuthProviderRecord.ordinal)
            )
        ]

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
                actor_principal=actor_principal,
                action=action,
                resource_type="asset",
                resource_id=str(asset_id),
                outcome=outcome,
                details_json=dict(details or {}),
                correlation_id=correlation_id or current_request_id(),
            )
        )
        self._session.flush()

    def record_workspace_audit_event(
        self,
        *,
        actor_principal: str,
        action: str,
        resource_type: str,
        resource_id: str,
        details: dict[str, object] | None = None,
    ) -> None:
        """Records a asset-authorized event for workspace-level operations."""

        self._session.add(
            AuditEventRecord(
                id=uuid4(),
                actor_principal=actor_principal,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
                outcome="success",
                details_json=dict(details or {}),
                correlation_id=current_request_id(),
            )
        )
        self._session.flush()

    def list_audit_events_page(  # noqa: C901
        self,
        *,
        asset_id: UUID | None = None,
        principals: set[str] | None = None,
        actor: str | None = None,
        action: str | None = None,
        resource_type: str | None = None,
        outcome: str | None = None,
        correlation_id: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> AuditEventPage:
        """Returns a bounded, asset-authorized audit page using stable keyset order."""

        if limit <= 0:
            raise ValueError("Audit page limit must be positive")
        query = select(AuditEventRecord)
        if asset_id is not None:
            query = query.where(
                AuditEventRecord.resource_type == "asset",
                AuditEventRecord.resource_id == str(asset_id),
            )
        if actor:
            query = query.where(AuditEventRecord.actor_principal == actor)
        if action:
            query = query.where(AuditEventRecord.action == action)
        if resource_type:
            query = query.where(AuditEventRecord.resource_type == resource_type)
        if outcome:
            query = query.where(AuditEventRecord.outcome == outcome)
        if correlation_id:
            query = query.where(AuditEventRecord.correlation_id == correlation_id)
        if created_after is not None:
            query = query.where(AuditEventRecord.created_at >= created_after)
        if created_before is not None:
            query = query.where(AuditEventRecord.created_at <= created_before)
        if principals is not None:
            if not principals:
                return AuditEventPage(items=[], next_cursor=None)
            visible_asset = exists(
                select(1).where(
                    func.replace(sql_cast(AssetRecord.id, String), "-", "")
                    == func.replace(AuditEventRecord.resource_id, "-", ""),
                    or_(
                        exists(
                            select(1).where(
                                AssetOwnerRecord.asset_id == AssetRecord.id,
                                AssetOwnerRecord.principal.in_(principals),
                            )
                        ),
                        exists(
                            select(1).where(
                                AssetGrantRecord.asset_id == AssetRecord.id,
                                AssetGrantRecord.principal.in_(principals),
                                AssetGrantRecord.capability == "read",
                            )
                        ),
                    ),
                )
            )
            query = query.where(AuditEventRecord.resource_type == "asset", visible_asset)
        if cursor:
            created_at, event_id = _decode_audit_cursor(cursor)
            query = query.where(
                or_(
                    AuditEventRecord.created_at < created_at,
                    and_(AuditEventRecord.created_at == created_at, AuditEventRecord.id < event_id),
                )
            )
        records = list(
            self._session.scalars(
                query.order_by(
                    AuditEventRecord.created_at.desc(), AuditEventRecord.id.desc()
                ).limit(limit + 1)
            )
        )
        next_cursor = None
        if len(records) > limit:
            records = records[:limit]
            last = records[-1]
            next_cursor = _encode_audit_cursor(last.created_at, last.id)
        return AuditEventPage(
            items=[
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
            ],
            next_cursor=next_cursor,
        )

    def get_workspace_summary(
        self,
    ) -> dict[str, object]:
        if self.get_workspace() is None:
            return _empty_workspace_summary()
        catalogs = self.list_workspace_catalogs()
        assets = self.list_workspace_assets()
        missing_policy_count = sum(1 for asset in assets if asset["policy_status"] == "missing")
        enabled_auth_provider_count = sum(
            1 for provider in self.list_auth_providers() if provider["enabled"]
        )
        return {
            "catalog_count": len(catalogs),
            "asset_count": len(assets),
            "unowned_asset_count": sum(1 for asset in assets if asset["owner_count"] == 0),
            "missing_policy_count": missing_policy_count,
            "runtime_configured": self.get_runtime_settings() is not None,
            "enabled_auth_provider_count": enabled_auth_provider_count,
        }

    def _catalog_by_name(self, *, name: str) -> CatalogRecord:
        catalog = self._session.scalar(select(CatalogRecord).where(CatalogRecord.name == name))
        if catalog is None:
            raise LookupError(f"No catalog {name!r}")
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
                    "policy_revision": record.policy_revision,
                }
            )
        return rows


def _assert_asset_revision(asset: AssetRecord, expected_revision: int | None) -> None:
    """Rejects stale metadata/binding writes after the asset row is locked."""

    if expected_revision is None:
        raise RevisionPreconditionRequired(
            "Asset revision is required for updates "
            f"(current {asset.revision}); reread before writing."
        )
    if asset.revision != expected_revision:
        raise ConfigurationConflictError(
            "Asset revision changed "
            f"(expected {expected_revision}, current {asset.revision}); reread before writing."
        )


def _assert_new_asset_revision(expected_revision: int | None) -> None:
    """Treat creation as a compare-and-set against the implicit revision zero."""

    if expected_revision is not None and expected_revision != 0:
        raise ConfigurationConflictError(
            "Asset revision changed "
            f"(expected {expected_revision}, current 0); reread before writing."
        )


def _encode_asset_cursor(record: AssetRecord, *, search: str = "") -> str:
    raw = json.dumps(
        {"target": record.target, "id": str(record.id), "search": search}, separators=(",", ":")
    )
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def _decode_asset_cursor(value: str, *, expected_search: str = "") -> tuple[str, UUID]:
    try:
        padded = value + "=" * (-len(value) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        data = json.loads(raw)
        if str(data.get("search", "")) != expected_search:
            raise ValueError("Asset cursor does not match the requested search")
        return str(data["target"]), UUID(str(data["id"]))
    except Exception as exc:
        raise ValueError("Invalid asset cursor") from exc


def _encode_audit_cursor(created_at: datetime, event_id: UUID) -> str:
    raw = json.dumps(
        {"created_at": _isoformat(created_at), "id": str(event_id)}, separators=(",", ":")
    )
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def _decode_audit_cursor(value: str) -> tuple[datetime, UUID]:
    try:
        padded = value + "=" * (-len(value) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        data = json.loads(raw)
        return (
            datetime.fromisoformat(str(data["created_at"]).replace("Z", "+00:00")),
            UUID(str(data["id"])),
        )
    except Exception as exc:
        raise ValueError("Invalid audit cursor") from exc


def _escape_like(value: str) -> str:
    """Escapes SQL LIKE metacharacters so search remains literal and bounded."""

    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _empty_workspace_summary() -> dict[str, object]:
    return {
        "catalog_count": 0,
        "asset_count": 0,
        "unowned_asset_count": 0,
        "missing_policy_count": 0,
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


def _preserve_redacted_args(
    incoming: Mapping[str, object], existing: Mapping[str, object] | None
) -> dict[str, object]:
    """Keep server-held secret values when a redacted GET is round-tripped."""

    if not existing:
        return dict(incoming)
    result: dict[str, object] = {}
    for key, value in incoming.items():
        old = existing.get(key)
        if value == "[redacted]" and old is not None:
            result[key] = old
        elif isinstance(value, Mapping) and isinstance(old, Mapping):
            result[key] = _preserve_redacted_args(
                cast(Mapping[str, object], value), cast(Mapping[str, object], old)
            )
        elif isinstance(value, list) and isinstance(old, list):
            result[key] = [
                _preserve_redacted_args(
                    cast(Mapping[str, object], item), cast(Mapping[str, object], old[index])
                )
                if isinstance(item, Mapping)
                and index < len(old)
                and isinstance(old[index], Mapping)
                else item
                for index, item in enumerate(value)
            ]
        else:
            result[key] = value
    return result


def _normalize_schema_fields(fields: list[dict[str, Any]]) -> list[dict[str, object]]:
    """Normalize schema identities without coercing unsafe caller values.

    These values are persisted as asset schema admission metadata and later used
    for schema-drift checks. Accept only bounded printable strings so direct
    service callers cannot bypass the HTTP model's limits.
    """

    normalized: list[dict[str, object]] = []
    seen_paths: set[tuple[str, ...]] = set()
    seen_ids: set[str] = set()
    for field in fields:
        raw_name = field.get("name")
        if not isinstance(raw_name, str):
            raise ValueError("Schema field name must be text")
        name = raw_name.strip()
        if not name:
            raise ValueError("Schema field name must be non-empty")
        _validate_schema_text(name, "Schema field name", max_length=256)
        raw_path = field.get("path")
        if (
            not isinstance(raw_path, list)
            or not raw_path
            or any(
                not isinstance(segment, str)
                or not segment.strip()
                or len(segment.strip()) > 256
                or any(ord(char) < 0x20 or ord(char) == 0x7F for char in segment)
                for segment in raw_path
            )
        ):
            raise ValueError("Schema field path must contain bounded printable text segments")
        path = [segment.strip() for segment in raw_path]
        path_key = tuple(path)
        if path_key in seen_paths:
            raise ValueError("Schema field paths must be unique")
        raw_field_id = field.get("field_id")
        if not isinstance(raw_field_id, str):
            raise ValueError("Schema field id must be text")
        field_id = raw_field_id.strip()
        _validate_schema_text(field_id, "Schema field id", max_length=128)
        if field_id.startswith("legacy:"):
            raise ValueError("Legacy schema field ids are unsupported")
        if not field_id.startswith("synthetic:"):
            field_id = canonical_provider_field_id(field_id)
        if not field_id:
            raise ValueError("Schema field id must be non-empty")
        if field_id in seen_ids:
            raise ValueError("Schema field ids must be unique")
        normalized.append(
            {
                "name": name,
                "field_id": field_id,
                "path": path,
                "type": str(field.get("type", "string")).strip() or "string",
                "nullable": bool(field.get("nullable", True)),
            }
        )
        seen_paths.add(path_key)
        seen_ids.add(field_id)
    return normalized


def _validate_schema_text(value: str, label: str, *, max_length: int) -> None:
    if not value or len(value) > max_length:
        raise ValueError(f"{label} must contain 1-{max_length} characters")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
        raise ValueError(f"{label} must contain printable text")


def _normalize_policy_rule(raw: dict[str, Any]) -> dict[str, Any]:
    effect = _normalize_policy_rule_effect(str(raw.get("effect", "allow")))
    return {**raw, "effect": effect}


def _normalize_policy_rule_effect(effect: str) -> Literal["allow", "allow_all"]:
    if effect not in {"allow", "allow_all"}:
        raise ValueError("Policy rules are explicit grants")
    return cast(Literal["allow", "allow_all"], effect)
