from __future__ import annotations

import json
from collections.abc import Generator
from contextlib import contextmanager
from hashlib import sha256
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.adapters.sql.database.orm import (
    AssetRecord,
    AssetSchemaFieldRecord,
    AuthProviderRecord,
    CatalogRecord,
    PolicyRuleRecord,
    RuntimeSettingsRecord,
)
from dal_obscura.application.reads.sources import LiveRuntime
from dal_obscura.kernel.plans.snapshot import (
    PublishedAssetSnapshot,
    PublishedCatalog,
    PublishedSchema,
    PublishedSource,
)
from dal_obscura.kernel.policy.models import AssetPolicy


class LiveConfigStore:
    """Reads live configuration for the deployment."""

    def __init__(self, session_maker: sessionmaker[Session]) -> None:
        self._session_maker = session_maker

    def get_asset_and_catalog(self, catalog: str, target: str) -> PublishedAssetSnapshot:
        """Capture one governed target, closing its transaction before provider IO."""
        with self._session_scope() as session:
            return self._live_asset(session, catalog=catalog, target=target)

    def _live_asset(
        self,
        session: Session,
        *,
        catalog: str,
        target: str,
    ) -> PublishedAssetSnapshot:
        row = session.execute(
            select(AssetRecord, CatalogRecord)
            .join(CatalogRecord, CatalogRecord.id == AssetRecord.catalog_id)
            .where(CatalogRecord.name == catalog, AssetRecord.target == target)
        ).one_or_none()
        if row is None:
            raise LookupError(f"No live asset for {catalog}/{target}")
        record, catalog_record = row
        policy_rules = [
            {
                "ordinal": rule.ordinal,
                "principals": list(rule.principals_json),
                "columns": list(rule.columns_json),
                "effect": rule.effect,
                "name": rule.name,
                "description": rule.description,
                "when": dict(rule.when_json),
                "masks": dict(rule.masks_json),
                "row_filter": rule.row_filter_sql,
            }
            for rule in session.scalars(
                select(PolicyRuleRecord)
                .where(PolicyRuleRecord.asset_id == record.id)
                .order_by(PolicyRuleRecord.ordinal)
            )
        ]
        policy_json: dict[str, Any] = {
            "version": record.policy_revision,
            "catalog": catalog,
            "target": target,
            "rules": policy_rules,
        }
        schema_fields = [
            {
                "name": field.name,
                "field_id": field.field_id,
                "path": list(field.path_json),
                "type": field.type,
                "nullable": field.nullable,
            }
            for field in session.scalars(
                select(AssetSchemaFieldRecord)
                .where(AssetSchemaFieldRecord.asset_id == record.id)
                .order_by(AssetSchemaFieldRecord.ordinal)
            )
        ]
        admitted_schema = None
        if schema_fields:
            schema_bytes = json.dumps(schema_fields, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
            admitted_schema = PublishedSchema(
                encoding=1,
                fields=tuple(schema_fields),
                stable_ids=not any(
                    str(field["field_id"]).startswith("synthetic:") for field in schema_fields
                ),
                digest=sha256(schema_bytes).hexdigest(),
            )
        return PublishedAssetSnapshot(
            catalog=PublishedCatalog(
                name=catalog,
                plugin_id=catalog_record.plugin_id,
                revision=catalog_record.revision,
                options=catalog_record.options_json,
            ),
            target=target,
            source=PublishedSource(
                backend=record.backend,
                table=record.table_identifier,
                options=record.options_json,
            ),
            policy=AssetPolicy.from_json(policy_json),
            admitted_schema=admitted_schema,
            asset_id=record.id,
        )

    def get_runtime(self) -> LiveRuntime:
        with self._session_scope() as session:
            record = session.get(RuntimeSettingsRecord, 1)
            if record is None:
                raise LookupError("No live runtime settings")
            providers = session.scalars(
                select(AuthProviderRecord).order_by(AuthProviderRecord.ordinal)
            )
            return LiveRuntime(
                auth_chain={
                    "providers": [
                        {
                            "ordinal": provider.ordinal,
                            "module": provider.module,
                            "args": dict(provider.args_json),
                            "enabled": provider.enabled,
                        }
                        for provider in providers
                    ]
                },
                ticket={
                    "ttl_seconds": record.ticket_ttl_seconds,
                    "max_tickets": record.max_tickets,
                    "max_exchanges": record.max_ticket_exchanges,
                },
                path_rules=[dict(rule) for rule in record.path_rules_json],
            )

    @contextmanager
    def _session_scope(self) -> Generator[Session]:
        with self._session_maker() as session:
            dialect = session.get_bind().dialect.name
            if dialect == "postgresql":
                connection = session.connection(
                    execution_options={"isolation_level": "REPEATABLE READ"}
                )
                connection.exec_driver_sql("SET TRANSACTION READ ONLY")
            elif dialect == "sqlite":
                # sqlite3 legacy transaction mode does not BEGIN for SELECT.
                # Explicit BEGIN is essential: otherwise subsequent SELECTs
                # can observe policies from a later writer commit.
                session.connection().exec_driver_sql("BEGIN")
            else:
                raise ValueError(f"Unsupported configuration snapshot database: {dialect}")
            yield session
