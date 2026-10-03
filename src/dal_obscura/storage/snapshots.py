from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from hashlib import sha256
from typing import Any, Protocol, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.storage.database.orm import (
    AssetRecord,
    AssetSchemaFieldRecord,
    AuthProviderRecord,
    CatalogRecord,
    PolicyRuleRecord,
    RuntimeSettingsRecord,
)


@dataclass(frozen=True)
class LiveRuntime:
    """Active runtime settings compiled from the control plane.

    Example:
        ```python
        runtime = store.get_runtime()
        ttl = runtime.ticket["ttl_seconds"]
        ```
    """

    auth_chain: dict[str, Any]
    ticket: dict[str, Any]
    path_rules: list[dict[str, Any]] = dataclass_field(default_factory=list)


@dataclass(frozen=True)
class LiveAsset:
    """Live asset policy and backend configuration for one target.

    Example:
        ```python
        ```
    """

    catalog: str
    target: str
    backend: str
    compiled_config: dict[str, Any]
    policy_version: int
    asset_id: UUID | None = None


@dataclass(frozen=True)
class LiveCatalog:
    """Live catalog configuration for the deployment.

    Example:
        ```python
        catalogs = store.get_catalogs()
        ```
    """

    catalog: str
    config: dict[str, Any]
    plugin_id: str | None = None
    plugin_revision: int | None = None


class AdmittedPluginSnapshot(Protocol):
    """Minimal registry surface required by live-config resolution."""

    def admitted(self) -> Mapping[tuple[str, str], object]: ...


class LiveConfigStore:
    """Reads live configuration for the deployment."""

    def __init__(self, session_maker: sessionmaker[Session]) -> None:
        self._session_maker = session_maker

    def get_asset(self, *, catalog: str, target: str) -> LiveAsset:
        with self._session_scope() as session:
            return self._live_asset(session, catalog=catalog, target=target)

    def get_catalogs(self) -> list[LiveCatalog]:
        with self._session_scope() as session:
            return self._live_catalogs(session)

    def get_asset_and_catalog(self, *, catalog: str, target: str) -> tuple[LiveAsset, LiveCatalog]:
        """Materialize only the governed target in one repeatable database snapshot.

        The session ends before provider IO, schema discovery, or planning. No
        request holds a database connection while waiting on a data source.
        """
        with self._session_scope() as session:
            asset = self._live_asset(session, catalog=catalog, target=target)
            config = dict(_mapping(asset.compiled_config["catalog"]))
            return asset, LiveCatalog(
                catalog=catalog,
                config=config,
                plugin_id=str(config["plugin_id"]),
                plugin_revision=int(config["revision"]),
            )

    def _live_asset(
        self,
        session: Session,
        *,
        catalog: str,
        target: str,
    ) -> LiveAsset:
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
        catalog_plugin_id = catalog_record.plugin_id
        compiled_config: dict[str, Any] = {
            "catalog": {
                "plugin_id": catalog_plugin_id,
                "options": dict(catalog_record.options_json),
                "revision": catalog_record.revision,
            },
            "target": {
                "backend": record.backend,
                "table": record.table_identifier,
                "options": dict(record.options_json),
            },
            "policy": policy_json,
            "plugins": {"catalog": catalog_plugin_id, "table_format": record.backend},
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
        if schema_fields:
            schema_bytes = json.dumps(
                schema_fields, sort_keys=True, default=str, separators=(",", ":")
            ).encode("utf-8")
            compiled_config["schema"] = {
                "encoding": 1,
                "fields": schema_fields,
                "stable_ids": not any(
                    str(field["field_id"]).startswith("synthetic:") for field in schema_fields
                ),
                "digest": sha256(schema_bytes).hexdigest(),
            }
        return LiveAsset(
            catalog=catalog,
            target=target,
            backend=record.backend,
            compiled_config=compiled_config,
            policy_version=record.policy_revision,
            asset_id=record.id,
        )

    def _live_catalogs(
        self,
        session: Session,
    ) -> list[LiveCatalog]:
        records = session.scalars(select(CatalogRecord).order_by(CatalogRecord.name))
        return [
            LiveCatalog(
                catalog=record.name,
                config={
                    "type": ("iceberg" if record.plugin_id == "iceberg.sql" else "plugin"),
                    "plugin_id": record.plugin_id,
                    "options": dict(record.options_json),
                    "revision": record.revision,
                },
                plugin_id=record.plugin_id,
                plugin_revision=record.revision,
            )
            for record in records
        ]

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
    def _session_scope(self) -> Iterator[Session]:
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


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return cast(dict[str, Any], value).copy()
    return {}
