from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from hashlib import sha256
from threading import RLock
from typing import Any, Protocol, cast
from uuid import NAMESPACE_OID, UUID, uuid5

import pyarrow as pa
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.common.access_control.compiled_policy import CompiledPolicy
from dal_obscura.common.access_control.models import (
    AccessDecision,
    Policy,
    Principal,
)
from dal_obscura.common.access_control.policy_resolution import resolve_access
from dal_obscura.common.catalog.ports import TableFormat
from dal_obscura.common.config_store.orm import (
    AssetRecord,
    AssetSchemaFieldRecord,
    AuthProviderRecord,
    CatalogRecord,
    CellRecord,
    CellRuntimeSettingsRecord,
    CellTenantRecord,
    PolicyRuleRecord,
    TenantRecord,
)
from dal_obscura.common.plugin_api import PluginRegistry
from dal_obscura.common.query_planning.field_paths import parse_field_path, resolve_schema_path
from dal_obscura.common.schema_identity import (
    schema_field_id,
    schema_has_stable_ids,
    schema_scope_digest,
)
from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    CatalogRegistry,
    CatalogType,
    ServiceConfig,
)
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer
from dal_obscura.data_plane.infrastructure.adapters.secret_providers import (
    SecretProvider,
    resolve_secret_refs,
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

    config_revision: UUID
    auth_chain: dict[str, Any]
    ticket: dict[str, Any]
    path_rules: list[dict[str, Any]] = dataclass_field(default_factory=list)


@dataclass(frozen=True)
class LiveAsset:
    """Live asset policy and backend configuration for one target.

    Example:
        ```python
        asset = store.get_asset(tenant_id="default", catalog="analytics", target="orders")
        ```
    """

    config_revision: UUID
    tenant_id: UUID
    catalog: str
    target: str
    backend: str
    compiled_config: dict[str, Any]
    policy_version: int
    asset_id: UUID | None = None


@dataclass(frozen=True)
class LiveCatalog:
    """Live catalog configuration for one tenant.

    Example:
        ```python
        catalogs = store.get_catalogs(tenant_id="default")
        ```
    """

    config_revision: UUID
    tenant_id: UUID
    catalog: str
    config: dict[str, Any]
    plugin_id: str | None = None
    plugin_revision: int | None = None


class AdmittedPluginSnapshot(Protocol):
    """Minimal registry surface required by live-config resolution."""

    def admitted(self) -> Mapping[tuple[str, str], object]: ...


class LiveConfigStore:
    """Reads live configuration for one data-plane cell."""

    def __init__(
        self,
        session: Session | sessionmaker[Session],
        *,
        cell_id: UUID,
    ) -> None:
        if isinstance(session, Session):
            self._session: Session | None = session
            self._session_maker: sessionmaker[Session] | None = None
        else:
            self._session = None
            self._session_maker = session
        self._cell_id = cell_id
        self._lock = RLock()
        self._asset_cache: dict[tuple[UUID, UUID, str, str], LiveAsset] = {}
        self._catalog_cache: dict[tuple[UUID, UUID], list[LiveCatalog]] = {}
        self._runtime_cache: dict[UUID, LiveRuntime] = {}
        self._tenant_cache: dict[str, UUID] = {}
        self._cached_config_revision: UUID | None = None

    def configuration_revision(self) -> UUID:
        """Returns the current live-configuration generation identifier."""
        with self._session_scope() as session:
            return self._compute_configuration_revision(session)

    def get_asset(self, *, tenant_id: str, catalog: str, target: str) -> LiveAsset:
        with self._lock:
            tenant_uuid = self._tenant_uuid(tenant_id)
            with self._session_scope() as session:
                config_revision = self._compute_configuration_revision(session)
                self._observe_generation(config_revision)
                cache_key = (config_revision, tenant_uuid, catalog, target)
                cached = self._asset_cache.get(cache_key)
                if cached is not None:
                    return cached
                asset = self._live_asset(
                    session,
                    config_revision=config_revision,
                    tenant_id=tenant_uuid,
                    catalog=catalog,
                    target=target,
                )
            self._asset_cache[cache_key] = asset
            return asset

    def get_catalogs(self, *, tenant_id: str) -> list[LiveCatalog]:
        with self._lock:
            tenant_uuid = self._tenant_uuid(tenant_id)
            with self._session_scope() as session:
                config_revision = self._compute_configuration_revision(session)
                self._observe_generation(config_revision)
                cache_key = (config_revision, tenant_uuid)
                cached = self._catalog_cache.get(cache_key)
                if cached is not None:
                    return cached
                catalogs = self._live_catalogs(
                    session,
                    config_revision=config_revision,
                    tenant_id=tenant_uuid,
                )
            self._catalog_cache[cache_key] = catalogs
            return catalogs

    def get_asset_and_catalog(
        self,
        *,
        tenant_id: str,
        catalog: str,
        target: str,
    ) -> tuple[LiveAsset, LiveCatalog]:
        """Loads a target and its catalog from one live database view."""
        with self._lock:
            tenant_uuid = self._tenant_uuid(tenant_id)
            with self._session_scope() as session:
                config_revision = self._compute_configuration_revision(session)
                self._observe_generation(config_revision)
                asset_key = (config_revision, tenant_uuid, catalog, target)
                asset = self._asset_cache.get(asset_key)
                if asset is None:
                    asset = self._live_asset(
                        session,
                        config_revision=config_revision,
                        tenant_id=tenant_uuid,
                        catalog=catalog,
                        target=target,
                    )
                catalog_key = (config_revision, tenant_uuid)
                catalogs = self._catalog_cache.get(catalog_key)
                if catalogs is None:
                    catalogs = self._live_catalogs(
                        session,
                        config_revision=config_revision,
                        tenant_id=tenant_uuid,
                    )
            self._asset_cache[asset_key] = asset
            self._catalog_cache[catalog_key] = catalogs
            live_catalog = next((item for item in catalogs if item.catalog == catalog), None)
            if live_catalog is None:
                raise LookupError(f"No live catalog for {catalog!r}")
            return asset, live_catalog

    def _live_asset(
        self,
        session: Session,
        *,
        config_revision: UUID,
        tenant_id: UUID,
        catalog: str,
        target: str,
    ) -> LiveAsset:
        row = session.execute(
            select(AssetRecord, CatalogRecord)
            .join(CatalogRecord, CatalogRecord.id == AssetRecord.catalog_id)
            .join(TenantRecord, TenantRecord.id == AssetRecord.tenant_id)
            .join(CellRecord, CellRecord.id == AssetRecord.cell_id)
            .join(
                CellTenantRecord,
                (CellTenantRecord.cell_id == AssetRecord.cell_id)
                & (CellTenantRecord.tenant_id == AssetRecord.tenant_id),
            )
            .where(
                AssetRecord.cell_id == self._cell_id,
                AssetRecord.tenant_id == tenant_id,
                CatalogRecord.name == catalog,
                AssetRecord.target == target,
                TenantRecord.status == "active",
                CellRecord.status == "active",
            )
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
                "type": "iceberg" if catalog_plugin_id == "iceberg.sql" else "plugin",
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
                schema_fields,
                sort_keys=True,
                default=str,
                separators=(",", ":"),
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
            config_revision=config_revision,
            tenant_id=record.tenant_id,
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
        *,
        config_revision: UUID,
        tenant_id: UUID,
    ) -> list[LiveCatalog]:
        records = session.scalars(
            select(CatalogRecord)
            .join(TenantRecord, TenantRecord.id == CatalogRecord.tenant_id)
            .join(CellRecord, CellRecord.id == CatalogRecord.cell_id)
            .join(
                CellTenantRecord,
                (CellTenantRecord.cell_id == CatalogRecord.cell_id)
                & (CellTenantRecord.tenant_id == CatalogRecord.tenant_id),
            )
            .where(
                CatalogRecord.cell_id == self._cell_id,
                CatalogRecord.tenant_id == tenant_id,
                TenantRecord.status == "active",
                CellRecord.status == "active",
            )
            .order_by(CatalogRecord.name)
        )
        return [
            LiveCatalog(
                config_revision=config_revision,
                tenant_id=record.tenant_id,
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

    def _tenant_uuid(self, tenant_id: str) -> UUID:
        cached = self._tenant_cache.get(tenant_id)
        if cached is not None:
            return cached
        try:
            tenant_uuid = UUID(tenant_id)
        except ValueError:
            with self._session_scope() as session:
                tenant_uuid = self._tenant_uuid_by_slug(session, tenant_id)
        self._tenant_cache[tenant_id] = tenant_uuid
        return tenant_uuid

    def _tenant_uuid_by_slug(self, session: Session, slug: str) -> UUID:
        record = session.scalar(
            select(TenantRecord)
            .join(CellTenantRecord, CellTenantRecord.tenant_id == TenantRecord.id)
            .where(
                TenantRecord.slug == slug,
                TenantRecord.status == "active",
                CellTenantRecord.cell_id == self._cell_id,
            )
        )
        if record is None:
            raise LookupError(f"No tenant with slug {slug!r}")
        return record.id

    def get_runtime(self) -> LiveRuntime:
        with self._lock:
            with self._session_scope() as session:
                config_revision = self._compute_configuration_revision(session)
                self._observe_generation(config_revision)
                cached = self._runtime_cache.get(config_revision)
                if cached is not None:
                    return cached
                record = session.get(CellRuntimeSettingsRecord, self._cell_id)
                if record is None:
                    raise LookupError(f"No live runtime settings for cell {self._cell_id}")
                providers = session.scalars(
                    select(AuthProviderRecord)
                    .where(AuthProviderRecord.cell_id == self._cell_id)
                    .order_by(AuthProviderRecord.ordinal)
                )
                runtime = LiveRuntime(
                    config_revision=config_revision,
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
            self._runtime_cache[config_revision] = runtime
            return runtime

    def _observe_generation(self, config_revision: UUID) -> None:
        if config_revision == self._cached_config_revision:
            return
        self._asset_cache.clear()
        self._catalog_cache.clear()
        self._runtime_cache.clear()
        self._tenant_cache.clear()
        self._cached_config_revision = config_revision

    def _compute_configuration_revision(self, session: Session) -> UUID:
        cell = session.get(CellRecord, self._cell_id)
        if cell is None or cell.status != "active":
            raise LookupError(f"No active cell {self._cell_id}")
        return uuid5(
            NAMESPACE_OID,
            f"{cell.id}:{cell.configuration_revision}:{cell.status}",
        )

    @contextmanager
    def _session_scope(self):
        if self._session is not None:
            yield self._session
            return
        if self._session_maker is None:
            raise RuntimeError("LiveConfigStore is missing a session factory")
        with self._session_maker() as session:
            yield session


class LiveConfigAuthorizer:
    """Authorization adapter backed by live asset policy JSON."""

    def __init__(self, store: LiveConfigStore) -> None:
        self._store = store

    def authorize(
        self,
        principal: Principal,
        target: str,
        catalog: str | None,
        requested_columns: Iterable[str],
    ) -> AccessDecision:
        if catalog is None:
            raise ValueError("Catalog name is required to authorize live assets")
        tenant_id = _tenant_id(principal)
        asset = self._store.get_asset(tenant_id=tenant_id, catalog=catalog, target=target)
        policy = _policy_from_asset(asset)
        allowed_columns, masks, row_filter = resolve_access(
            policy,
            principal,
            target,
            catalog,
            requested_columns,
        )
        return AccessDecision(
            allowed_columns=allowed_columns,
            masks=masks,
            row_filter=row_filter,
            policy_version=_effective_policy_version(asset),
            asset_id=None if asset.asset_id is None else str(asset.asset_id),
        )

    def current_policy_version(
        self,
        target: str,
        catalog: str | None,
        *,
        tenant_id: str,
    ) -> int | None:
        if catalog is None:
            return None
        try:
            return _effective_policy_version(
                self._store.get_asset(
                    tenant_id=tenant_id,
                    catalog=catalog,
                    target=target,
                )
            )
        except LookupError:
            return None


class LiveConfigCatalogRegistry:
    """Catalog registry that resolves tables from active live asset config."""

    def __init__(
        self,
        store: LiveConfigStore,
        *,
        secret_provider: SecretProvider | None = None,
        plugin_registry: PluginRegistry | AdmittedPluginSnapshot | None = None,
        path_enforcer: PathRuleEnforcer | None = None,
    ) -> None:
        self._store = store
        self._secret_provider = secret_provider
        self._plugin_registry = plugin_registry
        self._path_enforcer = path_enforcer
        self._registry_cache: dict[tuple[UUID, UUID, str, str], CatalogRegistry] = {}

    def close(self) -> None:
        """Close cached catalog generations and release provider sessions."""

        registries = tuple(self._registry_cache.values())
        self._registry_cache.clear()
        first_error: Exception | None = None
        for registry in registries:
            try:
                registry.close()
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    def describe(self, catalog: str | None, target: str, *, tenant_id: str) -> TableFormat:
        if catalog is None:
            raise ValueError("Catalog name is required to resolve a target")
        asset, live_catalog = self._store.get_asset_and_catalog(
            tenant_id=tenant_id,
            catalog=catalog,
            target=target,
        )
        cache_key = (asset.config_revision, asset.tenant_id, asset.catalog, asset.target)
        registry = self._registry_cache.get(cache_key)
        if registry is not None:
            table_format = registry.describe(
                catalog,
                _asset_table_identifier(asset),
                tenant_id=tenant_id,
            )
            _validate_schema_admission(asset, table_format.get_schema())
            return table_format
        catalog_config = _catalog_config_for_asset(
            live_catalog,
            asset,
            plugin_registry=self._plugin_registry,
            path_enforcer=self._path_enforcer,
        )
        if self._secret_provider is not None:
            catalog_config = CatalogConfig(
                name=catalog_config.name,
                type=catalog_config.type,
                options=cast(
                    dict[str, Any],
                    resolve_secret_refs(
                        catalog_config.options,
                        provider=self._secret_provider,
                        expected_scope=f"catalog:{catalog_config.name}",
                    ),
                ),
                path_enforcer=catalog_config.path_enforcer,
                plugin_id=catalog_config.plugin_id,
                revision=catalog_config.revision,
            )
        registry = CatalogRegistry(
            ServiceConfig(catalogs={catalog: catalog_config}),
            plugin_registry=self._plugin_registry
            if isinstance(self._plugin_registry, PluginRegistry)
            else None,
        )
        self._registry_cache[cache_key] = registry
        table_format = registry.describe(
            catalog,
            _asset_table_identifier(asset),
            tenant_id=tenant_id,
        )
        _validate_schema_admission(asset, table_format.get_schema())
        return table_format


def _effective_policy_version(asset: LiveAsset) -> int:
    """Binds ticket authorization to the live config and policy revisions."""
    digest = sha256(f"{asset.config_revision}:{asset.policy_version}".encode()).digest()
    return int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)


def _policy_from_asset(asset: LiveAsset) -> Policy:
    return CompiledPolicy.from_json(
        _mapping(asset.compiled_config.get("policy")),
        version=asset.policy_version,
        catalog=asset.catalog,
        target=asset.target,
    ).to_policy()


def _catalog_config_from_live_catalog(
    catalog: LiveCatalog,
    *,
    plugin_id: str,
    path_enforcer: PathRuleEnforcer | None = None,
) -> CatalogConfig:
    config = _mapping(catalog.config)
    if "module" in config:
        raise ValueError("Catalog config uses a retired module identity")
    options = dict(_mapping(config.get("options")))
    if "provider_modules" in options:
        raise ValueError("Catalog config uses a retired provider_modules option")
    raw_revision = config.get("revision")
    revision = (
        raw_revision
        if isinstance(raw_revision, int)
        and not isinstance(raw_revision, bool)
        and raw_revision >= 0
        else catalog.plugin_revision
        if catalog.plugin_revision is not None
        else 0
    )
    return CatalogConfig(
        name=catalog.catalog,
        type=_catalog_type(config),
        options=options,
        path_enforcer=path_enforcer,
        plugin_id=plugin_id,
        revision=revision,
    )


def _catalog_config_for_asset(
    catalog: LiveCatalog,
    asset: LiveAsset,
    *,
    plugin_registry: PluginRegistry | AdmittedPluginSnapshot | None = None,
    path_enforcer: PathRuleEnforcer | None = None,
) -> CatalogConfig:
    """Build the runtime catalog config with the live asset as its source of truth."""
    _validate_plugin_binding(asset, plugin_registry=plugin_registry)
    raw_plugins = asset.compiled_config.get("plugins")
    catalog_plugin_id = catalog.plugin_id or ""
    if isinstance(raw_plugins, dict) and isinstance(raw_plugins.get("catalog"), str):
        catalog_plugin_id = str(raw_plugins["catalog"])
    config = _catalog_config_from_live_catalog(
        catalog,
        plugin_id=catalog_plugin_id,
        path_enforcer=path_enforcer,
    )
    target = _mapping(asset.compiled_config.get("target"))
    backend = str(target.get("backend") or asset.backend).lower()
    if config.plugin_id == "iceberg.sql":
        if backend != "iceberg":
            raise ValueError("Configured Iceberg catalogs require Iceberg assets")
        return config
    if plugin_registry is None:
        raise ValueError("Configured external catalog plugins require an admitted registry")
    return config


def _validate_plugin_binding(
    asset: LiveAsset,
    *,
    plugin_registry: PluginRegistry | AdmittedPluginSnapshot | None = None,
) -> None:
    """Rejects plugin identities the configured runtime cannot honor."""

    raw_plugins = asset.compiled_config.get("plugins")
    if not isinstance(raw_plugins, dict):
        raise ValueError("Live plugin binding is missing")
    catalog_plugin = raw_plugins.get("catalog")
    format_plugin = raw_plugins.get("table_format")
    if not isinstance(catalog_plugin, str) or not isinstance(format_plugin, str):
        raise ValueError("Live plugin binding is unsupported")
    if catalog_plugin == "iceberg.sql" and format_plugin != "iceberg":
        raise ValueError("Live plugin binding is unsupported")
    if catalog_plugin != "iceberg.sql" and plugin_registry is None:
        raise ValueError("Live plugin binding is unsupported without an admitted registry")
    if plugin_registry is not None:
        admitted = plugin_registry.admitted()
        if ("catalog", catalog_plugin) not in admitted or (
            "table_format",
            format_plugin,
        ) not in admitted:
            raise ValueError("Live plugin binding is not admitted")


def _asset_table_identifier(asset: LiveAsset) -> str:
    target = _mapping(asset.compiled_config.get("target"))
    table = target.get("table")
    if not isinstance(table, str) or not table.strip():
        raise ValueError(f"Live asset {asset.catalog}/{asset.target} has no table identifier")
    return table


def _validate_schema_admission(asset: LiveAsset, schema: pa.Schema) -> None:
    """Rejects a live asset when an admitted field identity has drifted."""

    admission = _mapping(asset.compiled_config.get("schema"))
    fields = admission.get("fields")
    if not isinstance(fields, list) or not fields:
        _reject_unbound_broad_policy(asset, schema)
        return
    stable_ids = admission.get("stable_ids")
    if stable_ids is True and not schema_has_stable_ids(schema):
        raise ValueError("Schema admission requires stable provider field IDs.")
    digest = admission.get("digest")
    if digest is not None:
        if not isinstance(digest, str):
            raise ValueError("Stored schema admission digest is invalid")
        encoded = json.dumps(
            fields,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        ).encode("utf-8")
        if sha256(encoded).hexdigest() != digest:
            raise ValueError("Stored schema admission digest does not match its fields")
    identities = _schema_identities(schema)
    for raw in fields:
        if not isinstance(raw, dict):
            raise ValueError("Stored schema admission is invalid")
        path = raw.get("path")
        field_id = raw.get("field_id")
        if (
            not isinstance(path, list)
            or not path
            or any(not isinstance(segment, str) for segment in path)
            or not isinstance(field_id, str)
        ):
            raise ValueError("Stored schema admission is invalid")
        identity = (tuple(path), field_id)
        actual = identities.get(identity)
        if actual is None:
            raise ValueError("Stored schema admission no longer matches the live table.")
        expected_type = raw.get("type")
        if (
            isinstance(expected_type, str)
            and expected_type
            and _canonical_type_name(expected_type) != _canonical_type_name(actual)
        ):
            raise ValueError(
                "Live schema field type no longer matches its admission. "
                f"Expected {expected_type!r}, live value is {actual!r}."
            )


def _reject_unbound_broad_policy(asset: LiveAsset, schema: pa.Schema) -> None:
    """Prevent legacy wildcard/parent grants from expanding on schema drift."""

    policy = asset.compiled_config.get("policy")
    if not isinstance(policy, dict):
        return
    rules = policy.get("rules")
    if not isinstance(rules, list):
        return
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        selectors: list[object] = []
        columns = rule.get("columns")
        if isinstance(columns, list):
            selectors.extend(columns)
        masks = rule.get("masks")
        if isinstance(masks, dict):
            selectors.extend(masks)
        for selector in selectors:
            if not isinstance(selector, str) or selector == "*":
                raise ValueError("Broad live policy requires schema admission.")
            try:
                field = resolve_schema_path(schema, parse_field_path(selector))
            except ValueError:
                continue
            if pa.types.is_nested(field.type):
                raise ValueError("Parent-field live policy requires schema admission.")


def _schema_identities(schema: pa.Schema) -> dict[tuple[tuple[str, ...], str], str]:
    result: dict[tuple[tuple[str, ...], str], str] = {}
    seen_ids: set[str] = set()
    scope_digest = schema_scope_digest(schema)

    def visit(field: pa.Field, path: tuple[str, ...]) -> None:
        field_id, _stable = schema_field_id(field, path, scope_digest=scope_digest)
        if field_id in seen_ids:
            raise ValueError("Live schema contains a duplicate field identity")
        seen_ids.add(field_id)
        result[(path, field_id)] = str(field.type)
        if pa.types.is_struct(field.type):
            for child in field.type:
                visit(child, (*path, child.name))
        elif pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
            visit(field.type.value_field, (*path, "$element"))
        elif pa.types.is_map(field.type):
            visit(field.type.key_field, (*path, "$key"))
            visit(field.type.item_field, (*path, "$value"))
        elif pa.types.is_fixed_size_list(field.type):
            visit(field.type.value_field, (*path, "$element"))

    for field in schema:
        visit(field, (field.name,))
    return result


def _canonical_type_name(value: str) -> str:
    aliases = {
        "large_string": "string",
        "large_binary": "binary",
        "long": "int64",
        "integer": "int32",
        "float": "float32",
        "double": "double",
        "boolean": "bool",
    }
    normalized = value.strip().lower()
    return re.sub(
        r"\b(?:large_string|large_binary|long|integer|float|double|boolean)\b",
        lambda match: aliases[match.group(0)],
        normalized,
    )


def _tenant_id(principal: Principal) -> str:
    return str(
        principal.attributes.get("tenant_id") or principal.attributes.get("tenant") or "default"
    )


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return cast(dict[str, Any], value).copy()
    return {}


def _catalog_type(config: dict[str, Any]) -> CatalogType:
    raw_type = config.get("type")
    if raw_type is None:
        raise ValueError("Live catalog config type is missing")
    return _known_catalog_type(str(raw_type))


def _known_catalog_type(value: str) -> CatalogType:
    if value in {"iceberg", "plugin"}:
        return value
    raise ValueError(f"Unsupported catalog type: {value}")
