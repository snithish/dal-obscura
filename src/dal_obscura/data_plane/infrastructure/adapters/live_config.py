from __future__ import annotations

import json
import re
from collections import OrderedDict
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from hashlib import sha256
from threading import Condition
from time import monotonic
from typing import Any, Protocol, cast
from uuid import UUID

import pyarrow as pa
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.common.access_control.compiled_policy import CompiledPolicy
from dal_obscura.common.access_control.models import AccessDecision, Policy, Principal
from dal_obscura.common.access_control.policy_resolution import resolve_access
from dal_obscura.common.catalog.ports import TableFormat
from dal_obscura.common.config_store.orm import (
    AssetRecord,
    AssetSchemaFieldRecord,
    AuthProviderRecord,
    CatalogRecord,
    PolicyRuleRecord,
    RuntimeSettingsRecord,
)
from dal_obscura.common.plugin_api import PluginRegistry
from dal_obscura.common.query_planning.field_paths import parse_field_path, resolve_schema_path
from dal_obscura.common.schema_identity import (
    schema_field_id,
    schema_has_stable_ids,
    schema_scope_digest,
)
from dal_obscura.data_plane.application.ports.access_context import (
    AccessContext,
    AccessContextUnavailable,
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
        asset = self._store.get_asset(catalog=catalog, target=target)
        return _authorize_asset(asset, principal, target, catalog, requested_columns)

    def current_policy_version(
        self,
        target: str,
        catalog: str | None,
    ) -> int | None:
        if catalog is None:
            return None
        try:
            return _effective_policy_version(self._store.get_asset(catalog=catalog, target=target))
        except LookupError:
            return None


def _authorize_asset(
    asset: LiveAsset,
    principal: Principal,
    target: str,
    catalog: str | None,
    requested_columns: Iterable[str],
) -> AccessDecision:
    if catalog != asset.catalog or target != asset.target:
        raise ValueError("Authorization context does not match the requested asset")
    policy = _policy_from_asset(asset)
    allowed_columns, masks, row_filter = resolve_access(
        policy, principal, target, catalog, requested_columns
    )
    return AccessDecision(
        allowed_columns=allowed_columns,
        masks=masks,
        row_filter=row_filter,
        policy_version=_effective_policy_version(asset),
        asset_id=None if asset.asset_id is None else str(asset.asset_id),
    )


@dataclass(frozen=True)
class _SnapshotAuthorizer:
    asset: LiveAsset

    def authorize(
        self,
        principal: Principal,
        target: str,
        catalog: str | None,
        requested_columns: Iterable[str],
    ) -> AccessDecision:
        return _authorize_asset(self.asset, principal, target, catalog, requested_columns)

    def current_policy_version(self, target: str, catalog: str | None) -> int | None:
        if target != self.asset.target or catalog != self.asset.catalog:
            return None
        return self.asset.policy_version


@dataclass
class _ProviderEntry:
    registry: CatalogRegistry | None = None
    users: int = 0
    retiring: bool = False


class LiveConfigCatalogRegistry:
    """Request snapshots with bounded, leased catalog-provider reuse.

    Only provider instances are cached. Policy and schema admission always come
    from a fresh database snapshot. Leases cover discovery and planning; an
    evicted or closed provider cannot be disposed while a request still uses it.
    """

    def __init__(
        self,
        store: LiveConfigStore,
        *,
        secret_provider: SecretProvider | None = None,
        plugin_registry: PluginRegistry | AdmittedPluginSnapshot | None = None,
        path_enforcer: PathRuleEnforcer | None = None,
        max_cached_providers: int = 32,
        provider_wait_seconds: float = 5.0,
    ) -> None:
        if max_cached_providers < 1:
            raise ValueError("Provider cache capacity must be positive")
        if provider_wait_seconds <= 0:
            raise ValueError("Provider wait timeout must be positive")
        self._provider_wait_seconds = provider_wait_seconds
        self._store = store
        self._secret_provider = secret_provider
        self._plugin_registry = plugin_registry
        self._path_enforcer = path_enforcer
        self._max_cached_providers = max_cached_providers
        self._providers: OrderedDict[str, _ProviderEntry] = OrderedDict()
        self._condition = Condition()
        self._closed = False

    def close(self) -> None:
        """Reject new leases, close idle providers, defer active ones to release."""
        with self._condition:
            self._closed = True
            idle = [key for key, entry in self._providers.items() if entry.users == 0]
            registries = [self._providers.pop(key).registry for key in idle]
            self._condition.notify_all()
        first_error: Exception | None = None
        for registry in registries:
            try:
                assert registry is not None
                registry.close()
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    @contextmanager
    def open(self, catalog: str | None, target: str) -> Iterator[AccessContext]:
        if catalog is None:
            raise ValueError("Catalog name is required to resolve a target")
        with self._condition:
            if self._closed:
                raise RuntimeError("Live catalog registry is closed")
        asset, live_catalog = self._store.get_asset_and_catalog(catalog=catalog, target=target)
        config = _catalog_config_for_asset(
            live_catalog,
            asset,
            plugin_registry=self._plugin_registry,
            path_enforcer=self._path_enforcer,
        )
        if self._secret_provider is not None:
            config = CatalogConfig(
                name=config.name,
                type=config.type,
                options=cast(
                    dict[str, Any],
                    resolve_secret_refs(
                        config.options,
                        provider=self._secret_provider,
                        expected_scope=f"catalog:{config.name}",
                    ),
                ),
                path_enforcer=config.path_enforcer,
                plugin_id=config.plugin_id,
                revision=config.revision,
            )
        # Resource revision is part of the public plugin handle identity. It is
        # not a deployment counter; unrelated asset/policy edits never affect it.
        key = sha256(
            json.dumps(
                {
                    "name": config.name,
                    "type": config.type,
                    "plugin_id": config.plugin_id,
                    "revision": config.revision,
                    "options": config.options,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        entry = self._acquire(key, catalog, config)
        try:
            assert entry.registry is not None
            table_format = entry.registry.describe(catalog, _asset_table_identifier(asset))
            _validate_schema_admission(asset, table_format.get_schema())
            yield AccessContext(table_format=table_format, authorizer=_SnapshotAuthorizer(asset))
        finally:
            self._release(key, entry)

    def describe(self, catalog: str | None, target: str) -> TableFormat:
        """Resolve a detached table format for direct catalog consumers."""
        with self.open(catalog, target) as context:
            return context.table_format

    def _acquire(self, key: str, catalog: str, config: CatalogConfig) -> _ProviderEntry:
        deadline = monotonic() + self._provider_wait_seconds
        while True:
            with self._condition:
                if self._closed:
                    raise RuntimeError("Live catalog registry is closed")
                entry = self._providers.get(key)
                if entry is not None and entry.registry is not None and not entry.retiring:
                    self._providers.move_to_end(key)
                    entry.users += 1
                    return entry
                if entry is None and len(self._providers) < self._max_cached_providers:
                    # Reserve capacity and coalesce same-key misses.
                    entry = _ProviderEntry(users=1)
                    self._providers[key] = entry
                    break
                idle_key = (
                    None
                    if entry is not None
                    else next((k for k, item in self._providers.items() if item.users == 0), None)
                )
                if idle_key is None:
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        raise AccessContextUnavailable(
                            "Catalog provider capacity is unavailable; retry later"
                        )
                    self._condition.wait(timeout=remaining)
                    continue
                idle = self._providers[idle_key]
                idle.users = 1
                idle.retiring = True
            # Keep the retiring slot reserved until close completes, without
            # making other warm providers wait on potentially blocking IO.
            try:
                assert idle.registry is not None
                idle.registry.close()
            finally:
                with self._condition:
                    self._providers.pop(idle_key)
                    self._condition.notify_all()
        # Construct outside the lock so unrelated cold and warm requests proceed.
        try:
            registry = CatalogRegistry(
                ServiceConfig(catalogs={catalog: config}),
                plugin_registry=self._plugin_registry
                if isinstance(self._plugin_registry, PluginRegistry)
                else None,
            )
        except BaseException:
            with self._condition:
                self._providers.pop(key)
                self._condition.notify_all()
            raise
        with self._condition:
            entry.registry = registry
            closed = self._closed
            self._condition.notify_all()
        if closed:
            self._release(key, entry)
            raise RuntimeError("Live catalog registry is closed")
        return entry

    def _release(self, key: str, entry: _ProviderEntry) -> None:
        with self._condition:
            entry.users -= 1
            close = self._closed and entry.users == 0
            if close:
                self._providers.pop(key)
            self._condition.notify_all()
        if close:
            assert entry.registry is not None
            entry.registry.close()


def _effective_policy_version(asset: LiveAsset) -> int:
    """The policy resource's CAS revision, without global invalidation."""
    return asset.policy_version


def _policy_from_asset(asset: LiveAsset) -> Policy:
    return CompiledPolicy.from_json(_mapping(asset.compiled_config.get("policy"))).to_policy()


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
        catalog, plugin_id=catalog_plugin_id, path_enforcer=path_enforcer
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
        encoded = json.dumps(fields, sort_keys=True, default=str, separators=(",", ":")).encode(
            "utf-8"
        )
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
    """Prevent unbound wildcard/parent grants from expanding on schema drift."""

    policy = asset.compiled_config.get("policy")
    if not isinstance(policy, dict):
        return
    rules = policy.get("rules")
    if not isinstance(rules, list):
        return
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        if rule.get("effect") == "allow_all":
            # Explicit bypass intentionally includes every current/future column.
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
