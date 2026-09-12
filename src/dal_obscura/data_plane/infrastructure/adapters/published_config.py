from __future__ import annotations

from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from threading import RLock
from typing import Any, cast
from uuid import UUID

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
    ActivePublicationRecord,
    PublishedAssetRecord,
    PublishedCatalogRecord,
    PublishedCellRuntimeRecord,
    TenantRecord,
)
from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    CatalogRegistry,
    CatalogType,
    ServiceConfig,
)
from dal_obscura.data_plane.infrastructure.adapters.secret_providers import (
    SecretProvider,
    resolve_secret_refs,
)


@dataclass(frozen=True)
class PublishedRuntime:
    """Active runtime settings compiled from the control plane.

    Example:
        ```python
        runtime = store.get_runtime()
        ttl = runtime.ticket["ttl_seconds"]
        ```
    """

    publication_id: UUID
    auth_chain: dict[str, Any]
    ticket: dict[str, Any]


@dataclass(frozen=True)
class PublishedAsset:
    """Published asset policy and backend configuration for one target.

    Example:
        ```python
        asset = store.get_asset(tenant_id="default", catalog="analytics", target="orders")
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
class PublishedCatalog:
    """Published catalog configuration for one tenant.

    Example:
        ```python
        catalogs = store.get_catalogs(tenant_id="default")
        ```
    """

    publication_id: UUID
    tenant_id: UUID
    catalog: str
    config: dict[str, Any]


class PublishedConfigStore:
    """Reads active published configuration for one data-plane cell."""

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
        self._asset_cache: dict[tuple[UUID, UUID, str, str], PublishedAsset] = {}
        self._catalog_cache: dict[tuple[UUID, UUID], list[PublishedCatalog]] = {}
        self._runtime_cache: dict[UUID, PublishedRuntime] = {}
        self._tenant_cache: dict[str, UUID] = {}
        self._cached_publication_id: UUID | None = None

    def active_publication_id(self) -> UUID:
        with self._session_scope() as session:
            return self._active_publication_id(session)

    def get_asset(self, *, tenant_id: str, catalog: str, target: str) -> PublishedAsset:
        with self._lock:
            tenant_uuid = self._tenant_uuid(tenant_id)
            with self._session_scope() as session:
                publication_id = self._active_publication_id(session)
                self._observe_generation(publication_id)
                cache_key = (publication_id, tenant_uuid, catalog, target)
                cached = self._asset_cache.get(cache_key)
                if cached is not None:
                    return cached
                asset = self._published_asset(
                    session,
                    publication_id=publication_id,
                    tenant_id=tenant_uuid,
                    catalog=catalog,
                    target=target,
                )
            self._asset_cache[cache_key] = asset
            return asset

    def get_catalogs(self, *, tenant_id: str) -> list[PublishedCatalog]:
        with self._lock:
            tenant_uuid = self._tenant_uuid(tenant_id)
            with self._session_scope() as session:
                publication_id = self._active_publication_id(session)
                self._observe_generation(publication_id)
                cache_key = (publication_id, tenant_uuid)
                cached = self._catalog_cache.get(cache_key)
                if cached is not None:
                    return cached
                catalogs = self._published_catalogs(
                    session,
                    publication_id=publication_id,
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
    ) -> tuple[PublishedAsset, PublishedCatalog]:
        """Loads a target and its catalog from one captured publication generation."""
        with self._lock:
            tenant_uuid = self._tenant_uuid(tenant_id)
            with self._session_scope() as session:
                publication_id = self._active_publication_id(session)
                self._observe_generation(publication_id)
                asset_key = (publication_id, tenant_uuid, catalog, target)
                asset = self._asset_cache.get(asset_key)
                if asset is None:
                    asset = self._published_asset(
                        session,
                        publication_id=publication_id,
                        tenant_id=tenant_uuid,
                        catalog=catalog,
                        target=target,
                    )
                catalog_key = (publication_id, tenant_uuid)
                catalogs = self._catalog_cache.get(catalog_key)
                if catalogs is None:
                    catalogs = self._published_catalogs(
                        session,
                        publication_id=publication_id,
                        tenant_id=tenant_uuid,
                    )
            self._asset_cache[asset_key] = asset
            self._catalog_cache[catalog_key] = catalogs
            published_catalog = next((item for item in catalogs if item.catalog == catalog), None)
            if published_catalog is None:
                raise LookupError(f"No published catalog for {catalog!r}")
            return asset, published_catalog

    def _published_asset(
        self,
        session: Session,
        *,
        publication_id: UUID,
        tenant_id: UUID,
        catalog: str,
        target: str,
    ) -> PublishedAsset:
        record = session.scalar(
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

    def _published_catalogs(
        self,
        session: Session,
        *,
        publication_id: UUID,
        tenant_id: UUID,
    ) -> list[PublishedCatalog]:
        records = session.scalars(
            select(PublishedCatalogRecord)
            .where(
                PublishedCatalogRecord.publication_id == publication_id,
                PublishedCatalogRecord.tenant_id == tenant_id,
            )
            .order_by(PublishedCatalogRecord.catalog)
        )
        return [
            PublishedCatalog(
                publication_id=record.publication_id,
                tenant_id=record.tenant_id,
                catalog=record.catalog,
                config=dict(record.config_json),
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
        record = session.scalar(select(TenantRecord).where(TenantRecord.slug == slug))
        if record is None:
            raise LookupError(f"No tenant with slug {slug!r}")
        return record.id

    def get_runtime(self) -> PublishedRuntime:
        with self._lock:
            with self._session_scope() as session:
                publication_id = self._active_publication_id(session)
                self._observe_generation(publication_id)
                cached = self._runtime_cache.get(publication_id)
                if cached is not None:
                    return cached
                record = session.scalar(
                    select(PublishedCellRuntimeRecord).where(
                        PublishedCellRuntimeRecord.publication_id == publication_id
                    )
                )
                if record is None:
                    raise LookupError(f"No published runtime for publication {publication_id}")
                runtime = PublishedRuntime(
                    publication_id=record.publication_id,
                    auth_chain=dict(record.auth_chain_json),
                    ticket=dict(record.ticket_json),
                )
            self._runtime_cache[publication_id] = runtime
            return runtime

    def _observe_generation(self, publication_id: UUID) -> None:
        if publication_id == self._cached_publication_id:
            return
        self._asset_cache.clear()
        self._catalog_cache.clear()
        self._runtime_cache.clear()
        self._cached_publication_id = publication_id

    def _active_publication_id(self, session: Session) -> UUID:
        record = session.get(ActivePublicationRecord, self._cell_id)
        if record is None:
            raise LookupError(f"No active publication for cell {self._cell_id}")
        return record.publication_id

    @contextmanager
    def _session_scope(self):
        if self._session is not None:
            yield self._session
            return
        if self._session_maker is None:
            raise RuntimeError("PublishedConfigStore is missing a session factory")
        with self._session_maker() as session:
            yield session


class PublishedConfigAuthorizer:
    """Authorization adapter backed by published asset policy JSON."""

    def __init__(self, store: PublishedConfigStore) -> None:
        self._store = store

    def authorize(
        self,
        principal: Principal,
        target: str,
        catalog: str | None,
        requested_columns: Iterable[str],
    ) -> AccessDecision:
        if catalog is None:
            raise ValueError("Catalog name is required to authorize published assets")
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


class PublishedConfigCatalogRegistry:
    """Catalog registry that resolves tables from active published asset config."""

    def __init__(
        self,
        store: PublishedConfigStore,
        *,
        secret_provider: SecretProvider | None = None,
    ) -> None:
        self._store = store
        self._secret_provider = secret_provider
        self._registry_cache: dict[tuple[UUID, UUID, str, str], CatalogRegistry] = {}

    def describe(self, catalog: str | None, target: str, *, tenant_id: str) -> TableFormat:
        if catalog is None:
            raise ValueError("Catalog name is required to resolve a target")
        asset, published_catalog = self._store.get_asset_and_catalog(
            tenant_id=tenant_id,
            catalog=catalog,
            target=target,
        )
        cache_key = (asset.publication_id, asset.tenant_id, asset.catalog, asset.target)
        registry = self._registry_cache.get(cache_key)
        if registry is not None:
            table_format = registry.describe(
                catalog,
                _asset_table_identifier(asset),
                tenant_id=tenant_id,
            )
            _validate_schema_admission(asset, table_format.get_schema())
            return table_format
        catalog_config = _catalog_config_for_asset(published_catalog, asset)
        if self._secret_provider is not None:
            catalog_config = CatalogConfig(
                name=catalog_config.name,
                type=catalog_config.type,
                options=cast(
                    dict[str, Any],
                    resolve_secret_refs(catalog_config.options, provider=self._secret_provider),
                ),
                path_enforcer=catalog_config.path_enforcer,
            )
        registry = CatalogRegistry(ServiceConfig(catalogs={catalog: catalog_config}))
        self._registry_cache[cache_key] = registry
        table_format = registry.describe(
            catalog,
            _asset_table_identifier(asset),
            tenant_id=tenant_id,
        )
        _validate_schema_admission(asset, table_format.get_schema())
        return table_format


def _effective_policy_version(asset: PublishedAsset) -> int:
    """Binds ticket authorization to one immutable publication generation."""
    digest = sha256(f"{asset.publication_id}:{asset.policy_version}".encode()).digest()
    return int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)


def _policy_from_asset(asset: PublishedAsset) -> Policy:
    return CompiledPolicy.from_json(
        _mapping(asset.compiled_config.get("policy")),
        version=asset.policy_version,
        catalog=asset.catalog,
        target=asset.target,
    ).to_policy()


def _catalog_config_from_published_catalog(catalog: PublishedCatalog) -> CatalogConfig:
    config = _mapping(catalog.config)
    options = dict(_mapping(config.get("options")))
    options.pop("provider_modules", None)
    return CatalogConfig(name=catalog.catalog, type=_catalog_type(config), options=options)


def _catalog_config_for_asset(catalog: PublishedCatalog, asset: PublishedAsset) -> CatalogConfig:
    """Build the runtime catalog config with the published asset as its source of truth."""
    config = _catalog_config_from_published_catalog(catalog)
    target = _mapping(asset.compiled_config.get("target"))
    backend = str(target.get("backend") or asset.backend).lower()
    if config.type == "iceberg":
        if backend != "iceberg":
            raise ValueError("Published Iceberg catalogs require Iceberg assets")
        return config
    raise ValueError(f"Unsupported published catalog type: {config.type}")


def _asset_table_identifier(asset: PublishedAsset) -> str:
    target = _mapping(asset.compiled_config.get("target"))
    table = target.get("table")
    if not isinstance(table, str) or not table.strip():
        raise ValueError(f"Published asset {asset.catalog}/{asset.target} has no table identifier")
    return table


def _validate_schema_admission(asset: PublishedAsset, schema: pa.Schema) -> None:
    """Rejects a published asset when an admitted field identity has drifted."""

    admission = _mapping(asset.compiled_config.get("schema"))
    fields = admission.get("fields")
    if not isinstance(fields, list) or not fields:
        return
    identities = _schema_identities(schema)
    for raw in fields:
        if not isinstance(raw, dict):
            raise ValueError("Published schema admission is invalid")
        path = raw.get("path")
        field_id = raw.get("field_id")
        if (
            not isinstance(path, list)
            or not path
            or any(not isinstance(segment, str) for segment in path)
            or not isinstance(field_id, str)
        ):
            raise ValueError("Published schema admission is invalid")
        identity = (tuple(path), field_id)
        actual = identities.get(identity)
        if actual is None:
            raise ValueError(
                "Published schema admission no longer matches the live table; review again."
            )
        expected_type = raw.get("type")
        if (
            isinstance(expected_type, str)
            and expected_type
            and _canonical_type_name(expected_type) != _canonical_type_name(actual)
        ):
            raise ValueError(
                "Published schema field type changed after review; review again."
            )


def _schema_identities(schema: pa.Schema) -> dict[tuple[tuple[str, ...], str], str]:
    result: dict[tuple[tuple[str, ...], str], str] = {}

    def visit(field: pa.Field, path: tuple[str, ...]) -> None:
        metadata = field.metadata or {}
        raw_id = metadata.get(b"PARQUET:field_id") or metadata.get(b"iceberg.field.id")
        if raw_id is not None:
            field_id = raw_id.decode("utf-8", "replace")
            if ":" not in field_id:
                field_id = f"iceberg:{field_id}"
            result[(path, field_id)] = str(field.type)
        if pa.types.is_struct(field.type):
            for child in field.type:
                visit(child, (*path, child.name))

    for field in schema:
        visit(field, (field.name,))
    return result


def _canonical_type_name(value: str) -> str:
    aliases = {
        "long": "int64",
        "integer": "int32",
        "float": "float32",
        "double": "double",
        "boolean": "bool",
    }
    return aliases.get(value.strip().lower(), value.strip().lower())


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
    if raw_type is not None:
        return _known_catalog_type(str(raw_type))
    module = str(config.get("module", ""))
    if module.endswith("IcebergCatalog"):
        return "iceberg"
    return _known_catalog_type(module)


def _known_catalog_type(value: str) -> CatalogType:
    if value == "iceberg":
        return cast(CatalogType, value)
    raise ValueError(f"Unsupported catalog type: {value}")
