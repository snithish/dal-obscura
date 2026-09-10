from __future__ import annotations

from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from threading import RLock
from typing import Any, cast
from uuid import UUID

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
            policy_version=asset.policy_version,
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
            return self._store.get_asset(
                tenant_id=tenant_id,
                catalog=catalog,
                target=target,
            ).policy_version
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
        asset = self._store.get_asset(tenant_id=tenant_id, catalog=catalog, target=target)
        cache_key = (asset.publication_id, asset.tenant_id, asset.catalog, asset.target)
        registry = self._registry_cache.get(cache_key)
        if registry is not None:
            return registry.describe(catalog, _asset_table_identifier(asset), tenant_id=tenant_id)
        published_catalogs = self._store.get_catalogs(tenant_id=tenant_id)
        published_catalog = next(
            (item for item in published_catalogs if item.catalog == catalog), None
        )
        if published_catalog is None:
            raise LookupError(f"No published catalog for {catalog!r}")
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
        return registry.describe(catalog, _asset_table_identifier(asset), tenant_id=tenant_id)


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
