from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

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
    publication_id: UUID
    auth_chain: dict[str, Any]
    ticket: dict[str, Any]


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
class PublishedCatalog:
    publication_id: UUID
    tenant_id: UUID
    catalog: str
    config: dict[str, Any]


class PublishedConfigStore:
    """Reads active published configuration for one data-plane cell."""

    def __init__(self, session: Session, *, cell_id: UUID) -> None:
        self._session = session
        self._cell_id = cell_id
        self._asset_cache: dict[tuple[UUID, UUID, str, str], PublishedAsset] = {}
        self._last_good_by_target: dict[tuple[UUID, str, str], PublishedAsset] = {}
        self._catalog_cache: dict[tuple[UUID, UUID], list[PublishedCatalog]] = {}
        self._last_good_catalogs_by_tenant: dict[UUID, list[PublishedCatalog]] = {}
        self._runtime_cache: dict[UUID, PublishedRuntime] = {}
        self._tenant_cache: dict[str, UUID] = {}

    def active_publication_id(self) -> UUID:
        record = self._session.get(ActivePublicationRecord, self._cell_id)
        if record is None:
            raise LookupError(f"No active publication for cell {self._cell_id}")
        return record.publication_id

    def get_asset(self, *, tenant_id: str, catalog: str, target: str) -> PublishedAsset:
        tenant_uuid = self._tenant_uuid(tenant_id)
        target_key = (tenant_uuid, catalog, target)
        try:
            publication_id = self.active_publication_id()
            cache_key = (publication_id, tenant_uuid, catalog, target)
            cached = self._asset_cache.get(cache_key)
            if cached is not None:
                return cached
            asset = self._published_asset(
                publication_id=publication_id,
                tenant_id=tenant_uuid,
                catalog=catalog,
                target=target,
            )
            self._asset_cache[cache_key] = asset
            self._last_good_by_target[target_key] = asset
            return asset
        except Exception:
            cached = self._last_good_by_target.get(target_key)
            if cached is not None:
                return cached
            raise

    def get_catalogs(self, *, tenant_id: str) -> list[PublishedCatalog]:
        tenant_uuid = self._tenant_uuid(tenant_id)
        try:
            publication_id = self.active_publication_id()
            cache_key = (publication_id, tenant_uuid)
            cached = self._catalog_cache.get(cache_key)
            if cached is not None:
                return cached
            catalogs = self._published_catalogs(
                publication_id=publication_id,
                tenant_id=tenant_uuid,
            )
            self._catalog_cache[cache_key] = catalogs
            self._last_good_catalogs_by_tenant[tenant_uuid] = catalogs
            return catalogs
        except Exception:
            cached = self._last_good_catalogs_by_tenant.get(tenant_uuid)
            if cached is not None:
                return cached
            raise

    def _published_asset(
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

    def _published_catalogs(
        self,
        *,
        publication_id: UUID,
        tenant_id: UUID,
    ) -> list[PublishedCatalog]:
        records = self._session.scalars(
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
            tenant_uuid = self._tenant_uuid_by_slug(tenant_id)
        self._tenant_cache[tenant_id] = tenant_uuid
        return tenant_uuid

    def _tenant_uuid_by_slug(self, slug: str) -> UUID:
        record = self._session.scalar(select(TenantRecord).where(TenantRecord.slug == slug))
        if record is None:
            raise LookupError(f"No tenant with slug {slug!r}")
        return record.id

    def get_runtime(self) -> PublishedRuntime:
        publication_id = self.active_publication_id()
        cached = self._runtime_cache.get(publication_id)
        if cached is not None:
            return cached
        record = self._session.scalar(
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
        self._registry_cache: dict[tuple[UUID, UUID], CatalogRegistry] = {}

    def describe(self, catalog: str | None, target: str, *, tenant_id: str) -> TableFormat:
        if catalog is None:
            raise ValueError("Catalog name is required to resolve a target")
        asset = self._store.get_asset(tenant_id=tenant_id, catalog=catalog, target=target)
        cache_key = (asset.publication_id, asset.tenant_id)
        registry = self._registry_cache.get(cache_key)
        if registry is not None:
            return registry.describe(catalog, target, tenant_id=tenant_id)
        published_catalogs = self._store.get_catalogs(tenant_id=tenant_id)
        catalogs = {
            item.catalog: _catalog_config_from_published_catalog(item)
            for item in published_catalogs
        }
        if catalog not in catalogs:
            raise LookupError(f"No published catalog for {catalog!r}")
        if self._secret_provider is not None:
            catalogs = {
                name: CatalogConfig(
                    name=config.name,
                    type=config.type,
                    options=cast(
                        dict[str, Any],
                        resolve_secret_refs(config.options, provider=self._secret_provider),
                    ),
                    path_enforcer=config.path_enforcer,
                )
                for name, config in catalogs.items()
            }
        registry = CatalogRegistry(ServiceConfig(catalogs=catalogs))
        self._registry_cache[cache_key] = registry
        return registry.describe(catalog, target, tenant_id=tenant_id)


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
    return CatalogConfig(name=catalog.catalog, type=_catalog_type(config), options=options)


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
    if value in {"iceberg", "files", "delta", "unity"}:
        return cast(CatalogType, value)
    raise ValueError(f"Unsupported catalog type: {value}")
