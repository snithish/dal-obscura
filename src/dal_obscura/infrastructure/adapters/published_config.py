from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Literal, cast
from uuid import UUID

from sqlalchemy.orm import Session

from dal_obscura.control_plane.infrastructure.repositories import (
    PublicationStore,
    PublishedAsset,
)
from dal_obscura.domain.access_control.models import (
    AccessDecision,
    AccessRule,
    DatasetPolicy,
    MaskRule,
    Policy,
    Principal,
    PrincipalConditionValue,
)
from dal_obscura.domain.access_control.policy_resolution import resolve_access
from dal_obscura.domain.catalog.ports import TableFormat
from dal_obscura.infrastructure.adapters.catalog_registry import DynamicCatalogRegistry
from dal_obscura.infrastructure.adapters.service_config import (
    CatalogConfig,
    CatalogTargetConfig,
    ServiceConfig,
)


class PublishedConfigStore:
    """Reads active published configuration for one data-plane cell."""

    def __init__(self, session: Session, *, cell_id: UUID) -> None:
        self._publication_store = PublicationStore(session)
        self._cell_id = cell_id
        self._asset_cache: dict[tuple[UUID, UUID, str, str], PublishedAsset] = {}
        self._last_good_by_target: dict[tuple[UUID, str, str], PublishedAsset] = {}

    def active_publication_id(self) -> UUID:
        return self._publication_store.get_active_publication(self._cell_id).publication_id

    def get_asset(self, *, tenant_id: str, catalog: str, target: str) -> PublishedAsset:
        tenant_uuid = UUID(tenant_id)
        target_key = (tenant_uuid, catalog, target)
        try:
            publication_id = self.active_publication_id()
            cache_key = (publication_id, tenant_uuid, catalog, target)
            cached = self._asset_cache.get(cache_key)
            if cached is not None:
                return cached
            asset = self._publication_store.get_published_asset(
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

    def __init__(self, store: PublishedConfigStore) -> None:
        self._store = store

    def describe(self, catalog: str | None, target: str, *, tenant_id: str) -> TableFormat:
        if catalog is None:
            raise ValueError("Catalog name is required to resolve a target")
        asset = self._store.get_asset(tenant_id=tenant_id, catalog=catalog, target=target)
        config = asset.compiled_config
        catalog_config = _catalog_config_from_asset(asset, config)
        registry = DynamicCatalogRegistry(
            ServiceConfig(catalogs={catalog: catalog_config}, paths=())
        )
        return registry.describe(catalog, target, tenant_id=tenant_id)


def _policy_from_asset(asset: PublishedAsset) -> Policy:
    rules = [
        _access_rule_from_json(raw)
        for raw in _mapping(asset.compiled_config.get("policy")).get("rules", [])
        if isinstance(raw, dict)
    ]
    return Policy(
        version=asset.policy_version,
        datasets=[DatasetPolicy(target=asset.target, catalog=asset.catalog, rules=rules)],
    )


def _access_rule_from_json(raw: dict[str, object]) -> AccessRule:
    masks_raw = _mapping(raw.get("masks"))
    return AccessRule(
        principals=[str(item) for item in _list(raw.get("principals"))],
        columns=[str(item) for item in _list(raw.get("columns"))],
        masks={
            name: MaskRule(
                type=str(_mapping(mask).get("type")),
                value=_mapping(mask).get("value"),
            )
            for name, mask in masks_raw.items()
            if isinstance(name, str) and isinstance(mask, dict) and _mapping(mask).get("type")
        },
        row_filter=_optional_str(raw.get("row_filter")),
        effect=cast(Literal["allow", "deny"], str(raw.get("effect", "allow"))),
        when=cast(dict[str, PrincipalConditionValue], _mapping(raw.get("when"))),
    )


def _catalog_config_from_asset(
    asset: PublishedAsset,
    compiled_config: dict[str, Any],
) -> CatalogConfig:
    catalog_raw = _mapping(compiled_config.get("catalog"))
    target_raw = _mapping(compiled_config.get("target"))
    table_identifier = _optional_str(target_raw.get("table")) or asset.target
    backend = _optional_str(target_raw.get("backend")) or asset.backend
    return CatalogConfig(
        name=asset.catalog,
        module=str(catalog_raw["module"]),
        options=dict(_mapping(catalog_raw.get("options"))),
        targets={
            asset.target: CatalogTargetConfig(
                backend=backend,
                table=table_identifier,
                options=dict(_mapping(target_raw.get("options"))),
            )
        },
    )


def _tenant_id(principal: Principal) -> str:
    return str(
        principal.attributes.get("tenant_id") or principal.attributes.get("tenant") or "default"
    )


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return cast(dict[str, Any], value).copy()
    return {}


def _list(value: object) -> list[object]:
    if isinstance(value, list):
        return list(cast(list[object], value))
    return []


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
