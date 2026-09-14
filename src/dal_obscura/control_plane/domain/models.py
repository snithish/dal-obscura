from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import UUID


@dataclass(frozen=True)
class CatalogDraft:
    """Draft catalog definition before publication.

    Example:
        ```python
        draft = CatalogDraft(
            id=id,
            cell_id=cell,
            tenant_id=tenant,
            name="analytics",
            module=module,
            options={},
        )
        ```
    """

    id: UUID
    cell_id: UUID
    tenant_id: UUID
    name: str
    module: str
    options: dict[str, Any]
    revision: int = 0


@dataclass(frozen=True)
class PolicyRuleDraft:
    """Draft access-control rule attached to an asset.

    Example:
        ```python
        rule = PolicyRuleDraft(0, "allow", ["analyst"], {}, ["id"], {}, None)
        ```
    """

    ordinal: int
    effect: Literal["allow"]
    principals: list[str]
    when: dict[str, str | list[str]]
    columns: list[str]
    masks: dict[str, object]
    row_filter: str | None


@dataclass
class AssetDraft:
    """Draft asset configuration before publication.

    Example:
        ```python
        asset.rules.append(rule)
        ```
    """

    id: UUID
    cell_id: UUID
    tenant_id: UUID
    catalog_id: UUID
    catalog_name: str
    target: str
    backend: str
    table_identifier: str | None
    options: dict[str, Any]
    rules: list[PolicyRuleDraft] = field(default_factory=list)
    schema_fields: list[dict[str, object]] = field(default_factory=list)


@dataclass(frozen=True)
class AuthProviderDraft:
    """Draft authentication provider configuration.

    Example:
        ```python
        provider = AuthProviderDraft(0, "example.Provider", {}, True)
        ```
    """

    ordinal: int
    module: str
    args: dict[str, Any]
    enabled: bool


@dataclass(frozen=True)
class CellRuntimeDraft:
    """Draft runtime ticket and fan-out settings for a cell.

    Example:
        ```python
        runtime = CellRuntimeDraft(ticket_ttl_seconds=300, max_tickets=32, max_ticket_exchanges=1)
        ```
    """

    ticket_ttl_seconds: int
    max_tickets: int
    max_ticket_exchanges: int
    path_rules: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class PublishDraft:
    """Complete draft snapshot to compile into a publication.

    Example:
        ```python
        publish = PublishDraft(
            cell_id=cell,
            tenants=[tenant],
            runtime=runtime,
            auth_providers=[],
            catalogs=[],
            assets=[],
        )
        ```
    """

    cell_id: UUID
    tenants: list[UUID]
    runtime: CellRuntimeDraft
    auth_providers: list[AuthProviderDraft]
    catalogs: list[CatalogDraft]
    assets: list[AssetDraft]


@dataclass(frozen=True)
class CompiledRuntime:
    """Runtime settings serialized into a publication manifest.

    Example:
        ```python
        compiled = CompiledRuntime(auth_chain={}, ticket={"ttl_seconds": 300})
        ```
    """

    auth_chain: dict[str, Any]
    ticket: dict[str, int]
    path_rules: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class CompiledCatalog:
    """Catalog configuration serialized into a publication manifest.

    Example:
        ```python
        catalog = CompiledCatalog(tenant_id=tenant, catalog="analytics", config={})
        ```
    """

    tenant_id: UUID
    catalog: str
    config: dict[str, Any]


@dataclass(frozen=True)
class CompiledAsset:
    """Asset configuration and policy version serialized into a publication.

    Example:
        ```python
        asset = CompiledAsset(tenant, "analytics", "orders", "iceberg", {}, 1)
        ```
    """

    tenant_id: UUID
    catalog: str
    target: str
    backend: str
    compiled_config: dict[str, Any]
    policy_version: int


@dataclass(frozen=True)
class CompiledPublication:
    """Compiled immutable publication manifest.

    Example:
        ```python
        publication = CompiledPublication(cell, runtime, catalogs, assets, manifest_hash)
        ```
    """

    cell_id: UUID
    runtime: CompiledRuntime
    catalogs: list[CompiledCatalog]
    assets: list[CompiledAsset]
    manifest_hash: str
