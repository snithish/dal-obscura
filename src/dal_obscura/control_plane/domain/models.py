from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import UUID


@dataclass(frozen=True)
class CatalogDraft:
    id: UUID
    cell_id: UUID
    tenant_id: UUID
    name: str
    module: str
    options: dict[str, Any]


@dataclass(frozen=True)
class PolicyRuleDraft:
    ordinal: int
    effect: Literal["allow", "deny"]
    principals: list[str]
    when: dict[str, str | list[str]]
    columns: list[str]
    masks: dict[str, object]
    row_filter: str | None


@dataclass
class AssetDraft:
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


@dataclass(frozen=True)
class AuthProviderDraft:
    ordinal: int
    module: str
    args: dict[str, Any]
    enabled: bool


@dataclass(frozen=True)
class CellRuntimeDraft:
    ticket_ttl_seconds: int
    max_tickets: int
    path_rules: list[dict[str, Any]]


@dataclass(frozen=True)
class PublishDraft:
    cell_id: UUID
    tenants: list[UUID]
    runtime: CellRuntimeDraft
    auth_providers: list[AuthProviderDraft]
    catalogs: list[CatalogDraft]
    assets: list[AssetDraft]


@dataclass(frozen=True)
class CompiledRuntime:
    auth_chain: dict[str, Any]
    ticket: dict[str, int]
    path_rules: list[dict[str, Any]]


@dataclass(frozen=True)
class CompiledCatalog:
    tenant_id: UUID
    catalog: str
    config: dict[str, Any]


@dataclass(frozen=True)
class CompiledAsset:
    tenant_id: UUID
    catalog: str
    target: str
    backend: str
    compiled_config: dict[str, Any]
    policy_version: int


@dataclass(frozen=True)
class CompiledPublication:
    cell_id: UUID
    runtime: CompiledRuntime
    catalogs: list[CompiledCatalog]
    assets: list[CompiledAsset]
    manifest_hash: str
