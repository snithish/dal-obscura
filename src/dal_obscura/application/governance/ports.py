from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from types import TracebackType
from typing import Any, Protocol, Self
from uuid import UUID

import pyarrow as pa
from dal_obscura_plugin_api import DiscoveryPage, PluginDescriptor
from dal_obscura_plugin_api.contracts import PluginKind

from dal_obscura.application.governance.values import (
    AssetPage,
    AssetSchemaDiscovery,
    AuditEventPage,
    CatalogDiscovery,
    WorkspaceState,
)
from dal_obscura.kernel.identity.actor import ControlPlaneActor
from dal_obscura.kernel.identity.principal import Principal
from dal_obscura.kernel.identity.session import LoginRateLimitDecision, LoginTransaction


class AssetsRepository(Protocol):
    def upsert_asset(
        self,
        *,
        catalog: str,
        target: str,
        backend: str,
        table_identifier: str | None,
        options: dict[str, Any],
        expected_revision: int | None = None,
    ) -> UUID: ...
    def replace_asset_owners(
        self, *, asset_id: UUID, owners: list[str], expected_revision: int | None = None
    ) -> list[str]: ...
    def replace_asset_schema_fields(
        self, *, asset_id: UUID, fields: list[dict[str, Any]], expected_revision: int | None = None
    ) -> list[dict[str, object]]: ...
    def list_workspace_assets(self) -> list[dict[str, object]]: ...
    def list_workspace_assets_for_principals(
        self, principals: set[str]
    ) -> list[dict[str, object]]: ...
    def list_workspace_assets_page(
        self,
        *,
        limit: int,
        cursor: str | None = None,
        search: str | None = None,
        principals: set[str] | None = None,
    ) -> AssetPage: ...
    def get_asset_binding(self, asset_id: UUID) -> dict[str, object]: ...
    def get_workspace_asset(self, asset_id: UUID) -> dict[str, object]: ...
    def lock_asset_for_update(self, asset_id: UUID) -> None: ...
    def list_asset_owners(self, asset_id: UUID) -> list[str]: ...
    def list_asset_grants(self, asset_id: UUID) -> list[dict[str, str]]: ...
    def replace_asset_grants(
        self, *, asset_id: UUID, grants: list[dict[str, str]], expected_revision: int | None = None
    ) -> list[dict[str, str]]: ...
    def list_asset_schema_fields(self, asset_id: UUID) -> list[dict[str, object]]: ...


class CatalogsRepository(Protocol):
    def upsert_catalog(
        self,
        *,
        name: str,
        plugin_id: str,
        options: dict[str, Any],
        expected_revision: int | None = None,
    ) -> UUID: ...
    def list_workspace_catalogs(self) -> list[dict[str, object]]: ...
    def get_workspace_catalog(self, name: str) -> dict[str, object]: ...
    def governed_table_identifiers(
        self, *, catalog_id: UUID, identifiers: list[str]
    ) -> set[str]: ...


class PoliciesRepository(Protocol):
    def replace_policy_rules(
        self, *, asset_id: UUID, rules: list[dict[str, Any]], expected_revision: int
    ) -> int: ...
    def revoke_asset_tickets(self, *, asset_id: UUID) -> int: ...
    def list_policy_rules(self, asset_id: UUID) -> list[dict[str, object]]: ...


class WorkspaceRepository(Protocol):
    def get_workspace(self) -> WorkspaceState | None: ...
    def ensure_workspace(self) -> WorkspaceState: ...
    def upsert_runtime_settings(
        self,
        *,
        ticket_ttl_seconds: int,
        max_tickets: int,
        max_ticket_exchanges: int,
        path_rules: list[dict[str, Any]] | None = None,
        expected_revision: int | None = None,
    ) -> None: ...
    def replace_auth_providers(
        self, *, providers: list[dict[str, Any]], expected_revision: int | None = None
    ) -> None: ...
    def get_auth_provider_revision(self) -> int: ...
    def lock_auth_providers_for_update(self) -> None: ...
    def get_runtime_settings(self) -> dict[str, object] | None: ...
    def list_auth_providers(self) -> list[dict[str, object]]: ...
    def get_workspace_summary(self, principals: set[str] | None = None) -> dict[str, object]: ...


class AuditRepository(Protocol):
    def record_asset_audit_event(
        self,
        *,
        asset_id: UUID,
        actor_principal: str,
        action: str,
        outcome: str = "success",
        details: dict[str, object] | None = None,
        correlation_id: str | None = None,
    ) -> None: ...
    def record_workspace_audit_event(
        self,
        *,
        actor_principal: str,
        action: str,
        resource_type: str,
        resource_id: str,
        details: dict[str, object] | None = None,
        correlation_id: str | None = None,
    ) -> None: ...
    def list_audit_events_page(
        self,
        *,
        asset_id: UUID | None = None,
        principals: set[str] | None = None,
        actor: str | None = None,
        action: str | None = None,
        resource_type: str | None = None,
        outcome: str | None = None,
        correlation_id: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> AuditEventPage: ...


class GovernanceUnitOfWork(Protocol):
    @property
    def login_rate_limits(self) -> LoginRateLimitsRepository: ...
    @property
    def login_transactions(self) -> LoginTransactionsRepository: ...
    @property
    def browser_sessions(self) -> BrowserSessionsRepository: ...
    @property
    def assets(self) -> AssetsRepository: ...
    @property
    def catalogs(self) -> CatalogsRepository: ...
    @property
    def policies(self) -> PoliciesRepository: ...
    @property
    def workspace(self) -> WorkspaceRepository: ...
    @property
    def audit(self) -> AuditRepository: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...
    def commit(self) -> None: ...


class GovernanceUnitOfWorkFactory(Protocol):
    def __call__(self) -> GovernanceUnitOfWork: ...


class GovernancePluginRegistry(Protocol):
    def admitted(self) -> Mapping[tuple[PluginKind, str], PluginDescriptor]: ...


class AssetSchemaLoader(Protocol):
    def load_asset_schema(self, captured: AssetSchemaDiscovery) -> pa.Schema: ...


class PreviewIdentityMapper(Protocol):
    def map_attributes(
        self, provider_args: dict[str, Any], claims: dict[str, object]
    ) -> dict[str, str]: ...
    def map_claims(self, provider: dict[str, object], claims: dict[str, object]) -> Principal: ...


class CatalogBrowser(Protocol):
    def discover(
        self,
        captured: CatalogDiscovery,
        *,
        session_key: str,
        continuation: str | None = None,
        limit: int = 100,
    ) -> DiscoveryPage: ...


class GovernancePluginDiagnostics(GovernancePluginRegistry, Protocol):
    def status_report(self) -> tuple[dict[str, object], ...]: ...


class BrowserSessionsRepository(Protocol):
    def issue(self, actor: ControlPlaneActor, *, ttl_seconds: int) -> str: ...
    def issue_with_csrf(self, actor: ControlPlaneActor, *, ttl_seconds: int) -> tuple[str, str]: ...
    def resolve(
        self, token: str, *, csrf_token: str | None = None, idle_ttl_seconds: int | None = None
    ) -> ControlPlaneActor | None: ...
    def revoke(self, token: str) -> bool: ...


class LoginTransactionsRepository(Protocol):
    def issue(
        self,
        *,
        state: str,
        nonce: str,
        code_verifier: str,
        redirect_uri: str,
        ttl_seconds: int = 600,
    ) -> None: ...
    def consume(self, state: str) -> LoginTransaction | None: ...


class LoginRateLimitsRepository(Protocol):
    def allow(
        self, client_key: str, *, max_attempts: int, window_seconds: int, block_seconds: int
    ) -> LoginRateLimitDecision: ...
    def record_failure(
        self, client_key: str, *, max_attempts: int, window_seconds: int, block_seconds: int
    ) -> LoginRateLimitDecision: ...
    def clear(self, client_key: str) -> None: ...
