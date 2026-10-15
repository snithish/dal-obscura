from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from types import TracebackType
from typing import Any
from uuid import UUID

from sqlalchemy.orm import Session

from dal_obscura.adapters.sql import assets as _db_assets
from dal_obscura.adapters.sql import audit as _db_audit
from dal_obscura.adapters.sql import catalogs as _db_catalogs
from dal_obscura.adapters.sql import policies as _db_policies
from dal_obscura.adapters.sql import workspace as _db_workspace
from dal_obscura.application.governance.values import AssetPage, AuditEventPage, WorkspaceState
from dal_obscura.identity.sessions import (
    BrowserSessionStore,
    LoginRateLimiter,
    LoginTransactionStore,
)
from dal_obscura.kernel.identity.actor import ControlPlaneActor
from dal_obscura.kernel.identity.session import LoginRateLimitDecision, LoginTransaction


class _SqlRepository:
    def __init__(self, get_session: Callable[[], Session]) -> None:
        self._get_session = get_session


class SqlAssetsRepository(_SqlRepository):
    def upsert_asset(
        self,
        *,
        catalog: str,
        target: str,
        backend: str,
        table_identifier: str | None,
        options: dict[str, Any],
        expected_revision: int | None = None,
    ) -> UUID:
        return _db_assets.upsert_asset(
            self._get_session(),
            catalog=catalog,
            target=target,
            backend=backend,
            table_identifier=table_identifier,
            options=options,
            expected_revision=expected_revision,
        )

    def replace_asset_owners(
        self, *, asset_id: UUID, owners: list[str], expected_revision: int | None = None
    ) -> list[str]:
        return _db_assets.replace_asset_owners(
            self._get_session(),
            asset_id=asset_id,
            owners=owners,
            expected_revision=expected_revision,
        )

    def replace_asset_schema_fields(
        self, *, asset_id: UUID, fields: list[dict[str, Any]], expected_revision: int | None = None
    ) -> list[dict[str, object]]:
        return _db_assets.replace_asset_schema_fields(
            self._get_session(),
            asset_id=asset_id,
            fields=fields,
            expected_revision=expected_revision,
        )

    def list_workspace_assets(self) -> list[dict[str, object]]:
        return _db_assets.list_workspace_assets(self._get_session())

    def list_workspace_assets_for_principals(self, principals: set[str]) -> list[dict[str, object]]:
        return _db_assets.list_workspace_assets_for_principals(self._get_session(), principals)

    def list_workspace_assets_page(
        self,
        *,
        limit: int,
        cursor: str | None = None,
        search: str | None = None,
        principals: set[str] | None = None,
    ) -> AssetPage:
        return _db_assets.list_workspace_assets_page(
            self._get_session(), limit=limit, cursor=cursor, search=search, principals=principals
        )

    def get_asset_binding(self, asset_id: UUID) -> dict[str, object]:
        return _db_assets.get_asset_binding(self._get_session(), asset_id)

    def get_workspace_asset(self, asset_id: UUID) -> dict[str, object]:
        return _db_assets.get_workspace_asset(self._get_session(), asset_id)

    def lock_asset_for_update(self, asset_id: UUID) -> None:
        return _db_assets.lock_asset_for_update(self._get_session(), asset_id)

    def list_asset_owners(self, asset_id: UUID) -> list[str]:
        return _db_assets.list_asset_owners(self._get_session(), asset_id)

    def list_asset_grants(self, asset_id: UUID) -> list[dict[str, str]]:
        return _db_assets.list_asset_grants(self._get_session(), asset_id)

    def replace_asset_grants(
        self, *, asset_id: UUID, grants: list[dict[str, str]], expected_revision: int | None = None
    ) -> list[dict[str, str]]:
        return _db_assets.replace_asset_grants(
            self._get_session(),
            asset_id=asset_id,
            grants=grants,
            expected_revision=expected_revision,
        )

    def list_asset_schema_fields(self, asset_id: UUID) -> list[dict[str, object]]:
        return _db_assets.list_asset_schema_fields(self._get_session(), asset_id)


class _SqlCatalogsRepository(_SqlRepository):
    def upsert_catalog(
        self,
        *,
        name: str,
        plugin_id: str,
        options: dict[str, Any],
        expected_revision: int | None = None,
    ) -> UUID:
        return _db_catalogs.upsert_catalog(
            self._get_session(),
            name=name,
            plugin_id=plugin_id,
            options=options,
            expected_revision=expected_revision,
        )

    def list_workspace_catalogs(self) -> list[dict[str, object]]:
        return _db_catalogs.list_workspace_catalogs(self._get_session())

    def get_workspace_catalog(self, name: str) -> dict[str, object]:
        return _db_catalogs.get_workspace_catalog(self._get_session(), name)

    def governed_table_identifiers(self, *, catalog_id: UUID, identifiers: list[str]) -> set[str]:
        return _db_catalogs.governed_table_identifiers(
            self._get_session(), catalog_id=catalog_id, identifiers=identifiers
        )


class _SqlPoliciesRepository(_SqlRepository):
    def replace_policy_rules(
        self, *, asset_id: UUID, rules: list[dict[str, Any]], expected_revision: int
    ) -> int:
        return _db_policies.replace_policy_rules(
            self._get_session(), asset_id=asset_id, rules=rules, expected_revision=expected_revision
        )

    def revoke_asset_tickets(self, *, asset_id: UUID) -> int:
        return _db_policies.revoke_asset_tickets(self._get_session(), asset_id=asset_id)

    def list_policy_rules(self, asset_id: UUID) -> list[dict[str, object]]:
        return _db_policies.list_policy_rules(self._get_session(), asset_id)


class _SqlWorkspaceRepository(_SqlRepository):
    def get_workspace(self) -> WorkspaceState | None:
        return _db_workspace.get_workspace(self._get_session())

    def ensure_workspace(self) -> WorkspaceState:
        return _db_workspace.ensure_workspace(self._get_session())

    def upsert_runtime_settings(
        self,
        *,
        ticket_ttl_seconds: int,
        max_tickets: int,
        max_ticket_exchanges: int,
        path_rules: list[dict[str, Any]] | None = None,
        expected_revision: int | None = None,
    ) -> None:
        return _db_workspace.upsert_runtime_settings(
            self._get_session(),
            ticket_ttl_seconds=ticket_ttl_seconds,
            max_tickets=max_tickets,
            max_ticket_exchanges=max_ticket_exchanges,
            path_rules=path_rules,
            expected_revision=expected_revision,
        )

    def replace_auth_providers(
        self, *, providers: list[dict[str, Any]], expected_revision: int | None = None
    ) -> None:
        return _db_workspace.replace_auth_providers(
            self._get_session(), providers=providers, expected_revision=expected_revision
        )

    def get_auth_provider_revision(self) -> int:
        return _db_workspace.get_auth_provider_revision(self._get_session())

    def lock_auth_providers_for_update(self) -> None:
        return _db_workspace.lock_auth_providers_for_update(self._get_session())

    def get_runtime_settings(self) -> dict[str, object] | None:
        return _db_workspace.get_runtime_settings(self._get_session())

    def list_auth_providers(self) -> list[dict[str, object]]:
        return _db_workspace.list_auth_providers(self._get_session())

    def get_workspace_summary(self, principals: set[str] | None = None) -> dict[str, object]:
        return _db_workspace.get_workspace_summary(self._get_session(), principals)


class _SqlAuditRepository(_SqlRepository):
    def record_asset_audit_event(
        self,
        *,
        asset_id: UUID,
        actor_principal: str,
        action: str,
        outcome: str = "success",
        details: dict[str, object] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        return _db_audit.record_asset_audit_event(
            self._get_session(),
            asset_id=asset_id,
            actor_principal=actor_principal,
            action=action,
            outcome=outcome,
            details=details,
            correlation_id=correlation_id,
        )

    def record_workspace_audit_event(
        self,
        *,
        actor_principal: str,
        action: str,
        resource_type: str,
        resource_id: str,
        details: dict[str, object] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        return _db_audit.record_workspace_audit_event(
            self._get_session(),
            actor_principal=actor_principal,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details,
            correlation_id=correlation_id,
        )

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
    ) -> AuditEventPage:
        return _db_audit.list_audit_events_page(
            self._get_session(),
            asset_id=asset_id,
            principals=principals,
            actor=actor,
            action=action,
            resource_type=resource_type,
            outcome=outcome,
            correlation_id=correlation_id,
            created_after=created_after,
            created_before=created_before,
            limit=limit,
            cursor=cursor,
        )


class SqlGovernanceUnitOfWork:
    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory
        self._session: Session | None = None
        self._committed = False

    def __enter__(self) -> SqlGovernanceUnitOfWork:
        if self._session is not None:
            raise RuntimeError("Governance unit of work is already active")
        self._session = self._session_factory()
        self._committed = False
        self.browser_sessions = _SqlBrowserSessionsRepository(self._active_session)
        self.login_transactions = _SqlLoginTransactionsRepository(self._active_session)
        self.login_rate_limits = _SqlLoginRateLimitsRepository(self._active_session)
        self.assets = SqlAssetsRepository(self._active_session)
        self.catalogs = _SqlCatalogsRepository(self._active_session)
        self.policies = _SqlPoliciesRepository(self._active_session)
        self.workspace = _SqlWorkspaceRepository(self._active_session)
        self.audit = _SqlAuditRepository(self._active_session)
        return self

    def _active_session(self) -> Session:
        if self._session is None:
            raise RuntimeError("Governance unit of work is not active")
        return self._session

    def commit(self) -> None:
        self._active_session().commit()
        self._committed = True

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        session, self._session = self._session, None
        if session is None:
            return
        cleanup_error: BaseException | None = None
        if not self._committed or exc is not None:
            try:
                session.rollback()
            except BaseException as error:
                cleanup_error = error
        try:
            session.close()
        except BaseException as error:
            cleanup_error = cleanup_error or error
        if exc is None and cleanup_error is not None:
            raise cleanup_error


class SqlGovernanceUnitOfWorkFactory:
    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    def __call__(self) -> SqlGovernanceUnitOfWork:
        return SqlGovernanceUnitOfWork(self._session_factory)


class _SqlBrowserSessionsRepository(_SqlRepository):
    def issue(self, actor: ControlPlaneActor, *, ttl_seconds: int) -> str:
        return BrowserSessionStore(self._get_session()).issue(actor, ttl_seconds=ttl_seconds)

    def issue_with_csrf(self, actor: ControlPlaneActor, *, ttl_seconds: int) -> tuple[str, str]:
        return BrowserSessionStore(self._get_session()).issue_with_csrf(
            actor, ttl_seconds=ttl_seconds
        )

    def resolve(
        self, token: str, *, csrf_token: str | None = None, idle_ttl_seconds: int | None = None
    ) -> ControlPlaneActor | None:
        return BrowserSessionStore(self._get_session()).resolve(
            token, csrf_token=csrf_token, idle_ttl_seconds=idle_ttl_seconds
        )

    def revoke(self, token: str) -> bool:
        return BrowserSessionStore(self._get_session()).revoke(token)


class _SqlLoginTransactionsRepository(_SqlRepository):
    def issue(
        self,
        *,
        state: str,
        nonce: str,
        code_verifier: str,
        redirect_uri: str,
        ttl_seconds: int = 600,
    ) -> None:
        return LoginTransactionStore(self._get_session()).issue(
            state=state,
            nonce=nonce,
            code_verifier=code_verifier,
            redirect_uri=redirect_uri,
            ttl_seconds=ttl_seconds,
        )

    def consume(self, state: str) -> LoginTransaction | None:
        return LoginTransactionStore(self._get_session()).consume(state)


class _SqlLoginRateLimitsRepository(_SqlRepository):
    def allow(
        self, client_key: str, *, max_attempts: int, window_seconds: int, block_seconds: int
    ) -> LoginRateLimitDecision:
        return LoginRateLimiter(self._get_session()).allow(
            client_key,
            max_attempts=max_attempts,
            window_seconds=window_seconds,
            block_seconds=block_seconds,
        )

    def record_failure(
        self, client_key: str, *, max_attempts: int, window_seconds: int, block_seconds: int
    ) -> LoginRateLimitDecision:
        return LoginRateLimiter(self._get_session()).record_failure(
            client_key,
            max_attempts=max_attempts,
            window_seconds=window_seconds,
            block_seconds=block_seconds,
        )

    def clear(self, client_key: str) -> None:
        return LoginRateLimiter(self._get_session()).clear(client_key)
