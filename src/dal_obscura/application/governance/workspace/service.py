"""Workspace-level control-plane service functions."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

from dal_obscura_plugin_api.storage import PathRuleEnforcer

from dal_obscura.application.governance.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.application.governance.ports import (
    GovernanceUnitOfWork,
    GovernanceUnitOfWorkFactory,
    PreviewIdentityMapper,
)
from dal_obscura.application.governance.values import WorkspaceState
from dal_obscura.application.governance.workspace import attributes as identity_attributes
from dal_obscura.application.governance.workspace.auth_providers import (
    redact_auth_provider,
    validate_auth_provider_payloads,
)
from dal_obscura.kernel.identity.actor import ControlPlaneActor


def required_workspace(store: GovernanceUnitOfWork) -> WorkspaceState:
    """Returns the default workspace context or raises when none exists."""

    context = store.workspace.get_workspace()
    if context is None:
        raise LookupError("No workspace has been configured")
    return context


def get_workspace_summary(
    store: GovernanceUnitOfWork,
    actor: ControlPlaneActor | None = None,
) -> dict[str, object]:
    """Returns a summary of the current workspace."""

    return store.workspace.get_workspace_summary(
        None if actor is None or actor.platform_admin else actor.owner_principals()
    )


def get_workspace_runtime_settings(store: GovernanceUnitOfWork) -> dict[str, object] | None:
    """Returns runtime ticket settings for the workspace when configured."""

    context = store.workspace.get_workspace()
    if context is None:
        return None
    settings = store.workspace.get_runtime_settings()
    if settings is None:
        return None
    raw_path_rules = settings.get("path_rules", [])
    path_rules = raw_path_rules if isinstance(raw_path_rules, list) else []
    return {
        "ticket_ttl_seconds": settings["ticket_ttl_seconds"],
        "max_tickets": settings["max_tickets"],
        "max_ticket_exchanges": settings["max_ticket_exchanges"],
        "path_rules": list(path_rules),
        "revision": settings["revision"],
    }


def get_workspace_observations(
    store: GovernanceUnitOfWork,
    actor: ControlPlaneActor,
) -> dict[str, object]:
    """Returns bounded control-plane observations for the current workspace.

    This endpoint deliberately reports what the control-plane database knows;
    it never presents a configuration revision as proof that a Flight worker
    is healthy or has reloaded changed startup settings.
    """

    observed_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    context = store.workspace.get_workspace()
    if context is None:
        return {
            "available": False,
            "observed_at": observed_at,
            "source": "control-plane-db",
            "data_plane": {"status": "unobserved", "reason": "workspace_not_configured"},
        }
    if not actor.platform_admin:
        visible_assets = store.assets.list_workspace_assets_for_principals(actor.owner_principals())
        if not visible_assets:
            return {
                "available": False,
                "observed_at": observed_at,
                "source": "control-plane-db",
                "data_plane": {"status": "unobserved", "reason": "no_visible_assets"},
            }
    return {
        "available": True,
        "observed_at": observed_at,
        "source": "control-plane-db",
        "data_plane": {"status": "unobserved", "reason": "flight_health_probe_not_configured"},
    }


def list_workspace_auth_providers(store: GovernanceUnitOfWork) -> list[dict[str, object]]:
    """Lists configured authentication providers for the workspace."""

    context = store.workspace.get_workspace()
    if context is None:
        return []
    return [redact_auth_provider(item) for item in store.workspace.list_auth_providers()]


def workspace_auth_provider_revision(store: GovernanceUnitOfWork) -> int:
    context = store.workspace.get_workspace()
    return 0 if context is None else store.workspace.get_auth_provider_revision()


def replace_workspace_auth_providers(
    store: GovernanceUnitOfWork,
    providers: list[dict[str, Any]],
    *,
    expected_revision: int | None = None,
    actor_principal: str = "system",
    correlation_id: str | None = None,
) -> list[dict[str, object]]:
    """Replaces the workspace authentication provider chain."""

    validate_auth_provider_payloads(providers)
    store.workspace.ensure_workspace()
    store.workspace.replace_auth_providers(providers=providers, expected_revision=expected_revision)
    store.audit.record_workspace_audit_event(
        actor_principal=actor_principal,
        action="workspace.auth_providers.update",
        resource_type="workspace",
        resource_id="workspace",
        details={
            "provider_count": len(providers),
            "enabled_count": sum(1 for provider in providers if provider.get("enabled", True)),
        },
        correlation_id=correlation_id,
    )
    return list_workspace_auth_providers(store)


def upsert_workspace_runtime_settings(
    store: GovernanceUnitOfWork,
    ttl: int,
    max_tickets: int,
    max_ticket_exchanges: int,
    path_rules: list[dict[str, Any]] | None = None,
    expected_revision: int | None = None,
    *,
    actor_principal: str = "system",
    correlation_id: str | None = None,
) -> dict[str, object]:
    """Creates or updates workspace runtime ticket settings."""

    if path_rules is not None and not isinstance(path_rules, list):
        raise ValidationFailure("Runtime path rules must be a list")
    raw_path_rules = path_rules or []
    if any(not isinstance(rule, dict) for rule in raw_path_rules):
        raise ValidationFailure("Runtime path rules must contain objects")
    normalized_path_rules = [dict(rule) for rule in raw_path_rules]
    if any(
        set(rule) != {"root"}
        or not isinstance(rule.get("root"), str)
        or not str(rule["root"]).strip()
        for rule in normalized_path_rules
    ):
        raise ValidationFailure("Runtime path rules require exactly one non-empty string root")
    try:
        PathRuleEnforcer(normalized_path_rules)
    except (TypeError, ValueError) as exc:
        raise ValidationFailure("Runtime path rules are invalid") from exc
    store.workspace.ensure_workspace()
    store.workspace.upsert_runtime_settings(
        ticket_ttl_seconds=ttl,
        max_tickets=max_tickets,
        max_ticket_exchanges=max_ticket_exchanges,
        path_rules=normalized_path_rules,
        expected_revision=expected_revision,
    )
    store.audit.record_workspace_audit_event(
        actor_principal=actor_principal,
        action="workspace.runtime.update",
        resource_type="workspace",
        resource_id="workspace",
        details={
            "ticket_ttl_seconds": ttl,
            "max_tickets": max_tickets,
            "max_ticket_exchanges": max_ticket_exchanges,
            "path_rules": normalized_path_rules,
        },
        correlation_id=correlation_id,
    )
    settings = store.workspace.get_runtime_settings()
    if settings is None:
        return {}
    raw_path_rules = settings.get("path_rules", [])
    path_rules = (
        cast(list[dict[str, Any]], raw_path_rules) if isinstance(raw_path_rules, list) else []
    )
    return {
        "ticket_ttl_seconds": settings["ticket_ttl_seconds"],
        "max_tickets": settings["max_tickets"],
        "max_ticket_exchanges": settings["max_ticket_exchanges"],
        "path_rules": list(path_rules),
        "revision": settings["revision"],
    }


class WorkspaceCommands:
    def __init__(
        self, work_factory: GovernanceUnitOfWorkFactory, *, identity_mapper: PreviewIdentityMapper
    ) -> None:
        self._work_factory = work_factory
        self._identity_mapper = identity_mapper

    def get_workspace_summary(
        self, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> dict[str, object]:
        with self._work_factory() as work:
            return get_workspace_summary(work, actor)

    def get_workspace_observations(
        self, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> dict[str, object]:
        with self._work_factory() as work:
            return get_workspace_observations(work, actor)

    def list_identity_attributes(
        self, asset_id: UUID, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> list[dict[str, object]]:
        with self._work_factory() as work:
            return identity_attributes.list_attribute_catalog(work, asset_id, actor)

    def get_workspace_runtime_settings(
        self, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> dict[str, object] | None:
        _require_admin(actor)
        with self._work_factory() as work:
            return get_workspace_runtime_settings(work)

    def list_workspace_auth_providers(
        self, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> list[dict[str, object]]:
        _require_admin(actor)
        with self._work_factory() as work:
            return list_workspace_auth_providers(work)

    def workspace_auth_provider_revision(
        self, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> int:
        _require_admin(actor)
        with self._work_factory() as work:
            return workspace_auth_provider_revision(work)

    def replace_workspace_auth_providers(
        self,
        providers: list[dict[str, Any]],
        *,
        expected_revision: int | None = None,
        actor: ControlPlaneActor,
        correlation_id: str | None,
    ) -> list[dict[str, object]]:
        _require_admin(actor)
        with self._work_factory() as work:
            result = replace_workspace_auth_providers(
                work,
                providers,
                expected_revision=expected_revision,
                actor_principal=actor.identity_key(),
                correlation_id=correlation_id,
            )
            work.commit()
            return result

    def upsert_workspace_runtime_settings(
        self,
        ttl: int,
        max_tickets: int,
        max_ticket_exchanges: int,
        path_rules: list[dict[str, Any]] | None = None,
        expected_revision: int | None = None,
        *,
        actor: ControlPlaneActor,
        correlation_id: str | None,
    ) -> dict[str, object]:
        _require_admin(actor)
        with self._work_factory() as work:
            result = upsert_workspace_runtime_settings(
                work,
                ttl,
                max_tickets,
                max_ticket_exchanges,
                path_rules,
                expected_revision,
                actor_principal=actor.identity_key(),
                correlation_id=correlation_id,
            )
            work.commit()
            fallback: dict[str, object] = {
                "ticket_ttl_seconds": ttl,
                "max_tickets": max_tickets,
                "max_ticket_exchanges": max_ticket_exchanges,
                "path_rules": path_rules,
                "revision": 0,
            }
            return result or fallback

    def preview_attribute_mapping(
        self,
        ordinal: int,
        claims: dict[str, object],
        provider_args: dict[str, Any] | None = None,
        *,
        actor: ControlPlaneActor,
        correlation_id: str | None,
    ) -> dict[str, object]:
        _require_admin(actor)
        with self._work_factory() as work:
            return identity_attributes.preview_attributes(
                work, ordinal, claims, provider_args, identity_mapper=self._identity_mapper
            )


def _require_admin(actor: ControlPlaneActor) -> None:
    if not actor.platform_admin:
        raise AuthorizationFailure("Platform admin required")
