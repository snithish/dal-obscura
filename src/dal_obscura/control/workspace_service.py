"""Workspace-level control-plane service functions.

Example:
    ```python
    summary = get_workspace_summary(store)
    ```
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, cast

from sqlalchemy.orm import Session

from dal_obscura.control.access import ControlPlaneActor
from dal_obscura.control.auth_provider_validation import (
    redact_auth_provider,
    validate_auth_provider_payloads,
)
from dal_obscura.control.errors import ValidationFailure
from dal_obscura.sources.paths import PathRuleEnforcer
from dal_obscura.storage import assets as _db_assets
from dal_obscura.storage import audit as _db_audit
from dal_obscura.storage import workspace as _db_workspace


def required_workspace(store: Session):
    """Returns the default workspace context or raises when none exists.

    Example:
        ```python
        context = required_workspace_context(store)
        ```
    """

    context = _db_workspace.get_workspace(store)
    if context is None:
        raise LookupError("No workspace has been configured")
    return context


def get_workspace_summary(
    store: Session,
    actor: ControlPlaneActor | None = None,
) -> dict[str, object]:
    """Returns a summary of the current workspace.

    Example:
        ```python
        summary = get_workspace_summary(store)
        ```
    """

    return _db_workspace.get_workspace_summary(
        store, None if actor is None or actor.platform_admin else actor.owner_principals()
    )


def get_workspace_runtime_settings(store: Session) -> dict[str, object] | None:
    """Returns runtime ticket settings for the workspace when configured.

    Example:
        ```python
        settings = get_workspace_runtime_settings(store)
        ```
    """

    context = _db_workspace.get_workspace(store)
    if context is None:
        return None
    settings = _db_workspace.get_runtime_settings(store)
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
    store: Session,
    actor: ControlPlaneActor,
) -> dict[str, object]:
    """Returns bounded control-plane observations for the current workspace.

    This endpoint deliberately reports what the control-plane database knows;
    it never presents a configuration revision as proof that a Flight worker
    is healthy or has reloaded changed startup settings.
    """

    observed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    context = _db_workspace.get_workspace(store)
    if context is None:
        return {
            "available": False,
            "observed_at": observed_at,
            "source": "control-plane-db",
            "data_plane": {"status": "unobserved", "reason": "workspace_not_configured"},
        }
    if not actor.platform_admin:
        visible_assets = _db_assets.list_workspace_assets_for_principals(
            store, actor.owner_principals()
        )
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


def list_workspace_auth_providers(store: Session) -> list[dict[str, object]]:
    """Lists configured authentication providers for the workspace.

    Example:
        ```python
        providers = list_workspace_auth_providers(store)
        ```
    """

    context = _db_workspace.get_workspace(store)
    if context is None:
        return []
    return [redact_auth_provider(item) for item in _db_workspace.list_auth_providers(store)]


def workspace_auth_provider_revision(store: Session) -> int:
    context = _db_workspace.get_workspace(store)
    return 0 if context is None else _db_workspace.get_auth_provider_revision(store)


def replace_workspace_auth_providers(
    store: Session,
    providers: list[dict[str, Any]],
    *,
    expected_revision: int | None = None,
    actor_principal: str = "system",
) -> list[dict[str, object]]:
    """Replaces the workspace authentication provider chain.

    Example:
        ```python
        replace_workspace_auth_providers(store, [{"module": "example.Provider"}])
        ```
    """

    validate_auth_provider_payloads(providers)
    _db_workspace.ensure_workspace(store)
    _db_workspace.replace_auth_providers(
        store, providers=providers, expected_revision=expected_revision
    )
    _db_audit.record_workspace_audit_event(
        store,
        actor_principal=actor_principal,
        action="workspace.auth_providers.update",
        resource_type="workspace",
        resource_id="workspace",
        details={
            "provider_count": len(providers),
            "enabled_count": sum(1 for provider in providers if provider.get("enabled", True)),
        },
    )
    return list_workspace_auth_providers(store)


def upsert_workspace_runtime_settings(
    store: Session,
    ttl: int,
    max_tickets: int,
    max_ticket_exchanges: int,
    path_rules: list[dict[str, Any]] | None = None,
    expected_revision: int | None = None,
    *,
    actor_principal: str = "system",
) -> dict[str, object]:
    """Creates or updates workspace runtime ticket settings.

    Example:
        ```python
        upsert_workspace_runtime_settings(store, 300, 32, 1)
        ```
    """

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
    _db_workspace.ensure_workspace(store)
    _db_workspace.upsert_runtime_settings(
        store,
        ticket_ttl_seconds=ttl,
        max_tickets=max_tickets,
        max_ticket_exchanges=max_ticket_exchanges,
        path_rules=normalized_path_rules,
        expected_revision=expected_revision,
    )
    _db_audit.record_workspace_audit_event(
        store,
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
    )
    settings = _db_workspace.get_runtime_settings(store)
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
