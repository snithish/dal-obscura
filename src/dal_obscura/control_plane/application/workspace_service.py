"""Workspace-level control-plane service functions.

Example:
    ```python
    summary = get_workspace_summary(store)
    ```
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore


def required_workspace_context(store: PublicationStore):
    """Returns the default workspace context or raises when none exists.

    Example:
        ```python
        context = required_workspace_context(store)
        ```
    """

    context = store.get_default_workspace_context()
    if context is None:
        raise LookupError("No workspace has been configured")
    return context


def get_workspace_summary(
    store: PublicationStore,
    actor: ControlPlaneActor | None = None,
) -> dict[str, object]:
    """Returns a summary of the current workspace.

    Example:
        ```python
        summary = get_workspace_summary(store)
        ```
    """

    context = store.get_default_workspace_context()
    if context is None:
        return store.get_workspace_summary(None)
    if actor is None or actor.platform_admin:
        return store.get_workspace_summary(context)
    assets = store.list_workspace_assets_for_principals(context, actor.owner_principals())
    return {
        "catalog_count": len({str(asset["catalog"]) for asset in assets}),
        "asset_count": len(assets),
        "unowned_asset_count": sum(1 for asset in assets if asset["owner_count"] == 0),
        "missing_policy_count": sum(1 for asset in assets if asset["policy_status"] == "missing"),
        "draft_change_count": len(assets),
        "runtime_configured": False,
        "enabled_auth_provider_count": 0,
    }


def get_workspace_runtime_settings(store: PublicationStore) -> dict[str, object] | None:
    """Returns runtime ticket settings for the workspace when configured.

    Example:
        ```python
        settings = get_workspace_runtime_settings(store)
        ```
    """

    context = store.get_default_workspace_context()
    if context is None:
        return None
    settings = store.get_runtime_settings(context.cell_id)
    if settings is None:
        return None
    return {
        "ticket_ttl_seconds": settings["ticket_ttl_seconds"],
        "max_tickets": settings["max_tickets"],
        "max_ticket_exchanges": settings["max_ticket_exchanges"],
    }


def get_workspace_observations(
    store: PublicationStore,
    actor: ControlPlaneActor,
) -> dict[str, object]:
    """Returns bounded control-plane observations for the current workspace.

    This endpoint deliberately reports what the control-plane database knows;
    it never presents a publication record as proof that a Flight worker is
    healthy or serving that generation.
    """

    observed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    context = store.get_default_workspace_context()
    if context is None:
        return {
            "available": False,
            "observed_at": observed_at,
            "source": "control-plane-db",
            "generation": None,
            "data_plane": {"status": "unobserved", "reason": "workspace_not_configured"},
        }
    if not actor.platform_admin:
        visible_assets = store.list_workspace_assets_for_principals(
            context,
            actor.owner_principals(),
        )
        if not visible_assets:
            return {
                "available": False,
                "observed_at": observed_at,
                "source": "control-plane-db",
                "generation": None,
                "data_plane": {"status": "unobserved", "reason": "no_visible_assets"},
            }
    try:
        generation: dict[str, str] | None = store.get_active_publication_summary(
            context.cell_id
        )
    except LookupError:
        generation = None
    return {
        "available": True,
        "observed_at": observed_at,
        "source": "control-plane-db",
        "generation": generation,
        "data_plane": {
            "status": "unobserved",
            "reason": "flight_health_probe_not_configured",
        },
    }


def get_workspace_draft(store: PublicationStore) -> dict[str, object]:
    """Returns the draft publication state for the workspace.

    Example:
        ```python
        draft = get_workspace_draft(store)
        ```
    """

    context = required_workspace_context(store)
    return store.get_workspace_draft(context)


def list_workspace_auth_providers(store: PublicationStore) -> list[dict[str, object]]:
    """Lists configured authentication providers for the workspace.

    Example:
        ```python
        providers = list_workspace_auth_providers(store)
        ```
    """

    context = store.get_default_workspace_context()
    if context is None:
        return []
    return [_auth_provider_response(item) for item in store.list_auth_providers(context.cell_id)]


def replace_workspace_auth_providers(
    store: PublicationStore,
    providers: list[dict[str, Any]],
) -> None:
    """Replaces the workspace authentication provider chain.

    Example:
        ```python
        replace_workspace_auth_providers(store, [{"module": "example.Provider"}])
        ```
    """

    context = store.ensure_default_workspace_context()
    store.replace_auth_providers(cell_id=context.cell_id, providers=providers)


def upsert_workspace_runtime_settings(
    store: PublicationStore,
    ttl: int,
    max_tickets: int,
    max_ticket_exchanges: int,
) -> None:
    """Creates or updates workspace runtime ticket settings.

    Example:
        ```python
        upsert_workspace_runtime_settings(store, 300, 32, 1)
        ```
    """

    context = store.ensure_default_workspace_context()
    store.upsert_runtime_settings(
        cell_id=context.cell_id,
        ticket_ttl_seconds=ttl,
        max_tickets=max_tickets,
        max_ticket_exchanges=max_ticket_exchanges,
    )


def activate_workspace_publication(
    store: PublicationStore,
    activate_publication,
    publication_id: UUID,
) -> dict[str, str]:
    """Activates an existing publication for the workspace.

    Example:
        ```python
        result = activate_workspace_publication(store, activate_publication, publication_id)
        ```
    """

    context = required_workspace_context(store)
    activated = activate_publication(cell_id=context.cell_id, publication_id=publication_id)
    return {"publication_id": activated["publication_id"]}


def _auth_provider_response(provider: dict[str, object]) -> dict[str, object]:
    return {
        "id": provider["id"],
        "ordinal": provider["ordinal"],
        "module": provider["module"],
        "args": provider["args"],
        "enabled": provider["enabled"],
    }
