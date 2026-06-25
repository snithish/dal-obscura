from __future__ import annotations

from typing import Any
from uuid import UUID

from dal_obscura.control_plane.infrastructure.repositories import PublicationStore


def required_workspace_context(store: PublicationStore):
    context = store.get_default_workspace_context()
    if context is None:
        raise LookupError("No workspace has been configured")
    return context


def get_workspace_summary(store: PublicationStore) -> dict[str, object]:
    context = store.get_default_workspace_context()
    return store.get_workspace_summary(context)


def get_workspace_runtime_settings(store: PublicationStore) -> dict[str, object] | None:
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


def get_workspace_draft(store: PublicationStore) -> dict[str, object]:
    context = required_workspace_context(store)
    return store.get_workspace_draft(context)


def list_workspace_auth_providers(store: PublicationStore) -> list[dict[str, object]]:
    context = store.get_default_workspace_context()
    if context is None:
        return []
    return [_auth_provider_response(item) for item in store.list_auth_providers(context.cell_id)]


def replace_workspace_auth_providers(
    store: PublicationStore,
    providers: list[dict[str, Any]],
) -> None:
    context = store.ensure_default_workspace_context()
    store.replace_auth_providers(cell_id=context.cell_id, providers=providers)


def upsert_workspace_runtime_settings(
    store: PublicationStore,
    ttl: int,
    max_tickets: int,
    max_ticket_exchanges: int,
) -> None:
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
