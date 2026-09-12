"""Policy-version and publication service functions.

Example:
    ```python
    versions = list_policy_version_history(store)
    ```
"""

from __future__ import annotations

import hashlib
import json
from typing import cast
from uuid import UUID, uuid4

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.compiler import PublicationCompiler
from dal_obscura.control_plane.application.errors import PublicationConflictError, ValidationFailure
from dal_obscura.control_plane.application.policy_service import ensure_asset_capability
from dal_obscura.control_plane.application.review_service import verify_review_token
from dal_obscura.control_plane.domain.models import CompiledCatalog, CompiledPublication
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore


def list_workspace_publications(store: PublicationStore) -> list[dict[str, object]]:
    """Lists publications for the default workspace.

    Example:
        ```python
        publications = list_workspace_publications(store)
        ```
    """

    context = store.get_default_workspace_context()
    if context is None:
        return []
    return [_publication_list_response(item) for item in store.list_publications(context.cell_id)]


def list_policy_version_history(
    store: PublicationStore,
    *,
    actor: ControlPlaneActor | None = None,
) -> list[dict[str, object]]:
    """Lists asset-scoped policy-version history for the workspace.

    Example:
        ```python
        history = list_policy_version_history(store)
        ```
    """

    context = store.get_default_workspace_context()
    if context is None:
        return []
    history = store.list_policy_version_history(context)
    if actor is None or actor.platform_admin:
        return history
    visible_asset_ids = {
        str(asset["id"])
        for asset in store.list_workspace_assets_for_principals(context, actor.owner_principals())
    }
    return [item for item in history if str(item.get("asset_id")) in visible_asset_ids]


def list_asset_policy_version_history(
    store: PublicationStore,
    asset_id: UUID,
    *,
    actor: ControlPlaneActor,
) -> list[dict[str, object]]:
    """Lists immutable versions for one asset after capability enforcement."""

    ensure_asset_capability(store, asset_id, actor, "read")
    return [
        item
        for item in list_policy_version_history(store, actor=actor)
        if str(item.get("asset_id")) == str(asset_id)
    ]


def get_asset_policy_version(
    store: PublicationStore,
    asset_id: UUID,
    policy_version: int,
    *,
    actor: ControlPlaneActor,
) -> dict[str, object]:
    """Returns an immutable policy body without exposing compiled catalog secrets."""

    ensure_asset_capability(store, asset_id, actor, "read")
    historical = store.get_published_asset_policy(
        asset_id=asset_id,
        policy_version=policy_version,
    )
    return {
        "asset_id": str(asset_id),
        "policy_version": historical["policy_version"],
        "rules": historical["rules"],
    }


def create_workspace_publication(
    store: PublicationStore,
    create_publication,
) -> dict[str, object]:
    """Creates a full workspace publication snapshot.

    Example:
        ```python
        publication = create_workspace_publication(store, create_publication)
        ```
    """

    context = _required_workspace_context(store)
    publication = create_publication(context.cell_id)
    return {
        "publication_id": publication["publication_id"],
        "asset_count": publication["asset_count"],
        "catalog_count": publication["catalog_count"],
        "manifest_hash": publication["manifest_hash"],
    }


def create_asset_policy_version(  # noqa: C901
    store: PublicationStore,
    asset_id: UUID,
    *,
    actor: ControlPlaneActor,
    create_publication,
    activate_publication,
    expected_draft_revision: int | None = None,
    expected_publication_id: UUID | None = None,
    review_token: str | None = None,
    require_review: bool = False,
    review_secret: str = "",
    idempotency_key: str | None = None,
) -> dict[str, object]:
    """Publishes and activates a new policy version for one asset.

    Example:
        ```python
        result = create_asset_policy_version(
            store,
            asset_id,
            actor=actor,
            create_publication=create_publication,
            activate_publication=activate_publication,
        )
        ```
    """

    ensure_asset_capability(store, asset_id, actor, "publish")
    # Serialize publication attempts for this asset in PostgreSQL. This closes
    # the read-then-create idempotency race across independent API processes.
    store.lock_asset_for_publication(asset_id)
    request_hash = _publication_request_hash(
        expected_draft_revision=expected_draft_revision,
        expected_publication_id=expected_publication_id,
        review_token=review_token,
    )
    if idempotency_key:
        existing = store.get_publication_operation(
            asset_id=asset_id,
            actor_principal=actor.principal,
            idempotency_key=idempotency_key,
        )
        if existing is not None:
            if existing["request_hash"] != request_hash:
                raise PublicationConflictError(
                    "Idempotency key was already used with a different publication request."
                )
            return cast(dict[str, object], existing["result"])
    personal_draft = store.get_asset_policy_draft(
        asset_id=asset_id,
        author_principal=actor.principal,
    )
    if expected_draft_revision is not None:
        current_revision = (
            0 if personal_draft is None else int(cast(int | str, personal_draft["revision"]))
        )
        if current_revision != expected_draft_revision:
            raise PublicationConflictError(
                "Policy draft revision changed; reread the draft before publishing."
            )
    if require_review:
        if not review_token:
            raise ValidationFailure("A current server review is required before publishing.")
        verify_review_token(
            store,
            asset_id,
            actor,
            review_token,
            secret=review_secret,
        )
    asset, catalog = store.load_asset_publish_draft(
        asset_id,
        author_principal=actor.principal if personal_draft is not None else None,
    )
    if not asset.rules and personal_draft is None:
        raise ValidationFailure("Cannot publish a policy version without policy rules.")
    compiler = PublicationCompiler()
    compiled_asset = compiler.compile_asset(asset, catalog)
    try:
        active = store.load_active_compiled_publication_config(asset.cell_id)
        active_pointer = store.get_active_publication(asset.cell_id)
    except LookupError:
        if expected_publication_id is not None:
            raise PublicationConflictError(
                "No active publication matches the requested generation."
            ) from None
        draft = store.load_publish_draft(asset.cell_id)
        selected = next((item for item in draft.assets if item.id == asset.id), None)
        if selected is None:
            raise ValidationFailure(
                "Asset is not part of the workspace publication draft."
            ) from None
        selected.rules = list(asset.rules)
        _validate_publish_readiness(
            store,
            draft,
            explicit_deny_all_asset_ids={asset.id} if personal_draft is not None else set(),
        )
        compiled = compiler.compile(draft)
        publication_id = uuid4()
        store.insert_compiled_publication(publication_id=publication_id, compiled=compiled)
        store.activate_initial_publication(
            cell_id=asset.cell_id,
            publication_id=publication_id,
        )
        store.record_asset_audit_event(
            asset_id=asset_id,
            actor_principal=actor.principal,
            action="policy.publication.activate",
            details={
                "policy_version": compiled_asset.policy_version,
                "publication_id": str(publication_id),
                "initial": True,
            },
        )
        result: dict[str, object] = {
            "asset_id": str(asset.id),
            "policy_version": compiled_asset.policy_version,
        }
        if idempotency_key:
            store.save_publication_operation(
                asset_id=asset_id,
                actor_principal=actor.principal,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                result=result,
            )
        return result
    if (
        expected_publication_id is not None
        and expected_publication_id != active_pointer.publication_id
    ):
        raise PublicationConflictError("Active publication changed; reread before publishing.")

    replaced = False
    assets = []
    for current in active.assets:
        if (
            current.tenant_id == compiled_asset.tenant_id
            and current.catalog == compiled_asset.catalog
            and current.target == compiled_asset.target
        ):
            assets.append(compiled_asset)
            replaced = True
        else:
            assets.append(current)
    if not replaced:
        assets.append(compiled_asset)
    catalogs = _catalogs_with_selected(active.catalogs, catalog)
    compiled = compiler.compile_policy_version(
        cell_id=asset.cell_id,
        runtime=active.runtime,
        catalogs=catalogs,
        assets=assets,
    )
    publication_id = uuid4()
    store.insert_compiled_publication(
        publication_id=publication_id,
        compiled=compiled,
    )
    store.activate_publication_if_current(
        cell_id=asset.cell_id,
        publication_id=publication_id,
        expected_publication_id=active_pointer.publication_id,
    )
    store.record_asset_audit_event(
        asset_id=asset_id,
        actor_principal=actor.principal,
        action="policy.publication.activate",
        details={
            "policy_version": compiled_asset.policy_version,
            "publication_id": str(publication_id),
            "initial": False,
        },
    )
    result: dict[str, object] = {
        "asset_id": str(asset.id),
        "policy_version": compiled_asset.policy_version,
    }
    if idempotency_key:
        store.save_publication_operation(
            asset_id=asset_id,
            actor_principal=actor.principal,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            result=result,
        )
    return result


def get_publication_operation(
    store: PublicationStore,
    asset_id: UUID,
    idempotency_key: str,
    *,
    actor: ControlPlaneActor,
) -> dict[str, object]:
    """Returns a caller-scoped committed publication operation."""

    ensure_asset_capability(store, asset_id, actor, "publish")
    if not 1 <= len(idempotency_key) <= 128:
        raise ValidationFailure("Idempotency key must contain between 1 and 128 characters")
    operation = store.get_publication_operation(
        asset_id=asset_id,
        actor_principal=actor.principal,
        idempotency_key=idempotency_key,
    )
    if operation is None:
        raise LookupError("No publication operation exists for this idempotency key")
    return {
        "id": operation["id"],
        "status": operation["status"],
        "result": operation["result"],
    }


def _publication_request_hash(
    *,
    expected_draft_revision: int | None,
    expected_publication_id: UUID | None,
    review_token: str | None,
) -> str:
    payload = {
        "expected_draft_revision": expected_draft_revision,
        "expected_publication_id": None
        if expected_publication_id is None
        else str(expected_publication_id),
        "review_token": review_token,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def create_publication(store: PublicationStore, cell_id: UUID) -> dict[str, object]:
    """Compiles and stores a publication for one cell.

    Example:
        ```python
        publication = create_publication(store, cell_id)
        ```
    """

    draft = store.load_publish_draft(cell_id)
    _validate_publish_readiness(store, draft)
    compiled = PublicationCompiler().compile(draft)
    publication_id = uuid4()
    store.insert_compiled_publication(
        publication_id=publication_id,
        compiled=compiled,
    )
    return _publication_response(publication_id, compiled)


def activate_publication(
    store: PublicationStore,
    cell_id: UUID,
    publication_id: UUID,
) -> dict[str, str]:
    """Marks a publication active for one cell.

    Example:
        ```python
        active = activate_publication(store, cell_id, publication_id)
        ```
    """

    store.activate_publication(cell_id=cell_id, publication_id=publication_id)
    return {"cell_id": str(cell_id), "publication_id": str(publication_id)}


def _required_workspace_context(store: PublicationStore):
    context = store.get_default_workspace_context()
    if context is None:
        raise LookupError("No workspace has been configured")
    return context


def _publication_response(publication_id: UUID, compiled: CompiledPublication) -> dict[str, object]:
    return {
        "publication_id": str(publication_id),
        "cell_id": str(compiled.cell_id),
        "asset_count": len(compiled.assets),
        "catalog_count": len(compiled.catalogs),
        "manifest_hash": compiled.manifest_hash,
    }


def _publication_list_response(publication: dict[str, object]) -> dict[str, object]:
    return {
        "id": publication["id"],
        "schema_version": publication["schema_version"],
        "status": publication["status"],
        "manifest_hash": publication["manifest_hash"],
        "active": publication["active"],
    }


def _catalogs_with_selected(active_catalogs, catalog):
    for active in active_catalogs:
        if active.tenant_id == catalog.tenant_id and active.catalog == catalog.name:
            return list(active_catalogs)
    return [
        *active_catalogs,
        CompiledCatalog(
            tenant_id=catalog.tenant_id,
            catalog=catalog.name,
            config={"module": catalog.module, "options": dict(catalog.options)},
        ),
    ]


def _validate_publish_readiness(
    store: PublicationStore,
    draft,
    *,
    explicit_deny_all_asset_ids: set[UUID] | None = None,
) -> None:
    if len(draft.catalogs) == 0:
        raise ValidationFailure("Cannot publish until at least one catalog is configured.")
    if len(draft.assets) == 0:
        raise ValidationFailure("Cannot publish until at least one asset is promoted.")
    if not any(provider.enabled for provider in draft.auth_providers):
        raise ValidationFailure("Cannot publish until at least one auth provider is enabled.")
    missing_owner_count = sum(1 for asset in draft.assets if not store.list_asset_owners(asset.id))
    if missing_owner_count:
        raise ValidationFailure(
            f"Cannot publish until {missing_owner_count} "
            f"{_plural(missing_owner_count, 'asset has', 'assets have')} an assigned owner."
        )
    explicit_deny_all_asset_ids = explicit_deny_all_asset_ids or set()
    missing_policy_count = sum(
        1
        for asset in draft.assets
        if not asset.rules and asset.id not in explicit_deny_all_asset_ids
    )
    if missing_policy_count:
        raise ValidationFailure(
            f"Cannot publish until {missing_policy_count} "
            f"{_plural(missing_policy_count, 'asset has', 'assets have')} policy rules."
        )


def _plural(count: int, singular: str, plural: str) -> str:
    return singular if count == 1 else plural
