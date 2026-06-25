from __future__ import annotations

from uuid import UUID, uuid4

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.compiler import PublicationCompiler
from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.application.policy_service import ensure_policy_editor
from dal_obscura.control_plane.domain.models import CompiledCatalog, CompiledPublication
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore


def list_workspace_publications(store: PublicationStore) -> list[dict[str, object]]:
    context = store.get_default_workspace_context()
    if context is None:
        return []
    return [_publication_list_response(item) for item in store.list_publications(context.cell_id)]


def list_policy_version_history(store: PublicationStore) -> list[dict[str, object]]:
    context = store.get_default_workspace_context()
    if context is None:
        return []
    return store.list_policy_version_history(context)


def create_workspace_publication(
    store: PublicationStore,
    create_publication,
) -> dict[str, object]:
    context = _required_workspace_context(store)
    publication = create_publication(context.cell_id)
    return {
        "publication_id": publication["publication_id"],
        "asset_count": publication["asset_count"],
        "catalog_count": publication["catalog_count"],
        "manifest_hash": publication["manifest_hash"],
    }


def create_asset_policy_version(
    store: PublicationStore,
    asset_id: UUID,
    *,
    actor: ControlPlaneActor,
    create_publication,
    activate_publication,
) -> dict[str, object]:
    ensure_policy_editor(store, asset_id, actor)
    asset, catalog = store.load_asset_publish_draft(asset_id)
    if not asset.rules:
        raise ValidationFailure("Cannot publish a policy version without policy rules.")
    compiler = PublicationCompiler()
    compiled_asset = compiler.compile_asset(asset, catalog)
    try:
        active = store.load_active_compiled_publication(asset.cell_id)
    except LookupError:
        publication = create_publication(asset.cell_id)
        activate_publication(
            cell_id=asset.cell_id,
            publication_id=UUID(str(publication["publication_id"])),
        )
        return {
            "asset_id": str(asset.id),
            "policy_version": compiled_asset.policy_version,
        }

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
    store.activate_publication(cell_id=asset.cell_id, publication_id=publication_id)
    return {
        "asset_id": str(asset.id),
        "policy_version": compiled_asset.policy_version,
    }


def create_publication(store: PublicationStore, cell_id: UUID) -> dict[str, object]:
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


def _validate_publish_readiness(store: PublicationStore, draft) -> None:
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
    missing_policy_count = sum(1 for asset in draft.assets if not asset.rules)
    if missing_policy_count:
        raise ValidationFailure(
            f"Cannot publish until {missing_policy_count} "
            f"{_plural(missing_policy_count, 'asset has', 'assets have')} policy rules."
        )


def _plural(count: int, singular: str, plural: str) -> str:
    return singular if count == 1 else plural
