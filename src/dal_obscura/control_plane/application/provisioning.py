from __future__ import annotations

from typing import Any, cast
from uuid import UUID, uuid4

from sqlalchemy.orm import Session

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.compiler import PublicationCompiler
from dal_obscura.control_plane.application.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.control_plane.domain.models import CompiledCatalog, CompiledPublication
from dal_obscura.control_plane.infrastructure.catalog_discovery import discover_catalog_tables
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore


class ProvisioningService:
    """Application service used by the FastAPI control-plane routes."""

    def __init__(self, session: Session) -> None:
        self._store = PublicationStore(session)

    def create_tenant(self, slug: str, display_name: str) -> dict[str, str]:
        tenant_id = uuid4()
        self._store.create_tenant(tenant_id=tenant_id, slug=slug, display_name=display_name)
        return {"id": str(tenant_id), "slug": slug, "display_name": display_name}

    def create_cell(self, name: str, region: str) -> dict[str, str]:
        cell_id = uuid4()
        self._store.create_cell(cell_id=cell_id, name=name, region=region)
        return {"id": str(cell_id), "name": name, "region": region}

    def create_cell_for_tenant(
        self,
        tenant_id: UUID,
        name: str,
        region: str,
        shard_key: str,
    ) -> dict[str, str]:
        cell = self.create_cell(name=name, region=region)
        self.assign_tenant(UUID(cell["id"]), tenant_id, shard_key)
        return cell

    def list_tenants(self) -> list[dict[str, str]]:
        return self._store.list_tenants()

    def list_cells(self) -> list[dict[str, str]]:
        return self._store.list_cells()

    def list_cells_for_tenant(self, tenant_id: UUID) -> list[dict[str, str]]:
        return self._store.list_cells_for_tenant(tenant_id)

    def list_cell_tenant_assignments(self) -> list[dict[str, str]]:
        return self._store.list_cell_tenant_assignments()

    def get_runtime_settings(self, cell_id: UUID) -> dict[str, object] | None:
        return self._store.get_runtime_settings(cell_id)

    def list_catalogs(self, cell_id: UUID) -> list[dict[str, object]]:
        return self._store.list_catalogs(cell_id)

    def list_assets(self, cell_id: UUID) -> list[dict[str, object]]:
        return self._store.list_assets(cell_id)

    def list_policy_rules(self, asset_id: UUID) -> list[dict[str, object]]:
        return self._store.list_policy_rules(asset_id)

    def list_auth_providers(self, cell_id: UUID) -> list[dict[str, object]]:
        return self._store.list_auth_providers(cell_id)

    def get_cell_draft(self, cell_id: UUID) -> dict[str, object]:
        return self._store.get_cell_draft(cell_id)

    def list_publications(self, cell_id: UUID) -> list[dict[str, object]]:
        return self._store.list_publications(cell_id)

    def get_active_publication_summary(self, cell_id: UUID) -> dict[str, str]:
        return self._store.get_active_publication_summary(cell_id)

    def get_workspace_summary(self) -> dict[str, object]:
        context = self._store.get_default_workspace_context()
        return self._store.get_workspace_summary(context)

    def get_workspace_runtime_settings(self) -> dict[str, object] | None:
        context = self._store.get_default_workspace_context()
        if context is None:
            return None
        settings = self._store.get_runtime_settings(context.cell_id)
        if settings is None:
            return None
        return {
            "ticket_ttl_seconds": settings["ticket_ttl_seconds"],
            "max_tickets": settings["max_tickets"],
            "max_ticket_exchanges": settings["max_ticket_exchanges"],
        }

    def list_workspace_auth_providers(self) -> list[dict[str, object]]:
        context = self._store.get_default_workspace_context()
        if context is None:
            return []
        return [
            _auth_provider_response(item)
            for item in self._store.list_auth_providers(context.cell_id)
        ]

    def list_workspace_publications(self) -> list[dict[str, object]]:
        context = self._store.get_default_workspace_context()
        if context is None:
            return []
        return [
            _publication_list_response(item)
            for item in self._store.list_publications(context.cell_id)
        ]

    def list_workspace_catalogs(self) -> list[dict[str, object]]:
        context = self._store.get_default_workspace_context()
        if context is None:
            return []
        return self._store.list_workspace_catalogs(context)

    def discover_workspace_catalog_tables(self, name: str) -> dict[str, object]:
        context = self._required_workspace_context()
        catalog = self._store.get_workspace_catalog(context, name)
        catalog_options = cast(dict[str, Any], catalog["options"])
        tables = discover_catalog_tables(
            str(catalog["name"]),
            str(catalog["module"]),
            catalog_options,
        )
        governed_targets = {
            value
            for asset in self._store.list_workspace_assets(context)
            if asset["catalog"] == catalog["name"]
            for value in (asset["name"], asset["table_identifier"])
            if isinstance(value, str) and value
        }
        return {
            "catalog": catalog["name"],
            "tables": [
                {
                    **table,
                    "target": table["name"],
                    "governed": table["name"] in governed_targets
                    or table["table_identifier"] in governed_targets,
                }
                for table in tables
            ],
        }

    def list_workspace_assets(self) -> list[dict[str, object]]:
        context = self._store.get_default_workspace_context()
        if context is None:
            return []
        return self._store.list_workspace_assets(context)

    def get_workspace_asset(self, asset_id: UUID) -> dict[str, object]:
        return self._store.get_workspace_asset(asset_id)

    def get_workspace_draft(self) -> dict[str, object]:
        context = self._required_workspace_context()
        return self._store.get_workspace_draft(context)

    def create_workspace_publication(self) -> dict[str, object]:
        context = self._required_workspace_context()
        publication = self.create_publication(context.cell_id)
        return {
            "publication_id": publication["publication_id"],
            "asset_count": publication["asset_count"],
            "catalog_count": publication["catalog_count"],
            "manifest_hash": publication["manifest_hash"],
        }

    def create_asset_policy_version(
        self,
        asset_id: UUID,
        *,
        actor: ControlPlaneActor,
    ) -> dict[str, object]:
        self._ensure_policy_editor(asset_id, actor)
        asset, catalog = self._store.load_asset_publish_draft(asset_id)
        if not asset.rules:
            raise ValidationFailure("Cannot publish a policy version without policy rules.")
        compiler = PublicationCompiler()
        compiled_asset = compiler.compile_asset(asset, catalog)
        try:
            active = self._store.load_active_compiled_publication(asset.cell_id)
        except LookupError:
            publication = self.create_publication(asset.cell_id)
            self.activate_publication(
                cell_id=asset.cell_id,
                publication_id=UUID(str(publication["publication_id"])),
            )
            return {
                "asset_id": str(asset.id),
                "publication_id": publication["publication_id"],
                "policy_version": compiled_asset.policy_version,
                "manifest_hash": publication["manifest_hash"],
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
        self._store.insert_compiled_publication(
            publication_id=publication_id,
            compiled=compiled,
        )
        self._store.activate_publication(cell_id=asset.cell_id, publication_id=publication_id)
        return {
            "asset_id": str(asset.id),
            "publication_id": str(publication_id),
            "policy_version": compiled_asset.policy_version,
            "manifest_hash": compiled.manifest_hash,
        }

    def activate_workspace_publication(self, publication_id: UUID) -> dict[str, str]:
        context = self._required_workspace_context()
        activated = self.activate_publication(
            cell_id=context.cell_id,
            publication_id=publication_id,
        )
        return {"publication_id": activated["publication_id"]}

    def assign_tenant(self, cell_id: UUID, tenant_id: UUID, shard_key: str) -> None:
        self._store.assign_tenant_to_cell(
            cell_id=cell_id,
            tenant_id=tenant_id,
            shard_key=shard_key,
        )

    def upsert_runtime_settings(
        self,
        cell_id: UUID,
        ttl: int,
        max_tickets: int,
        max_ticket_exchanges: int,
    ) -> None:
        self._store.upsert_runtime_settings(
            cell_id=cell_id,
            ticket_ttl_seconds=ttl,
            max_tickets=max_tickets,
            max_ticket_exchanges=max_ticket_exchanges,
        )

    def upsert_workspace_runtime_settings(
        self,
        ttl: int,
        max_tickets: int,
        max_ticket_exchanges: int,
    ) -> None:
        context = self._store.ensure_default_workspace_context()
        self.upsert_runtime_settings(
            cell_id=context.cell_id,
            ttl=ttl,
            max_tickets=max_tickets,
            max_ticket_exchanges=max_ticket_exchanges,
        )

    def upsert_catalog(
        self,
        cell_id: UUID,
        tenant_id: UUID,
        name: str,
        module: str,
        options: dict[str, Any],
    ) -> dict[str, str]:
        catalog_id = self._store.upsert_catalog(
            cell_id=cell_id,
            tenant_id=tenant_id,
            name=name,
            module=module,
            options=options,
        )
        return {"id": str(catalog_id), "name": name}

    def upsert_workspace_catalog(
        self,
        name: str,
        module: str,
        options: dict[str, Any],
    ) -> dict[str, str]:
        context = self._store.ensure_default_workspace_context()
        return self.upsert_catalog(
            cell_id=context.cell_id,
            tenant_id=context.tenant_id,
            name=name,
            module=module,
            options=options,
        )

    def upsert_asset(
        self,
        cell_id: UUID,
        tenant_id: UUID,
        catalog: str,
        target: str,
        backend: str,
        table_identifier: str | None,
        options: dict[str, Any],
    ) -> dict[str, str]:
        asset_id = self._store.upsert_asset(
            cell_id=cell_id,
            tenant_id=tenant_id,
            catalog=catalog,
            target=target,
            backend=backend,
            table_identifier=table_identifier,
            options=options,
        )
        return {"id": str(asset_id), "catalog": catalog, "target": target}

    def upsert_workspace_asset(
        self,
        catalog: str,
        target: str,
        backend: str,
        table_identifier: str | None,
        options: dict[str, Any],
    ) -> dict[str, str]:
        context = self._required_workspace_context()
        return self.upsert_asset(
            cell_id=context.cell_id,
            tenant_id=context.tenant_id,
            catalog=catalog,
            target=target,
            backend=backend,
            table_identifier=table_identifier,
            options=options,
        )

    def replace_policy_rules(
        self,
        asset_id: UUID,
        rules: list[dict[str, Any]],
        *,
        actor: ControlPlaneActor,
    ) -> None:
        self._ensure_policy_editor(asset_id, actor)
        self._store.replace_policy_rules(asset_id=asset_id, rules=rules)

    def replace_asset_owners(self, asset_id: UUID, owners: list[str]) -> list[str]:
        return self._store.replace_asset_owners(asset_id=asset_id, owners=owners)

    def replace_asset_schema_fields(
        self,
        asset_id: UUID,
        fields: list[dict[str, Any]],
    ) -> list[dict[str, object]]:
        return self._store.replace_asset_schema_fields(asset_id=asset_id, fields=fields)

    def replace_auth_providers(self, cell_id: UUID, providers: list[dict[str, Any]]) -> None:
        self._store.replace_auth_providers(cell_id=cell_id, providers=providers)

    def replace_workspace_auth_providers(self, providers: list[dict[str, Any]]) -> None:
        context = self._store.ensure_default_workspace_context()
        self.replace_auth_providers(cell_id=context.cell_id, providers=providers)

    def create_publication(self, cell_id: UUID) -> dict[str, object]:
        draft = self._store.load_publish_draft(cell_id)
        self._validate_publish_readiness(draft)
        compiled = PublicationCompiler().compile(draft)
        publication_id = uuid4()
        self._store.insert_compiled_publication(
            publication_id=publication_id,
            compiled=compiled,
        )
        return _publication_response(publication_id, compiled)

    def activate_publication(self, cell_id: UUID, publication_id: UUID) -> dict[str, str]:
        self._store.activate_publication(cell_id=cell_id, publication_id=publication_id)
        return {"cell_id": str(cell_id), "publication_id": str(publication_id)}

    def _required_workspace_context(self):
        context = self._store.get_default_workspace_context()
        if context is None:
            raise LookupError("No workspace has been configured")
        return context

    def _ensure_policy_editor(self, asset_id: UUID, actor: ControlPlaneActor) -> None:
        if actor.platform_admin:
            return
        owners = set(self._store.list_asset_owners(asset_id))
        if owners.intersection(actor.owner_principals()):
            return
        raise AuthorizationFailure("Only platform admins or asset owners can change policies.")

    def _validate_publish_readiness(self, draft) -> None:
        if len(draft.catalogs) == 0:
            raise ValidationFailure("Cannot publish until at least one catalog is configured.")
        if len(draft.assets) == 0:
            raise ValidationFailure("Cannot publish until at least one asset is promoted.")
        if not any(provider.enabled for provider in draft.auth_providers):
            raise ValidationFailure("Cannot publish until at least one auth provider is enabled.")
        missing_owner_count = sum(
            1 for asset in draft.assets if not self._store.list_asset_owners(asset.id)
        )
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


def _publication_response(publication_id: UUID, compiled: CompiledPublication) -> dict[str, object]:
    return {
        "publication_id": str(publication_id),
        "cell_id": str(compiled.cell_id),
        "asset_count": len(compiled.assets),
        "catalog_count": len(compiled.catalogs),
        "manifest_hash": compiled.manifest_hash,
    }


def _auth_provider_response(provider: dict[str, object]) -> dict[str, object]:
    return {
        "id": provider["id"],
        "ordinal": provider["ordinal"],
        "module": provider["module"],
        "args": provider["args"],
        "enabled": provider["enabled"],
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


def _plural(count: int, singular: str, plural: str) -> str:
    return singular if count == 1 else plural
