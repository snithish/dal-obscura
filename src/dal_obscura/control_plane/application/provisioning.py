from __future__ import annotations

from datetime import datetime
from typing import Any, cast
from uuid import UUID

from dal_obscura_plugin_api import PluginKind
from sqlalchemy.orm import Session

from dal_obscura.common.plugin_api import PluginLifecycleState, PluginRegistry
from dal_obscura.control_plane.application import (
    asset_service,
    audit_service,
    catalog_service,
    evaluation_service,
    identity_attributes,
    policy_service,
    schema_service,
    workspace_service,
)
from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.control_plane.infrastructure.catalog_discovery import discover_catalog_tables
from dal_obscura.control_plane.infrastructure.repositories import ConfigStore
from dal_obscura.data_plane.infrastructure.adapters.secret_providers import SecretProvider


class ProvisioningService:
    """Application service used by the FastAPI control-plane routes."""

    def __init__(
        self,
        session: Session,
        *,
        catalog_egress_allowlist: tuple[str, ...] = (),
        plugin_registry: PluginRegistry | None = None,
        secret_provider: SecretProvider | None = None,
    ) -> None:
        self._store = ConfigStore(session)
        self._catalog_egress_allowlist = catalog_egress_allowlist
        self._plugin_registry = plugin_registry
        self._secret_provider = secret_provider

    def set_plugin_lifecycle(
        self,
        *,
        kind: str,
        plugin_id: str,
        target: PluginLifecycleState,
        actor: ControlPlaneActor,
    ) -> dict[str, str]:
        """Apply and audit one explicit process-local plugin lifecycle change."""

        if not actor.platform_admin:
            raise AuthorizationFailure("Platform admin required")
        if kind not in {"catalog", "table_format"}:
            raise ValidationFailure("Unsupported plugin kind")
        if self._plugin_registry is None:
            raise ValidationFailure("Plugin registry was not admitted during application startup")
        try:
            lifecycle = self._plugin_registry.set_lifecycle(
                cast(PluginKind, kind), plugin_id, target
            )
        except ValueError as exc:
            raise ValidationFailure(str(exc)) from exc
        self._store.ensure_workspace()
        self._store.record_workspace_audit_event(
            actor_principal=actor.identity_key(),
            action="plugin.lifecycle.update",
            resource_type="plugin",
            resource_id=f"{kind}:{plugin_id}",
            details={"target": lifecycle.value},
        )
        return {"kind": kind, "plugin_id": plugin_id, "lifecycle": lifecycle.value}

    def get_runtime_settings(
        self,
    ) -> dict[str, object] | None:
        return self._store.get_runtime_settings()

    def list_catalogs(
        self,
    ) -> list[dict[str, object]]:
        return self._store.list_catalogs()

    def list_assets(
        self,
    ) -> list[dict[str, object]]:
        return self._store.list_assets()

    def list_policy_rules(
        self,
        asset_id: UUID,
        *,
        actor: ControlPlaneActor | None = None,
    ) -> list[dict[str, object]]:
        return policy_service.list_policy_rules(self._store, asset_id, actor=actor)

    def list_auth_providers(
        self,
    ) -> list[dict[str, object]]:
        return self._store.list_auth_providers()

    def get_workspace_summary(
        self,
        actor: ControlPlaneActor | None = None,
    ) -> dict[str, object]:
        return workspace_service.get_workspace_summary(self._store, actor)

    def get_workspace_runtime_settings(self) -> dict[str, object] | None:
        return workspace_service.get_workspace_runtime_settings(self._store)

    def get_workspace_observations(
        self,
        actor: ControlPlaneActor,
    ) -> dict[str, object]:
        return workspace_service.get_workspace_observations(self._store, actor)

    def list_workspace_auth_providers(self) -> list[dict[str, object]]:
        return workspace_service.list_workspace_auth_providers(self._store)

    def list_audit_events_page(
        self,
        *,
        actor: ControlPlaneActor,
        asset_id: UUID | None = None,
        actor_filter: str | None = None,
        action: str | None = None,
        resource_type: str | None = None,
        outcome: str | None = None,
        correlation_id: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> dict[str, object]:
        return audit_service.list_audit_events_page(
            self._store,
            actor=actor,
            asset_id=asset_id,
            actor_filter=actor_filter,
            action=action,
            resource_type=resource_type,
            outcome=outcome,
            correlation_id=correlation_id,
            created_after=created_after,
            created_before=created_before,
            limit=limit,
            cursor=cursor,
        )

    def list_workspace_catalogs(self) -> list[dict[str, object]]:
        return catalog_service.list_workspace_catalogs(self._store)

    def discover_workspace_catalog_tables(
        self,
        name: str,
        *,
        actor: ControlPlaneActor | None = None,
    ) -> dict[str, object]:
        return catalog_service.discover_workspace_catalog_tables(
            self._store,
            name,
            discover=discover_catalog_tables,
            egress_allowlist=self._catalog_egress_allowlist,
            session_key=actor.identity_key() if actor is not None else None,
            plugin_registry=self._plugin_registry,
            secret_provider=self._secret_provider,
        )

    def diagnose_workspace_catalog(
        self,
        name: str,
        *,
        actor: ControlPlaneActor | None = None,
    ) -> dict[str, object]:
        return catalog_service.diagnose_workspace_catalog(
            self._store,
            name,
            discover=discover_catalog_tables,
            egress_allowlist=self._catalog_egress_allowlist,
            session_key=actor.identity_key() if actor is not None else None,
            plugin_registry=self._plugin_registry,
            secret_provider=self._secret_provider,
        )

    def list_workspace_assets(
        self,
        actor: ControlPlaneActor | None = None,
    ) -> list[dict[str, object]]:
        return asset_service.list_workspace_assets(self._store, actor)

    def list_workspace_assets_page(
        self,
        actor: ControlPlaneActor,
        *,
        limit: int,
        cursor: str | None = None,
        search: str | None = None,
    ) -> dict[str, object]:
        return asset_service.list_workspace_assets_page(
            self._store, actor, limit=limit, cursor=cursor, search=search
        )

    def get_workspace_asset(
        self,
        asset_id: UUID,
        actor: ControlPlaneActor | None = None,
    ) -> dict[str, object]:
        return asset_service.get_workspace_asset(self._store, asset_id, actor)

    def get_asset_access(
        self,
        asset_id: UUID,
        actor: ControlPlaneActor,
    ) -> dict[str, object]:
        return asset_service.get_asset_access(self._store, asset_id, actor)

    def get_asset_schema(
        self,
        asset_id: UUID,
        actor: ControlPlaneActor,
    ) -> dict[str, object]:
        return schema_service.get_asset_schema(
            self._store,
            asset_id,
            actor,
            egress_allowlist=self._catalog_egress_allowlist,
            plugin_registry=self._plugin_registry,
            secret_provider=self._secret_provider,
        )

    def upsert_runtime_settings(
        self,
        ttl: int,
        max_tickets: int,
        max_ticket_exchanges: int,
    ) -> None:
        self._store.upsert_runtime_settings(
            ticket_ttl_seconds=ttl,
            max_tickets=max_tickets,
            max_ticket_exchanges=max_ticket_exchanges,
        )

    def upsert_workspace_runtime_settings(
        self,
        ttl: int,
        max_tickets: int,
        max_ticket_exchanges: int,
        path_rules: list[dict[str, Any]] | None = None,
        expected_revision: int | None = None,
        actor: ControlPlaneActor | None = None,
    ) -> dict[str, object]:
        return workspace_service.upsert_workspace_runtime_settings(
            self._store,
            ttl=ttl,
            max_tickets=max_tickets,
            max_ticket_exchanges=max_ticket_exchanges,
            path_rules=path_rules,
            expected_revision=expected_revision,
            actor_principal="system" if actor is None else actor.identity_key(),
        )

    def upsert_catalog(
        self,
        name: str,
        plugin_id: str,
        options: dict[str, Any],
    ) -> dict[str, str]:
        catalog_id = self._store.upsert_catalog(name=name, plugin_id=plugin_id, options=options)
        return {"id": str(catalog_id), "name": name}

    def upsert_workspace_catalog(
        self,
        name: str,
        plugin_id: str,
        options: dict[str, Any],
        expected_revision: int | None = None,
        actor: ControlPlaneActor | None = None,
    ) -> dict[str, str]:
        return catalog_service.upsert_workspace_catalog(
            self._store,
            name=name,
            plugin_id=plugin_id,
            options=options,
            expected_revision=expected_revision,
            egress_allowlist=self._catalog_egress_allowlist,
            actor_principal="system" if actor is None else actor.identity_key(),
            plugin_registry=self._plugin_registry,
        )

    def upsert_asset(
        self,
        catalog: str,
        target: str,
        backend: str,
        table_identifier: str | None,
        options: dict[str, Any],
    ) -> dict[str, str]:
        asset_id = self._store.upsert_asset(
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
        expected_revision: int | None = None,
    ) -> dict[str, str]:
        return asset_service.upsert_workspace_asset(
            self._store,
            catalog=catalog,
            target=target,
            backend=backend,
            table_identifier=table_identifier,
            options=options,
            expected_revision=expected_revision,
            plugin_registry=self._plugin_registry,
        )

    def replace_policy_rules(
        self,
        asset_id: UUID,
        rules: list[dict[str, Any]],
        *,
        actor: ControlPlaneActor,
        expected_revision: int | None = None,
        revoke_existing_tokens: bool = False,
    ) -> dict[str, int]:
        return policy_service.replace_policy_rules(
            self._store,
            asset_id,
            rules,
            actor=actor,
            expected_revision=expected_revision,
            revoke_existing_tokens=revoke_existing_tokens,
        )

    def revoke_asset_tickets(
        self,
        asset_id: UUID,
        *,
        actor: ControlPlaneActor,
    ) -> dict[str, int]:
        return policy_service.revoke_asset_tickets(self._store, asset_id, actor=actor)

    def replace_asset_owners(
        self,
        asset_id: UUID,
        owners: list[str],
        expected_revision: int | None = None,
        actor: ControlPlaneActor | None = None,
    ) -> list[str]:
        return asset_service.replace_asset_owners(
            self._store, asset_id, owners, expected_revision=expected_revision, actor=actor
        )

    def list_asset_grants(self, asset_id: UUID) -> list[dict[str, str]]:
        return asset_service.list_asset_grants(self._store, asset_id)

    def lock_asset_for_update(self, asset_id: UUID) -> None:
        """Locks an asset before authorization that participates in a mutation.

        Grant-manager authorization must observe the same asset revision that
        the subsequent replacement writes.  Exposing the repository lock through
        the application service keeps that ordering out of the route adapter.
        """

        self._store.lock_asset_for_update(asset_id)

    def replace_asset_grants(
        self,
        asset_id: UUID,
        grants: list[dict[str, str]],
        expected_revision: int | None = None,
        actor: ControlPlaneActor | None = None,
    ) -> list[dict[str, str]]:
        return asset_service.replace_asset_grants(
            self._store, asset_id, grants, expected_revision=expected_revision, actor=actor
        )

    def ensure_asset_capability(
        self,
        asset_id: UUID,
        actor: ControlPlaneActor,
        capability: str,
    ) -> None:
        policy_service.ensure_asset_capability(self._store, asset_id, actor, capability)

    def replace_asset_schema_fields(
        self,
        asset_id: UUID,
        fields: list[dict[str, Any]],
        expected_revision: int | None = None,
    ) -> list[dict[str, object]]:
        return asset_service.replace_asset_schema_fields(
            self._store, asset_id, fields, expected_revision=expected_revision
        )

    def preview_asset_policy(
        self,
        asset_id: UUID,
        *,
        principal: str,
        groups: list[str],
        claims: dict[str, object],
        actor: ControlPlaneActor | None = None,
        requested_columns: list[str] | None = None,
    ) -> dict[str, object]:
        return policy_service.preview_asset_policy(
            self._store,
            asset_id,
            principal=principal,
            groups=groups,
            claims=claims,
            actor=actor,
            requested_columns=requested_columns,
        )

    def list_attribute_catalog(
        self, asset_id: UUID, actor: ControlPlaneActor
    ) -> list[dict[str, object]]:
        policy_service.ensure_asset_reader(self._store, asset_id, actor)
        return identity_attributes.attribute_catalog(self._store)

    def preview_identity_attributes(
        self, ordinal: int, claims: dict[str, object], provider_args: dict[str, Any] | None = None
    ) -> dict[str, object]:
        return identity_attributes.preview_attributes(self._store, ordinal, claims, provider_args)

    def evaluate_asset_policy(
        self,
        asset_id: UUID,
        actor: ControlPlaneActor,
        *,
        principal: str,
        groups: list[str],
        claims: dict[str, object],
        rows: list[dict[str, object]] | None,
        provider_ordinal: int | None = None,
    ) -> dict[str, object]:
        return evaluation_service.evaluate_asset_policy(
            self._store,
            asset_id,
            actor,
            principal=principal,
            groups=groups,
            claims=claims,
            rows=rows,
            provider_ordinal=provider_ordinal,
            egress_allowlist=self._catalog_egress_allowlist,
            plugin_registry=self._plugin_registry,
            secret_provider=self._secret_provider,
        )

    def replace_auth_providers(self, providers: list[dict[str, Any]]) -> None:
        self._store.replace_auth_providers(providers=providers)

    def replace_workspace_auth_providers(
        self,
        providers: list[dict[str, Any]],
        expected_revision: int | None = None,
        actor: ControlPlaneActor | None = None,
    ) -> list[dict[str, object]]:
        return workspace_service.replace_workspace_auth_providers(
            self._store,
            providers,
            expected_revision=expected_revision,
            actor_principal="system" if actor is None else actor.identity_key(),
        )

    def workspace_auth_provider_revision(self) -> int:
        return workspace_service.workspace_auth_provider_revision(self._store)

    def _required_workspace(self):
        return workspace_service.required_workspace(self._store)

    def _ensure_policy_editor(self, asset_id: UUID, actor: ControlPlaneActor) -> None:
        policy_service.ensure_policy_editor(self._store, asset_id, actor)
