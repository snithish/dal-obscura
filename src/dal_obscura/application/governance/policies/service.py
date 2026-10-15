"""Workspace asset-policy service functions."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from dal_obscura.application.governance.assets.access import (
    ensure_asset_capability,
    ensure_asset_owner,
)
from dal_obscura.application.governance.policies import evaluation as evaluation_service
from dal_obscura.application.governance.policies.compiler import compile_policy_rule_payloads
from dal_obscura.application.governance.ports import (
    AssetSchemaLoader,
    GovernancePluginRegistry,
    GovernanceUnitOfWork,
    GovernanceUnitOfWorkFactory,
    PreviewIdentityMapper,
)
from dal_obscura.application.reads.ports import ProjectionCompiler, RowTransformPort
from dal_obscura.kernel.identity.actor import ControlPlaneActor


def replace_policy_rules(
    store: GovernanceUnitOfWork,
    asset_id: UUID,
    rules: list[dict[str, Any]],
    *,
    actor: ControlPlaneActor,
    expected_revision: int | None = None,
    revoke_existing_tokens: bool = False,
    correlation_id: str | None = None,
) -> dict[str, int]:
    """Validates and replaces policy rules for one asset."""

    store.assets.lock_asset_for_update(asset_id)
    ensure_asset_capability(store.assets, asset_id, actor, "edit")
    schema_fields = store.assets.list_asset_schema_fields(asset_id)
    compiled_rules = compile_policy_rule_payloads(rules, schema_fields)
    if expected_revision is None:
        current_revision = store.assets.get_workspace_asset(asset_id)["policy_revision"]
        if not isinstance(current_revision, int):
            raise RuntimeError("Asset policy revision is unavailable")
        expected_revision = current_revision
    if revoke_existing_tokens:
        ensure_asset_owner(store.assets, asset_id, actor)
    revision = store.policies.replace_policy_rules(
        asset_id=asset_id,
        rules=compiled_rules,
        expected_revision=expected_revision,
    )
    revoked_count = (
        store.policies.revoke_asset_tickets(asset_id=asset_id) if revoke_existing_tokens else 0
    )
    store.audit.record_asset_audit_event(
        asset_id=asset_id,
        actor_principal=actor.identity_key(),
        action="asset.policy.replace",
        details={"policy_revision": revision, "revoked_token_count": revoked_count},
        correlation_id=correlation_id,
    )
    return {"policy_revision": revision, "revoked_token_count": revoked_count}


def revoke_asset_tickets(
    store: GovernanceUnitOfWork,
    asset_id: UUID,
    *,
    actor: ControlPlaneActor,
    correlation_id: str | None = None,
) -> dict[str, int]:
    """Revokes all unexpired asset tokens; only owners and platform admins may do so."""

    store.assets.lock_asset_for_update(asset_id)
    ensure_asset_owner(store.assets, asset_id, actor)
    revoked_count = store.policies.revoke_asset_tickets(asset_id=asset_id)
    store.audit.record_asset_audit_event(
        asset_id=asset_id,
        actor_principal=actor.identity_key(),
        action="asset.tokens.revoke",
        details={"revoked_token_count": revoked_count},
        correlation_id=correlation_id,
    )
    return {"revoked_token_count": revoked_count}


class PoliciesCommands:
    def __init__(
        self,
        work_factory: GovernanceUnitOfWorkFactory,
        *,
        schema_loader: AssetSchemaLoader,
        plugin_registry: GovernancePluginRegistry | None,
        compiler: ProjectionCompiler,
        transform: RowTransformPort,
        identity_mapper: PreviewIdentityMapper,
        egress_allowlist: tuple[str, ...] = (),
    ) -> None:
        self._work_factory = work_factory
        self._schema_loader = schema_loader
        self._plugin_registry = plugin_registry
        self._compiler = compiler
        self._transform = transform
        self._identity_mapper = identity_mapper
        self._egress_allowlist = egress_allowlist

    def replace_asset_policy(
        self,
        asset_id: UUID,
        rules: list[dict[str, Any]],
        *,
        actor: ControlPlaneActor,
        correlation_id: str | None,
        expected_revision: int | None = None,
        revoke_existing_tokens: bool = False,
    ) -> dict[str, int]:
        with self._work_factory() as work:
            result = replace_policy_rules(
                work,
                asset_id,
                rules,
                actor=actor,
                expected_revision=expected_revision,
                revoke_existing_tokens=revoke_existing_tokens,
                correlation_id=correlation_id,
            )
            work.commit()
            return result

    def revoke_asset_tickets(
        self, asset_id: UUID, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> dict[str, int]:
        with self._work_factory() as work:
            result = revoke_asset_tickets(
                work, asset_id, actor=actor, correlation_id=correlation_id
            )
            work.commit()
            return result

    def evaluate_asset_policy(
        self,
        asset_id: UUID,
        *,
        principal: str,
        groups: list[str],
        claims: dict[str, object],
        rows: list[dict[str, object]] | None,
        provider_ordinal: int | None = None,
        actor: ControlPlaneActor,
        correlation_id: str | None,
    ) -> dict[str, object]:
        with self._work_factory() as work:
            captured = evaluation_service.capture_asset_evaluation(
                work,
                asset_id,
                actor,
                principal=principal,
                groups=groups,
                claims=claims,
                rows=rows,
                provider_ordinal=provider_ordinal,
                egress_allowlist=self._egress_allowlist,
                plugin_registry=self._plugin_registry,
                identity_mapper=self._identity_mapper,
            )
        schema = self._schema_loader.load_asset_schema(captured.source)
        payload = evaluation_service.evaluate_asset_policy(
            captured, schema, compiler=self._compiler, transform=self._transform
        )
        with self._work_factory() as work:
            evaluation_service.ensure_asset_evaluation_current(work, captured, actor)
        return payload
