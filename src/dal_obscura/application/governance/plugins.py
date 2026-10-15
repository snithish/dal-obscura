"""Governed access to the startup-admitted plugin generation."""

from __future__ import annotations

from typing import cast

from dal_obscura_plugin_api import PluginDescriptor, PluginKind

from dal_obscura.application.governance.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.application.governance.ports import (
    GovernancePluginControl,
    GovernanceUnitOfWorkFactory,
)
from dal_obscura.kernel.identity.actor import ControlPlaneActor


class PluginCommands:
    def __init__(
        self, work_factory: GovernanceUnitOfWorkFactory, *, registry: GovernancePluginControl
    ) -> None:
        self._work_factory = work_factory
        self._registry = registry

    def list_plugins(
        self, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> dict[str, object]:
        _require_admin(actor)
        descriptors = list(self._registry.admitted().values())
        return {
            "plugins": [_descriptor_payload(descriptor) for descriptor in descriptors],
            "pairs": _pair_payload(descriptors),
            "states": list(self._registry.status_report()),
        }

    def set_plugin_lifecycle(
        self,
        *,
        kind: str,
        plugin_id: str,
        target: str,
        actor: ControlPlaneActor,
        correlation_id: str | None,
    ) -> dict[str, str]:
        _require_admin(actor)
        if kind not in {"catalog", "table_format", "secret_backend"}:
            raise ValidationFailure("Unsupported plugin kind")
        with self._work_factory() as work:
            try:
                lifecycle = self._registry.set_lifecycle(cast(PluginKind, kind), plugin_id, target)
            except ValueError as exc:
                raise ValidationFailure(str(exc)) from exc
            work.workspace.ensure_workspace()
            work.audit.record_workspace_audit_event(
                actor_principal=actor.identity_key(),
                action="plugin.lifecycle.update",
                resource_type="plugin",
                resource_id=f"{kind}:{plugin_id}",
                details={"target": lifecycle},
                correlation_id=correlation_id,
            )
            work.commit()
            return {"kind": kind, "plugin_id": plugin_id, "lifecycle": lifecycle}


def _require_admin(actor: ControlPlaneActor) -> None:
    if not actor.platform_admin:
        raise AuthorizationFailure("Platform admin required")


def _descriptor_payload(descriptor: PluginDescriptor) -> dict[str, object]:
    return {
        **descriptor.to_json(),
        "display_name": descriptor.display_name or descriptor.plugin_id,
        "status": "admitted",
    }


def _pair_payload(descriptors: list[PluginDescriptor]) -> list[dict[str, object]]:
    catalogs = [item for item in descriptors if item.kind == "catalog"]
    formats = [item for item in descriptors if item.kind == "table_format"]
    pairs: list[dict[str, object]] = []
    for catalog in catalogs:
        for table_format in formats:
            capabilities = catalog.capabilities & table_format.capabilities
            handle_versions = catalog.handle_versions & table_format.handle_versions
            status = (
                "admitted"
                if table_format.plugin_id in catalog.output_formats
                and handle_versions
                and capabilities
                else "incompatible"
            )
            pairs.append(
                {
                    "catalog_plugin_id": catalog.plugin_id,
                    "format_plugin_id": table_format.plugin_id,
                    "capabilities": sorted(capabilities),
                    "handle_versions": sorted(handle_versions),
                    "status": status,
                }
            )
    return pairs
