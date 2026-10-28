"""Governed access to the startup-admitted plugin generation."""

from __future__ import annotations

from dal_obscura_plugin_api import PluginDescriptor

from dal_obscura.application.governance.errors import AuthorizationFailure
from dal_obscura.application.governance.ports import (
    GovernancePluginDiagnostics,
)
from dal_obscura.kernel.identity.actor import ControlPlaneActor


class PluginQueries:
    def __init__(self, *, registry: GovernancePluginDiagnostics) -> None:
        self._registry = registry

    def list_plugins(self, *, actor: ControlPlaneActor) -> dict[str, object]:
        _require_admin(actor)
        descriptors = list(self._registry.admitted().values())
        return {
            "plugins": [_descriptor_payload(descriptor) for descriptor in descriptors],
            "pairs": _pair_payload(descriptors),
            "states": list(self._registry.status_report()),
        }


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
