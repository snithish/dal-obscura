"""Authenticated plugin descriptor and capability routes."""

from __future__ import annotations

from dal_obscura_plugin_api import PluginDescriptor, PluginKind
from fastapi import APIRouter, Depends

from dal_obscura.common.plugin_api import PluginLifecycleState
from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import (
    PluginLifecycleRequest,
    PluginLifecycleResponse,
    PluginListResponse,
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds the authenticated descriptor route."""

    api = APIRouter()

    @api.get(
        "/v1/plugins",
        dependencies=[Depends(deps.require_admin)],
        response_model=PluginListResponse,
        response_model_exclude_none=True,
    )
    def list_plugins() -> PluginListResponse:
        if deps.plugin_registry is None:
            raise RuntimeError("Plugin registry was not admitted during application startup")
        descriptors = list(deps.plugin_registry.admitted().values())
        states = list(deps.plugin_registry.status_report())
        return PluginListResponse.model_validate(
            {
                "plugins": [_descriptor_payload(descriptor) for descriptor in descriptors],
                "pairs": _pair_payload(descriptors),
                "states": states,
            }
        )

    @api.patch(
        "/v1/plugins/{kind}/{plugin_id}/lifecycle",
        response_model=PluginLifecycleResponse,
        dependencies=[Depends(deps.require_admin)],
    )
    def set_plugin_lifecycle(
        kind: PluginKind,
        plugin_id: str,
        request: PluginLifecycleRequest,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> PluginLifecycleResponse:
        target = PluginLifecycleState(request.target)
        return deps.with_service(
            lambda service: service.set_plugin_lifecycle(
                kind=kind,
                plugin_id=plugin_id,
                target=target,
                actor=actor,
            )
        )

    return api


def _descriptor_payload(descriptor: PluginDescriptor) -> dict[str, object]:
    return {
        "kind": descriptor.kind,
        "plugin_id": descriptor.plugin_id,
        "api_version": descriptor.api_version,
        "config_version": descriptor.config_version,
        "distribution": descriptor.distribution,
        "version": descriptor.version,
        "display_name": descriptor.display_name or descriptor.plugin_id,
        "capabilities": sorted(descriptor.capabilities),
        "output_formats": sorted(descriptor.output_formats),
        "handle_versions": sorted(descriptor.handle_versions),
        "config_schema": dict(descriptor.config_schema),
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
