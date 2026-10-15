"""Authenticated plugin descriptor and capability routes."""

from __future__ import annotations

from dal_obscura_plugin_api import PluginKind
from fastapi import APIRouter, Depends

from dal_obscura.interfaces.http.request_context import current_request_id
from dal_obscura.interfaces.http.routes.deps import ControlPlaneDeps
from dal_obscura.interfaces.http.routes.plugin_schemas import (
    PluginLifecycleRequest,
    PluginLifecycleResponse,
    PluginListResponse,
)
from dal_obscura.kernel.identity.actor import ControlPlaneActor


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds the authenticated descriptor route."""

    api = APIRouter()
    admin_actor = Depends(deps.require_admin)

    @api.get(
        "/v1/plugins",
        dependencies=[Depends(deps.require_admin)],
        response_model=PluginListResponse,
        response_model_exclude_none=True,
    )
    def list_plugins(
        actor: ControlPlaneActor = admin_actor,
    ) -> PluginListResponse:
        return PluginListResponse.model_validate(
            deps.plugin_commands.list_plugins(actor=actor, correlation_id=current_request_id())
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
        return PluginLifecycleResponse.model_validate(
            deps.plugin_commands.set_plugin_lifecycle(
                kind=kind,
                plugin_id=plugin_id,
                target=request.target,
                actor=actor,
                correlation_id=current_request_id(),
            )
        )

    return api
