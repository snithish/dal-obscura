"""Authenticated plugin descriptor and capability routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from dal_obscura.interfaces.http.routes.deps import ControlPlaneDeps
from dal_obscura.interfaces.http.routes.plugin_schemas import (
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
        return PluginListResponse.model_validate(deps.plugin_queries.list_plugins(actor=actor))

    return api
