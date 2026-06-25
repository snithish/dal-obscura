from __future__ import annotations

from fastapi import APIRouter, Depends

from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps


def router(deps: ControlPlaneDeps) -> APIRouter:
    api = APIRouter()

    @api.get("/v1/workspace/summary", dependencies=[Depends(deps.require_actor)])
    def get_workspace_summary() -> object:
        return deps.with_service(lambda service: service.get_workspace_summary())

    return api
