from __future__ import annotations

from importlib import resources

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from starlette.staticfiles import StaticFiles

_ASSET_PACKAGE = "dal_obscura.control_plane.interfaces"


def install_ui(app: FastAPI) -> None:
    asset_root = resources.files(_ASSET_PACKAGE).joinpath("ui_assets")
    static_root = asset_root.joinpath("static")

    app.mount("/ui/static", StaticFiles(directory=str(static_root)), name="control-plane-ui-static")

    @app.get("/ui", response_class=HTMLResponse, include_in_schema=False)
    def control_plane_ui() -> HTMLResponse:
        shell = asset_root.joinpath("shell.html").read_text(encoding="utf-8")
        return HTMLResponse(shell)
