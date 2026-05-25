from __future__ import annotations

from importlib import resources
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from starlette.staticfiles import StaticFiles

_ASSET_PACKAGE = "dal_obscura.control_plane.interfaces"


def install_ui(app: FastAPI) -> None:
    dist_root = _ui_dist_root()
    assets_root = dist_root / "static"
    if assets_root.exists():
        app.mount(
            "/ui/static",
            StaticFiles(directory=str(assets_root)),
            name="control-plane-ui-assets",
        )

    @app.get("/ui", response_class=HTMLResponse, include_in_schema=False)
    @app.get("/ui/{path:path}", response_class=HTMLResponse, include_in_schema=False)
    def control_plane_ui(path: str = "") -> HTMLResponse:
        del path
        return HTMLResponse(_index_html(dist_root))


def _ui_dist_root() -> Path:
    return Path(str(resources.files(_ASSET_PACKAGE).joinpath("ui_assets").joinpath("dist")))


def _index_html(dist_root: Path) -> str:
    index = dist_root / "index.html"
    if index.exists():
        return index.read_text(encoding="utf-8")
    return _development_index()


def _development_index() -> str:
    return """<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>dal-obscura control plane</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/ui/static/index.js"></script>
  </body>
</html>
"""
