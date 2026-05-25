# Control Plane React UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the HTMX control-plane UI with a hardened pnpm/Vite React app and JSON workspace API foundation.

**Architecture:** FastAPI remains the control-plane API and static host. React owns the UI under `/ui`, while new workspace-oriented JSON endpoints hide tenant/cell from the frontend and initially map to the existing single/default runtime records.

**Tech Stack:** FastAPI, SQLAlchemy, React, TypeScript, Vite, pnpm, Tailwind, Zod, React Hook Form, Radix primitives.

---

### Task 1: Remove HTMX Contract From Tests First

**Files:**
- Modify: `tests/interfaces/control_plane/test_ui_shell.py`
- Modify: `tests/interfaces/control_plane/test_api_inventory_reads.py`

- [ ] **Step 1: Replace UI shell tests with React static-host expectations**

```python
def test_ui_serves_react_index_without_embedding_admin_token():
    client = _client()

    response = client.get("/ui")

    assert response.status_code == 200
    assert '<div id="root">' in response.text
    assert "dal-obscura control plane" in response.text
    assert "test-admin" not in response.text
    assert "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN" not in response.text
    assert "htmx" not in response.text.lower()
    assert "tenant" not in response.text.lower()
    assert "cell" not in response.text.lower()
```

- [ ] **Step 2: Add static fallback expectations**

```python
def test_ui_client_routes_fall_back_to_react_index():
    client = _client()

    response = client.get("/ui/assets/example")

    assert response.status_code == 200
    assert '<div id="root">' in response.text
```

- [ ] **Step 3: Replace HTMX partial tests with JSON-only assertions**

```python
def test_v1_routes_ignore_hx_request_and_return_json():
    client = _client()

    response = client.get(
        "/v1/tenants",
        headers={"authorization": "Bearer test-admin", "HX-Request": "true"},
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    assert response.json() == []
```

- [ ] **Step 4: Run focused tests and confirm failure**

Run: `uv run pytest tests/interfaces/control_plane/test_ui_shell.py tests/interfaces/control_plane/test_api_inventory_reads.py -q`

Expected: tests fail because `ui.py` still serves HTMX shell/partials.

### Task 2: Serve React Build and Remove HTMX UI Code

**Files:**
- Modify: `src/dal_obscura/control_plane/interfaces/ui.py`
- Modify: `src/dal_obscura/control_plane/interfaces/api.py`
- Delete: `src/dal_obscura/control_plane/interfaces/ui_assets/shell.html`
- Delete: `src/dal_obscura/control_plane/interfaces/ui_assets/static/app.js`
- Delete: `src/dal_obscura/control_plane/interfaces/ui_assets/static/styles.css`
- Delete: `src/dal_obscura/control_plane/interfaces/ui_assets/static/vendor/htmx/2.0.4/htmx.min.js`

- [ ] **Step 1: Replace `ui.py` with static React serving**

```python
from __future__ import annotations

from importlib import resources
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from starlette.staticfiles import StaticFiles

_ASSET_PACKAGE = "dal_obscura.control_plane.interfaces"


def install_ui(app: FastAPI) -> None:
    dist_root = _ui_dist_root()
    assets_root = dist_root / "assets"
    if assets_root.exists():
        app.mount("/ui/assets", StaticFiles(directory=str(assets_root)), name="control-plane-ui-assets")

    @app.get("/ui", response_class=HTMLResponse, include_in_schema=False)
    @app.get("/ui/{path:path}", response_class=HTMLResponse, include_in_schema=False)
    def control_plane_ui(path: str = "") -> HTMLResponse:
        del path
        return HTMLResponse(_index_html(dist_root))


def _ui_dist_root() -> Path:
    return Path(str(resources.files(_ASSET_PACKAGE).joinpath("ui_assets", "dist")))


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
    <script type="module" src="/ui/assets/index.js"></script>
  </body>
</html>
"""
```

- [ ] **Step 2: Remove HTMX imports and `wants_html` branches from `api.py`**

Keep request parsing for form posts only where existing tests still need it, but make `/v1` return JSON regardless of `HX-Request`.

- [ ] **Step 3: Remove old UI asset files**

Run: `git rm src/dal_obscura/control_plane/interfaces/ui_assets/shell.html src/dal_obscura/control_plane/interfaces/ui_assets/static/app.js src/dal_obscura/control_plane/interfaces/ui_assets/static/styles.css src/dal_obscura/control_plane/interfaces/ui_assets/static/vendor/htmx/2.0.4/htmx.min.js`

- [ ] **Step 4: Run focused tests**

Run: `uv run pytest tests/interfaces/control_plane/test_ui_shell.py tests/interfaces/control_plane/test_api_inventory_reads.py -q`

Expected: pass.

### Task 3: Add Hardened pnpm/Vite React Scaffold

**Files:**
- Create: `pnpm-workspace.yaml`
- Create: `ui/package.json`
- Create: `ui/index.html`
- Create: `ui/tsconfig.json`
- Create: `ui/tsconfig.node.json`
- Create: `ui/vite.config.ts`
- Create: `ui/src/main.tsx`
- Create: `ui/src/app/App.tsx`
- Create: `ui/src/app/routes.tsx`
- Create: `ui/src/api/client.ts`
- Create: `ui/src/styles/global.css`
- Create: `ui/src/features/assets/AssetsPage.tsx`
- Create: `ui/src/features/catalogs/CatalogsPage.tsx`
- Create: `ui/src/features/publish/PublishPage.tsx`
- Create: `ui/src/features/settings/SettingsPage.tsx`
- Create: `ui/src/features/policies/PoliciesPage.tsx`

- [ ] **Step 1: Add pnpm workspace security config**

```yaml
packages:
  - ui

minimumReleaseAge: 20160
minimumReleaseAgeStrict: true
minimumReleaseAgeIgnoreMissingTime: false
trustPolicy: no-downgrade
blockExoticSubdeps: true
strictDepBuilds: true
dangerouslyAllowAllBuilds: false
verifyDepsBeforeRun: error

allowBuilds:
  esbuild: true
```

- [ ] **Step 2: Add minimal React package**

Use dependencies only for React, React Router, React Hook Form, Zod, Radix Dialog/Select/Popover, Tailwind/Vite tooling, and TypeScript.

- [ ] **Step 3: Add app shell and placeholder feature pages**

The visible labels must include Assets, Catalogs, Policies, Publish, and Settings. Visible UI must not include tenant or cell.

- [ ] **Step 4: Run frontend install/build**

Run: `pnpm install`

Run: `pnpm build --filter ui`

Expected: build creates `ui/dist`.

### Task 4: Package React Build With Python Distribution

**Files:**
- Modify: `pyproject.toml`
- Add generated build output under `src/dal_obscura/control_plane/interfaces/ui_assets/dist/`

- [ ] **Step 1: Update package data**

```toml
[tool.setuptools.package-data]
"dal_obscura.control_plane.interfaces" = ["ui_assets/dist/**/*"]
```

- [ ] **Step 2: Copy built UI into package asset directory**

Run: `rm -rf src/dal_obscura/control_plane/interfaces/ui_assets/dist`

Run: `cp -R ui/dist src/dal_obscura/control_plane/interfaces/ui_assets/dist`

- [ ] **Step 3: Verify `/ui` serves built React HTML**

Run: `uv run pytest tests/interfaces/control_plane/test_ui_shell.py -q`

Expected: pass.

### Task 5: Add Workspace JSON API Foundation

**Files:**
- Modify: `src/dal_obscura/control_plane/application/provisioning.py`
- Modify: `src/dal_obscura/control_plane/infrastructure/repositories.py`
- Modify: `src/dal_obscura/control_plane/interfaces/api.py`
- Create: `tests/interfaces/control_plane/test_workspace_api.py`

- [ ] **Step 1: Add failing tests for workspace endpoints**

Test `GET /v1/workspace/summary`, `GET /v1/catalogs`, `GET /v1/assets`, `GET /v1/assets/{asset_id}`, `GET /v1/publications/draft`, `POST /v1/publications`, and activation wrappers.

- [ ] **Step 2: Implement default workspace lookup**

The default workspace picks the first assigned cell/tenant pair ordered by tenant slug and cell name. If none exists, summary returns zeros and list endpoints return empty lists.

- [ ] **Step 3: Implement service/repository methods**

Add methods that return UI-friendly JSON with no visible tenant or cell field names.

- [ ] **Step 4: Add FastAPI routes**

Routes require the same admin token dependency as existing endpoints.

- [ ] **Step 5: Run focused tests**

Run: `uv run pytest tests/interfaces/control_plane/test_workspace_api.py tests/interfaces/control_plane/test_api_inventory_reads.py -q`

Expected: pass.

### Task 6: Documentation and Verification

**Files:**
- Create: `docs/frontend.md`
- Modify: `README.md`

- [ ] **Step 1: Add frontend conventions doc**

Document stack, folder layout, API helper pattern, form pattern, error handling, styling, pnpm supply-chain policy, dependency admission, and local commands.

- [ ] **Step 2: Update README commands**

Add React UI local dev and production build notes.

- [ ] **Step 3: Run final checks**

Run: `uv run pytest tests/interfaces/control_plane tests/control_plane -q`

Run: `uv run ruff check .`

Run: `uv run ty check`

Run: `pnpm --filter ui build`

Expected: all pass.
