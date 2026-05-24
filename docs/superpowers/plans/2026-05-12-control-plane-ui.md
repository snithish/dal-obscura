# Control Plane UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a same-process, no-framework operator dashboard for the control plane where `/v1` remains the only state API.

**Architecture:** Add JSON inventory/read endpoints under `/v1`, then serve a presentation-only `/ui` shell with static CSS and JavaScript. The shell and static assets must never query repositories or call `ProvisioningService`; all control-plane state reads and writes flow through protected `/v1` FastAPI endpoints.

**Tech Stack:** FastAPI, SQLAlchemy, Pydantic, `importlib.resources`, Starlette `StaticFiles`, vanilla browser JavaScript, pytest, httpx `TestClient`.

---

## File Structure

Create or modify these files:

- Modify `src/dal_obscura/control_plane/infrastructure/repositories.py`
  - Add read-model methods that return plain `dict`/`list` data, not ORM rows.
- Modify `src/dal_obscura/control_plane/application/provisioning.py`
  - Expose read methods on `ProvisioningService` that delegate to `PublicationStore`.
- Modify `src/dal_obscura/control_plane/interfaces/api.py`
  - Add protected `GET /v1/...` endpoints and mount the UI shell.
- Create `src/dal_obscura/control_plane/interfaces/ui.py`
  - Serve `/ui` and `/ui/static`, with no imports from application, infrastructure, or ORM modules.
- Create `src/dal_obscura/control_plane/interfaces/ui_assets/shell.html`
  - Static shell markup for the dashboard.
- Create `src/dal_obscura/control_plane/interfaces/ui_assets/static/styles.css`
  - Dashboard layout and component styling.
- Create `src/dal_obscura/control_plane/interfaces/ui_assets/static/app.js`
  - Browser-only API calls, section navigation, rendering, token handling, drawers, forms, and toasts.
- Modify `pyproject.toml`
  - Include UI assets as package data.
- Create `tests/interfaces/control_plane/test_api_inventory_reads.py`
  - API read endpoint tests.
- Create `tests/interfaces/control_plane/test_ui_shell.py`
  - UI shell, static asset, and boundary tests.
- Modify `README.md`
  - Document `/ui` after implementation.

Keep resource-state access in this direction only:

```text
browser UI -> /v1 FastAPI endpoints -> ProvisioningService -> PublicationStore -> DB
```

`/ui` routes are presentation-only.

---

### Task 1: Basic Inventory Read API

**Files:**
- Create: `tests/interfaces/control_plane/test_api_inventory_reads.py`
- Modify: `src/dal_obscura/control_plane/infrastructure/repositories.py`
- Modify: `src/dal_obscura/control_plane/application/provisioning.py`
- Modify: `src/dal_obscura/control_plane/interfaces/api.py`

- [ ] **Step 1: Write failing tests for tenants, cells, and assignments**

Create `tests/interfaces/control_plane/test_api_inventory_reads.py` with:

```python
from __future__ import annotations

from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import Base
from dal_obscura.control_plane.interfaces.api import create_app


ADMIN_HEADERS = {"authorization": "Bearer test-admin"}


def _client() -> TestClient:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return TestClient(create_app(session_factory(engine), admin_token="test-admin"))


def _create_tenant_and_cell(client: TestClient) -> tuple[dict[str, str], dict[str, str]]:
    tenant = client.post(
        "/v1/tenants",
        json={"slug": "default", "display_name": "Default"},
        headers=ADMIN_HEADERS,
    ).json()
    cell = client.post(
        "/v1/cells",
        json={"name": "default", "region": "local"},
        headers=ADMIN_HEADERS,
    ).json()
    client.put(
        f"/v1/cells/{cell['id']}/tenants/{tenant['id']}",
        json={"shard_key": "default"},
        headers=ADMIN_HEADERS,
    )
    return tenant, cell


def test_inventory_reads_require_admin_token():
    client = _client()

    assert client.get("/v1/tenants").status_code == 401
    assert client.get("/v1/cells").status_code == 401
    assert client.get("/v1/cell-tenant-assignments").status_code == 401


def test_lists_tenants_cells_and_assignments_after_writes():
    client = _client()
    tenant, cell = _create_tenant_and_cell(client)

    tenants = client.get("/v1/tenants", headers=ADMIN_HEADERS).json()
    cells = client.get("/v1/cells", headers=ADMIN_HEADERS).json()
    assignments = client.get("/v1/cell-tenant-assignments", headers=ADMIN_HEADERS).json()

    assert tenants == [
        {
            "id": tenant["id"],
            "slug": "default",
            "display_name": "Default",
            "status": "active",
        }
    ]
    assert cells == [
        {
            "id": cell["id"],
            "name": "default",
            "region": "local",
            "status": "active",
        }
    ]
    assert assignments == [
        {
            "cell_id": cell["id"],
            "tenant_id": tenant["id"],
            "shard_key": "default",
        }
    ]
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/interfaces/control_plane/test_api_inventory_reads.py -q
```

Expected: failures with `404 Not Found` for `GET /v1/tenants`, `GET /v1/cells`, and `GET /v1/cell-tenant-assignments`.

- [ ] **Step 3: Add repository read methods**

In `src/dal_obscura/control_plane/infrastructure/repositories.py`, add these methods to `PublicationStore` after `create_tenant()`:

```python
    def list_tenants(self) -> list[dict[str, str]]:
        return [
            {
                "id": str(record.id),
                "slug": record.slug,
                "display_name": record.display_name,
                "status": record.status,
            }
            for record in self._session.scalars(select(TenantRecord).order_by(TenantRecord.slug))
        ]

    def list_cells(self) -> list[dict[str, str]]:
        return [
            {
                "id": str(record.id),
                "name": record.name,
                "region": record.region,
                "status": record.status,
            }
            for record in self._session.scalars(select(CellRecord).order_by(CellRecord.name))
        ]

    def list_cell_tenant_assignments(self) -> list[dict[str, str]]:
        return [
            {
                "cell_id": str(record.cell_id),
                "tenant_id": str(record.tenant_id),
                "shard_key": record.shard_key,
            }
            for record in self._session.scalars(
                select(CellTenantRecord).order_by(
                    CellTenantRecord.cell_id,
                    CellTenantRecord.tenant_id,
                )
            )
        ]
```

- [ ] **Step 4: Add service read methods**

In `src/dal_obscura/control_plane/application/provisioning.py`, add these methods to `ProvisioningService` after `create_cell()`:

```python
    def list_tenants(self) -> list[dict[str, str]]:
        return self._store.list_tenants()

    def list_cells(self) -> list[dict[str, str]]:
        return self._store.list_cells()

    def list_cell_tenant_assignments(self) -> list[dict[str, str]]:
        return self._store.list_cell_tenant_assignments()
```

- [ ] **Step 5: Add API routes**

In `src/dal_obscura/control_plane/interfaces/api.py`, add these routes before `@app.post("/v1/tenants"...`:

```python
    @app.get("/v1/tenants", dependencies=[Depends(require_admin)])
    def list_tenants() -> object:
        return with_service(lambda service: service.list_tenants())

    @app.get("/v1/cells", dependencies=[Depends(require_admin)])
    def list_cells() -> object:
        return with_service(lambda service: service.list_cells())

    @app.get("/v1/cell-tenant-assignments", dependencies=[Depends(require_admin)])
    def list_cell_tenant_assignments() -> object:
        return with_service(lambda service: service.list_cell_tenant_assignments())
```

- [ ] **Step 6: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/interfaces/control_plane/test_api_inventory_reads.py -q
```

Expected: all tests in the file pass.

- [ ] **Step 7: Commit**

```bash
git add tests/interfaces/control_plane/test_api_inventory_reads.py \
  src/dal_obscura/control_plane/infrastructure/repositories.py \
  src/dal_obscura/control_plane/application/provisioning.py \
  src/dal_obscura/control_plane/interfaces/api.py
git commit -m "feat(control-plane): add basic inventory reads"
```

---

### Task 2: Draft Resource Read API

**Files:**
- Modify: `tests/interfaces/control_plane/test_api_inventory_reads.py`
- Modify: `src/dal_obscura/control_plane/infrastructure/repositories.py`
- Modify: `src/dal_obscura/control_plane/application/provisioning.py`
- Modify: `src/dal_obscura/control_plane/interfaces/api.py`

- [ ] **Step 1: Extend tests for runtime, catalogs, assets, policies, auth, and draft aggregate**

Append to `tests/interfaces/control_plane/test_api_inventory_reads.py`:

```python
ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)
DEFAULT_AUTH_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.identity_default.DefaultIdentityAdapter"
)


def _provision_draft(client: TestClient) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    tenant, cell = _create_tenant_and_cell(client)
    client.put(
        f"/v1/tenants/{tenant['id']}/cells/{cell['id']}/runtime-settings",
        json={
            "ticket_ttl_seconds": 900,
            "max_tickets": 64,
            "max_ticket_exchanges": 2,
            "path_rules": [{"glob": "s3://warehouse/*", "allow": True}],
        },
        headers=ADMIN_HEADERS,
    )
    client.put(
        f"/v1/tenants/{tenant['id']}/cells/{cell['id']}/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    asset = client.put(
        f"/v1/tenants/{tenant['id']}/cells/{cell['id']}/assets/analytics/default.users",
        json={"backend": "iceberg", "table_identifier": "prod.users", "options": {"snapshot": 1}},
        headers=ADMIN_HEADERS,
    ).json()
    client.put(
        f"/v1/assets/{asset['id']}/policy-rules",
        json={
            "rules": [
                {
                    "ordinal": 10,
                    "effect": "allow",
                    "principals": ["user1"],
                    "when": {"tenant": "default"},
                    "columns": ["id", "email"],
                    "masks": {"email": {"type": "email"}},
                    "row_filter": "region = 'us'",
                }
            ]
        },
        headers=ADMIN_HEADERS,
    )
    client.put(
        f"/v1/cells/{cell['id']}/auth-providers",
        json={
            "providers": [
                {
                    "ordinal": 1,
                    "module": DEFAULT_AUTH_MODULE,
                    "args": {"jwt_secret": {"key": "DAL_OBSCURA_JWT_SECRET"}},
                    "enabled": True,
                }
            ]
        },
        headers=ADMIN_HEADERS,
    )
    return tenant, cell, asset


def test_reads_cell_draft_resources_after_writes():
    client = _client()
    tenant, cell, asset = _provision_draft(client)

    runtime = client.get(
        f"/v1/cells/{cell['id']}/runtime-settings",
        headers=ADMIN_HEADERS,
    ).json()
    catalogs = client.get(f"/v1/cells/{cell['id']}/catalogs", headers=ADMIN_HEADERS).json()
    assets = client.get(f"/v1/cells/{cell['id']}/assets", headers=ADMIN_HEADERS).json()
    rules = client.get(f"/v1/assets/{asset['id']}/policy-rules", headers=ADMIN_HEADERS).json()
    auth = client.get(f"/v1/cells/{cell['id']}/auth-providers", headers=ADMIN_HEADERS).json()
    draft = client.get(f"/v1/cells/{cell['id']}/draft", headers=ADMIN_HEADERS).json()

    assert runtime == {
        "cell_id": cell["id"],
        "ticket_ttl_seconds": 900,
        "max_tickets": 64,
        "max_ticket_exchanges": 2,
        "path_rules": [{"glob": "s3://warehouse/*", "allow": True}],
    }
    assert catalogs[0]["tenant_id"] == tenant["id"]
    assert catalogs[0]["name"] == "analytics"
    assert catalogs[0]["module"] == ICEBERG_CATALOG_MODULE
    assert catalogs[0]["options"] == {"type": "sql", "uri": "sqlite:///catalog.db"}
    assert assets[0]["id"] == asset["id"]
    assert assets[0]["catalog"] == "analytics"
    assert assets[0]["target"] == "default.users"
    assert assets[0]["options"] == {"snapshot": 1}
    assert rules == [
        {
            "id": rules[0]["id"],
            "asset_id": asset["id"],
            "ordinal": 10,
            "effect": "allow",
            "principals": ["user1"],
            "when": {"tenant": "default"},
            "columns": ["id", "email"],
            "masks": {"email": {"type": "email"}},
            "row_filter": "region = 'us'",
        }
    ]
    assert auth == [
        {
            "id": auth[0]["id"],
            "cell_id": cell["id"],
            "ordinal": 1,
            "module": DEFAULT_AUTH_MODULE,
            "args": {"jwt_secret": {"key": "DAL_OBSCURA_JWT_SECRET"}},
            "enabled": True,
        }
    ]
    assert draft["cell"] == {
        "id": cell["id"],
        "name": "default",
        "region": "local",
        "status": "active",
    }
    assert draft["runtime_settings"] == runtime
    assert draft["catalogs"] == catalogs
    assert draft["assets"][0]["policy_rules"] == rules
    assert draft["auth_providers"] == auth


def test_unknown_cell_draft_returns_404():
    client = _client()

    response = client.get(
        "/v1/cells/00000000-0000-0000-0000-000000000001/draft",
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "No cell 00000000-0000-0000-0000-000000000001"
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/interfaces/control_plane/test_api_inventory_reads.py -q
```

Expected: new tests fail with `404 Not Found` for the new read endpoints.

- [ ] **Step 3: Add repository read helpers**

In `src/dal_obscura/control_plane/infrastructure/repositories.py`, add these methods to `PublicationStore` before `load_publish_draft()`:

```python
    def get_cell(self, cell_id: UUID) -> dict[str, str]:
        record = self._session.get(CellRecord, cell_id)
        if record is None:
            raise LookupError(f"No cell {cell_id}")
        return {
            "id": str(record.id),
            "name": record.name,
            "region": record.region,
            "status": record.status,
        }

    def get_runtime_settings(self, cell_id: UUID) -> dict[str, object] | None:
        record = self._session.get(CellRuntimeSettingsRecord, cell_id)
        if record is None:
            return None
        return {
            "cell_id": str(record.cell_id),
            "ticket_ttl_seconds": record.ticket_ttl_seconds,
            "max_tickets": record.max_tickets,
            "max_ticket_exchanges": record.max_ticket_exchanges,
            "path_rules": list(record.path_rules_json),
        }

    def list_catalogs(self, cell_id: UUID) -> list[dict[str, object]]:
        return [
            {
                "id": str(record.id),
                "cell_id": str(record.cell_id),
                "tenant_id": str(record.tenant_id),
                "name": record.name,
                "module": record.module,
                "options": dict(record.options_json),
            }
            for record in self._session.scalars(
                select(CatalogRecord)
                .where(CatalogRecord.cell_id == cell_id)
                .order_by(CatalogRecord.name)
            )
        ]

    def list_assets(self, cell_id: UUID) -> list[dict[str, object]]:
        catalog_records = list(
            self._session.scalars(select(CatalogRecord).where(CatalogRecord.cell_id == cell_id))
        )
        catalog_by_id = {record.id: record for record in catalog_records}
        assets = []
        for record in self._session.scalars(
            select(AssetRecord).where(AssetRecord.cell_id == cell_id).order_by(AssetRecord.target)
        ):
            catalog = catalog_by_id[record.catalog_id]
            assets.append(
                {
                    "id": str(record.id),
                    "cell_id": str(record.cell_id),
                    "tenant_id": str(record.tenant_id),
                    "catalog_id": str(record.catalog_id),
                    "catalog": catalog.name,
                    "target": record.target,
                    "backend": record.backend,
                    "table_identifier": record.table_identifier,
                    "options": dict(record.options_json),
                }
            )
        return assets

    def list_policy_rules(self, asset_id: UUID) -> list[dict[str, object]]:
        return [
            {
                "id": str(record.id),
                "asset_id": str(record.asset_id),
                "ordinal": record.ordinal,
                "effect": record.effect,
                "principals": list(record.principals_json),
                "when": dict(record.when_json),
                "columns": list(record.columns_json),
                "masks": dict(record.masks_json),
                "row_filter": record.row_filter_sql,
            }
            for record in self._session.scalars(
                select(PolicyRuleRecord)
                .where(PolicyRuleRecord.asset_id == asset_id)
                .order_by(PolicyRuleRecord.ordinal)
            )
        ]

    def list_auth_providers(self, cell_id: UUID) -> list[dict[str, object]]:
        return [
            {
                "id": str(record.id),
                "cell_id": str(record.cell_id),
                "ordinal": record.ordinal,
                "module": record.module,
                "args": dict(record.args_json),
                "enabled": record.enabled,
            }
            for record in self._session.scalars(
                select(AuthProviderRecord)
                .where(AuthProviderRecord.cell_id == cell_id)
                .order_by(AuthProviderRecord.ordinal)
            )
        ]

    def get_cell_draft(self, cell_id: UUID) -> dict[str, object]:
        cell = self.get_cell(cell_id)
        assignments = [
            item
            for item in self.list_cell_tenant_assignments()
            if item["cell_id"] == str(cell_id)
        ]
        assets = []
        for asset in self.list_assets(cell_id):
            assets.append({**asset, "policy_rules": self.list_policy_rules(UUID(str(asset["id"])))})
        return {
            "cell": cell,
            "assignments": assignments,
            "runtime_settings": self.get_runtime_settings(cell_id),
            "catalogs": self.list_catalogs(cell_id),
            "assets": assets,
            "auth_providers": self.list_auth_providers(cell_id),
        }
```

- [ ] **Step 4: Add service read methods**

In `src/dal_obscura/control_plane/application/provisioning.py`, add:

```python
    def get_runtime_settings(self, cell_id: UUID) -> dict[str, object] | None:
        return self._store.get_runtime_settings(cell_id)

    def list_catalogs(self, cell_id: UUID) -> list[dict[str, object]]:
        return self._store.list_catalogs(cell_id)

    def list_assets(self, cell_id: UUID) -> list[dict[str, object]]:
        return self._store.list_assets(cell_id)

    def list_policy_rules(self, asset_id: UUID) -> list[dict[str, object]]:
        return self._store.list_policy_rules(asset_id)

    def list_auth_providers(self, cell_id: UUID) -> list[dict[str, object]]:
        return self._store.list_auth_providers(cell_id)

    def get_cell_draft(self, cell_id: UUID) -> dict[str, object]:
        return self._store.get_cell_draft(cell_id)
```

- [ ] **Step 5: Map lookup failures to 404 and add API routes**

In `src/dal_obscura/control_plane/interfaces/api.py`, update `with_service()`:

```python
            except ValidationFailure as exc:
                session.rollback()
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except LookupError as exc:
                session.rollback()
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except Exception:
                session.rollback()
                raise
```

Then add these routes after the basic inventory routes:

```python
    @app.get("/v1/cells/{cell_id}/runtime-settings", dependencies=[Depends(require_admin)])
    def get_runtime_settings(cell_id: UUID) -> object:
        return with_service(lambda service: service.get_runtime_settings(cell_id))

    @app.get("/v1/cells/{cell_id}/catalogs", dependencies=[Depends(require_admin)])
    def list_catalogs(cell_id: UUID) -> object:
        return with_service(lambda service: service.list_catalogs(cell_id))

    @app.get("/v1/cells/{cell_id}/assets", dependencies=[Depends(require_admin)])
    def list_assets(cell_id: UUID) -> object:
        return with_service(lambda service: service.list_assets(cell_id))

    @app.get("/v1/assets/{asset_id}/policy-rules", dependencies=[Depends(require_admin)])
    def list_policy_rules(asset_id: UUID) -> object:
        return with_service(lambda service: service.list_policy_rules(asset_id))

    @app.get("/v1/cells/{cell_id}/auth-providers", dependencies=[Depends(require_admin)])
    def list_auth_providers(cell_id: UUID) -> object:
        return with_service(lambda service: service.list_auth_providers(cell_id))

    @app.get("/v1/cells/{cell_id}/draft", dependencies=[Depends(require_admin)])
    def get_cell_draft(cell_id: UUID) -> object:
        return with_service(lambda service: service.get_cell_draft(cell_id))
```

- [ ] **Step 6: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/interfaces/control_plane/test_api_inventory_reads.py -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add tests/interfaces/control_plane/test_api_inventory_reads.py \
  src/dal_obscura/control_plane/infrastructure/repositories.py \
  src/dal_obscura/control_plane/application/provisioning.py \
  src/dal_obscura/control_plane/interfaces/api.py
git commit -m "feat(control-plane): expose draft resource reads"
```

---

### Task 3: Publication Read API

**Files:**
- Modify: `tests/interfaces/control_plane/test_api_inventory_reads.py`
- Modify: `src/dal_obscura/control_plane/infrastructure/repositories.py`
- Modify: `src/dal_obscura/control_plane/application/provisioning.py`
- Modify: `src/dal_obscura/control_plane/interfaces/api.py`

- [ ] **Step 1: Write failing tests for publication list and active publication**

Append to `tests/interfaces/control_plane/test_api_inventory_reads.py`:

```python
def test_reads_publications_and_active_publication():
    client = _client()
    _, cell, _ = _provision_draft(client)

    assert client.get(
        f"/v1/cells/{cell['id']}/active-publication",
        headers=ADMIN_HEADERS,
    ).status_code == 404

    publication = client.post(
        f"/v1/cells/{cell['id']}/publications",
        headers=ADMIN_HEADERS,
    ).json()
    publications_before_activation = client.get(
        f"/v1/cells/{cell['id']}/publications",
        headers=ADMIN_HEADERS,
    ).json()

    assert publications_before_activation == [
        {
            "id": publication["publication_id"],
            "cell_id": cell["id"],
            "schema_version": 1,
            "status": "published",
            "manifest_hash": publication["manifest_hash"],
            "active": False,
        }
    ]

    client.post(
        f"/v1/cells/{cell['id']}/publications/{publication['publication_id']}/activate",
        headers=ADMIN_HEADERS,
    )
    active = client.get(
        f"/v1/cells/{cell['id']}/active-publication",
        headers=ADMIN_HEADERS,
    ).json()
    publications_after_activation = client.get(
        f"/v1/cells/{cell['id']}/publications",
        headers=ADMIN_HEADERS,
    ).json()

    assert active == {
        "cell_id": cell["id"],
        "publication_id": publication["publication_id"],
        "manifest_hash": publication["manifest_hash"],
        "status": "published",
    }
    assert publications_after_activation[0]["active"] is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/interfaces/control_plane/test_api_inventory_reads.py::test_reads_publications_and_active_publication -q
```

Expected: failure with `404 Not Found` for `GET /v1/cells/{cell_id}/publications`.

- [ ] **Step 3: Add repository publication read methods**

In `src/dal_obscura/control_plane/infrastructure/repositories.py`, add before `insert_compiled_publication()`:

```python
    def list_publications(self, cell_id: UUID) -> list[dict[str, object]]:
        active = self._session.get(ActivePublicationRecord, cell_id)
        active_publication_id = active.publication_id if active is not None else None
        return [
            {
                "id": str(record.id),
                "cell_id": str(record.cell_id),
                "schema_version": record.schema_version,
                "status": record.status,
                "manifest_hash": record.manifest_hash,
                "active": record.id == active_publication_id,
            }
            for record in self._session.scalars(
                select(ConfigPublicationRecord)
                .where(ConfigPublicationRecord.cell_id == cell_id)
                .order_by(ConfigPublicationRecord.created_at)
            )
        ]

    def get_active_publication_summary(self, cell_id: UUID) -> dict[str, str]:
        active = self._session.get(ActivePublicationRecord, cell_id)
        if active is None:
            raise LookupError(f"No active publication for cell {cell_id}")
        publication = self._session.get(ConfigPublicationRecord, active.publication_id)
        if publication is None:
            raise LookupError(f"No publication {active.publication_id}")
        return {
            "cell_id": str(active.cell_id),
            "publication_id": str(active.publication_id),
            "manifest_hash": publication.manifest_hash,
            "status": publication.status,
        }
```

- [ ] **Step 4: Add service methods**

In `src/dal_obscura/control_plane/application/provisioning.py`, add:

```python
    def list_publications(self, cell_id: UUID) -> list[dict[str, object]]:
        return self._store.list_publications(cell_id)

    def get_active_publication_summary(self, cell_id: UUID) -> dict[str, str]:
        return self._store.get_active_publication_summary(cell_id)
```

- [ ] **Step 5: Add API routes**

In `src/dal_obscura/control_plane/interfaces/api.py`, add:

```python
    @app.get("/v1/cells/{cell_id}/publications", dependencies=[Depends(require_admin)])
    def list_publications(cell_id: UUID) -> object:
        return with_service(lambda service: service.list_publications(cell_id))

    @app.get("/v1/cells/{cell_id}/active-publication", dependencies=[Depends(require_admin)])
    def get_active_publication_summary(cell_id: UUID) -> object:
        return with_service(lambda service: service.get_active_publication_summary(cell_id))
```

- [ ] **Step 6: Run focused tests**

Run:

```bash
uv run pytest tests/interfaces/control_plane/test_api_inventory_reads.py -q
```

Expected: all tests pass.

- [ ] **Step 7: Run existing publish flow test**

Run:

```bash
uv run pytest tests/interfaces/control_plane/test_api_publish_flow.py -q
```

Expected: existing publish flow test passes.

- [ ] **Step 8: Commit**

```bash
git add tests/interfaces/control_plane/test_api_inventory_reads.py \
  src/dal_obscura/control_plane/infrastructure/repositories.py \
  src/dal_obscura/control_plane/application/provisioning.py \
  src/dal_obscura/control_plane/interfaces/api.py
git commit -m "feat(control-plane): expose publication reads"
```

---

### Task 4: Presentation-Only UI Shell

**Files:**
- Create: `tests/interfaces/control_plane/test_ui_shell.py`
- Create: `src/dal_obscura/control_plane/interfaces/ui.py`
- Create: `src/dal_obscura/control_plane/interfaces/ui_assets/shell.html`
- Create: `src/dal_obscura/control_plane/interfaces/ui_assets/static/styles.css`
- Create: `src/dal_obscura/control_plane/interfaces/ui_assets/static/app.js`
- Modify: `src/dal_obscura/control_plane/interfaces/api.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: Write failing UI shell and boundary tests**

Create `tests/interfaces/control_plane/test_ui_shell.py`:

```python
from __future__ import annotations

import ast
import inspect

from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import Base
from dal_obscura.control_plane.interfaces import ui
from dal_obscura.control_plane.interfaces.api import create_app


def _client() -> TestClient:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return TestClient(create_app(session_factory(engine), admin_token="test-admin"))


def test_ui_shell_is_served_without_embedding_admin_token():
    client = _client()

    response = client.get("/ui")

    assert response.status_code == 200
    assert "dal-obscura control plane" in response.text
    assert "test-admin" not in response.text
    assert "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN" not in response.text
    assert 'data-api-root="/v1"' in response.text


def test_ui_static_app_targets_v1_api_only_for_state():
    client = _client()

    response = client.get("/ui/static/app.js")

    assert response.status_code == 200
    assert "/v1" in response.text
    assert "/ui/actions" not in response.text
    assert "/ui/forms" not in response.text
    assert "ProvisioningService" not in response.text


def test_ui_module_does_not_import_application_or_repository_layers():
    forbidden_prefixes = (
        "dal_obscura.control_plane.application",
        "dal_obscura.control_plane.infrastructure",
        "dal_obscura.common.config_store",
    )
    tree = ast.parse(inspect.getsource(ui))
    imported_modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.append(node.module)
        elif isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)

    assert not [
        module
        for module in imported_modules
        if module.startswith(forbidden_prefixes)
    ]
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/interfaces/control_plane/test_ui_shell.py -q
```

Expected: import or route failures because `dal_obscura.control_plane.interfaces.ui` and `/ui` do not exist.

- [ ] **Step 3: Add package-data config**

In `pyproject.toml`, add after `[tool.pytest.ini_options]`:

```toml
[tool.setuptools.package-data]
"dal_obscura.control_plane.interfaces" = ["ui_assets/**/*"]
```

- [ ] **Step 4: Add the UI adapter**

Create `src/dal_obscura/control_plane/interfaces/ui.py`:

```python
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
```

- [ ] **Step 5: Mount the UI adapter**

In `src/dal_obscura/control_plane/interfaces/api.py`, add this import:

```python
from dal_obscura.control_plane.interfaces.ui import install_ui
```

Then call `install_ui(app)` immediately after creating the `FastAPI` app:

```python
    app = FastAPI(title="dal-obscura control plane")
    install_ui(app)
```

- [ ] **Step 6: Add shell HTML**

Create `src/dal_obscura/control_plane/interfaces/ui_assets/shell.html`:

```html
<!doctype html>
<html lang="en" data-api-root="/v1">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>dal-obscura control plane</title>
    <link rel="stylesheet" href="/ui/static/styles.css">
  </head>
  <body>
    <div class="app-shell">
      <aside class="sidebar" aria-label="Resource sections">
        <div class="brand">
          <span class="brand-mark">do</span>
          <span>dal-obscura</span>
        </div>
        <nav class="nav-list" id="section-nav">
          <button type="button" class="nav-item is-active" data-section="tenants">Tenants</button>
          <button type="button" class="nav-item" data-section="cells">Cells</button>
          <button type="button" class="nav-item" data-section="assignments">Assignments</button>
          <button type="button" class="nav-item" data-section="runtime">Runtime</button>
          <button type="button" class="nav-item" data-section="catalogs">Catalogs</button>
          <button type="button" class="nav-item" data-section="assets">Assets</button>
          <button type="button" class="nav-item" data-section="policies">Policies</button>
          <button type="button" class="nav-item" data-section="auth">Auth</button>
          <button type="button" class="nav-item" data-section="publications">Publications</button>
        </nav>
      </aside>
      <main class="main-panel">
        <header class="topbar">
          <div>
            <p class="eyebrow">Control plane</p>
            <h1 id="section-title">Tenants</h1>
          </div>
          <form id="token-form" class="token-form">
            <label for="admin-token">Admin token</label>
            <input id="admin-token" name="admin-token" type="password" autocomplete="off">
            <button type="submit">Use token</button>
          </form>
        </header>
        <section id="toast-region" class="toast-region" aria-live="polite"></section>
        <section id="content-region" class="content-region" aria-live="polite">
          <div class="empty-state">
            <h2>Connect to the API</h2>
            <p>Enter the control-plane admin token, then choose a resource section.</p>
          </div>
        </section>
      </main>
    </div>
    <script type="module" src="/ui/static/app.js"></script>
  </body>
</html>
```

- [ ] **Step 7: Add minimal CSS**

Create `src/dal_obscura/control_plane/interfaces/ui_assets/static/styles.css`:

```css
:root {
  color-scheme: light;
  --bg: #f5f7fa;
  --panel: #ffffff;
  --ink: #17212b;
  --muted: #5f6d7a;
  --line: #d9e1e8;
  --accent: #1e6f5c;
  --danger: #a33d3d;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  background: var(--bg);
  color: var(--ink);
}

button,
input,
select,
textarea {
  font: inherit;
}

.app-shell {
  display: grid;
  grid-template-columns: 248px 1fr;
  min-height: 100vh;
}

.sidebar {
  background: #111820;
  color: #e7eef5;
  padding: 18px 14px;
}

.brand {
  display: flex;
  align-items: center;
  gap: 10px;
  font-weight: 700;
  margin-bottom: 28px;
}

.brand-mark {
  display: grid;
  place-items: center;
  width: 32px;
  height: 32px;
  border-radius: 6px;
  background: var(--accent);
  color: #fff;
  font-size: 13px;
}

.nav-list {
  display: grid;
  gap: 4px;
}

.nav-item {
  border: 0;
  border-radius: 6px;
  background: transparent;
  color: #b8c4ce;
  cursor: pointer;
  padding: 10px 12px;
  text-align: left;
}

.nav-item.is-active,
.nav-item:hover {
  background: #2a3d4d;
  color: #fff;
}

.main-panel {
  min-width: 0;
  padding: 22px;
}

.topbar {
  align-items: end;
  display: flex;
  gap: 20px;
  justify-content: space-between;
  margin-bottom: 18px;
}

.eyebrow {
  color: var(--muted);
  font-size: 12px;
  margin: 0 0 4px;
  text-transform: uppercase;
}

h1 {
  font-size: 28px;
  margin: 0;
}

.token-form {
  align-items: end;
  display: flex;
  gap: 8px;
}

.token-form label {
  color: var(--muted);
  font-size: 12px;
}

.token-form input,
.filter-input,
.drawer-field,
.json-field {
  border: 1px solid var(--line);
  border-radius: 5px;
  padding: 8px 10px;
}

.token-form button,
.primary-action {
  background: var(--accent);
  border: 0;
  border-radius: 5px;
  color: #fff;
  cursor: pointer;
  padding: 9px 12px;
}

.content-region {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 8px;
  min-height: 520px;
  overflow: hidden;
}

.section-toolbar {
  align-items: center;
  border-bottom: 1px solid var(--line);
  display: flex;
  gap: 10px;
  justify-content: space-between;
  padding: 14px;
}

.table-wrap {
  overflow: auto;
}

.resource-table {
  border-collapse: collapse;
  width: 100%;
}

.resource-table th,
.resource-table td {
  border-bottom: 1px solid var(--line);
  padding: 11px 14px;
  text-align: left;
  vertical-align: top;
}

.resource-table th {
  color: var(--muted);
  font-size: 12px;
  text-transform: uppercase;
}

.empty-state,
.error-state {
  padding: 40px;
}

.error-state {
  color: var(--danger);
}

.toast-region {
  position: fixed;
  right: 18px;
  top: 18px;
  z-index: 10;
}

.toast {
  background: #17212b;
  border-radius: 6px;
  color: #fff;
  margin-bottom: 8px;
  padding: 10px 12px;
}

@media (max-width: 840px) {
  .app-shell {
    grid-template-columns: 1fr;
  }

  .sidebar {
    position: static;
  }

  .topbar,
  .token-form {
    align-items: stretch;
    flex-direction: column;
  }
}
```

- [ ] **Step 8: Add the initial JavaScript implementation**

Create `src/dal_obscura/control_plane/interfaces/ui_assets/static/app.js`:

```javascript
const apiRoot = document.documentElement.dataset.apiRoot || "/v1";
const state = {
  token: sessionStorage.getItem("dal-obscura-admin-token") || "",
  section: "tenants",
};

const title = document.querySelector("#section-title");
const content = document.querySelector("#content-region");
const tokenForm = document.querySelector("#token-form");
const tokenInput = document.querySelector("#admin-token");
const nav = document.querySelector("#section-nav");
const toasts = document.querySelector("#toast-region");

if (state.token) {
  tokenInput.value = state.token;
}

tokenForm.addEventListener("submit", (event) => {
  event.preventDefault();
  state.token = tokenInput.value.trim();
  sessionStorage.setItem("dal-obscura-admin-token", state.token);
  showToast("Token set for this browser session");
  loadSection(state.section);
});

nav.addEventListener("click", (event) => {
  const button = event.target.closest("[data-section]");
  if (!button) return;
  state.section = button.dataset.section;
  document.querySelectorAll(".nav-item").forEach((item) => {
    item.classList.toggle("is-active", item === button);
  });
  title.textContent = button.textContent;
  loadSection(state.section);
});

async function apiGet(path) {
  const response = await fetch(`${apiRoot}${path}`, {
    headers: { Authorization: `Bearer ${state.token}` },
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json();
}

async function loadSection(section) {
  if (!state.token) {
    renderEmpty("Connect to the API", "Enter the control-plane admin token first.");
    return;
  }
  const path = sectionPath(section);
  if (!path) {
    renderEmpty("Choose context", "This section needs a selected cell or asset.");
    return;
  }
  try {
    const data = await apiGet(path);
    renderTable(section, Array.isArray(data) ? data : [data]);
  } catch (error) {
    renderError(error.message);
  }
}

function sectionPath(section) {
  const paths = {
    tenants: "/tenants",
    cells: "/cells",
    assignments: "/cell-tenant-assignments",
  };
  return paths[section] || null;
}

function renderTable(section, rows) {
  if (rows.length === 0) {
    renderEmpty(`No ${section}`, "No records were returned by the API.");
    return;
  }
  const columns = Object.keys(rows[0]);
  content.innerHTML = `
    <div class="section-toolbar">
      <input class="filter-input" type="search" placeholder="Filter rows" data-filter>
      <button class="primary-action" type="button" data-refresh>Refresh</button>
    </div>
    <div class="table-wrap">
      <table class="resource-table">
        <thead>
          <tr>${columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr>
        </thead>
        <tbody>
          ${rows.map((row) => renderRow(columns, row)).join("")}
        </tbody>
      </table>
    </div>
  `;
  content.querySelector("[data-refresh]").addEventListener("click", () => loadSection(section));
  content.querySelector("[data-filter]").addEventListener("input", (event) => {
    const query = event.target.value.toLowerCase();
    content.querySelectorAll("tbody tr").forEach((row) => {
      row.hidden = !row.textContent.toLowerCase().includes(query);
    });
  });
}

function renderRow(columns, row) {
  return `<tr>${columns.map((column) => `<td>${escapeHtml(formatValue(row[column]))}</td>`).join("")}</tr>`;
}

function formatValue(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function renderEmpty(heading, message) {
  content.innerHTML = `<div class="empty-state"><h2>${escapeHtml(heading)}</h2><p>${escapeHtml(message)}</p></div>`;
}

function renderError(message) {
  content.innerHTML = `<div class="error-state"><h2>API request failed</h2><p>${escapeHtml(message)}</p></div>`;
}

function showToast(message) {
  const toast = document.createElement("div");
  toast.className = "toast";
  toast.textContent = message;
  toasts.append(toast);
  window.setTimeout(() => toast.remove(), 2500);
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[char]));
}
```

- [ ] **Step 9: Run tests**

Run:

```bash
uv run pytest tests/interfaces/control_plane/test_ui_shell.py -q
```

Expected: all UI shell tests pass.

- [ ] **Step 10: Commit**

```bash
git add tests/interfaces/control_plane/test_ui_shell.py \
  src/dal_obscura/control_plane/interfaces/api.py \
  src/dal_obscura/control_plane/interfaces/ui.py \
  src/dal_obscura/control_plane/interfaces/ui_assets/shell.html \
  src/dal_obscura/control_plane/interfaces/ui_assets/static/styles.css \
  src/dal_obscura/control_plane/interfaces/ui_assets/static/app.js \
  pyproject.toml
git commit -m "feat(control-plane): serve presentation-only UI shell"
```

---

### Task 5: Richer Resource Navigation and Forms Through `/v1`

**Files:**
- Modify: `tests/interfaces/control_plane/test_ui_shell.py`
- Modify: `src/dal_obscura/control_plane/interfaces/ui_assets/static/app.js`
- Modify: `src/dal_obscura/control_plane/interfaces/ui_assets/static/styles.css`

- [ ] **Step 1: Add static smoke tests for section API mapping**

Append to `tests/interfaces/control_plane/test_ui_shell.py`:

```python
def test_ui_javascript_maps_resource_sections_to_v1_endpoints():
    client = _client()

    app_js = client.get("/ui/static/app.js").text

    expected_paths = [
        'tenants: { list: "/tenants"',
        'cells: { list: "/cells"',
        'assignments: { list: "/cell-tenant-assignments"',
        'runtime: { list: "/cells/{cell_id}/runtime-settings"',
        'catalogs: { list: "/cells/{cell_id}/catalogs"',
        'assets: { list: "/cells/{cell_id}/assets"',
        'policies: { list: "/assets/{asset_id}/policy-rules"',
        'auth: { list: "/cells/{cell_id}/auth-providers"',
        'publications: { list: "/cells/{cell_id}/publications"',
    ]
    for path in expected_paths:
        assert path in app_js
    assert 'fetch(`${apiRoot}${resolvedPath}`' in app_js
    assert "/ui/" not in app_js.replace("/ui/static/app.js", "")
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/interfaces/control_plane/test_ui_shell.py::test_ui_javascript_maps_resource_sections_to_v1_endpoints -q
```

Expected: assertion failure because the first `app.js` only maps tenants, cells, and assignments.

- [ ] **Step 3: Replace section path mapping with context-aware resources**

In `src/dal_obscura/control_plane/interfaces/ui_assets/static/app.js`, replace the `sectionPath()` function with:

```javascript
const sectionConfig = {
  tenants: { list: "/tenants", context: [] },
  cells: { list: "/cells", context: [] },
  assignments: { list: "/cell-tenant-assignments", context: [] },
  runtime: { list: "/cells/{cell_id}/runtime-settings", context: ["cell_id"] },
  catalogs: { list: "/cells/{cell_id}/catalogs", context: ["cell_id"] },
  assets: { list: "/cells/{cell_id}/assets", context: ["cell_id"] },
  policies: { list: "/assets/{asset_id}/policy-rules", context: ["asset_id"] },
  auth: { list: "/cells/{cell_id}/auth-providers", context: ["cell_id"] },
  publications: { list: "/cells/{cell_id}/publications", context: ["cell_id"] },
};

function sectionPath(section) {
  const config = sectionConfig[section];
  if (!config) return null;
  return resolvePath(config.list);
}

function resolvePath(path) {
  return path.replace(/\{([^}]+)\}/g, (_match, key) => {
    const value = window.prompt(`Enter ${key}`);
    if (!value) {
      throw new Error(`Missing ${key}`);
    }
    return encodeURIComponent(value);
  });
}
```

Then update `apiGet()` so the fetch line uses the `resolvedPath` variable:

```javascript
async function apiGet(path) {
  const resolvedPath = path;
  const response = await fetch(`${apiRoot}${resolvedPath}`, {
    headers: { Authorization: `Bearer ${state.token}` },
  });
```

- [ ] **Step 4: Add a generic JSON-backed create/edit drawer**

Add this markup rendering function to `app.js` after `renderTable()`:

```javascript
function renderJsonDrawer(titleText, submitLabel, initialValue, onSubmit) {
  const drawer = document.createElement("dialog");
  drawer.className = "drawer";
  drawer.innerHTML = `
    <form method="dialog" class="drawer-form">
      <header class="drawer-header">
        <h2>${escapeHtml(titleText)}</h2>
        <button type="button" data-close>Close</button>
      </header>
      <textarea class="json-field" rows="18">${escapeHtml(JSON.stringify(initialValue, null, 2))}</textarea>
      <footer class="drawer-footer">
        <button class="primary-action" type="submit">${escapeHtml(submitLabel)}</button>
      </footer>
    </form>
  `;
  drawer.querySelector("[data-close]").addEventListener("click", () => drawer.close());
  drawer.querySelector("form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      const payload = JSON.parse(drawer.querySelector(".json-field").value);
      await onSubmit(payload);
      drawer.close();
    } catch (error) {
      showToast(error.message);
    }
  });
  document.body.append(drawer);
  drawer.showModal();
  drawer.addEventListener("close", () => drawer.remove());
}
```

This is intentionally generic for the first version. Later tasks can replace it with per-resource forms while still posting to `/v1`.

- [ ] **Step 5: Add drawer styling**

Append to `styles.css`:

```css
.drawer {
  border: 1px solid var(--line);
  border-radius: 8px;
  max-width: 760px;
  padding: 0;
  width: min(760px, calc(100vw - 32px));
}

.drawer::backdrop {
  background: rgba(17, 24, 32, 0.38);
}

.drawer-form {
  display: grid;
  gap: 14px;
  padding: 16px;
}

.drawer-header,
.drawer-footer {
  align-items: center;
  display: flex;
  justify-content: space-between;
}

.json-field {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  min-height: 360px;
  resize: vertical;
  width: 100%;
}
```

- [ ] **Step 6: Run UI shell tests**

Run:

```bash
uv run pytest tests/interfaces/control_plane/test_ui_shell.py -q
```

Expected: all UI shell tests pass.

- [ ] **Step 7: Commit**

```bash
git add tests/interfaces/control_plane/test_ui_shell.py \
  src/dal_obscura/control_plane/interfaces/ui_assets/static/app.js \
  src/dal_obscura/control_plane/interfaces/ui_assets/static/styles.css
git commit -m "feat(control-plane): add resource-section UI navigation"
```

---

### Task 6: README and Final Verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add README coverage**

In `README.md`, after the control-plane start command block, add:

````markdown
Open the operator dashboard from the same control-plane process:

```bash
open http://localhost:8820/ui
```

The dashboard shell is served by the control plane, but all state reads and
writes go through the protected `/v1` API. Enter
`DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN` in the browser when prompted. The token
is not rendered into the HTML shell by the service.
````

- [ ] **Step 2: Run focused tests**

Run:

```bash
uv run pytest tests/interfaces/control_plane/test_api_inventory_reads.py tests/interfaces/control_plane/test_ui_shell.py tests/interfaces/control_plane/test_api_publish_flow.py -q
```

Expected: all focused control-plane API and UI tests pass.

- [ ] **Step 3: Run lint**

Run:

```bash
uv run ruff check .
```

Expected: no lint errors.

- [ ] **Step 4: Run formatting check**

Run:

```bash
uv run ruff format --check .
```

Expected: all files already formatted.

- [ ] **Step 5: Run type check**

Run:

```bash
uv run ty check
```

Expected: type check passes.

- [ ] **Step 6: Run non-heavy pytest**

Run:

```bash
uv run pytest
```

Expected: default non-heavy pytest suite passes.

- [ ] **Step 7: Commit**

```bash
git add README.md
git commit -m "docs(control-plane): document operator dashboard"
```

---

## Self-Review Checklist

- Spec coverage:
  - Same-process `/ui`: Task 4.
  - `/v1` as only state API: Tasks 1 through 5, especially UI boundary tests.
  - Resource-section navigation: Tasks 4 and 5.
  - Read/write admin console: Tasks 1 through 5 add reads and preserve existing write endpoints for UI use.
  - Admin-token handling without Jinja exposure: Task 4.
  - Error handling: Task 4 adds visible API errors; Task 5 keeps failed context prompts visible through toasts.
  - Tests-first implementation: every implementation task starts with failing tests.
- No placeholders:
  - Each task names exact files, commands, expected failures, and implementation snippets.
- Type consistency:
  - Service methods delegate to repository methods using `UUID` where path params supply UUIDs.
  - API routes return `object`, consistent with existing control-plane routes.
  - UI static code uses `/v1` API paths only.
