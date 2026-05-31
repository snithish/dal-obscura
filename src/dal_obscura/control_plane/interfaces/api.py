from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast
from urllib.parse import parse_qs
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.application.provisioning import ProvisioningService
from dal_obscura.control_plane.interfaces.ui import install_ui


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TenantRequest(_StrictModel):
    slug: str = Field(min_length=1)
    display_name: str = Field(min_length=1)


class CellRequest(_StrictModel):
    name: str = Field(min_length=1)
    region: str = Field(min_length=1)


class TenantCellRequest(CellRequest):
    shard_key: str = Field(default="default", min_length=1)


class TenantCellAssignmentRequest(_StrictModel):
    cell_id: UUID
    shard_key: str = Field(default="default", min_length=1)


class CellTenantRequest(_StrictModel):
    shard_key: str = Field(default="default", min_length=1)


class RuntimeSettingsRequest(_StrictModel):
    ticket_ttl_seconds: int = Field(gt=0)
    max_tickets: int = Field(gt=0)
    max_ticket_exchanges: int = Field(gt=0)
    path_rules: list[dict[str, Any]] = Field(default_factory=list)


class CatalogRequest(_StrictModel):
    module: str = Field(min_length=1)
    options: dict[str, Any] = Field(default_factory=dict)


class AssetRequest(_StrictModel):
    backend: str = Field(min_length=1)
    table_identifier: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)


class PolicyRulesRequest(_StrictModel):
    rules: list[dict[str, Any]]


class AssetOwnersRequest(_StrictModel):
    owners: list[str] = Field(default_factory=list)


class AuthProvidersRequest(_StrictModel):
    providers: list[dict[str, Any]]


def create_app(session_maker: sessionmaker[Session], *, admin_token: str) -> FastAPI:  # noqa: C901
    app = FastAPI(title="dal-obscura control plane")
    install_ui(app)

    def require_admin(authorization: str = Header(default="")) -> None:
        expected = f"Bearer {admin_token}"
        if authorization != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")

    def with_service(callback: Callable[[ProvisioningService], object]) -> object:
        with session_maker() as session:
            service = ProvisioningService(session)
            try:
                result = callback(service)
                session.commit()
                return result
            except ValidationFailure as exc:
                session.rollback()
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except LookupError as exc:
                session.rollback()
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except Exception:
                session.rollback()
                raise

    @app.get("/v1/tenants", dependencies=[Depends(require_admin)])
    def list_tenants() -> object:
        return with_service(lambda service: service.list_tenants())

    @app.get("/v1/workspace/summary", dependencies=[Depends(require_admin)])
    def get_workspace_summary() -> object:
        return with_service(lambda service: service.get_workspace_summary())

    @app.get("/v1/catalogs", dependencies=[Depends(require_admin)])
    def list_workspace_catalogs() -> object:
        return with_service(lambda service: service.list_workspace_catalogs())

    @app.get("/v1/settings/runtime", dependencies=[Depends(require_admin)])
    def get_workspace_runtime_settings() -> object:
        return with_service(lambda service: service.get_workspace_runtime_settings())

    @app.get("/v1/settings/auth-providers", dependencies=[Depends(require_admin)])
    def list_workspace_auth_providers() -> object:
        return with_service(lambda service: service.list_workspace_auth_providers())

    @app.get("/v1/assets", dependencies=[Depends(require_admin)])
    def list_workspace_assets() -> object:
        return with_service(lambda service: service.list_workspace_assets())

    @app.get("/v1/cells", dependencies=[Depends(require_admin)])
    def list_cells() -> object:
        return with_service(lambda service: service.list_cells())

    @app.get("/v1/tenants/{tenant_id}/cells", dependencies=[Depends(require_admin)])
    def list_tenant_cells(tenant_id: UUID) -> object:
        return with_service(lambda service: service.list_cells_for_tenant(tenant_id))

    @app.get("/v1/cell-tenant-assignments", dependencies=[Depends(require_admin)])
    def list_cell_tenant_assignments() -> object:
        return with_service(lambda service: service.list_cell_tenant_assignments())

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

    @app.get("/v1/assets/{asset_id}", dependencies=[Depends(require_admin)])
    def get_workspace_asset(asset_id: UUID) -> object:
        return with_service(lambda service: service.get_workspace_asset(asset_id))

    @app.get("/v1/publications/draft", dependencies=[Depends(require_admin)])
    def get_workspace_publication_draft() -> object:
        return with_service(lambda service: service.get_workspace_draft())

    @app.get("/v1/publications", dependencies=[Depends(require_admin)])
    def list_workspace_publications() -> object:
        return with_service(lambda service: service.list_workspace_publications())

    @app.get("/v1/cells/{cell_id}/auth-providers", dependencies=[Depends(require_admin)])
    def list_auth_providers(cell_id: UUID) -> object:
        return with_service(lambda service: service.list_auth_providers(cell_id))

    @app.get("/v1/cells/{cell_id}/draft", dependencies=[Depends(require_admin)])
    def get_cell_draft(cell_id: UUID) -> object:
        return with_service(lambda service: service.get_cell_draft(cell_id))

    @app.get("/v1/cells/{cell_id}/publications", dependencies=[Depends(require_admin)])
    def list_publications(cell_id: UUID) -> object:
        return with_service(lambda service: service.list_publications(cell_id))

    @app.get("/v1/cells/{cell_id}/active-publication", dependencies=[Depends(require_admin)])
    def get_active_publication_summary(cell_id: UUID) -> object:
        return with_service(lambda service: service.get_active_publication_summary(cell_id))

    @app.post("/v1/tenants", dependencies=[Depends(require_admin)])
    async def create_tenant(request: Request) -> object:
        payload = TenantRequest.model_validate(await _request_payload(request))
        return with_service(
            lambda service: service.create_tenant(
                slug=payload.slug,
                display_name=payload.display_name,
            )
        )

    @app.post("/v1/cells", dependencies=[Depends(require_admin)])
    async def create_cell(request: Request) -> object:
        payload = CellRequest.model_validate(await _request_payload(request))
        return with_service(
            lambda service: service.create_cell(name=payload.name, region=payload.region)
        )

    @app.post("/v1/tenants/{tenant_id}/cells", dependencies=[Depends(require_admin)])
    async def create_tenant_cell(tenant_id: UUID, request: Request) -> object:
        payload = TenantCellRequest.model_validate(await _request_payload(request))
        return with_service(
            lambda service: service.create_cell_for_tenant(
                tenant_id=tenant_id,
                name=payload.name,
                region=payload.region,
                shard_key=payload.shard_key,
            )
        )

    @app.put("/v1/cells/{cell_id}/tenants/{tenant_id}", dependencies=[Depends(require_admin)])
    def assign_tenant(cell_id: UUID, tenant_id: UUID, request: CellTenantRequest) -> object:
        return with_service(
            lambda service: service.assign_tenant(
                cell_id=cell_id,
                tenant_id=tenant_id,
                shard_key=request.shard_key,
            )
        ) or {"cell_id": str(cell_id), "tenant_id": str(tenant_id)}

    @app.post(
        "/v1/tenants/{tenant_id}/cell-assignments",
        dependencies=[Depends(require_admin)],
    )
    async def assign_cell_to_tenant(tenant_id: UUID, request: Request) -> object:
        payload = TenantCellAssignmentRequest.model_validate(await _request_payload(request))
        with_service(
            lambda service: service.assign_tenant(
                cell_id=payload.cell_id,
                tenant_id=tenant_id,
                shard_key=payload.shard_key,
            )
        )
        return with_service(lambda service: service.list_cells_for_tenant(tenant_id))

    @app.put(
        "/v1/tenants/{tenant_id}/cells/{cell_id}/runtime-settings",
        dependencies=[Depends(require_admin)],
    )
    def upsert_runtime_settings(
        tenant_id: UUID,
        cell_id: UUID,
        request: RuntimeSettingsRequest,
    ) -> object:
        del tenant_id
        return with_service(
            lambda service: service.upsert_runtime_settings(
                cell_id=cell_id,
                ttl=request.ticket_ttl_seconds,
                max_tickets=request.max_tickets,
                max_ticket_exchanges=request.max_ticket_exchanges,
                path_rules=request.path_rules,
            )
        ) or {"cell_id": str(cell_id)}

    @app.put("/v1/settings/runtime", dependencies=[Depends(require_admin)])
    def upsert_workspace_runtime_settings(request: RuntimeSettingsRequest) -> object:
        return with_service(
            lambda service: service.upsert_workspace_runtime_settings(
                ttl=request.ticket_ttl_seconds,
                max_tickets=request.max_tickets,
                max_ticket_exchanges=request.max_ticket_exchanges,
                path_rules=request.path_rules,
            )
        ) or {
            "ticket_ttl_seconds": request.ticket_ttl_seconds,
            "max_tickets": request.max_tickets,
            "max_ticket_exchanges": request.max_ticket_exchanges,
            "path_rules": request.path_rules,
        }

    @app.put(
        "/v1/tenants/{tenant_id}/cells/{cell_id}/catalogs/{name}",
        dependencies=[Depends(require_admin)],
    )
    async def upsert_catalog(
        tenant_id: UUID,
        cell_id: UUID,
        name: str,
        request: Request,
    ) -> object:
        payload = CatalogRequest.model_validate(await _request_payload(request))
        return with_service(
            lambda service: service.upsert_catalog(
                cell_id=cell_id,
                tenant_id=tenant_id,
                name=name,
                module=payload.module,
                options=payload.options,
            )
        )

    @app.put("/v1/catalogs/{name}", dependencies=[Depends(require_admin)])
    async def upsert_workspace_catalog(name: str, request: Request) -> object:
        payload = CatalogRequest.model_validate(await _request_payload(request))
        return with_service(
            lambda service: service.upsert_workspace_catalog(
                name=name,
                module=payload.module,
                options=payload.options,
            )
        )

    @app.put(
        "/v1/tenants/{tenant_id}/cells/{cell_id}/assets/{catalog}/{target}",
        dependencies=[Depends(require_admin)],
    )
    def upsert_asset(
        tenant_id: UUID,
        cell_id: UUID,
        catalog: str,
        target: str,
        request: AssetRequest,
    ) -> object:
        return with_service(
            lambda service: service.upsert_asset(
                cell_id=cell_id,
                tenant_id=tenant_id,
                catalog=catalog,
                target=target,
                backend=request.backend,
                table_identifier=request.table_identifier,
                options=request.options,
            )
        )

    @app.put("/v1/assets/{asset_id}/policy-rules", dependencies=[Depends(require_admin)])
    def replace_policy_rules(asset_id: UUID, request: PolicyRulesRequest) -> object:
        return with_service(
            lambda service: service.replace_policy_rules(asset_id=asset_id, rules=request.rules)
        ) or {"asset_id": str(asset_id)}

    @app.put("/v1/assets/{asset_id}/owners", dependencies=[Depends(require_admin)])
    def replace_asset_owners(asset_id: UUID, request: AssetOwnersRequest) -> object:
        owners = with_service(
            lambda service: service.replace_asset_owners(asset_id=asset_id, owners=request.owners)
        )
        return {"asset_id": str(asset_id), "owners": owners}

    @app.put("/v1/assets/{catalog}/{target}", dependencies=[Depends(require_admin)])
    def upsert_workspace_asset(catalog: str, target: str, request: AssetRequest) -> object:
        return with_service(
            lambda service: service.upsert_workspace_asset(
                catalog=catalog,
                target=target,
                backend=request.backend,
                table_identifier=request.table_identifier,
                options=request.options,
            )
        )

    @app.put("/v1/cells/{cell_id}/auth-providers", dependencies=[Depends(require_admin)])
    def replace_auth_providers(cell_id: UUID, request: AuthProvidersRequest) -> object:
        return with_service(
            lambda service: service.replace_auth_providers(
                cell_id=cell_id,
                providers=request.providers,
            )
        ) or {"cell_id": str(cell_id)}

    @app.put("/v1/settings/auth-providers", dependencies=[Depends(require_admin)])
    def replace_workspace_auth_providers(request: AuthProvidersRequest) -> object:
        return with_service(
            lambda service: service.replace_workspace_auth_providers(
                providers=request.providers,
            )
        ) or {"providers": request.providers}

    @app.post("/v1/cells/{cell_id}/publications", dependencies=[Depends(require_admin)])
    def create_publication(cell_id: UUID) -> object:
        return with_service(lambda service: service.create_publication(cell_id))

    @app.post("/v1/publications", dependencies=[Depends(require_admin)])
    def create_workspace_publication() -> object:
        return with_service(lambda service: service.create_workspace_publication())

    @app.post(
        "/v1/cells/{cell_id}/publications/{publication_id}/activate",
        dependencies=[Depends(require_admin)],
    )
    def activate_publication(cell_id: UUID, publication_id: UUID) -> object:
        return with_service(
            lambda service: service.activate_publication(
                cell_id=cell_id,
                publication_id=publication_id,
            )
        )

    @app.post(
        "/v1/publications/{publication_id}/activate",
        dependencies=[Depends(require_admin)],
    )
    def activate_workspace_publication(publication_id: UUID) -> object:
        return with_service(lambda service: service.activate_workspace_publication(publication_id))

    return app


async def _request_payload(request: Request) -> dict[str, object]:
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        raw = await request.json()
        return cast(dict[str, object], raw) if isinstance(raw, dict) else {}
    if "application/x-www-form-urlencoded" in content_type:
        raw = (await request.body()).decode("utf-8")
        payload: dict[str, object] = {
            key: values[-1]
            for key, values in parse_qs(raw, keep_blank_values=True).items()
            if values
        }
        if isinstance(payload.get("options"), str):
            payload["options"] = _json_object(payload["options"])
        return payload
    return {}


def _json_object(raw: object) -> dict[str, object]:
    if not isinstance(raw, str) or not raw.strip():
        return {}
    import json

    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("options must be a JSON object")
    return {str(key): item for key, item in value.items()}
