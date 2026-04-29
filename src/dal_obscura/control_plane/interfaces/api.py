from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.application.provisioning import ProvisioningService


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TenantRequest(_StrictModel):
    slug: str = Field(min_length=1)
    display_name: str = Field(min_length=1)


class CellRequest(_StrictModel):
    name: str = Field(min_length=1)
    region: str = Field(min_length=1)


class CellTenantRequest(_StrictModel):
    shard_key: str = Field(default="default", min_length=1)


class RuntimeSettingsRequest(_StrictModel):
    ticket_ttl_seconds: int = Field(gt=0)
    max_tickets: int = Field(gt=0)
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


class AuthProvidersRequest(_StrictModel):
    providers: list[dict[str, Any]]


def create_app(session_maker: sessionmaker[Session], *, admin_token: str) -> FastAPI:  # noqa: C901
    app = FastAPI(title="dal-obscura control plane")

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
            except Exception:
                session.rollback()
                raise

    @app.post("/v1/tenants", dependencies=[Depends(require_admin)])
    def create_tenant(request: TenantRequest) -> object:
        return with_service(
            lambda service: service.create_tenant(
                slug=request.slug,
                display_name=request.display_name,
            )
        )

    @app.post("/v1/cells", dependencies=[Depends(require_admin)])
    def create_cell(request: CellRequest) -> object:
        return with_service(
            lambda service: service.create_cell(name=request.name, region=request.region)
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
                path_rules=request.path_rules,
            )
        ) or {"cell_id": str(cell_id)}

    @app.put(
        "/v1/tenants/{tenant_id}/cells/{cell_id}/catalogs/{name}",
        dependencies=[Depends(require_admin)],
    )
    def upsert_catalog(
        tenant_id: UUID,
        cell_id: UUID,
        name: str,
        request: CatalogRequest,
    ) -> object:
        return with_service(
            lambda service: service.upsert_catalog(
                cell_id=cell_id,
                tenant_id=tenant_id,
                name=name,
                module=request.module,
                options=request.options,
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

    @app.put("/v1/cells/{cell_id}/auth-providers", dependencies=[Depends(require_admin)])
    def replace_auth_providers(cell_id: UUID, request: AuthProvidersRequest) -> object:
        return with_service(
            lambda service: service.replace_auth_providers(
                cell_id=cell_id,
                providers=request.providers,
            )
        ) or {"cell_id": str(cell_id)}

    @app.post("/v1/cells/{cell_id}/publications", dependencies=[Depends(require_admin)])
    def create_publication(cell_id: UUID) -> object:
        return with_service(lambda service: service.create_publication(cell_id))

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

    return app
