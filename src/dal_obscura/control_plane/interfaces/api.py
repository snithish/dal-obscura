from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any, cast
from urllib.parse import parse_qs, urlencode
from urllib.request import Request as UrlRequest
from urllib.request import urlopen
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.control_plane.application.provisioning import ProvisioningService
from dal_obscura.control_plane.interfaces.ui import install_ui
from dal_obscura.data_plane.application.ports.identity import AuthenticationRequest
from dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks import (
    OidcJwksIdentityProvider,
)


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


class AssetSchemaFieldRequest(_StrictModel):
    name: str = Field(min_length=1)
    type: str = Field(default="string", min_length=1)
    nullable: bool = True


class AssetSchemaFieldsRequest(_StrictModel):
    fields: list[AssetSchemaFieldRequest] = Field(default_factory=list)


class AuthProvidersRequest(_StrictModel):
    providers: list[dict[str, Any]]


class DemoLoginRequest(_StrictModel):
    login_hint: str = Field(min_length=1)


OidcActorResolver = Callable[[str], object]


def create_oidc_actor_resolver(
    *,
    issuer: str,
    audience: str | None,
    jwks_url: str | None,
    subject_claim: str,
    group_claims: tuple[str, ...],
) -> OidcActorResolver:
    provider = OidcJwksIdentityProvider(
        issuer=issuer,
        audience=audience or None,
        jwks_url=jwks_url or None,
        subject_claim=subject_claim,
        group_claims=group_claims,
    )

    def resolve(token: str) -> dict[str, object]:
        principal = provider.authenticate(
            AuthenticationRequest(headers={"authorization": f"Bearer {token}"})
        )
        return {"principal": principal.id, "groups": principal.groups}

    return resolve


def create_app(  # noqa: C901
    session_maker: sessionmaker[Session],
    *,
    admin_token: str,
    oidc_actor_resolver: OidcActorResolver | None = None,
    oidc_admin_group: str | None = None,
    ui_auth_config: Mapping[str, object] | None = None,
) -> FastAPI:
    app = FastAPI(title="dal-obscura control plane")
    install_ui(app)

    def require_actor(authorization: str = Header(default="")) -> ControlPlaneActor:
        expected = f"Bearer {admin_token}"
        if authorization != expected:
            if oidc_actor_resolver is None:
                raise HTTPException(status_code=401, detail="Unauthorized")
            actor = _oidc_actor_from_header(
                authorization,
                resolver=oidc_actor_resolver,
                admin_group=oidc_admin_group,
            )
            if actor is None:
                raise HTTPException(status_code=401, detail="Unauthorized")
            return actor
        return ControlPlaneActor.for_platform_admin("platform:admin")

    def require_admin(actor: ControlPlaneActor = Depends(require_actor)) -> ControlPlaneActor:  # noqa: B008
        if not actor.platform_admin:
            raise HTTPException(status_code=403, detail="Platform admin required")
        return actor

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
            except AuthorizationFailure as exc:
                session.rollback()
                raise HTTPException(status_code=403, detail=str(exc)) from exc
            except LookupError as exc:
                session.rollback()
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except Exception:
                session.rollback()
                raise

    @app.get("/v1/session")
    def get_session(actor: ControlPlaneActor = Depends(require_actor)) -> object:  # noqa: B008
        return _actor_response(actor)

    @app.get("/v1/ui-auth-config")
    def get_ui_auth_config() -> object:
        if ui_auth_config is None:
            raise HTTPException(status_code=404, detail="UI auth is not configured")
        return _public_ui_auth_config(ui_auth_config)

    @app.post("/v1/demo-login")
    def demo_login(request: DemoLoginRequest) -> object:
        if ui_auth_config is None:
            raise HTTPException(status_code=404, detail="Demo login is not configured")
        demo_login_config = _demo_login_config(ui_auth_config)
        if not demo_login_config:
            raise HTTPException(status_code=404, detail="Demo login is not configured")
        username = request.login_hint.strip()
        passwords = cast(dict[str, str], demo_login_config["passwords"])
        if username not in passwords:
            raise HTTPException(status_code=404, detail="Demo persona is not configured")
        return {"access_token": _exchange_demo_password_token(demo_login_config, username)}

    @app.get("/v1/tenants", dependencies=[Depends(require_actor)])
    def list_tenants() -> object:
        return with_service(lambda service: service.list_tenants())

    @app.get("/v1/workspace/summary", dependencies=[Depends(require_actor)])
    def get_workspace_summary() -> object:
        return with_service(lambda service: service.get_workspace_summary())

    @app.get("/v1/catalogs", dependencies=[Depends(require_actor)])
    def list_workspace_catalogs() -> object:
        return with_service(lambda service: service.list_workspace_catalogs())

    @app.get("/v1/catalogs/{name}/tables", dependencies=[Depends(require_actor)])
    def discover_workspace_catalog_tables(name: str) -> object:
        return with_service(lambda service: service.discover_workspace_catalog_tables(name))

    @app.get("/v1/settings/runtime", dependencies=[Depends(require_actor)])
    def get_workspace_runtime_settings() -> object:
        return with_service(lambda service: service.get_workspace_runtime_settings())

    @app.get("/v1/settings/auth-providers", dependencies=[Depends(require_actor)])
    def list_workspace_auth_providers() -> object:
        return with_service(lambda service: service.list_workspace_auth_providers())

    @app.get("/v1/assets", dependencies=[Depends(require_actor)])
    def list_workspace_assets() -> object:
        return with_service(lambda service: service.list_workspace_assets())

    @app.get("/v1/cells", dependencies=[Depends(require_actor)])
    def list_cells() -> object:
        return with_service(lambda service: service.list_cells())

    @app.get("/v1/tenants/{tenant_id}/cells", dependencies=[Depends(require_actor)])
    def list_tenant_cells(tenant_id: UUID) -> object:
        return with_service(lambda service: service.list_cells_for_tenant(tenant_id))

    @app.get("/v1/cell-tenant-assignments", dependencies=[Depends(require_actor)])
    def list_cell_tenant_assignments() -> object:
        return with_service(lambda service: service.list_cell_tenant_assignments())

    @app.get("/v1/cells/{cell_id}/runtime-settings", dependencies=[Depends(require_actor)])
    def get_runtime_settings(cell_id: UUID) -> object:
        return with_service(lambda service: service.get_runtime_settings(cell_id))

    @app.get("/v1/cells/{cell_id}/catalogs", dependencies=[Depends(require_actor)])
    def list_catalogs(cell_id: UUID) -> object:
        return with_service(lambda service: service.list_catalogs(cell_id))

    @app.get("/v1/cells/{cell_id}/assets", dependencies=[Depends(require_actor)])
    def list_assets(cell_id: UUID) -> object:
        return with_service(lambda service: service.list_assets(cell_id))

    @app.get("/v1/assets/{asset_id}/policy-rules", dependencies=[Depends(require_actor)])
    def list_policy_rules(asset_id: UUID) -> object:
        return with_service(lambda service: service.list_policy_rules(asset_id))

    @app.get("/v1/assets/{asset_id}", dependencies=[Depends(require_actor)])
    def get_workspace_asset(asset_id: UUID) -> object:
        return with_service(lambda service: service.get_workspace_asset(asset_id))

    @app.get("/v1/publications/draft", dependencies=[Depends(require_actor)])
    def get_workspace_publication_draft() -> object:
        return with_service(lambda service: service.get_workspace_draft())

    @app.get("/v1/publications", dependencies=[Depends(require_actor)])
    def list_workspace_publications() -> object:
        return with_service(lambda service: service.list_workspace_publications())

    @app.get("/v1/cells/{cell_id}/auth-providers", dependencies=[Depends(require_actor)])
    def list_auth_providers(cell_id: UUID) -> object:
        return with_service(lambda service: service.list_auth_providers(cell_id))

    @app.get("/v1/cells/{cell_id}/draft", dependencies=[Depends(require_actor)])
    def get_cell_draft(cell_id: UUID) -> object:
        return with_service(lambda service: service.get_cell_draft(cell_id))

    @app.get("/v1/cells/{cell_id}/publications", dependencies=[Depends(require_actor)])
    def list_publications(cell_id: UUID) -> object:
        return with_service(lambda service: service.list_publications(cell_id))

    @app.get("/v1/cells/{cell_id}/active-publication", dependencies=[Depends(require_actor)])
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
            )
        ) or {"cell_id": str(cell_id)}

    @app.put("/v1/settings/runtime", dependencies=[Depends(require_admin)])
    def upsert_workspace_runtime_settings(request: RuntimeSettingsRequest) -> object:
        return with_service(
            lambda service: service.upsert_workspace_runtime_settings(
                ttl=request.ticket_ttl_seconds,
                max_tickets=request.max_tickets,
                max_ticket_exchanges=request.max_ticket_exchanges,
            )
        ) or {
            "ticket_ttl_seconds": request.ticket_ttl_seconds,
            "max_tickets": request.max_tickets,
            "max_ticket_exchanges": request.max_ticket_exchanges,
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

    @app.put("/v1/assets/{asset_id}/policy-rules")
    def replace_policy_rules(
        asset_id: UUID,
        request: PolicyRulesRequest,
        actor: ControlPlaneActor = Depends(require_actor),  # noqa: B008
    ) -> object:
        return with_service(
            lambda service: service.replace_policy_rules(
                asset_id=asset_id,
                rules=request.rules,
                actor=actor,
            )
        ) or {"asset_id": str(asset_id)}

    @app.post("/v1/assets/{asset_id}/policy-versions")
    def create_asset_policy_version(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(require_actor),  # noqa: B008
    ) -> object:
        return with_service(
            lambda service: service.create_asset_policy_version(
                asset_id=asset_id,
                actor=actor,
            )
        )

    @app.put("/v1/assets/{asset_id}/owners", dependencies=[Depends(require_admin)])
    def replace_asset_owners(asset_id: UUID, request: AssetOwnersRequest) -> object:
        owners = with_service(
            lambda service: service.replace_asset_owners(asset_id=asset_id, owners=request.owners)
        )
        return {"asset_id": str(asset_id), "owners": owners}

    @app.put("/v1/assets/{asset_id}/schema-fields", dependencies=[Depends(require_admin)])
    def replace_asset_schema_fields(
        asset_id: UUID,
        request: AssetSchemaFieldsRequest,
    ) -> object:
        fields = with_service(
            lambda service: service.replace_asset_schema_fields(
                asset_id=asset_id,
                fields=[field.model_dump() for field in request.fields],
            )
        )
        return {"asset_id": str(asset_id), "fields": fields}

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


def _actor_response(actor: ControlPlaneActor) -> dict[str, object]:
    return {
        "principal": actor.principal,
        "groups": list(actor.groups),
        "platform_admin": actor.platform_admin,
    }


def _public_login_shortcuts(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    shortcuts: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        shortcut = cast(Mapping[str, object], item)
        label = str(shortcut.get("label", "")).strip()
        login_hint = str(shortcut.get("login_hint", "")).strip()
        if label and login_hint:
            shortcut = {"label": label, "login_hint": login_hint}
            shortcuts.append(shortcut)
    return shortcuts


def _public_ui_auth_config(config: Mapping[str, object]) -> dict[str, object]:
    public_keys = (
        "authority",
        "client_id",
        "redirect_uri",
        "post_logout_redirect_uri",
        "scope",
    )
    public: dict[str, object] = {
        key: value for key in public_keys if (value := str(config.get(key, "")).strip())
    }
    demo_passwords = _demo_login_passwords(config)
    login_shortcuts = _public_login_shortcuts(config.get("login_shortcuts"))
    if login_shortcuts:
        public["login_shortcuts"] = [
            {
                **shortcut,
                **(
                    {"demo_login_path": "/v1/demo-login"}
                    if shortcut["login_hint"] in demo_passwords
                    else {}
                ),
            }
            for shortcut in login_shortcuts
        ]
    return public


def _demo_login_config(config: Mapping[str, object]) -> dict[str, object]:
    value = config.get("demo_login")
    if not isinstance(value, Mapping):
        return {}
    demo_login = cast(Mapping[str, object], value)
    token_url = str(demo_login.get("token_url", "")).strip()
    client_id = str(demo_login.get("client_id", "")).strip()
    client_secret = str(demo_login.get("client_secret", "")).strip()
    passwords = _demo_login_passwords(config)
    if not token_url or not client_id or not client_secret or not passwords:
        return {}
    return {
        "token_url": token_url,
        "client_id": client_id,
        "client_secret": client_secret,
        "passwords": passwords,
    }


def _demo_login_passwords(config: Mapping[str, object]) -> dict[str, str]:
    value = config.get("demo_login")
    if not isinstance(value, Mapping):
        return {}
    passwords = cast(Mapping[str, object], value).get("passwords")
    if not isinstance(passwords, Mapping):
        return {}
    return {
        str(username).strip(): str(password).strip()
        for username, password in cast(Mapping[object, object], passwords).items()
        if str(username).strip() and str(password).strip()
    }


def _exchange_demo_password_token(config: Mapping[str, object], username: str) -> str:
    passwords = cast(dict[str, str], config["passwords"])
    body = urlencode(
        {
            "grant_type": "password",
            "client_id": str(config["client_id"]),
            "client_secret": str(config["client_secret"]),
            "username": username,
            "password": passwords[username],
        }
    ).encode()
    request = UrlRequest(
        str(config["token_url"]),
        data=body,
        headers={"content-type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    token = str(payload.get("access_token", "")).strip()
    if not token:
        raise HTTPException(status_code=502, detail="Demo identity provider did not return a token")
    return token


def _oidc_actor_from_header(
    authorization: str,
    *,
    resolver: OidcActorResolver,
    admin_group: str | None,
) -> ControlPlaneActor | None:
    token = _bearer_token(authorization)
    if token is None:
        return None
    try:
        resolved = resolver(token)
    except Exception:
        return None
    principal = _attribute_text(resolved, "principal")
    if not principal:
        return None
    groups = tuple(_attribute_list(resolved, "groups"))
    return ControlPlaneActor(
        principal=principal,
        groups=groups,
        platform_admin=bool(admin_group and admin_group in groups),
    )


def _bearer_token(authorization: str) -> str | None:
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    token = parts[1].strip()
    return token or None


def _attribute_text(value: object, name: str) -> str:
    raw = (
        cast(Mapping[str, object], value).get(name)
        if isinstance(value, Mapping)
        else getattr(value, name, None)
    )
    if raw is None or isinstance(raw, list | tuple | dict):
        return ""
    return str(raw).strip()


def _attribute_list(value: object, name: str) -> list[str]:
    raw = (
        cast(Mapping[str, object], value).get(name, ())
        if isinstance(value, Mapping)
        else getattr(value, name, ())
    )
    if isinstance(raw, str):
        return [raw] if raw else []
    if not isinstance(raw, list | tuple | set):
        return []
    return [str(item).strip() for item in raw if str(item).strip()]
