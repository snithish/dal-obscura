"""Keep the public API aligned with the direct-edit control-plane contract."""

from __future__ import annotations

from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.control_plane.interfaces.api import create_app


def test_openapi_routes_match_the_live_configuration_contract() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    client = TestClient(create_app(session_factory(engine), admin_token="test-admin"))

    paths = client.get("/openapi.json").json()["paths"]

    assert set(paths) == {
        "/auth/callback",
        "/auth/login",
        "/healthz",
        "/readyz",
        "/v1/assets",
        "/v1/assets/page",
        "/v1/assets/{asset_id}",
        "/v1/assets/{asset_id}/access",
        "/v1/assets/{asset_id}/grants",
        "/v1/assets/{asset_id}/owners",
        "/v1/assets/{asset_id}/policy",
        "/v1/assets/{asset_id}/policy-evaluate",
        "/v1/assets/{asset_id}/schema",
        "/v1/assets/{asset_id}/schema-fields",
        "/v1/assets/{asset_id}/tickets/revoke",
        "/v1/assets/{catalog}/{target}",
        "/v1/audit/events/page",
        "/v1/catalogs",
        "/v1/catalogs/{name}",
        "/v1/catalogs/{name}/diagnostics",
        "/v1/catalogs/{name}/tables",
        "/v1/logout",
        "/v1/plugins",
        "/v1/plugins/{kind}/{plugin_id}/lifecycle",
        "/v1/session",
        "/v1/session/bootstrap",
        "/v1/session/options",
        "/v1/settings/auth-providers",
        "/v1/settings/auth-providers/revision",
        "/v1/settings/runtime",
        "/v1/ui-auth-config",
        "/v1/workspace/observations",
        "/v1/workspace/summary",
    }
    assert set(paths["/v1/assets/{asset_id}/grants"]) == {"get", "put"}
    assert set(paths["/v1/assets/{asset_id}/access"]) == {"get"}
    assert set(paths["/v1/assets/{asset_id}/policy"]) == {"put"}
    assert set(paths["/v1/assets/{asset_id}/tickets/revoke"]) == {"post"}
    assert set(paths["/v1/assets/{catalog}/{target}"]) == {"put"}
    assert set(paths["/v1/settings/auth-providers/revision"]) == {"get"}

    assert _response_schema(paths, "/v1/assets/{asset_id}/policy", "put") == (
        "PolicyMutationResponse"
    )
    assert _response_schema(paths, "/v1/assets/{asset_id}/tickets/revoke", "post") == (
        "AssetTokenRevocationResponse"
    )
    assert _response_schema(paths, "/v1/assets/{asset_id}", "get") == "AssetDetailResponse"
    assert _response_schema(paths, "/v1/assets/{asset_id}/schema", "get") == ("AssetSchemaResponse")
    assert _response_schema(paths, "/v1/assets/{asset_id}/schema-fields", "put") == (
        "AssetSchemaFieldsResponse"
    )
    assert _response_schema(paths, "/v1/assets/{asset_id}/grants", "put") == ("AssetGrantsResponse")
    assert _response_schema(paths, "/v1/assets/{asset_id}/owners", "put") == ("AssetOwnersResponse")


def _response_schema(paths: dict, path: str, method: str) -> str:
    return paths[path][method]["responses"]["200"]["content"]["application/json"]["schema"][
        "$ref"
    ].rsplit("/", 1)[-1]
