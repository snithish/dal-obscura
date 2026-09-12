"""Keep the P00 route inventory aligned with the public OpenAPI contract."""

from __future__ import annotations

from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.control_plane.interfaces.api import create_app


def test_openapi_routes_match_the_p00_inventory() -> None:
    """Endpoint additions must update the reviewed route inventory explicitly."""
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
        "/v1/assets/{asset_id}",
        "/v1/assets/{asset_id}/draft",
        "/v1/assets/{asset_id}/owners",
        "/v1/assets/{asset_id}/grants",
        "/v1/assets/{asset_id}/schema",
        "/v1/assets/{asset_id}/policy-preview",
        "/v1/assets/{asset_id}/policy-evaluate",
        "/v1/assets/{asset_id}/policy-rules",
        "/v1/assets/{asset_id}/policy-versions",
        "/v1/assets/{asset_id}/policy-versions/{policy_version}",
        "/v1/assets/{asset_id}/policy-versions/{policy_version}/restore",
        "/v1/assets/{asset_id}/schema-fields",
        "/v1/assets/{catalog}/{target}",
        "/v1/catalogs",
        "/v1/catalogs/{name}",
        "/v1/catalogs/{name}/tables",
        "/v1/demo-login",
        "/v1/logout",
        "/v1/policy-versions",
        "/v1/session",
        "/v1/settings/auth-providers",
        "/v1/settings/runtime",
        "/v1/ui-auth-config",
        "/v1/workspace/summary",
    }
    assert set(paths["/v1/assets/{asset_id}/policy-rules"]) == {"get", "put"}
    assert set(paths["/v1/assets/{asset_id}/grants"]) == {"get", "put"}
    assert set(paths["/v1/assets/{catalog}/{target}"]) == {"put"}
