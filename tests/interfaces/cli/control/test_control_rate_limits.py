"""Startup rate-limit settings govern real browser login requests."""

import time

import pytest

from dal_obscura.adapters.sql.database.db import migrate_config_store


@pytest.mark.socket
@pytest.mark.parametrize("budget", ["client", "aggregate"])
def test_proxy_login_limits_separate_clients_and_bound_aggregate(
    engine_factory, tmp_path, control_server, budget
):
    database_url = f"sqlite+pysqlite:///{tmp_path / 'proxy-rates.db'}"
    migrate_config_store(engine_factory(database_url))
    environment = {
        "DAL_OBSCURA_DATABASE_URL": database_url,
        "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "test-admin",
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER": "https://issuer.example",
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_JWKS_URL": "https://issuer.example/jwks",
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID": "ui",
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI": "https://console.example/auth/callback",
        "DAL_OBSCURA_CONTROL_PLANE_TRUSTED_PROXY_PEERS": "127.0.0.1/32",
        "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_ATTEMPTS": "2" if budget == "client" else "5",
        "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_AGGREGATE_ATTEMPTS": "77"
        if budget == "client"
        else "2",
    }
    with control_server(environment) as client:

        def login(address):
            return client.get(
                "/auth/login",
                headers={
                    "authorization": "Bearer invalid",
                    "x-forwarded-for": address,
                },
            )

        assert login("10.0.0.8").status_code == 303
        if budget == "client":
            assert login("10.0.0.8").status_code == 303
            assert login("10.0.0.8").status_code == 429
            assert login("10.0.0.9").status_code == 303
        else:
            assert login("10.0.0.9").status_code == 303
            assert login("10.0.0.10").status_code == 429


@pytest.mark.socket
def test_login_failure_window_resets_before_configured_block(
    engine_factory, tmp_path, control_server
):
    database_url = f"sqlite+pysqlite:///{tmp_path / 'window-rates.db'}"
    migrate_config_store(engine_factory(database_url))
    environment = {
        "DAL_OBSCURA_DATABASE_URL": database_url,
        "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "test-admin",
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER": "https://issuer.example",
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_JWKS_URL": "https://issuer.example/jwks",
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID": "ui",
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI": "https://console.example/auth/callback",
        "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_ATTEMPTS": "2",
        "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_WINDOW_SECONDS": "2",
        "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_BLOCK_SECONDS": "42",
    }
    with control_server(environment) as client:
        headers = {"authorization": "Bearer invalid"}
        assert client.get("/auth/login", headers=headers).status_code == 303
        time.sleep(2.1)
        assert client.get("/auth/login", headers=headers).status_code == 303
        assert client.get("/auth/login", headers=headers).status_code == 303
        blocked = client.get("/auth/login", headers=headers)
        assert blocked.status_code == 429
        assert 0 < int(blocked.headers["retry-after"]) <= 42
