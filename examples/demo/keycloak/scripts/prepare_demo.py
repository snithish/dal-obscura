from __future__ import annotations

import secrets
from pathlib import Path

DEMO_DIR = Path(__file__).resolve().parents[1]
RUNTIME_DIR = DEMO_DIR / ".runtime"
REALM_TEMPLATE = DEMO_DIR / "keycloak" / "realm.template.json"
REALM_FILE = RUNTIME_DIR / "keycloak" / "realm.json"

KEYCLOAK_ENV = RUNTIME_DIR / "keycloak.env"
POSTGRES_ENV = RUNTIME_DIR / "postgres.env"
CONTROL_PLANE_ENV = RUNTIME_DIR / "control-plane.env"
DATA_PLANE_ENV = RUNTIME_DIR / "data-plane.env"
CLIENT_ENV = RUNTIME_DIR / "client.env"
SETUP_ENV = RUNTIME_DIR / "setup.env"

SECRET_KEYS = {
    "KC_BOOTSTRAP_ADMIN_PASSWORD": "kc-admin",
    "POSTGRES_PASSWORD": "postgres",
    "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "control-admin",
    "DAL_OBSCURA_TICKET_SECRET": "ticket",
    "OIDC_CLI_CLIENT_SECRET": "cli-client",
    "DEMO_ADMIN_PASSWORD": "demo-admin",
    "ASSET_OWNER_PASSWORD": "asset-owner",
    "US_ANALYST_PASSWORD": "us-analyst",
    "EU_ANALYST_PASSWORD": "eu-analyst",
    "DATA_STEWARD_PASSWORD": "data-steward",
    "BLOCKED_USER_PASSWORD": "blocked-user",
}

STATIC_VALUES = {
    "CONTROL_PLANE_URL": "http://control-plane:8820",
    "KC_HOSTNAME": "http://127.0.0.1:8080",
    "POSTGRES_USER": "dal_obscura",
    "POSTGRES_DB": "dal_obscura",
    "DAL_OBSCURA_CONTROL_PLANE_HOST": "0.0.0.0",
    "DAL_OBSCURA_CONTROL_PLANE_PORT": "8820",
    "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER": ("http://127.0.0.1:8080/realms/dal-obscura-demo"),
    "DAL_OBSCURA_CONTROL_PLANE_OIDC_AUDIENCE": "dal-obscura",
    "DAL_OBSCURA_CONTROL_PLANE_OIDC_JWKS_URL": (
        "http://keycloak:8080/realms/dal-obscura-demo/protocol/openid-connect/certs"
    ),
    "DAL_OBSCURA_CONTROL_PLANE_OIDC_SUBJECT_CLAIM": "preferred_username",
    "DAL_OBSCURA_CONTROL_PLANE_OIDC_GROUP_CLAIMS": "groups",
    "DAL_OBSCURA_CONTROL_PLANE_OIDC_ADMIN_GROUP": "platform-admins",
    "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER": ("http://127.0.0.1:8080/realms/dal-obscura-demo"),
    "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID": "dal-obscura-ui",
    "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI": ("http://127.0.0.1:8820/ui/auth/callback"),
    "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGOUT_REDIRECT_URI": ("http://127.0.0.1:8820/ui"),
    "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_SCOPE": "openid profile",
    "DAL_OBSCURA_CONTROL_PLANE_UI_LOGIN_SHORTCUTS": (
        "Platform owner=demo-admin;Data asset owner=asset-owner"
    ),
    "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_TOKEN_URL": (
        "http://keycloak:8080/realms/dal-obscura-demo/protocol/openid-connect/token"
    ),
    "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_CLIENT_ID": "dal-obscura-cli",
    "DAL_OBSCURA_LOCATION": "grpc://0.0.0.0:8815",
    "DAL_OBSCURA_JSON_LOGS": "true",
    "DEMO_DIR": "/workspace/demo",
    "FLIGHT_URI": "grpc+tcp://dal-obscura:8815",
    "OIDC_TOKEN_URL": (
        "http://keycloak:8080/realms/dal-obscura-demo/protocol/openid-connect/token"
    ),
    "OIDC_CLIENT_ID": "dal-obscura-cli",
    "DEMO_CATALOG": "retail_demo",
    "DEMO_TARGET": "retail.customer_revenue",
    "DEMO_ICEBERG_CATALOG": "retail_demo",
    "DEMO_ICEBERG_TARGET": "retail.customer_revenue",
    "DEMO_DELTA_CATALOG": "retail_delta",
    "DEMO_DELTA_TARGET": "retail.customer_revenue_delta",
}


def main() -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    (RUNTIME_DIR / "keycloak").mkdir(parents=True, exist_ok=True)
    (RUNTIME_DIR / "setup.done").unlink(missing_ok=True)
    values = _load_or_create_values()
    _write_env_files(values)
    _render_realm(values)
    print(f"Prepared demo runtime in {RUNTIME_DIR}")


def _load_or_create_values() -> dict[str, str]:
    existing = _read_all_env_files()
    values = {**existing, **STATIC_VALUES}
    changed = False
    for key, prefix in SECRET_KEYS.items():
        if not values.get(key):
            values[key] = _secret(prefix)
            changed = True
    values["DAL_OBSCURA_DATABASE_URL"] = _postgres_database_url(values)
    values["DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_CLIENT_SECRET"] = values[
        "OIDC_CLI_CLIENT_SECRET"
    ]
    values["DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_PASSWORDS"] = (
        f"demo-admin={values['DEMO_ADMIN_PASSWORD']};asset-owner={values['ASSET_OWNER_PASSWORD']}"
    )
    if changed:
        _write_env_files(values)
    return values


def _read_all_env_files() -> dict[str, str]:
    values: dict[str, str] = {}
    for path in (
        KEYCLOAK_ENV,
        POSTGRES_ENV,
        CONTROL_PLANE_ENV,
        DATA_PLANE_ENV,
        CLIENT_ENV,
        SETUP_ENV,
    ):
        values.update(_read_env_file(path))
    return values


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _write_env_files(values: dict[str, str]) -> None:
    _write_env_file(
        KEYCLOAK_ENV,
        values,
        ("KC_BOOTSTRAP_ADMIN_PASSWORD", "KC_HOSTNAME"),
    )
    _write_env_file(
        POSTGRES_ENV,
        values,
        ("POSTGRES_USER", "POSTGRES_DB", "POSTGRES_PASSWORD"),
    )
    _write_env_file(
        CONTROL_PLANE_ENV,
        values,
        (
            "DAL_OBSCURA_DATABASE_URL",
            "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN",
            "DAL_OBSCURA_CONTROL_PLANE_HOST",
            "DAL_OBSCURA_CONTROL_PLANE_PORT",
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER",
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_AUDIENCE",
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_JWKS_URL",
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_SUBJECT_CLAIM",
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_GROUP_CLAIMS",
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_ADMIN_GROUP",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGOUT_REDIRECT_URI",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_SCOPE",
            "DAL_OBSCURA_CONTROL_PLANE_UI_LOGIN_SHORTCUTS",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_TOKEN_URL",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_CLIENT_ID",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_CLIENT_SECRET",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_PASSWORDS",
        ),
    )
    _write_env_file(
        DATA_PLANE_ENV,
        values,
        (
            "DAL_OBSCURA_DATABASE_URL",
            "DAL_OBSCURA_TICKET_SECRET",
            "DAL_OBSCURA_LOCATION",
            "DAL_OBSCURA_JSON_LOGS",
            "DAL_OBSCURA_CELL_ID",
        ),
    )
    _write_env_file(
        CLIENT_ENV,
        values,
        (
            "DEMO_DIR",
            "FLIGHT_URI",
            "OIDC_TOKEN_URL",
            "OIDC_CLIENT_ID",
            "OIDC_CLI_CLIENT_SECRET",
            "DEMO_CATALOG",
            "DEMO_TARGET",
            "DEMO_ICEBERG_CATALOG",
            "DEMO_ICEBERG_TARGET",
            "DEMO_DELTA_CATALOG",
            "DEMO_DELTA_TARGET",
            "DEMO_ADMIN_PASSWORD",
            "ASSET_OWNER_PASSWORD",
            "US_ANALYST_PASSWORD",
            "EU_ANALYST_PASSWORD",
            "DATA_STEWARD_PASSWORD",
            "BLOCKED_USER_PASSWORD",
        ),
    )
    _write_env_file(
        SETUP_ENV,
        values,
        (
            "DEMO_DIR",
            "CONTROL_PLANE_URL",
            "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN",
        ),
    )


def _write_env_file(path: Path, values: dict[str, str], keys: tuple[str, ...]) -> None:
    path.write_text(
        "\n".join(f"{key}={values[key]}" for key in keys if values.get(key)) + "\n",
        encoding="utf-8",
    )


def _postgres_database_url(values: dict[str, str]) -> str:
    return (
        "postgresql+psycopg://"
        f"{values['POSTGRES_USER']}:{values['POSTGRES_PASSWORD']}"
        f"@postgres:5432/{values['POSTGRES_DB']}"
    )


def _render_realm(values: dict[str, str]) -> None:
    rendered = REALM_TEMPLATE.read_text(encoding="utf-8")
    for key, value in values.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", value)
    REALM_FILE.write_text(rendered, encoding="utf-8")


def _secret(prefix: str) -> str:
    return f"{prefix}-{secrets.token_urlsafe(24)}"


if __name__ == "__main__":
    main()
