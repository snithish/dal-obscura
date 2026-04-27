from __future__ import annotations

import json
import os
from contextlib import suppress
from pathlib import Path
from typing import Any

import pyarrow as pa
import yaml
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

RUNTIME_DIR = Path(os.environ.get("RUNTIME_DIR", "/workspace/runtime"))
CATALOG = "example_catalog"
TARGET = "default.users"
PRINCIPAL = "example-user"
PORT = 8815

DEFAULT_JWT = "dal_obscura.infrastructure.adapters.identity_default.DefaultIdentityAdapter"
OIDC = "dal_obscura.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider"
API_KEY = "dal_obscura.infrastructure.adapters.identity_api_key.ApiKeyIdentityProvider"
MTLS = "dal_obscura.infrastructure.adapters.identity_mtls.MtlsIdentityProvider"
TRUSTED = (
    "dal_obscura.infrastructure.adapters.identity_trusted_headers.TrustedHeaderIdentityProvider"
)

SPIFFE_CLIENT_ID = "spiffe://example.org/ns/default/sa/dal-obscura-client"


def main() -> None:
    mode = os.environ["EXAMPLE_AUTH_MODE"]
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    _create_table()
    _write_catalog_config()
    _write_policy_config()
    _write_app_config(mode)
    print(json.dumps({"catalog": CATALOG, "target": TARGET, "principal": PRINCIPAL}))


def _create_table() -> None:
    warehouse = RUNTIME_DIR / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        CATALOG,
        type="sql",
        uri=f"sqlite:///{RUNTIME_DIR / f'{CATALOG}.db'}",
        warehouse=str(warehouse),
    )
    with suppress(Exception):
        catalog.create_namespace("default")
    with suppress(Exception):
        catalog.drop_table(TARGET)
    table = catalog.create_table(
        TARGET,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="email", field_type=StringType(), required=False),
            NestedField(field_id=3, name="region", field_type=StringType(), required=False),
        ),
        properties={"format-version": "2"},
    )
    table.append(
        pa.table(
            {
                "id": [1, 2, 3],
                "email": ["alice@example.com", "bob@example.com", "carol@example.com"],
                "region": ["us", "eu", "us"],
            },
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("email", pa.string()),
                    pa.field("region", pa.string()),
                ]
            ),
        )
    )


def _write_catalog_config() -> None:
    _write_yaml(
        RUNTIME_DIR / "catalogs.yaml",
        {
            "catalogs": {
                CATALOG: {
                    "module": "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog",
                    "options": {
                        "type": "sql",
                        "uri": f"sqlite:///{RUNTIME_DIR / f'{CATALOG}.db'}",
                        "warehouse": str(RUNTIME_DIR / "warehouse"),
                    },
                }
            }
        },
    )


def _write_policy_config() -> None:
    _write_yaml(
        RUNTIME_DIR / "policies.yaml",
        {
            "version": 1,
            "catalogs": {
                CATALOG: {
                    "targets": {
                        TARGET: {
                            "rules": [
                                {
                                    "principals": [PRINCIPAL],
                                    "columns": ["id", "email", "region"],
                                    "row_filter": "region = 'us'",
                                }
                            ]
                        }
                    }
                }
            },
        },
    )


def _write_app_config(mode: str) -> None:
    app: dict[str, Any] = {
        "location": f"grpc://0.0.0.0:{PORT}",
        "catalog_file": str(RUNTIME_DIR / "catalogs.yaml"),
        "policy_file": str(RUNTIME_DIR / "policies.yaml"),
        "secret_provider": {
            "module": "dal_obscura.infrastructure.adapters.secret_providers.EnvSecretProvider",
            "args": {},
        },
        "ticket": {
            "ttl_seconds": 900,
            "max_tickets": 64,
            "secret": {"key": "DAL_OBSCURA_TICKET_SECRET"},
        },
        "auth": _auth_config(mode),
        "logging": {"level": "INFO", "json": True},
    }
    if mode in {"mtls", "mtls-spiffe"}:
        app["location"] = f"grpc+tls://0.0.0.0:{PORT}"
        app["transport"] = {
            "tls": {
                "cert": {"key": "SERVER_CERT"},
                "key": {"key": "SERVER_KEY"},
                "client_ca": {"key": "CLIENT_CA"},
                "verify_client": True,
            }
        }
    _write_yaml(RUNTIME_DIR / "app.yaml", app)


def _auth_config(mode: str) -> dict[str, Any]:
    if mode == "shared-jwt":
        return {"module": DEFAULT_JWT, "args": {"jwt_secret": {"key": "DAL_OBSCURA_JWT_SECRET"}}}
    if mode == "keycloak-oidc":
        return {
            "module": OIDC,
            "args": {
                "issuer": "http://keycloak:8080/realms/dal-obscura",
                "audience": "dal-obscura",
                "subject_claim": "preferred_username",
            },
        }
    if mode == "api-key":
        return {
            "module": API_KEY,
            "args": {
                "keys": [
                    {
                        "id": PRINCIPAL,
                        "secret": {"key": "DAL_OBSCURA_API_KEY"},
                        "groups": ["compose-example"],
                    }
                ]
            },
        }
    if mode == "mtls":
        return {
            "module": MTLS,
            "args": {
                "identities": [
                    {
                        "peer_identity": "example-client",
                        "id": PRINCIPAL,
                        "groups": ["compose-example"],
                    }
                ]
            },
        }
    if mode == "mtls-spiffe":
        return {
            "module": MTLS,
            "args": {
                "identities": [
                    {
                        "peer_identity": SPIFFE_CLIENT_ID,
                        "id": PRINCIPAL,
                        "groups": ["compose-example", "spiffe"],
                        "attributes": {"spiffe_id": SPIFFE_CLIENT_ID},
                    }
                ]
            },
        }
    if mode == "trusted-headers":
        return {"module": TRUSTED, "args": {"shared_secret": {"key": "DAL_OBSCURA_PROXY_SECRET"}}}
    if mode == "composite-provider":
        return {
            "providers": [
                {
                    "module": API_KEY,
                    "args": {
                        "keys": [
                            {
                                "id": PRINCIPAL,
                                "secret": {"key": "DAL_OBSCURA_API_KEY"},
                                "groups": ["compose-example"],
                            }
                        ]
                    },
                },
                {"module": DEFAULT_JWT, "args": {"jwt_secret": {"key": "DAL_OBSCURA_JWT_SECRET"}}},
            ]
        }
    raise SystemExit(f"Unsupported EXAMPLE_AUTH_MODE: {mode}")


def _write_yaml(path: Path, value: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(value, sort_keys=False))


if __name__ == "__main__":
    main()
