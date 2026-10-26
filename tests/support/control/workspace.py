from __future__ import annotations

from fastapi.testclient import TestClient

ADMIN_HEADERS = {"authorization": "Bearer test-admin"}
ICEBERG_CATALOG_ID = "iceberg.catalog"
DEFAULT_AUTH_MODULE = "dal_obscura.identity.oidc.OidcJwksIdentityProvider"


def _provision_asset(client: TestClient) -> dict[str, str]:
    client.put(
        "/v1/settings/runtime",
        json={"ticket_ttl_seconds": 900, "max_tickets": 64, "max_ticket_exchanges": 2},
        headers=ADMIN_HEADERS,
    )
    client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    asset = client.put(
        "/v1/assets/analytics/default.users",
        json={"backend": "iceberg", "table_identifier": "prod.users", "options": {}},
        headers=ADMIN_HEADERS,
    ).json()
    saved = client.put(
        f"/v1/assets/{asset['id']}/policy",
        json={
            "expected_revision": 0,
            "rules": [
                {
                    "ordinal": 10,
                    "effect": "allow",
                    "principals": ["user1"],
                    "when": {"department": "default"},
                    "columns": ["id", "email"],
                    "masks": {"email": {"type": "email"}},
                    "row_filter": "region = 'us'",
                }
            ],
        },
        headers=ADMIN_HEADERS,
    )
    assert saved.status_code == 200, saved.text
    client.put(
        "/v1/settings/auth-providers",
        json={
            "providers": [
                {
                    "ordinal": 1,
                    "module": DEFAULT_AUTH_MODULE,
                    "args": {"issuer": "https://issuer.example"},
                    "enabled": True,
                }
            ]
        },
        headers=ADMIN_HEADERS,
    )
    return asset


def _keys_recursive(value: object) -> set[str]:
    if isinstance(value, dict):
        keys = {str(key) for key in value}
        for item in value.values():
            keys.update(_keys_recursive(item))
        return keys
    if isinstance(value, list):
        keys: set[str] = set()
        for item in value:
            keys.update(_keys_recursive(item))
        return keys
    return set()
