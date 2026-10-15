"""Fixture construction shared by adjacent behavioral suites."""

from __future__ import annotations

from uuid import UUID

from fastapi.testclient import TestClient
from httpx import Response

from tests.support.control.workspace import ADMIN_HEADERS, ICEBERG_CATALOG_ID
from tests.support.control.workspace import DEFAULT_AUTH_MODULE as OIDC_AUTH_MODULE


def _replace_live_policy(
    client: TestClient, asset_id: UUID, rules: list[dict], headers
) -> Response:
    current = client.get(f"/v1/assets/{asset_id}", headers=headers)
    assert current.status_code == 200, current.text
    return client.put(
        f"/v1/assets/{asset_id}/policy",
        json={"expected_revision": current.json()["policy_revision"], "rules": rules},
        headers=headers,
    )


def _provision_owned_asset(client: TestClient, *, target: str = "default.users") -> UUID:
    asset = _provision_asset_without_owner(client, target=target)
    response = client.put(
        f"/v1/assets/{asset}/owners",
        json={"owners": ["group:asset-owners"], "expected_revision": 0},
        headers=ADMIN_HEADERS,
    )
    assert response.status_code == 200
    return asset


def _provision_asset_without_owner(client: TestClient, *, target: str = "default.users") -> UUID:
    client.put(
        "/v1/settings/runtime",
        json={"ticket_ttl_seconds": 900, "max_tickets": 64, "max_ticket_exchanges": 1},
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
        f"/v1/assets/analytics/{target}",
        json={"backend": "iceberg", "table_identifier": target, "options": {}},
        headers=ADMIN_HEADERS,
    ).json()
    client.put(
        "/v1/settings/auth-providers",
        json={
            "providers": [
                {
                    "ordinal": 1,
                    "module": OIDC_AUTH_MODULE,
                    "args": {
                        "issuer": "http://keycloak:8080/realms/dal-obscura-demo",
                        "audience": "dal-obscura",
                        "jwks_url": (
                            "http://keycloak:8080/realms/dal-obscura-demo/"
                            "protocol/openid-connect/certs"
                        ),
                        "subject_claim": "preferred_username",
                        "group_claims": ["groups"],
                    },
                    "enabled": True,
                }
            ]
        },
        headers=ADMIN_HEADERS,
    )
    _replace_live_policy(client, asset["id"], [_allow_rule(row_filter=None)], ADMIN_HEADERS)
    return UUID(asset["id"])


def _allow_rule(*, row_filter: str | None) -> dict[str, object]:
    return {
        "ordinal": 10,
        "effect": "allow",
        "principals": ["group:analysts"],
        "when": {},
        "columns": ["id", "email", "region"],
        "masks": {"email": {"type": "email"}},
        "row_filter": row_filter,
    }
