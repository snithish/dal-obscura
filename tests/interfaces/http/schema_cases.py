"""Fixture construction shared by adjacent behavioral suites."""

from __future__ import annotations

from fastapi.testclient import TestClient

from tests.support.control.workspace import (
    ADMIN_HEADERS,
    ICEBERG_CATALOG_ID,
)


def _provision_reviewable_asset(client: TestClient) -> dict[str, object]:
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
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["user1"], "expected_revision": 0},
        headers=ADMIN_HEADERS,
    )
    saved = client.put(
        f"/v1/assets/{asset['id']}/policy",
        json={
            "expected_revision": 0,
            "rules": [
                {
                    "ordinal": 10,
                    "effect": "allow",
                    "principals": ["user1"],
                    "columns": ["id"],
                    "masks": {},
                    "row_filter": None,
                }
            ],
        },
        headers=ADMIN_HEADERS,
    )
    assert saved.status_code == 200, saved.json()
    return asset
