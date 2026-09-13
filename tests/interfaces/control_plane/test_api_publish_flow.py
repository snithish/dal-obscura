from __future__ import annotations

from fastapi.testclient import TestClient
from sqlalchemy import select

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.common.config_store.orm import PublishedCellRuntimeRecord
from dal_obscura.control_plane.interfaces.api import create_app

ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)


def test_api_provisions_and_activates_default_policy_version():
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    app = create_app(session_factory(engine), admin_token="test-admin")
    client = TestClient(app)
    headers = {"authorization": "Bearer test-admin"}

    client.put(
        "/v1/settings/runtime",
        json={
            "ticket_ttl_seconds": 900,
            "max_tickets": 64,
            "max_ticket_exchanges": 2,
        },
        headers=headers,
    )
    catalog = client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=headers,
    ).json()
    asset = client.put(
        "/v1/assets/analytics/default.users",
        json={"backend": "iceberg", "table_identifier": "prod.users", "options": {}},
        headers=headers,
    ).json()
    draft = client.get(f"/v1/assets/{asset['id']}/draft", headers=headers).json()
    client.put(
        f"/v1/assets/{asset['id']}/draft",
        json={
            "expected_revision": draft["revision"],
            "rules": [
                {
                    "ordinal": 10,
                    "effect": "allow",
                    "principals": ["user1"],
                    "when": {},
                    "columns": ["id", "email", "region"],
                    "masks": {"email": {"type": "email"}},
                    "row_filter": "region = 'us'",
                }
            ],
        },
        headers=headers,
    )
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["user:owner@example.com"]},
        headers=headers,
    )
    client.put(
        "/v1/settings/auth-providers",
        json={
            "providers": [
                {
                    "ordinal": 1,
                    "module": (
                        "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks."
                        "OidcJwksIdentityProvider"
                    ),
                    "args": {"issuer": "https://issuer.example"},
                    "enabled": True,
                }
            ]
        },
        headers=headers,
    )

    published_response = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        headers={**headers, "Idempotency-Key": "publish-001"},
    )
    published = published_response.json()
    replayed = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        headers={**headers, "Idempotency-Key": "publish-001"},
    ).json()

    assert published_response.status_code == 200
    assert replayed == published

    operation = client.get(
        f"/v1/assets/{asset['id']}/policy-operations/publish-001",
        headers=headers,
    )

    assert catalog["name"] == "analytics"
    assert published["asset_id"] == asset["id"]
    assert published["policy_version"] > 0
    assert operation.status_code == 200
    assert operation.json()["status"] == "committed"
    assert operation.json()["result"] == published

    conflicting = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        json={"expected_draft_revision": 0},
        headers={**headers, "Idempotency-Key": "publish-001"},
    )
    assert conflicting.status_code == 409

    session_maker = session_factory(engine)
    with session_maker() as db_session:
        runtime = db_session.scalar(select(PublishedCellRuntimeRecord))
    assert runtime is not None
    assert runtime.ticket_json["max_exchanges"] == 2
