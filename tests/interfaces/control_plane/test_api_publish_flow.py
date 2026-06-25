from __future__ import annotations

from uuid import UUID

from fastapi.testclient import TestClient
from sqlalchemy import select

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import Base, PublishedCellRuntimeRecord
from dal_obscura.control_plane.interfaces.api import create_app

ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)


def test_api_provisions_and_activates_default_cell_publication():
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
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
    client.put(
        f"/v1/assets/{asset['id']}/policy-rules",
        json={
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
            ]
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
                        "dal_obscura.data_plane.infrastructure.adapters.identity_default."
                        "DefaultIdentityAdapter"
                    ),
                    "args": {"jwt_secret": {"secret": "DAL_OBSCURA_JWT_SECRET"}},
                    "enabled": True,
                }
            ]
        },
        headers=headers,
    )

    published = client.post("/v1/publications", headers=headers).json()
    activated = client.post(
        f"/v1/publications/{published['publication_id']}/activate",
        headers=headers,
    ).json()

    assert catalog["name"] == "analytics"
    assert activated["publication_id"] == published["publication_id"]
    assert published["asset_count"] == 1

    session_maker = session_factory(engine)
    with session_maker() as db_session:
        runtime = db_session.scalar(
            select(PublishedCellRuntimeRecord).where(
                PublishedCellRuntimeRecord.publication_id == UUID(published["publication_id"])
            )
        )
    assert runtime is not None
    assert runtime.ticket_json["max_exchanges"] == 2
