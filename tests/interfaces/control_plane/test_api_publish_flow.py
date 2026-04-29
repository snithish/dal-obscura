from __future__ import annotations

from fastapi.testclient import TestClient

from dal_obscura.control_plane.infrastructure.db import create_engine_from_url, session_factory
from dal_obscura.control_plane.infrastructure.orm import Base
from dal_obscura.control_plane.interfaces.api import create_app


def test_api_provisions_and_activates_default_cell_publication():
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    app = create_app(session_factory(engine), admin_token="test-admin")
    client = TestClient(app)
    headers = {"authorization": "Bearer test-admin"}

    tenant = client.post(
        "/v1/tenants",
        json={"slug": "default", "display_name": "Default"},
        headers=headers,
    ).json()
    cell = client.post(
        "/v1/cells",
        json={"name": "default", "region": "local"},
        headers=headers,
    ).json()
    client.put(
        f"/v1/cells/{cell['id']}/tenants/{tenant['id']}",
        json={"shard_key": "default"},
        headers=headers,
    )
    client.put(
        f"/v1/tenants/{tenant['id']}/cells/{cell['id']}/runtime-settings",
        json={"ticket_ttl_seconds": 900, "max_tickets": 64, "path_rules": []},
        headers=headers,
    )
    catalog = client.put(
        f"/v1/tenants/{tenant['id']}/cells/{cell['id']}/catalogs/analytics",
        json={
            "module": "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog",
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=headers,
    ).json()
    asset = client.put(
        f"/v1/tenants/{tenant['id']}/cells/{cell['id']}/assets/analytics/default.users",
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
        f"/v1/cells/{cell['id']}/auth-providers",
        json={
            "providers": [
                {
                    "ordinal": 1,
                    "module": (
                        "dal_obscura.infrastructure.adapters.identity_default."
                        "DefaultIdentityAdapter"
                    ),
                    "args": {"jwt_secret": {"key": "DAL_OBSCURA_JWT_SECRET"}},
                    "enabled": True,
                }
            ]
        },
        headers=headers,
    )

    published = client.post(f"/v1/cells/{cell['id']}/publications", headers=headers).json()
    activated = client.post(
        f"/v1/cells/{cell['id']}/publications/{published['publication_id']}/activate",
        headers=headers,
    ).json()

    assert catalog["name"] == "analytics"
    assert activated["publication_id"] == published["publication_id"]
    assert published["asset_count"] == 1
