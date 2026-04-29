from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import quote
from uuid import UUID

from fastapi.testclient import TestClient
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from dal_obscura.control_plane.infrastructure.db import session_factory
from dal_obscura.control_plane.interfaces.api import create_app


@dataclass(frozen=True)
class ProvisionedConfig:
    cell_id: UUID
    tenant_id: UUID
    publication_id: UUID


def provision_default_published_asset(
    *,
    db_session: Session,
    catalog_name: str,
    target: str,
    catalog_options: dict[str, object],
    policy_rules: list[dict[str, object]],
    auth_provider: dict[str, object],
    table_identifier: str | None = None,
) -> ProvisionedConfig:
    engine = db_session.get_bind()
    if not isinstance(engine, Engine):
        raise TypeError("provision_default_published_asset requires an Engine-bound session")
    app = create_app(session_factory(engine), admin_token="test-admin")
    client = TestClient(app)
    headers = {"authorization": "Bearer test-admin"}

    tenant = _checked_json(
        client.post(
            "/v1/tenants",
            json={"slug": f"tenant-{target}", "display_name": "Default"},
            headers=headers,
        )
    )
    cell = _checked_json(
        client.post(
            "/v1/cells",
            json={"name": f"cell-{target}", "region": "local"},
            headers=headers,
        )
    )
    _checked_json(
        client.put(
            f"/v1/cells/{cell['id']}/tenants/{tenant['id']}",
            json={"shard_key": "default"},
            headers=headers,
        )
    )
    _checked_json(
        client.put(
            f"/v1/tenants/{tenant['id']}/cells/{cell['id']}/runtime-settings",
            json={"ticket_ttl_seconds": 900, "max_tickets": 64, "path_rules": []},
            headers=headers,
        )
    )
    _checked_json(
        client.put(
            f"/v1/tenants/{tenant['id']}/cells/{cell['id']}/catalogs/{catalog_name}",
            json={
                "module": "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog",
                "options": catalog_options,
            },
            headers=headers,
        )
    )
    asset = _checked_json(
        client.put(
            f"/v1/tenants/{tenant['id']}/cells/{cell['id']}/assets/"
            f"{quote(catalog_name, safe='')}/{quote(target, safe='')}",
            json={
                "backend": "iceberg",
                "table_identifier": table_identifier or target,
                "options": {},
            },
            headers=headers,
        )
    )
    _checked_json(
        client.put(
            f"/v1/assets/{asset['id']}/policy-rules",
            json={"rules": policy_rules},
            headers=headers,
        )
    )
    _checked_json(
        client.put(
            f"/v1/cells/{cell['id']}/auth-providers",
            json={"providers": [auth_provider]},
            headers=headers,
        )
    )
    publication = _checked_json(
        client.post(f"/v1/cells/{cell['id']}/publications", headers=headers)
    )
    _checked_json(
        client.post(
            f"/v1/cells/{cell['id']}/publications/{publication['publication_id']}/activate",
            headers=headers,
        )
    )

    return ProvisionedConfig(
        cell_id=UUID(str(cell["id"])),
        tenant_id=UUID(str(tenant["id"])),
        publication_id=UUID(str(publication["publication_id"])),
    )


def _checked_json(response) -> dict[str, Any]:
    response.raise_for_status()
    return response.json()
