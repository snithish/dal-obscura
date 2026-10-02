from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from fastapi.testclient import TestClient
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from dal_obscura.common.config_store.db import session_factory
from dal_obscura.control_plane.interfaces.api import create_app

ICEBERG_CATALOG_ID = "iceberg.sql"


@dataclass(frozen=True)
class ProvisionedConfig:
    pass


def provision_default_live_asset(
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
        raise TypeError("provision_default_live_asset requires an Engine-bound session")
    app = create_app(session_factory(engine), admin_token="test-admin")
    with TestClient(app) as client:
        headers = {"authorization": "Bearer test-admin"}

        _checked_json(
            client.put(
                "/v1/settings/runtime",
                json={"ticket_ttl_seconds": 900, "max_tickets": 64, "max_ticket_exchanges": 1},
                headers=headers,
            )
        )
        _checked_json(
            client.put(
                f"/v1/catalogs/{catalog_name}",
                json={"plugin_id": ICEBERG_CATALOG_ID, "options": catalog_options},
                headers=headers,
            )
        )
        asset = _checked_json(
            client.put(
                f"/v1/assets/{quote(catalog_name, safe='')}/{quote(target, safe='')}",
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
                f"/v1/assets/{asset['id']}/policy",
                json={"expected_revision": 0, "rules": policy_rules},
                headers=headers,
            )
        )
        _checked_json(
            client.put(
                "/v1/settings/auth-providers", json={"providers": [auth_provider]}, headers=headers
            )
        )

        return ProvisionedConfig()


def _checked_json(response) -> dict[str, Any]:
    response.raise_for_status()
    return response.json()
