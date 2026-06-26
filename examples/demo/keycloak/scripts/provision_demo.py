from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore

DEMO_DIR = Path(os.environ.get("DEMO_DIR", "/workspace/demo"))
RUNTIME_DIR = DEMO_DIR / ".runtime"
FIXTURE_FILE = DEMO_DIR / "fixtures" / "demo_fixture.json"
DATA_PLANE_ENV = RUNTIME_DIR / "data-plane.env"
CONTROL_PLANE_URL = os.environ.get("CONTROL_PLANE_URL", "http://control-plane:8820")
ADMIN_TOKEN = os.environ["DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN"]
DATABASE_URL = os.environ["DAL_OBSCURA_DATABASE_URL"]
ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)
OIDC_AUTH_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider"
)


def main() -> None:
    fixture = _read_fixture()
    _wait_for_control_plane()
    cell_id = _provision_workspace(fixture)
    _upsert_env_value(DATA_PLANE_ENV, "DAL_OBSCURA_CELL_ID", cell_id)
    print(
        json.dumps(
            {
                "catalogs": [catalog["name"] for catalog in fixture["catalogs"]],
                "cell_id": cell_id,
            }
        )
    )


def _read_fixture() -> dict[str, Any]:
    fixture = json.loads(FIXTURE_FILE.read_text(encoding="utf-8"))
    if not isinstance(fixture, dict):
        raise ValueError("fixture must be a JSON object")
    return fixture


def _wait_for_control_plane() -> None:
    deadline = time.monotonic() + 90
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            _request("GET", "/v1/session")
            return
        except Exception as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"control plane did not become ready: {last_error}") from last_error


def _workspace_cell_id() -> str:
    engine = create_engine_from_url(DATABASE_URL)
    session_maker = session_factory(engine)
    with session_maker() as session:
        context = PublicationStore(session).get_default_workspace_context()
        if context is None:
            raise RuntimeError("workspace provisioning did not create a runtime context")
        return str(context.cell_id)


def _provision_workspace(fixture: dict[str, Any]) -> str:
    warehouse_path = "/workspace/demo/.runtime/warehouse"
    _request(
        "PUT",
        "/v1/settings/runtime",
        {
            "ticket_ttl_seconds": 600,
            "max_tickets": 16,
            "max_ticket_exchanges": 1,
        },
    )
    cell_id = _workspace_cell_id()
    _upsert_catalogs(fixture, warehouse_path)
    asset_ids = []
    for table_fixture in fixture["tables"]:
        asset_ids.append(_promote_table(fixture, table_fixture))
    _request(
        "PUT",
        "/v1/settings/auth-providers",
        {
            "providers": [
                {
                    "ordinal": 10,
                    "module": OIDC_AUTH_MODULE,
                    "args": {
                        "issuer": "http://127.0.0.1:8080/realms/dal-obscura-demo",
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
    )
    for asset_id in asset_ids:
        _request("POST", f"/v1/assets/{asset_id}/policy-versions")
    return cell_id


def _upsert_catalogs(fixture: dict[str, Any], warehouse_path: str) -> None:
    for catalog in fixture["catalogs"]:
        catalog_name = str(catalog["name"])
        kind = str(catalog["kind"])
        if kind == "iceberg_sql":
            body = {
                "module": ICEBERG_CATALOG_MODULE,
                "options": {
                    "type": "sql",
                    "uri": f"sqlite:///{RUNTIME_DIR / f'{catalog_name}.db'}",
                    "warehouse": warehouse_path,
                },
            }
        elif kind == "delta_static":
            body = {
                "module": "delta",
                "options": {
                    "tables": {
                        str(table["target"]): str(table["table_path"])
                        for table in fixture["tables"]
                        if str(table["catalog"]) == catalog_name
                    }
                },
            }
        else:
            raise RuntimeError(f"unsupported demo catalog kind {kind!r}")
        _request("PUT", f"/v1/catalogs/{catalog_name}", body)


def _promote_table(fixture: dict[str, Any], table_fixture: dict[str, Any]) -> str:
    catalog_name = str(table_fixture["catalog"])
    target = str(table_fixture["target"])
    discovered = _request("GET", f"/v1/catalogs/{catalog_name}/tables")
    if not isinstance(discovered, dict):
        raise RuntimeError("catalog discovery returned an unexpected response")
    discovered_payload = cast(dict[str, Any], discovered)
    tables = cast(list[dict[str, Any]], discovered_payload.get("tables", []))
    discovered_by_target = {str(table["target"]): table for table in tables}
    if target not in discovered_by_target:
        raise RuntimeError(f"catalog discovery did not find {target}")
    table = discovered_by_target[target]
    asset = _request(
        "PUT",
        f"/v1/assets/{catalog_name}/{target}",
        {
            "backend": table["backend"],
            "table_identifier": table["table_identifier"],
            "options": {},
        },
    )
    if not isinstance(asset, dict):
        raise RuntimeError("asset upsert returned an unexpected response")
    asset_payload = cast(dict[str, Any], asset)
    asset_id = str(asset_payload["id"])
    _request(
        "PUT",
        f"/v1/assets/{asset_id}/schema-fields",
        {
            "fields": [
                {
                    "name": field["name"],
                    "type": field["type"],
                    "nullable": not bool(field.get("required", False)),
                }
                for field in table_fixture["schema"]
            ]
        },
    )
    _request("PUT", f"/v1/assets/{asset_id}/owners", {"owners": fixture["owners"]})
    _request("PUT", f"/v1/assets/{asset_id}/policy-rules", {"rules": fixture["policies"]})
    return asset_id


def _request(method: str, path: str, body: object | None = None) -> object:
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {ADMIN_TOKEN}",
    }
    if body is not None:
        headers["content-type"] = "application/json"
    request = Request(
        f"{CONTROL_PLANE_URL}{path}",
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=10) as response:
            payload = response.read()
            return json.loads(payload.decode("utf-8")) if payload else {}
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} failed with {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"{method} {path} failed: {exc.reason}") from exc


def _upsert_env_value(path: Path, key: str, value: str) -> None:
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    prefix = f"{key}="
    next_line = f"{key}={value}"
    replaced = False
    updated: list[str] = []
    for line in lines:
        if line.startswith(prefix):
            updated.append(next_line)
            replaced = True
        else:
            updated.append(line)
    if not replaced:
        updated.append(next_line)
    path.write_text("\n".join(updated) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
