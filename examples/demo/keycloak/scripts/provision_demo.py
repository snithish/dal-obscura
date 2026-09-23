from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.identity import encode_federated_group, encode_federated_identity
from dal_obscura.control_plane.infrastructure.repositories import ConfigStore

DEMO_DIR = Path(os.environ.get("DEMO_DIR", "/workspace/demo"))
RUNTIME_DIR = DEMO_DIR / ".runtime"
FIXTURE_FILE = DEMO_DIR / "fixtures" / "demo_fixture.json"
DATA_PLANE_ENV = RUNTIME_DIR / "data-plane.env"
CONTROL_PLANE_URL = os.environ.get("CONTROL_PLANE_URL", "http://control-plane:8820")
ADMIN_TOKEN = os.environ.get("DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN", "")
DATABASE_URL = os.environ.get("DAL_OBSCURA_DATABASE_URL", "")
ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)
OIDC_AUTH_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider"
)
DEMO_OIDC_ISSUER = os.environ.get(
    "DEMO_OIDC_ISSUER",
    "http://127.0.0.1:8081/realms/dal-obscura-demo",
)


def main() -> None:
    if not ADMIN_TOKEN or not DATABASE_URL:
        raise RuntimeError("demo provisioning requires the control-plane token and database URL")
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
        context = ConfigStore(session).get_default_workspace_context()
        if context is None:
            raise RuntimeError("workspace provisioning did not create a runtime context")
        return str(context.cell_id)


def _provision_workspace(fixture: dict[str, Any]) -> str:
    workspace_state = _workspace_state(fixture)
    if workspace_state == "complete":
        return _workspace_cell_id()

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
    _request(
        "PUT",
        "/v1/settings/auth-providers",
        {
            "providers": [
                {
                    "ordinal": 10,
                    "module": OIDC_AUTH_MODULE,
                    "args": {
                        "issuer": DEMO_OIDC_ISSUER,
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
    for table_fixture in fixture["tables"]:
        _configure_table_policy(fixture, table_fixture)
    return cell_id


def _workspace_state(fixture: dict[str, Any]) -> str:
    """Classify the workspace before any mutating setup request.

    A fresh database is safe to initialize.  A fully provisioned database is
    safe to reuse after checking the expected demo assets and their live policies.
    Any other state is ambiguous, so setup fails instead of overwriting
    customer-authored configuration.  Operators can use the explicit reset
    workflow when they really intend to destroy the demo state.
    """

    summary = _request("GET", "/v1/workspace/summary")
    if not isinstance(summary, dict):
        raise RuntimeError("workspace summary returned an unexpected response")
    summary_payload = cast(dict[str, Any], summary)
    if _summary_is_empty(summary_payload):
        return "empty"

    expected_catalog_count = len(fixture["catalogs"])
    expected_asset_count = len(fixture["tables"])
    if not _summary_meets_expected_counts(
        summary_payload,
        expected_catalog_count=expected_catalog_count,
        expected_asset_count=expected_asset_count,
    ):
        raise RuntimeError(
            "demo workspace is partially configured; refusing to overwrite existing "
            "catalogs, assets, owners, or policies. Recover the workspace or run the "
            "explicit reset workflow before starting the demo again."
        )

    assets_response = _request("GET", "/v1/assets")
    assets = _as_list(assets_response, "assets")
    asset_by_target = {
        (str(asset.get("catalog")), str(asset.get("target"))): asset for asset in assets
    }
    expected_targets = {
        (str(table["catalog"]), str(table["target"])) for table in fixture["tables"]
    }
    expected_assets = [asset_by_target.get(target) for target in expected_targets]
    complete = _summary_meets_expected_counts(
        summary_payload,
        expected_catalog_count=len(fixture["catalogs"]),
        expected_asset_count=len(expected_targets),
    ) and all(
        asset is not None and asset.get("policy_status") == "configured"
        for asset in expected_assets
    )
    if complete:
        return "complete"
    raise RuntimeError(
        "demo workspace is partially configured; refusing to overwrite existing "
        "catalogs, assets, owners, or policies. Recover the workspace or run the "
        "explicit reset workflow before starting the demo again."
    )


def _summary_is_empty(summary: dict[str, Any]) -> bool:
    return (
        int(summary.get("catalog_count", 0)) == 0
        and int(summary.get("asset_count", 0)) == 0
        and int(summary.get("unowned_asset_count", 0)) == 0
        and int(summary.get("missing_policy_count", 0)) == 0
        and not bool(summary.get("runtime_configured"))
        and int(summary.get("enabled_auth_provider_count", 0)) == 0
    )


def _summary_meets_expected_counts(
    summary: dict[str, Any],
    *,
    expected_catalog_count: int,
    expected_asset_count: int,
) -> bool:
    return (
        int(summary.get("catalog_count", 0)) >= expected_catalog_count
        and int(summary.get("asset_count", 0)) >= expected_asset_count
        and int(summary.get("unowned_asset_count", 0)) == 0
        and int(summary.get("missing_policy_count", 0)) == 0
        and bool(summary.get("runtime_configured"))
        and int(summary.get("enabled_auth_provider_count", 0)) > 0
    )


def _as_list(value: object, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise RuntimeError(f"{label} returned an unexpected response")
    return cast(list[dict[str, Any]], value)


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
        else:
            raise RuntimeError(f"unsupported demo catalog kind {kind!r}")
        _request("PUT", f"/v1/catalogs/{catalog_name}", body)


def _configure_table_policy(fixture: dict[str, Any], table_fixture: dict[str, Any]) -> str:
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
    revision = _asset_revision(asset_id)
    live_schema = _request("GET", f"/v1/assets/{asset_id}/schema")
    if not isinstance(live_schema, dict):
        raise RuntimeError("asset schema returned an unexpected response")
    schema_payload = cast(dict[str, Any], live_schema)
    if schema_payload.get("stable_field_ids") is not True:
        raise RuntimeError(f"{target} does not expose stable provider field IDs")
    _request(
        "PUT",
        f"/v1/assets/{asset_id}/schema-fields",
        {
            "expected_revision": revision,
            "fields": _flatten_schema_fields(schema_payload.get("fields")),
        },
    )
    revision = _asset_revision(asset_id)
    _request(
        "PUT",
        f"/v1/assets/{asset_id}/owners",
        {"owners": _scoped_demo_owners(fixture["owners"]), "expected_revision": revision},
    )
    detail_response = _request("GET", f"/v1/assets/{asset_id}")
    if not isinstance(detail_response, dict):
        raise RuntimeError("asset policy revision returned an unexpected response")
    detail = cast(dict[str, Any], detail_response)
    if not isinstance(detail.get("policy_revision"), int):
        raise RuntimeError("asset policy revision returned an unexpected response")
    _request(
        "PUT",
        f"/v1/assets/{asset_id}/policy",
        {"expected_revision": detail["policy_revision"], "rules": fixture["policies"]},
    )
    return asset_id


def _flatten_schema_fields(raw_fields: object) -> list[dict[str, Any]]:
    """Convert the authoritative nested provider schema to admitted field records."""
    if not isinstance(raw_fields, list):
        raise RuntimeError("asset schema fields returned an unexpected response")
    result: list[dict[str, Any]] = []
    for field in raw_fields:
        if not isinstance(field, dict):
            raise RuntimeError("asset schema fields returned an unexpected response")
        result.extend(_flatten_schema_field(cast(dict[str, Any], field)))
    return result


def _flatten_schema_field(node: dict[str, Any]) -> list[dict[str, Any]]:
    name = node.get("name")
    field_id = node.get("field_id")
    if not isinstance(name, str) or not name or isinstance(field_id, bool):
        raise RuntimeError("asset schema field is missing its stable identity")
    if not isinstance(field_id, int) or field_id < 0:
        raise RuntimeError("asset schema field has an invalid provider identity")
    path = node.get("path")
    if not isinstance(path, dict) or not isinstance(path.get("segments"), list):
        raise RuntimeError("asset schema field has an invalid provider path")
    field = {
        "name": name,
        "field_id": str(field_id),
        "path": _schema_path_segments(path["segments"]),
        "type": str(node.get("type", "string")),
        "nullable": bool(node.get("nullable", True)),
    }
    children = node.get("children", [])
    if not isinstance(children, list) or not all(isinstance(item, dict) for item in children):
        raise RuntimeError("asset schema field children are malformed")
    return [
        field,
        *(child_field for child in children for child_field in _flatten_schema_field(child)),
    ]


def _schema_path_segments(segments: list[object]) -> list[str]:
    result: list[str] = []
    collection_segments = {"list_element": "$element", "map_key": "$key", "map_value": "$value"}
    for raw_segment in segments:
        if not isinstance(raw_segment, dict):
            raise RuntimeError("asset schema field path is malformed")
        segment = cast(dict[str, Any], raw_segment)
        kind = segment.get("kind")
        if kind == "field" and isinstance(segment.get("name"), str):
            result.append(segment["name"])
        elif kind in collection_segments:
            result.append(collection_segments[kind])
        else:
            raise RuntimeError("asset schema field path contains an unsupported segment")
    return result


def _asset_revision(asset_id: str) -> int:
    asset = _request("GET", f"/v1/assets/{asset_id}")
    if not isinstance(asset, dict):
        raise RuntimeError("asset lookup returned no revision")
    asset_payload = cast(dict[str, Any], asset)
    if not isinstance(asset_payload.get("revision"), int):
        raise RuntimeError("asset lookup returned no revision")
    return asset_payload["revision"]


def _scoped_demo_owners(raw_owners: object) -> list[str]:
    """Return owner keys that match the demo OIDC identity namespace.

    Fixture policy principals intentionally stay unscoped because they are
    evaluated by the data-plane identity provider.  Control-plane ownership
    is persisted with the issuer prefix so a same-named identity from another
    provider cannot gain edit or grant-management access.
    """

    if not isinstance(raw_owners, list):
        raise ValueError("demo fixture owners must be a list")
    owners: list[str] = []
    for raw_owner in raw_owners:
        owner = str(raw_owner).strip()
        if not owner:
            continue
        if "|" in owner:
            owners.append(owner)
        elif owner.startswith("group:"):
            owners.append(encode_federated_group(DEMO_OIDC_ISSUER.rstrip("/"), owner[6:]))
        else:
            owners.append(encode_federated_identity(DEMO_OIDC_ISSUER.rstrip("/"), owner))
    if not owners:
        raise ValueError("demo fixture must define at least one owner")
    return owners


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
