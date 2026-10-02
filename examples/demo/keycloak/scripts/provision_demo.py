"""Resume first-time provisioning through the real control-plane API."""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from dal_obscura.identity.names import encode_federated_group, encode_federated_identity
from dal_obscura.policy.paths import FieldPath

FIXTURE_FILE = Path(__file__).resolve().parents[1] / "fixtures/demo_fixture.json"
CONTROL_PLANE_URL = os.environ.get("CONTROL_PLANE_URL", "http://control-plane:8820")
DEMO_OIDC_ISSUER = os.environ.get(
    "DEMO_OIDC_ISSUER", "http://localhost:20080/realms/dal-obscura-demo"
)
DEMO_RUNTIME_SETTINGS = {"ticket_ttl_seconds": 600, "max_tickets": 16, "max_ticket_exchanges": 1}
OIDC_AUTH_MODULE = "dal_obscura.identity.oidc.OidcJwksIdentityProvider"


def _demo_auth_provider() -> dict[str, Any]:
    return {
        "ordinal": 10,
        "module": OIDC_AUTH_MODULE,
        "enabled": True,
        "args": {
            "issuer": DEMO_OIDC_ISSUER,
            "audience": "dal-obscura",
            "jwks_url": os.environ.get(
                "DEMO_OIDC_JWKS_URL",
                "http://keycloak:8080/realms/dal-obscura-demo/protocol/openid-connect/certs",
            ),
            "subject_claim": "preferred_username",
            "group_claims": ["groups"],
            "attribute_claims": {},
        },
    }


def main() -> None:
    _provision_workspace(json.loads(FIXTURE_FILE.read_text()))
    print("Initial catalog, assets, owners, and live policies ready; existing edits preserved.")


def _provision_workspace(fixture: dict[str, Any]) -> None:
    if _request("GET", "/v1/settings/runtime") is None:
        _request("PUT", "/v1/settings/runtime", {**DEMO_RUNTIME_SETTINGS, "expected_revision": 0})
    if not _request("GET", "/v1/settings/auth-providers"):
        revision = _request("GET", "/v1/settings/auth-providers/revision")
        _request(
            "PUT",
            "/v1/settings/auth-providers",
            {
                "providers": [_demo_auth_provider()],
                "expected_revision": revision["revision"],
            },
        )
    catalogs = {item["name"] for item in _request("GET", "/v1/catalogs")}
    for catalog in fixture["catalogs"]:
        if catalog["name"] not in catalogs:
            _request(
                "PUT",
                f"/v1/catalogs/{catalog['name']}",
                {
                    "plugin_id": catalog["plugin_id"],
                    "options": {
                        "type": "sql",
                        "uri": {
                            "secret": "LOCAL_ICEBERG_URI",
                            "scope": f"catalog:{catalog['name']}",
                        },
                        "warehouse": os.environ.get("ICEBERG_WAREHOUSE", "/warehouse"),
                    },
                },
            )
    assets = {(item["catalog"], item["name"]): item for item in _request("GET", "/v1/assets")}
    for table in fixture["tables"]:
        asset = assets.get((table["catalog"], table["target"]))
        if asset is None:
            asset = _request(
                "PUT",
                f"/v1/assets/{table['catalog']}/{table['target']}",
                {
                    "backend": table["backend"],
                    "table_identifier": table["target"],
                    "options": {},
                },
            )
        _finish_asset(str(asset["id"]), fixture)


def _finish_asset(asset_id: str, fixture: dict[str, Any]) -> None:
    detail = _request("GET", f"/v1/assets/{asset_id}")
    # Even an explicitly saved empty policy is authored state, never a setup gap.
    if detail["policy_revision"] > 0:
        return
    if not detail["schema_fields"]:
        schema = _request("GET", f"/v1/assets/{asset_id}/schema")
        if schema.get("stable_field_ids") is not True:
            raise RuntimeError("Local Iceberg catalog must expose stable provider field IDs.")
        _request(
            "PUT",
            f"/v1/assets/{asset_id}/schema-fields",
            {
                "expected_revision": detail["revision"],
                "fields": _flatten_schema_fields(schema["fields"]),
            },
        )
    detail = _request("GET", f"/v1/assets/{asset_id}")
    if not detail["owners"]:
        _request(
            "PUT",
            f"/v1/assets/{asset_id}/owners",
            {
                "expected_revision": detail["revision"],
                "owners": _scoped_demo_owners(fixture["owners"]),
            },
        )
    detail = _request("GET", f"/v1/assets/{asset_id}")
    if detail["policy_revision"] == 0:
        _request(
            "PUT",
            f"/v1/assets/{asset_id}/policy",
            {
                "expected_revision": 0,
                "rules": fixture["policies"],
            },
        )


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
        "name": FieldPath.from_wire(path).to_human(),
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


@lru_cache(maxsize=1)
def _admin_token() -> str:
    request = Request(
        os.environ["OIDC_TOKEN_URL"],
        method="POST",
        data=urlencode(
            {
                "grant_type": "password",
                "client_id": "dal-obscura-cli",
                "client_secret": os.environ["OIDC_CLI_CLIENT_SECRET"],
                "username": "demo-admin",
                "password": os.environ["DEMO_ADMIN_PASSWORD"],
            }
        ).encode(),
        headers={"content-type": "application/x-www-form-urlencoded"},
    )
    with urlopen(request, timeout=15) as response:
        return json.loads(response.read())["access_token"]


def _request(method: str, path: str, body: object | None = None) -> Any:
    data = None if body is None else json.dumps(body).encode()
    request = Request(
        CONTROL_PLANE_URL + path,
        data=data,
        method=method,
        headers={
            "authorization": f"Bearer {_admin_token()}",
            "content-type": "application/json",
        },
    )
    try:
        with urlopen(request, timeout=15) as response:
            return json.loads(response.read())
    except HTTPError as exc:
        # Do not echo request bodies, provider URLs, or credentials into logs.
        raise RuntimeError(
            f"Provisioning {method} {path} returned HTTP {exc.code}; rerun ./demo init to resume."
        ) from exc
    except URLError as exc:
        raise RuntimeError(
            f"Provisioning {method} {path} could not reach the control plane."
        ) from exc


if __name__ == "__main__":
    main()
