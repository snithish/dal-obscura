from __future__ import annotations

import json
import os
import shlex
from contextlib import suppress
from pathlib import Path
from typing import Any

import pyarrow as pa
from fastapi.testclient import TestClient
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, DoubleType, IntegerType, LongType, NestedField, StringType

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import Base
from dal_obscura.control_plane.interfaces.api import create_app

RUNTIME_DIR = Path(os.environ.get("RUNTIME_DIR", "/workspace/runtime"))
ADMIN_TOKEN = "local-example-admin"
CATALOG_NAME = "example_catalog"
TENANT_SLUG = "default"
TABLE_TARGET = "default.users"
ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)

TYPE_BUILDERS: dict[str, tuple[type, pa.DataType]] = {
    "string": (StringType, pa.string()),
    "long": (LongType, pa.int64()),
    "int": (IntegerType, pa.int32()),
    "double": (DoubleType, pa.float64()),
    "boolean": (BooleanType, pa.bool_()),
}
FIXTURE: dict[str, Any] = {
    "catalog": CATALOG_NAME,
    "tables": [
        {
            "target": TABLE_TARGET,
            "schema": [
                {"name": "id", "type": "long", "required": True},
                {"name": "email", "type": "string"},
                {"name": "region", "type": "string"},
            ],
            "rows": [
                {"id": 1, "email": "alice@example.com", "region": "us"},
                {"id": 2, "email": "bob@example.com", "region": "eu"},
                {"id": 3, "email": "carol@example.com", "region": "us"},
            ],
        }
    ],
    "policies": [
        {
            "target": TABLE_TARGET,
            "rules": [
                {
                    "principals": ["example-user"],
                    "columns": ["id", "email", "region"],
                    "row_filter": "region = 'us'",
                }
            ],
        }
    ],
}


def main() -> None:
    fixture = FIXTURE
    auth_flow = os.environ["AUTH_FLOW"]
    catalog_name = str(_require_key(fixture, "catalog")).strip()
    if not catalog_name:
        raise ValueError("Fixture catalog must be non-empty")

    tables = _read_tables(fixture)
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    _create_tables(catalog_name, tables)
    cell_id = _provision_control_plane(catalog_name, fixture, tables, auth_flow)
    _write_data_plane_env(cell_id, auth_flow)
    print(
        json.dumps(
            {
                "catalog": catalog_name,
                "tables": [table["target"] for table in tables],
                "cell_id": cell_id,
            }
        )
    )


def _read_tables(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    raw_tables = fixture.get("tables")
    if not isinstance(raw_tables, list) or not raw_tables:
        raise ValueError("Fixture must define a non-empty tables list")
    tables: list[dict[str, Any]] = []
    for raw_table in raw_tables:
        if not isinstance(raw_table, dict):
            raise ValueError("Each fixture table must be an object")
        target = str(_require_key(raw_table, "target")).strip()
        if not target:
            raise ValueError("Fixture table target must be non-empty")
        schema = raw_table.get("schema")
        if not isinstance(schema, list) or not schema:
            raise ValueError(f"Fixture table {target!r} must define a non-empty schema")
        tables.append({"target": target, "schema": schema, "rows": _table_rows(raw_table, target)})
    return tables


def _table_rows(table: dict[str, Any], target: str) -> list[dict[str, Any]]:
    rows = table.get("rows", [])
    csv_path = table.get("csv")
    if rows and csv_path:
        raise ValueError(f"Fixture table {target!r} must not define both rows and csv")
    if rows:
        if not isinstance(rows, list):
            raise ValueError(f"Fixture table {target!r} rows must be a list")
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError(f"Fixture table {target!r} rows must be objects")
            normalized_rows.append(dict(row))
        return normalized_rows
    if csv_path:
        raise ValueError("CSV-backed example fixtures are not supported")
    return []


def _create_tables(catalog_name: str, tables: list[dict[str, Any]]) -> None:
    warehouse = RUNTIME_DIR / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        catalog_name,
        type="sql",
        uri=f"sqlite:///{RUNTIME_DIR / f'{catalog_name}.db'}",
        warehouse=str(warehouse),
    )
    for table in tables:
        target = str(table["target"])
        namespace = ".".join(target.split(".")[:-1])
        with suppress(Exception):
            if namespace:
                catalog.create_namespace(namespace)
        with suppress(Exception):
            catalog.drop_table(target)
        iceberg_schema, arrow_schema = _schemas(table["schema"], target)
        created = catalog.create_table(
            target,
            schema=iceberg_schema,
            properties={"format-version": "2"},
        )
        created.append(
            pa.Table.from_pylist(
                _cast_rows(table["rows"], table["schema"], target),
                schema=arrow_schema,
            )
        )


def _schemas(fields: list[dict[str, Any]], target: str) -> tuple[Schema, pa.Schema]:
    iceberg_fields: list[NestedField] = []
    arrow_fields: list[pa.Field] = []
    for index, raw_field in enumerate(fields, start=1):
        if not isinstance(raw_field, dict):
            raise ValueError(f"Fixture table {target!r} schema entries must be objects")
        name = str(_require_key(raw_field, "name")).strip()
        type_name = str(_require_key(raw_field, "type")).strip().lower()
        required = bool(raw_field.get("required", False))
        if type_name not in TYPE_BUILDERS:
            supported = ", ".join(sorted(TYPE_BUILDERS))
            raise ValueError(
                f"Fixture table {target!r} field {name!r} uses unsupported "
                f"type {type_name!r}; supported: {supported}"
            )
        iceberg_type, arrow_type = TYPE_BUILDERS[type_name]
        iceberg_fields.append(
            NestedField(field_id=index, name=name, field_type=iceberg_type(), required=required)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=not required))
    return Schema(*iceberg_fields), pa.schema(arrow_fields)


def _cast_rows(
    rows: list[dict[str, Any]],
    fields: list[dict[str, Any]],
    target: str,
) -> list[dict[str, Any]]:
    field_types = {str(field["name"]): str(field["type"]).lower() for field in fields}
    required_fields = {str(field["name"]) for field in fields if bool(field.get("required", False))}
    casted_rows: list[dict[str, Any]] = []
    for row in rows:
        casted_row: dict[str, Any] = {}
        for name, type_name in field_types.items():
            value = row.get(name)
            if value in {None, ""}:
                if name in required_fields:
                    raise ValueError(
                        f"Fixture table {target!r} is missing required value for {name!r}"
                    )
                casted_row[name] = None
                continue
            casted_row[name] = _cast_value(type_name, value)
        casted_rows.append(casted_row)
    return casted_rows


def _cast_value(type_name: str, value: Any) -> Any:
    if type_name == "string":
        return str(value)
    if type_name in {"long", "int"}:
        return int(value)
    if type_name == "double":
        return float(value)
    if type_name == "boolean":
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
        raise ValueError(f"Unsupported boolean value {value!r}")
    raise ValueError(f"Unsupported fixture type {type_name!r}")


def _read_policies(
    fixture: dict[str, Any],
    tables: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    policies: list[dict[str, Any]] = []
    raw_policies = fixture.get("policies", [])
    if raw_policies:
        if not isinstance(raw_policies, list):
            raise ValueError("Fixture policies must be a list")
        for raw_policy in raw_policies:
            if not isinstance(raw_policy, dict):
                raise ValueError("Fixture policies must contain objects")
            target = str(_require_key(raw_policy, "target")).strip()
            rules = raw_policy.get("rules")
            if not isinstance(rules, list) or not rules:
                raise ValueError(f"Fixture policy {target!r} must define a non-empty rules list")
            policies.append({"target": target, "rules": rules})
    else:
        for table in tables:
            rules = []
            for rule in table.get("rules", []):
                if not isinstance(rule, dict):
                    raise ValueError(f"Fixture table {table['target']!r} rules must be objects")
                rules.append(rule)
            if rules:
                policies.append({"target": str(table["target"]), "rules": rules})
    if not policies:
        raise ValueError("Fixture must define policies or per-table rules")
    return policies


def _provision_control_plane(
    catalog_name: str,
    fixture: dict[str, Any],
    tables: list[dict[str, Any]],
    auth_flow: str,
) -> str:
    database_path = RUNTIME_DIR / "control-plane.db"
    database_path.unlink(missing_ok=True)
    database_url = f"sqlite+pysqlite:///{database_path}"
    engine = create_engine_from_url(database_url)
    Base.metadata.create_all(engine)
    client = TestClient(create_app(session_factory(engine), admin_token=ADMIN_TOKEN))
    headers = {"authorization": f"Bearer {ADMIN_TOKEN}"}

    tenant = _request(
        client,
        "post",
        "/v1/tenants",
        headers,
        {"slug": TENANT_SLUG, "display_name": "Default Example Tenant"},
    )
    cell = _request(
        client,
        "post",
        "/v1/cells",
        headers,
        {"name": "local-example", "region": "local"},
    )
    tenant_id = str(tenant["id"])
    cell_id = str(cell["id"])
    _request(
        client,
        "put",
        f"/v1/cells/{cell_id}/tenants/{tenant_id}",
        headers,
        {"shard_key": TENANT_SLUG},
    )
    _request(
        client,
        "put",
        f"/v1/tenants/{tenant_id}/cells/{cell_id}/runtime-settings",
        headers,
        {"ticket_ttl_seconds": 900, "max_tickets": 64, "path_rules": []},
    )
    _request(
        client,
        "put",
        f"/v1/tenants/{tenant_id}/cells/{cell_id}/catalogs/{catalog_name}",
        headers,
        {
            "module": ICEBERG_CATALOG_MODULE,
            "options": {
                "type": "sql",
                "uri": f"sqlite:///{RUNTIME_DIR / f'{catalog_name}.db'}",
                "warehouse": str(RUNTIME_DIR / "warehouse"),
            },
        },
    )
    policies = {policy["target"]: policy["rules"] for policy in _read_policies(fixture, tables)}
    for table in tables:
        target = str(table["target"])
        asset = _request(
            client,
            "put",
            f"/v1/tenants/{tenant_id}/cells/{cell_id}/assets/{catalog_name}/{target}",
            headers,
            {"backend": "iceberg", "table_identifier": target, "options": {}},
        )
        _request(
            client,
            "put",
            f"/v1/assets/{asset['id']}/policy-rules",
            headers,
            {"rules": _compiled_policy_rules(policies[target])},
        )
    _request(
        client,
        "put",
        f"/v1/cells/{cell_id}/auth-providers",
        headers,
        {"providers": _auth_providers(auth_flow)},
    )
    publication = _request(client, "post", f"/v1/cells/{cell_id}/publications", headers)
    _request(
        client,
        "post",
        f"/v1/cells/{cell_id}/publications/{publication['publication_id']}/activate",
        headers,
    )
    return cell_id


def _compiled_policy_rules(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compiled: list[dict[str, Any]] = []
    for ordinal, rule in enumerate(rules, start=1):
        compiled.append(
            {
                "ordinal": ordinal * 10,
                "principals": list(rule["principals"]),
                "columns": list(rule["columns"]),
                "effect": str(rule.get("effect", "allow")),
                "when": dict(rule.get("when", {})),
                "masks": dict(rule.get("masks", {})),
                "row_filter": rule.get("row_filter"),
            }
        )
    return compiled


def _auth_providers(auth_flow: str) -> list[dict[str, Any]]:
    if auth_flow == "shared-jwt":
        return [
            _provider(
                "dal_obscura.data_plane.infrastructure.adapters.identity_default.DefaultIdentityAdapter",
                {"jwt_secret": {"key": "DAL_OBSCURA_JWT_SECRET"}},
            )
        ]
    if auth_flow == "api-key":
        return [
            _provider(
                "dal_obscura.data_plane.infrastructure.adapters.identity_api_key.ApiKeyIdentityProvider",
                {"keys": [_api_key_record()]},
            )
        ]
    if auth_flow == "composite-provider":
        return [
            _provider(
                "dal_obscura.data_plane.infrastructure.adapters.identity_api_key.ApiKeyIdentityProvider",
                {"keys": [_api_key_record()]},
                ordinal=10,
            ),
            _provider(
                "dal_obscura.data_plane.infrastructure.adapters.identity_default.DefaultIdentityAdapter",
                {"jwt_secret": {"key": "DAL_OBSCURA_JWT_SECRET"}},
                ordinal=20,
            ),
        ]
    if auth_flow == "keycloak-oidc":
        return [
            _provider(
                "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider",
                {
                    "issuer": "http://keycloak:8080/realms/dal-obscura",
                    "audience": "dal-obscura",
                    "jwks_url": (
                        "http://keycloak:8080/realms/dal-obscura/protocol/openid-connect/certs"
                    ),
                    "subject_claim": "preferred_username",
                },
            )
        ]
    if auth_flow == "mtls":
        return [
            _provider(
                "dal_obscura.data_plane.infrastructure.adapters.identity_mtls.MtlsIdentityProvider",
                {
                    "identities": [
                        {
                            "peer_identity": "urn:dal-obscura:example-client",
                            "id": "example-user",
                            "groups": ["compose-example"],
                            "attributes": {"client_id": "urn:dal-obscura:example-client"},
                        }
                    ]
                },
            )
        ]
    if auth_flow == "mtls-spiffe":
        return [
            _provider(
                "dal_obscura.data_plane.infrastructure.adapters.identity_mtls.MtlsIdentityProvider",
                {
                    "identities": [
                        {
                            "peer_identity": "spiffe://example.org/ns/default/sa/dal-obscura-client",
                            "id": "example-user",
                            "groups": ["compose-example", "spiffe"],
                            "attributes": {
                                "spiffe_id": (
                                    "spiffe://example.org/ns/default/sa/dal-obscura-client"
                                )
                            },
                        }
                    ]
                },
            )
        ]
    if auth_flow == "trusted-headers":
        return [
            _provider(
                "dal_obscura.data_plane.infrastructure.adapters.identity_trusted_headers.TrustedHeaderIdentityProvider",
                {"shared_secret": {"key": "DAL_OBSCURA_PROXY_SECRET"}},
            )
        ]
    raise ValueError(f"Unsupported auth example flow {auth_flow!r}")


def _provider(
    module: str,
    args: dict[str, Any],
    *,
    ordinal: int = 10,
) -> dict[str, Any]:
    return {"ordinal": ordinal, "module": module, "args": args, "enabled": True}


def _api_key_record() -> dict[str, Any]:
    return {
        "id": "example-user",
        "secret": {"key": "DAL_OBSCURA_API_KEY"},
        "groups": ["compose-example"],
    }


def _write_data_plane_env(cell_id: str, auth_flow: str) -> None:
    location = (
        "grpc+tls://0.0.0.0:8815" if auth_flow in {"mtls", "mtls-spiffe"} else "grpc://0.0.0.0:8815"
    )
    values = {
        "DAL_OBSCURA_DATABASE_URL": f"sqlite+pysqlite:///{RUNTIME_DIR / 'control-plane.db'}",
        "DAL_OBSCURA_CELL_ID": cell_id,
        "DAL_OBSCURA_LOCATION": location,
        "DAL_OBSCURA_JSON_LOGS": "true",
    }
    lines = [f"export {key}={shlex.quote(value)}" for key, value in values.items()]
    (RUNTIME_DIR / "data-plane.env").write_text("\n".join(lines) + "\n")


def _request(
    client: TestClient,
    method: str,
    path: str,
    headers: dict[str, str],
    json_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = client.request(method, path, headers=headers, json=json_body)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Control-plane route {path!r} returned a non-object response")
    return payload


def _require_key(raw: dict[str, Any], key: str) -> Any:
    if key not in raw:
        raise ValueError(f"Missing required key {key!r}")
    return raw[key]


if __name__ == "__main__":
    main()
