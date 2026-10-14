"""Provision governance through real HTTP boundaries, including schema admission."""

import json
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pyarrow as pa
from dal_obscura_plugin_api.paths import (
    FieldPath,
    FieldSegment,
    ListElementSegment,
    MapKeySegment,
    MapValueSegment,
)
from dal_obscura_plugin_api.schema.identity import schema_field_id, schema_scope_digest
from dal_obscura_plugin_api.schema.index import walk_schema_fields
from deltalake import DeltaTable
from fixture import BUCKET, COLUMNS, MASKS, TARGET


def request(method, path, body=None):
    call = Request(
        "http://control:8820" + path,
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={
            "Authorization": "Bearer disposable-scenario-admin",
            "Content-Type": "application/json",
        },
    )
    try:
        with urlopen(call, timeout=90) as response:
            return json.loads(response.read())
    except HTTPError as error:
        print(error.read().decode())
        raise


def flatten(nodes, identities=None):
    result = []
    tokens = {"list_element": "$element", "map_key": "$key", "map_value": "$value"}
    for node in nodes:
        path = node["path"]
        result.append(
            {
                "name": FieldPath.from_wire(path).to_human(),
                "field_id": identities[node["human_path"]]
                if identities is not None
                else str(node["field_id"]),
                "path": [
                    segment["name"] if segment["kind"] == "field" else tokens[segment["kind"]]
                    for segment in path["segments"]
                ],
                "type": node["type"],
                "nullable": node["nullable"],
            }
        )
        result.extend(flatten(node.get("children", []), identities))
    return result


def main():
    mode = os.environ["SCENARIO"]
    request(
        "PUT",
        "/v1/settings/runtime",
        {
            "expected_revision": 0,
            "ticket_ttl_seconds": 3600,
            "max_tickets": 3,
            "max_ticket_exchanges": 1,
        },
    )
    request(
        "PUT",
        "/v1/settings/auth-providers",
        {
            "expected_revision": 0,
            "providers": [
                {
                    "ordinal": 1,
                    "enabled": True,
                    "module": "dal_obscura.identity.oidc.OidcJwksIdentityProvider",
                    "args": {
                        "issuer": "http://issuer:8080",
                        "audience": "scenario",
                        "jwks_url": "http://issuer:8080/jwks",
                        "subject_claim": "sub",
                        "group_claims": ["groups"],
                        "attribute_claims": {},
                    },
                }
            ],
        },
    )
    options = (
        {"root": f"s3://{BUCKET}/delta", "tables_path": "tables.json"}
        if mode == "delta"
        else {
            "type": "sql",
            "uri": {"secret": "ICEBERG_URI", "scope": "catalog:scenario"},
            "warehouse": f"s3://{BUCKET}/iceberg",
        }
    )
    request(
        "PUT",
        "/v1/catalogs/scenario",
        {
            "plugin_id": "delta.directory" if mode == "delta" else "iceberg.catalog",
            "options": options,
        },
    )
    discovered = request("GET", "/v1/catalogs/scenario/tables")
    assert len(discovered["tables"]) == 101, discovered
    asset = request(
        "PUT",
        f"/v1/assets/scenario/{TARGET}",
        {"backend": mode, "table_identifier": TARGET, "options": {}},
    )
    asset_id = asset["id"]
    schema = request("GET", f"/v1/assets/{asset_id}/schema")
    identities = None
    if mode == "delta":
        provider_schema = pa.schema(
            DeltaTable(
                f"s3://{BUCKET}/delta/customers",
                storage_options={
                    "AWS_ENDPOINT_URL": os.environ["AWS_ENDPOINT_URL"],
                    "AWS_ALLOW_HTTP": "true",
                },
            )
            .schema()
            .to_arrow()
        )
        digest = schema_scope_digest(provider_schema)
        identities = {
            path.to_human(): schema_field_id(
                field,
                [
                    segment.name
                    if isinstance(segment, FieldSegment)
                    else {
                        ListElementSegment: "$element",
                        MapKeySegment: "$key",
                        MapValueSegment: "$value",
                    }[type(segment)]
                    for segment in path.segments
                ],
                scope_digest=digest,
            )[0]
            for field, path in walk_schema_fields(provider_schema)
        }
    detail = request("GET", f"/v1/assets/{asset_id}")
    request(
        "PUT",
        f"/v1/assets/{asset_id}/schema-fields",
        {"expected_revision": detail["revision"], "fields": flatten(schema["fields"], identities)},
    )
    request(
        "PUT",
        f"/v1/assets/{asset_id}/policy",
        {
            "expected_revision": 0,
            "rules": [
                {
                    "name": "Nested PII protection",
                    "effect": "allow",
                    "principals": ["group:analysts"],
                    "columns": COLUMNS,
                    "masks": MASKS,
                }
            ],
        },
    )
    Path("/state/governance.json").write_text(
        json.dumps(
            {
                "catalog_tables": len(discovered["tables"]),
                "asset_id": asset_id,
                "schema": schema,
                "masks": MASKS,
                "columns": COLUMNS,
            },
            indent=2,
        )
    )
    print("101 catalog tables discovered; target schema admitted and seven nested PII masks saved.")


if __name__ == "__main__":
    main()
