from __future__ import annotations

import argparse
import importlib
import json
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import jwt
import pyarrow as pa
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

create_iceberg_table = importlib.import_module("tests.support.iceberg").create_iceberg_table

from dal_obscura.control_plane.application.provisioning import ProvisioningService  # noqa: E402
from dal_obscura.control_plane.infrastructure.db import (  # noqa: E402
    create_engine_from_url,
    session_factory,
)
from dal_obscura.control_plane.infrastructure.orm import Base  # noqa: E402

JWT_SECRET = "spark-jwt-secret-32-characters-long"
TICKET_SECRET = "spark-ticket-secret-32-characters"
CATALOG_NAME = "spark_catalog"
WAREHOUSE_NAME = "warehouse"
TABLE_ID = "default.complex_users"
ROWS_PER_BATCH = 25_000
APPEND_BATCH_COUNT = 5
TOTAL_ROWS = ROWS_PER_BATCH * APPEND_BATCH_COUNT
POLICY_ROW_FILTER = "region = 'us' AND active = true"
SUPPORTED_MASK_TYPES = ["default", "email", "hash", "keep_last", "null", "redact"]
ALLOWED_COLUMNS = [
    "id",
    "region",
    "market",
    "active",
    "vip",
    "score",
    "created_at",
    "signup_date",
    "credit_limit",
    "account_balance",
    "account_number",
    "email",
    "phone",
    "status",
    "notes",
    "nickname",
    "tags",
    "attributes",
    "user",
    "user.email",
    "user.address.zip",
    "user.preferences.theme",
    "account",
    "account.manager.region",
    "devices",
    "support_ticket",
    "support_ticket.ticket_id",
]
MASKS = {
    "id": {"type": "hash"},
    "account_number": {"type": "keep_last", "value": 4},
    "nickname": {"type": "null"},
    "notes": {"type": "redact", "value": "[redacted-note]"},
    "status": {"type": "default", "value": "partner-visible"},
    "email": {"type": "email"},
    "user.address.zip": {"type": "hash"},
    "user.preferences.theme": {"type": "redact", "value": "[hidden-theme]"},
    "support_ticket.ticket_id": {"type": "hash"},
}


def _arrow_schema() -> pa.Schema:
    preference_type = pa.struct(
        [
            pa.field("name", pa.string()),
            pa.field("enabled", pa.bool_()),
            pa.field("theme", pa.string()),
        ]
    )
    address_type = pa.struct(
        [
            pa.field("zip", pa.int64()),
            pa.field("city", pa.string()),
            pa.field("country", pa.string()),
        ]
    )
    user_type = pa.struct(
        [
            pa.field("email", pa.string()),
            pa.field("alternate_email", pa.string()),
            pa.field("address", address_type),
            pa.field("preferences", pa.list_(preference_type)),
        ]
    )
    manager_type = pa.struct(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("region", pa.string()),
        ]
    )
    account_type = pa.struct(
        [
            pa.field("status", pa.string()),
            pa.field("tier", pa.string()),
            pa.field("manager", manager_type),
        ]
    )
    device_type = pa.struct(
        [
            pa.field("device_id", pa.string()),
            pa.field("platform", pa.string()),
            pa.field("trusted", pa.bool_()),
        ]
    )
    support_ticket_type = pa.struct(
        [
            pa.field("ticket_id", pa.string()),
            pa.field("channel", pa.string()),
            pa.field("opened_at", pa.timestamp("us")),
            pa.field("resolution", pa.string()),
        ]
    )
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("region", pa.string()),
            pa.field("market", pa.string()),
            pa.field("active", pa.bool_()),
            pa.field("vip", pa.bool_()),
            pa.field("risk_bucket", pa.int64()),
            pa.field("score", pa.float64()),
            pa.field("created_at", pa.timestamp("us")),
            pa.field("updated_at", pa.timestamp("us")),
            pa.field("signup_date", pa.date32()),
            pa.field("credit_limit", pa.decimal128(12, 2)),
            pa.field("account_balance", pa.decimal128(12, 2)),
            pa.field("account_number", pa.string()),
            pa.field("email", pa.string()),
            pa.field("phone", pa.string()),
            pa.field("status", pa.string()),
            pa.field("notes", pa.string()),
            pa.field("nickname", pa.string()),
            pa.field("tags", pa.list_(pa.string())),
            pa.field("attributes", pa.map_(pa.string(), pa.string())),
            pa.field("user", user_type),
            pa.field("account", account_type),
            pa.field("devices", pa.list_(device_type)),
            pa.field("support_ticket", support_ticket_type),
        ]
    )


def _iceberg_schema() -> Schema:
    return Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "region", StringType(), required=False),
        NestedField(3, "market", StringType(), required=False),
        NestedField(4, "active", BooleanType(), required=False),
        NestedField(5, "vip", BooleanType(), required=False),
        NestedField(6, "risk_bucket", LongType(), required=False),
        NestedField(7, "score", DoubleType(), required=False),
        NestedField(8, "created_at", TimestampType(), required=False),
        NestedField(9, "updated_at", TimestampType(), required=False),
        NestedField(10, "signup_date", DateType(), required=False),
        NestedField(11, "credit_limit", DecimalType(12, 2), required=False),
        NestedField(12, "account_balance", DecimalType(12, 2), required=False),
        NestedField(13, "account_number", StringType(), required=False),
        NestedField(14, "email", StringType(), required=False),
        NestedField(15, "phone", StringType(), required=False),
        NestedField(16, "status", StringType(), required=False),
        NestedField(17, "notes", StringType(), required=False),
        NestedField(18, "nickname", StringType(), required=False),
        NestedField(
            19,
            "tags",
            ListType(element_id=20, element_type=StringType(), element_required=False),
            required=False,
        ),
        NestedField(
            21,
            "attributes",
            MapType(
                key_id=22,
                key_type=StringType(),
                value_id=23,
                value_type=StringType(),
                value_required=False,
            ),
            required=False,
        ),
        NestedField(
            24,
            "user",
            StructType(
                NestedField(25, "email", StringType(), required=False),
                NestedField(26, "alternate_email", StringType(), required=False),
                NestedField(
                    27,
                    "address",
                    StructType(
                        NestedField(28, "zip", LongType(), required=False),
                        NestedField(29, "city", StringType(), required=False),
                        NestedField(30, "country", StringType(), required=False),
                    ),
                    required=False,
                ),
                NestedField(
                    31,
                    "preferences",
                    ListType(
                        element_id=32,
                        element_type=StructType(
                            NestedField(33, "name", StringType(), required=False),
                            NestedField(34, "enabled", BooleanType(), required=False),
                            NestedField(35, "theme", StringType(), required=False),
                        ),
                        element_required=False,
                    ),
                    required=False,
                ),
            ),
            required=False,
        ),
        NestedField(
            36,
            "account",
            StructType(
                NestedField(37, "status", StringType(), required=False),
                NestedField(38, "tier", StringType(), required=False),
                NestedField(
                    39,
                    "manager",
                    StructType(
                        NestedField(40, "id", LongType(), required=False),
                        NestedField(41, "name", StringType(), required=False),
                        NestedField(42, "region", StringType(), required=False),
                    ),
                    required=False,
                ),
            ),
            required=False,
        ),
        NestedField(
            43,
            "devices",
            ListType(
                element_id=44,
                element_type=StructType(
                    NestedField(45, "device_id", StringType(), required=False),
                    NestedField(46, "platform", StringType(), required=False),
                    NestedField(47, "trusted", BooleanType(), required=False),
                ),
                element_required=False,
            ),
            required=False,
        ),
        NestedField(
            48,
            "support_ticket",
            StructType(
                NestedField(49, "ticket_id", StringType(), required=False),
                NestedField(50, "channel", StringType(), required=False),
                NestedField(51, "opened_at", TimestampType(), required=False),
                NestedField(52, "resolution", StringType(), required=False),
            ),
            required=False,
        ),
    )


def _partition_spec() -> PartitionSpec:
    return PartitionSpec(
        PartitionField(
            source_id=2,
            field_id=1000,
            transform=IdentityTransform(),
            name="region",
        ),
        PartitionField(
            source_id=3,
            field_id=1001,
            transform=IdentityTransform(),
            name="market",
        ),
    )


def _market_for_row(row_id: int) -> str:
    rem = row_id % 5
    if rem in (0, 1):
        return "enterprise"
    if rem in (2, 3):
        return "consumer"
    return "partner"


def _credit_limit_for_row(row_id: int) -> Decimal:
    return Decimal(100_000 + (row_id % 50_000)).scaleb(-2)


def _account_balance_for_row(row_id: int) -> Decimal:
    cents = 20_000 + ((row_id * 13) % 30_000)
    signed_cents = -cents if row_id % 9 == 0 else cents
    return Decimal(signed_cents).scaleb(-2)


def _build_batch_table(batch_index: int, schema: pa.Schema) -> pa.Table:
    start = batch_index * ROWS_PER_BATCH
    end = start + ROWS_PER_BATCH
    base_created_at = datetime(2024, 1, 1, 12, 0, 0)
    base_signup_date = date(2024, 1, 1)
    columns: dict[str, list[object]] = {name: [] for name in schema.names}

    for row_id in range(start, end):
        created_at = base_created_at + timedelta(minutes=row_id)
        updated_at = created_at + timedelta(minutes=5)
        region = "us" if row_id % 2 == 0 else "eu"
        market = _market_for_row(row_id)
        vip = row_id % 5 == 0
        support_ticket = None
        if row_id % 4 == 0:
            support_ticket = {
                "ticket_id": f"TKT-{row_id:07d}",
                "channel": "chat" if row_id % 8 == 0 else "email",
                "opened_at": created_at + timedelta(hours=1),
                "resolution": None if row_id % 8 == 0 else "resolved",
            }

        columns["id"].append(row_id)
        columns["region"].append(region)
        columns["market"].append(market)
        columns["active"].append(row_id % 3 != 0)
        columns["vip"].append(vip)
        columns["risk_bucket"].append(row_id % 10)
        columns["score"].append(float((row_id * 17) % 1_000) + 0.25)
        columns["created_at"].append(created_at)
        columns["updated_at"].append(updated_at)
        columns["signup_date"].append(base_signup_date + timedelta(days=row_id % 365))
        columns["credit_limit"].append(_credit_limit_for_row(row_id))
        columns["account_balance"].append(_account_balance_for_row(row_id))
        columns["account_number"].append(f"ACCT-{row_id:012d}")
        columns["email"].append(f"user{row_id}@example.com")
        columns["phone"].append(f"+31-555-{row_id:06d}")
        columns["status"].append("vip" if vip else "standard")
        columns["notes"].append(f"batch-{batch_index}-note-{row_id % 37}")
        columns["nickname"].append(None if row_id % 7 == 0 else f"nick-{row_id % 97}")
        columns["tags"].append([market, f"batch-{batch_index}", "priority" if vip else "steady"])
        columns["attributes"].append(
            {
                "batch": str(batch_index),
                "locale": "nl_NL" if region == "eu" else "en_US",
                "segment": "gold" if vip else "silver",
            }
        )
        columns["user"].append(
            {
                "email": f"user{row_id}@example.com",
                "alternate_email": (None if row_id % 6 == 0 else f"alt{row_id}@example.net"),
                "address": {
                    "zip": 10_000 + (row_id % 90_000),
                    "city": f"city-{row_id % 64}",
                    "country": "NL" if region == "eu" else "US",
                },
                "preferences": [
                    {
                        "name": "email",
                        "enabled": row_id % 2 == 0,
                        "theme": "dark" if row_id % 4 == 0 else "light",
                    },
                    {
                        "name": "sms",
                        "enabled": row_id % 3 == 0,
                        "theme": "contrast" if row_id % 5 == 0 else "minimal",
                    },
                ],
            }
        )
        columns["account"].append(
            {
                "status": "review" if row_id % 4 == 0 else "open",
                "tier": ("gold" if vip else "silver" if row_id % 3 == 0 else "bronze"),
                "manager": {
                    "id": 500 + (row_id % 11),
                    "name": f"manager-{row_id % 11}",
                    "region": "emea" if region == "eu" else "amer",
                },
            }
        )
        columns["devices"].append(
            [
                {
                    "device_id": f"dev-{row_id}-ios",
                    "platform": "ios",
                    "trusted": row_id % 2 == 0,
                },
                {
                    "device_id": f"dev-{row_id}-web",
                    "platform": "web",
                    "trusted": True,
                },
            ]
        )
        columns["support_ticket"].append(support_ticket)

    return pa.table(
        {name: pa.array(columns[name], type=schema.field(name).type) for name in schema.names},
        schema=schema,
    )


def _append_tables() -> list[pa.Table]:
    schema = _arrow_schema()
    return [_build_batch_table(batch_index, schema) for batch_index in range(APPEND_BATCH_COUNT)]


def _count_matching_rows(predicate) -> int:
    return sum(1 for row_id in range(TOTAL_ROWS) if predicate(row_id))


def _expected_metadata() -> dict[str, object]:
    policy_row_count = _count_matching_rows(lambda row_id: row_id % 2 == 0 and row_id % 3 != 0)
    enterprise_row_count = _count_matching_rows(
        lambda row_id: _market_for_row(row_id) == "enterprise"
    )
    policy_and_enterprise_row_count = _count_matching_rows(
        lambda row_id: (
            row_id % 2 == 0 and row_id % 3 != 0 and _market_for_row(row_id) == "enterprise"
        )
    )
    high_score_row_count = _count_matching_rows(
        lambda row_id: ((row_id * 17) % 1_000) + 0.25 >= 900.0
    )
    return {
        "catalog": CATALOG_NAME,
        "target": TABLE_ID,
        "row_count": TOTAL_ROWS,
        "append_batch_count": APPEND_BATCH_COUNT,
        "rows_per_batch": ROWS_PER_BATCH,
        "supports_multiple_tickets": True,
        "partitioned": True,
        "partition_fields": ["region"],
        "mask_rule_types": SUPPORTED_MASK_TYPES,
        "policy_row_filter": POLICY_ROW_FILTER,
        "counts": {
            "policy_row_count": policy_row_count,
            "enterprise_row_count": enterprise_row_count,
            "policy_and_enterprise_row_count": policy_and_enterprise_row_count,
            "high_score_row_count": high_score_row_count,
        },
        "projection_columns": {
            "masked": [
                "id",
                "region",
                "market",
                "active",
                "account_number",
                "email",
                "status",
                "notes",
                "nickname",
                "user",
                "account",
                "devices",
                "support_ticket",
            ],
            "nested": [
                "user.email",
                "user.address.zip",
                "user.preferences.theme",
                "account.manager.region",
                "support_ticket.ticket_id",
            ],
        },
        "sample_values": {
            "region_even": "us",
            "region_odd": "eu",
            "masked_email": "u***@example.com",
            "redacted_note": "[redacted-note]",
            "default_status": "partner-visible",
            "masked_preference_theme": "[hidden-theme]",
            "null_nickname": None,
            "hash_hex_length": 64,
        },
        "sample_us_even_ids": [0, 4, 8],
        "masked_zip_hash_length": 64,
        "filter_examples": {
            "pushdown": "market = 'enterprise'",
            "residual": "score >= 900.0",
            "partition": "region = 'us'",
        },
    }


def _provision_control_plane(output_dir: Path, table_id: str) -> tuple[str, str, str]:
    database_url = f"sqlite+pysqlite:///{output_dir / 'control-plane.db'}"
    engine = create_engine_from_url(database_url)
    Base.metadata.create_all(engine)

    with session_factory(engine)() as session:
        service = ProvisioningService(session)
        tenant = service.create_tenant(slug="spark-fixture", display_name="Spark Fixture")
        cell = service.create_cell(name="spark-local", region="local")
        tenant_id = UUID(tenant["id"])
        cell_id = UUID(cell["id"])
        service.assign_tenant(
            cell_id=cell_id,
            tenant_id=tenant_id,
            shard_key="spark-fixture",
        )
        service.upsert_runtime_settings(
            cell_id=cell_id,
            ttl=900,
            max_tickets=16,
            path_rules=[],
        )
        service.upsert_catalog(
            cell_id=cell_id,
            tenant_id=tenant_id,
            name=CATALOG_NAME,
            module="dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog",
            options={
                "type": "sql",
                "uri": f"sqlite:///{output_dir / f'{CATALOG_NAME}.db'}",
                "warehouse": str(output_dir / WAREHOUSE_NAME),
            },
        )
        asset = service.upsert_asset(
            cell_id=cell_id,
            tenant_id=tenant_id,
            catalog=CATALOG_NAME,
            target=table_id,
            backend="iceberg",
            table_identifier=table_id,
            options={},
        )
        service.replace_policy_rules(
            asset_id=UUID(asset["id"]),
            rules=[
                {
                    "ordinal": 10,
                    "principals": ["spark_user"],
                    "columns": ALLOWED_COLUMNS,
                    "effect": "allow",
                    "when": {},
                    "masks": MASKS,
                    "row_filter": POLICY_ROW_FILTER,
                },
                {
                    "ordinal": 20,
                    "principals": ["spark_ops"],
                    "columns": [
                        "id",
                        "region",
                        "market",
                        "vip",
                        "support_ticket",
                        "account",
                        "devices",
                    ],
                    "effect": "allow",
                    "when": {},
                    "masks": {},
                    "row_filter": "market = 'enterprise'",
                },
            ],
        )
        service.replace_auth_providers(
            cell_id=cell_id,
            providers=[
                {
                    "ordinal": 1,
                    "module": (
                        "dal_obscura.infrastructure.adapters.identity_default."
                        "DefaultIdentityAdapter"
                    ),
                    "args": {"jwt_secret": {"key": "DAL_OBSCURA_JWT_SECRET"}},
                    "enabled": True,
                }
            ],
        )
        publication = service.create_publication(cell_id=cell_id)
        service.activate_publication(
            cell_id=cell_id,
            publication_id=UUID(str(publication["publication_id"])),
        )
        session.commit()

    return database_url, cell["id"], tenant["id"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--port", required=True, type=int)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    table_id = create_iceberg_table(
        output_dir,
        CATALOG_NAME,
        WAREHOUSE_NAME,
        identifier=TABLE_ID,
        arrow_schema=_iceberg_schema(),
        append_tables=_append_tables(),
        partition_spec=_partition_spec(),
    )
    database_url, cell_id, tenant_id = _provision_control_plane(output_dir, table_id)

    user_token = jwt.encode(
        {"sub": "spark_user", "attributes": {"tenant_id": tenant_id}},
        JWT_SECRET,
        algorithm="HS256",
    )

    print(
        json.dumps(
            {
                "uri": f"grpc+tcp://localhost:{args.port}",
                "catalog": CATALOG_NAME,
                "target": table_id,
                "database_url": database_url,
                "cell_id": cell_id,
                "jwt_secret": JWT_SECRET,
                "ticket_secret": TICKET_SECRET,
                "user_token": user_token,
                "expected": _expected_metadata(),
            }
        )
    )


if __name__ == "__main__":
    main()
