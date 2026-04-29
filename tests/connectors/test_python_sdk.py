import pyarrow as pa

from dal_obscura.connectors.python_sdk import DalObscuraClient, DuckDBDalObscuraReader
from tests.support.flight import (
    StubTableFormat,
    build_flight_service,
    make_jwt,
    running_flight_client,
)


def _policy_rules() -> list[dict[str, object]]:
    return [
        {
            "principals": ["user1"],
            "columns": ["id", "email", "region"],
            "masks": {"email": {"type": "redact", "value": "[hidden]"}},
            "effect": "allow",
        }
    ]


def test_python_sdk_reads_authorized_arrow_table():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("email", pa.string()),
            pa.field("region", pa.string()),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(["a@example.com", "b@example.com", "c@example.com"], type=pa.string()),
            pa.array(["us", "eu", "us"], type=pa.string()),
        ],
        schema=schema,
    )
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )
    server = build_flight_service(table_format=table_format, policy_rules=_policy_rules())

    with running_flight_client(server) as flight_client:
        sdk = DalObscuraClient.from_flight_client(
            flight_client,
            auth_token=make_jwt("user1"),
        )

        result = sdk.read_table(
            catalog="analytics",
            target="test.table",
            columns=["id", "email"],
            row_filter="\"region\" = 'us'",
        )

    assert result.schema.names == ["id", "email"]
    assert result.column("id").to_pylist() == [1, 3]
    assert result.column("email").to_pylist() == ["[hidden]", "[hidden]"]


def test_duckdb_reader_exposes_sdk_results_as_relation():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(["us", "eu", "us"], type=pa.string()),
        ],
        schema=schema,
    )
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )
    server = build_flight_service(table_format=table_format, policy_rules=_policy_rules())

    with running_flight_client(server) as flight_client:
        sdk = DalObscuraClient.from_flight_client(
            flight_client,
            auth_token=make_jwt("user1"),
        )
        relation = DuckDBDalObscuraReader(sdk).relation(
            catalog="analytics",
            target="test.table",
            columns=["id", "region"],
            row_filter="\"region\" = 'us'",
        )

        result = relation.aggregate("sum(id) AS id_sum").fetchone()

    assert result == (4,)
