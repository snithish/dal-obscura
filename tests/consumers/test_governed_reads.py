"""Consumer qualification for the Python and DuckDB read surfaces."""

from __future__ import annotations

import os

import pytest

from dal_obscura.connectors.python_sdk import DalObscuraClient, DuckDBDalObscuraReader
from tests.support.arrow import metadata_batch, metadata_schema
from tests.support.flight import (
    StubTableFormat,
    build_flight_service,
    make_jwt,
    running_flight_client,
)
from tests.support.policy import allow_rule

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def require_consumer_lane() -> None:
    if os.getenv("DAL_OBSCURA_RUN_CONSUMER_TESTS") != "1":
        pytest.skip("set DAL_OBSCURA_RUN_CONSUMER_TESTS=1 for the loopback consumer lane")


def test_python_and_duckdb_consumers_receive_identical_nested_governed_data() -> None:
    schema = metadata_schema()
    batch = metadata_batch()
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="consumer.nested",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )
    policy = [
        allow_rule(
            ["id", "metadata"],
            masks={"metadata.preferences.theme": {"type": "redact", "value": "[hidden]"}},
        )
    ]
    server = build_flight_service(table_format=table_format, policy_rules=policy)

    with running_flight_client(server) as flight_client:
        sdk = DalObscuraClient.from_flight_client(
            flight_client,
            auth_token=make_jwt("user1"),
        )
        arrow_table = sdk.read_table(
            catalog="analytics",
            target="consumer.nested",
            columns=["id", "metadata"],
        )
        duckdb_row_count = (
            DuckDBDalObscuraReader(sdk)
            .relation(
                catalog="analytics",
                target="consumer.nested",
                columns=["id", "metadata"],
            )
            .aggregate("count(*) AS rows")
            .fetchone()
        )

    assert arrow_table.schema == schema
    assert arrow_table.num_rows == 2
    assert arrow_table.column("metadata").to_pylist()[0]["preferences"][0]["theme"] == "[hidden]"
    assert duckdb_row_count == (2,)
