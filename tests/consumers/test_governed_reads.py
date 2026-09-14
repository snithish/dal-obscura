"""Consumer qualification for the Python and DuckDB read surfaces."""

from __future__ import annotations

import os

import pyarrow as pa
import pytest

from dal_obscura.connectors.python_sdk import DalObscuraClient, DuckDBDalObscuraReader
from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    DynamicCatalogRegistry,
    ServiceConfig,
)
from tests.support.arrow import metadata_schema
from tests.support.flight import (
    InMemoryPolicyAuthorizer,
    StubTableFormat,
    build_flight_service,
    make_jwt,
    running_flight_client,
)
from tests.support.iceberg import create_iceberg_table, iceberg_sql_catalog_options
from tests.support.policy import allow_rule

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def require_consumer_lane() -> None:
    if os.getenv("DAL_OBSCURA_RUN_CONSUMER_TESTS") != "1":
        pytest.skip("set DAL_OBSCURA_RUN_CONSUMER_TESTS=1 for the loopback consumer lane")


def test_python_and_duckdb_consumers_receive_identical_nested_governed_data() -> None:
    schema = metadata_schema()
    batch = pa.RecordBatch.from_pylist(
        [
            {
                "id": 1,
                "metadata": {
                    "preferences": [
                        {"name": "web", "theme": "dark"},
                        {"name": "mobile", "theme": "light"},
                    ]
                },
            },
            {
                "id": 2,
                "metadata": {"preferences": [{"name": "desktop", "theme": "dark"}]},
            },
        ],
        schema=schema,
    )
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
        with DuckDBDalObscuraReader(sdk) as reader:
            duckdb_row_count = (
                reader.relation(
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


def test_python_and_duckdb_consumers_read_real_sql_iceberg_nested_data(tmp_path) -> None:
    """Exercise the consumer contract against the executable Iceberg adapter.

    The regular consumer test intentionally uses a tiny in-memory format for
    fast feedback.  This opt-in lane uses the same nested fixture with a real
    SQL catalog, metadata files and parquet data so a passing result covers
    catalog resolution, file planning, Arrow execution and both consumers.
    """

    schema = metadata_schema()
    table = pa.table(
        {
            "id": [1, 2],
            "metadata": [
                {
                    "preferences": [
                        {"name": "web", "theme": "dark"},
                        {"name": "mobile", "theme": "light"},
                    ]
                },
                {"preferences": [{"name": "desktop", "theme": "dark"}]},
            ],
        },
        schema=schema,
    )
    target = create_iceberg_table(
        tmp_path,
        "analytics",
        "warehouse",
        identifier="default.consumer_nested",
        arrow_schema=schema,
        append_tables=[table],
    )
    registry = DynamicCatalogRegistry(
        ServiceConfig(
            catalogs={
                "analytics": CatalogConfig(
                    name="analytics",
                    type="iceberg",
                    options=iceberg_sql_catalog_options(tmp_path, "analytics", "warehouse"),
                )
            }
        )
    )
    policy = [
        allow_rule(
            ["id", "metadata"],
            masks={"metadata.preferences.theme": {"type": "redact", "value": "[hidden]"}},
        )
    ]
    server = build_flight_service(
        catalog_registry=registry,
        authorizer=InMemoryPolicyAuthorizer(catalog="analytics", target=target, rules=policy),
    )

    try:
        with running_flight_client(server) as flight_client:
            sdk = DalObscuraClient.from_flight_client(
                flight_client,
                auth_token=make_jwt("user1"),
            )
            arrow_table = sdk.read_table(
                catalog="analytics",
                target=target,
                columns=["id", "metadata"],
            )
            with DuckDBDalObscuraReader(sdk) as reader:
                duckdb_row_count = (
                    reader.relation(
                        catalog="analytics",
                        target=target,
                        columns=["id", "metadata"],
                    )
                    .aggregate("count(*) AS rows")
                    .fetchone()
                )
    finally:
        registry.close()

    assert arrow_table.schema == schema
    assert arrow_table.num_rows == 2
    assert arrow_table.column("metadata").to_pylist()[0]["preferences"][0]["theme"] == "[hidden]"
    assert duckdb_row_count == (2,)
