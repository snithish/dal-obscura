"""One consumer contract, qualified independently against each executable backend."""

import os

import pytest

from dal_obscura.connectors.python_sdk import DalObscuraClient, DuckDBDalObscuraReader
from tests.support.arrow import metadata_schema
from tests.support.consumer_backends import consumer_server
from tests.support.flight import make_jwt, running_flight_client

pytestmark = pytest.mark.integration


@pytest.fixture(
    params=["memory", "iceberg.sql", "manifest.parquet", "iceberg.rest", "delta.directory"]
)
def governed_server(request, tmp_path):
    if os.getenv("DAL_OBSCURA_RUN_CONSUMER_TESTS") != "1":
        pytest.skip("set DAL_OBSCURA_RUN_CONSUMER_TESTS=1 for the loopback consumer lane")
    with consumer_server(request.param, tmp_path) as server:
        yield server


def test_python_and_duckdb_receive_identical_masked_nested_rows(governed_server):
    expected = [
        {
            "id": 1,
            "metadata": {
                "preferences": [
                    {"name": "web", "theme": "[hidden]"},
                    {"name": "mobile", "theme": "[hidden]"},
                ]
            },
        },
        {"id": 2, "metadata": {"preferences": [{"name": "desktop", "theme": "[hidden]"}]}},
    ]
    with running_flight_client(governed_server) as flight_client:
        sdk = DalObscuraClient.from_flight_client(flight_client, auth_token=make_jwt("user1"))

        arrow_table = sdk.read_table(
            catalog="analytics", target="default.users", columns=["id", "metadata"]
        )
        with DuckDBDalObscuraReader(sdk) as reader:
            duckdb_rows = (
                reader.relation(
                    catalog="analytics", target="default.users", columns=["id", "metadata"]
                )
                .order("id")
                .fetchall()
            )

    assert arrow_table.schema == metadata_schema()
    assert arrow_table.to_pylist() == expected
    assert duckdb_rows == [(1, expected[0]["metadata"]), (2, expected[1]["metadata"])]
