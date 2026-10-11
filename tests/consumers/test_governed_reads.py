"""One consumer contract, qualified independently against each executable backend."""

import os

import pyarrow as pa
import pytest

from dal_obscura.connectors.python_sdk import DalObscuraClient, DuckDBDalObscuraReader
from tests.support.arrow import metadata_schema
from tests.support.consumer_backends import consumer_server
from tests.support.flight import make_jwt, running_flight_client

pytestmark = pytest.mark.integration


@pytest.fixture(
    params=["memory", "iceberg.catalog", "manifest.parquet", "iceberg.rest", "delta.directory"]
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


@pytest.mark.parametrize("backend", ["manifest.parquet", "iceberg.catalog", "delta.directory"])
@pytest.mark.parametrize(
    "name", ["x" * 257, "界" * 1024, "x" * 16384], ids=["ascii-257", "unicode-1024", "ascii-16384"]
)
def test_long_field_names_survive_real_backend_and_authenticated_flight(tmp_path, backend, name):
    from dal_obscura.policy.paths import FieldPath, FieldSegment
    from dal_obscura.sources.task_codec import SourceTaskCodec
    from tests.support.consumer_backends import delta_registry, manifest_registry, sql_registry
    from tests.support.flight import InMemoryPolicyAuthorizer, build_flight_service
    from tests.support.policy import allow_rule

    table = pa.table({name: ["private", None], "region": ["us", "us"]})
    path = FieldPath((FieldSegment(name),)).to_human()
    factories = {
        "manifest.parquet": manifest_registry,
        "iceberg.catalog": sql_registry,
        "delta.directory": delta_registry,
    }
    policy = [
        allow_rule(
            columns=[path, "region"],
            masks={path: {"type": "redact", "value": "[hidden]"}},
            row_filter="region = 'us'",
        )
    ]
    with factories[backend](tmp_path, table) as (registry, plugins):
        server = build_flight_service(
            catalog_registry=registry,
            authorizer=InMemoryPolicyAuthorizer(
                catalog="analytics", target="default.users", rules=policy
            ),
            max_tickets=2,
            task_codec=SourceTaskCodec(plugins),
        )
        with running_flight_client(server) as flight_client:
            sdk = DalObscuraClient.from_flight_client(flight_client, auth_token=make_jwt("user1"))
            result = sdk.read_table(catalog="analytics", target="default.users", columns=[path])
    assert result.schema.equals(pa.schema([pa.field(name, pa.string())]), check_metadata=True)
    assert result.to_pylist() == [{name: "[hidden]"}, {name: None}]
