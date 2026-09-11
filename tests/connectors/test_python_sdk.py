import polars as pl
import pytest

from dal_obscura.connectors.python_sdk import DalObscuraClient, DuckDBDalObscuraReader
from tests.support.arrow import (
    id_email_region_batch,
    id_email_region_schema,
    metadata_batch,
    metadata_schema,
)
from tests.support.flight import (
    StubTableFormat,
    build_flight_service,
    make_jwt,
    running_flight_client,
)
from tests.support.policy import allow_rule


class _StreamingReader:
    def __init__(self, batch):
        self._batch = batch
        self._read = False
        self.closed = False

    def read_all(self):
        raise AssertionError("read_batches must not materialize the full result")

    def read_chunk(self):
        if self._read:
            raise StopIteration
        self._read = True
        return type("Chunk", (), {"data": self._batch})()

    def close(self):
        self.closed = True


class _StreamingFlightClient:
    def __init__(self, batch):
        self._batch = batch
        self.reader = None
        self.plan_options = []
        self.stream_options = []

    def get_flight_info(self, descriptor, *, options):
        del descriptor
        self.plan_options.append(options)
        endpoint = type("Endpoint", (), {"ticket": object()})()
        return type("Info", (), {"endpoints": [endpoint], "schema": self._batch.schema})()

    def do_get(self, ticket, *, options):
        del ticket
        self.stream_options.append(options)
        self.reader = _StreamingReader(self._batch)
        return self.reader


def _policy_rules() -> list[dict[str, object]]:
    return [
        allow_rule(
            ["id", "email", "region"],
            masks={"email": {"type": "redact", "value": "[hidden]"}},
        )
    ]


def test_python_sdk_read_batches_does_not_materialize_flight_stream():
    batch = id_email_region_batch([1], ["a@example.com"], ["us"])
    sdk = DalObscuraClient.from_flight_client(
        _StreamingFlightClient(batch),
        auth_token="token",
    )

    result = list(sdk.read_batches(catalog="analytics", target="users", columns=["id"]))

    assert result == [batch]


def test_python_sdk_batch_stream_closes_reader_when_consumer_stops_early():
    batch = id_email_region_batch([1], ["a@example.com"], ["us"])
    client = _StreamingFlightClient(batch)
    sdk = DalObscuraClient.from_flight_client(client, auth_token="token")

    with sdk.read_batches(catalog="analytics", target="users", columns=["id"]) as stream:
        assert next(stream) == batch

    assert client.reader is not None
    assert client.reader.closed is True


def test_python_sdk_resolves_refreshed_token_for_plan_and_ticket_stream():
    batch = id_email_region_batch([1], ["a@example.com"], ["us"])
    client = _StreamingFlightClient(batch)
    tokens = iter(["plan-token", "stream-token"])
    sdk = DalObscuraClient.from_flight_client(client, auth_token=lambda: next(tokens))

    assert list(sdk.read_batches(catalog="analytics", target="users", columns=["id"])) == [batch]

    assert client.plan_options[0].headers == [(b"authorization", b"Bearer plan-token")]
    assert client.stream_options[0].headers == [(b"authorization", b"Bearer stream-token")]


def test_python_sdk_rejects_empty_token_from_refresh_provider():
    sdk = DalObscuraClient.from_flight_client(
        _StreamingFlightClient(id_email_region_batch([1], ["a@example.com"], ["us"])),
        auth_token=lambda: "",
    )

    with pytest.raises(ValueError, match="empty token"):
        sdk.plan(catalog="analytics", target="users", columns=["id"])


def test_python_sdk_reads_authorized_arrow_table():
    schema = id_email_region_schema()
    batch = id_email_region_batch(
        [1, 2, 3],
        ["a@example.com", "b@example.com", "c@example.com"],
        ["us", "eu", "us"],
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


def test_python_sdk_materializes_governed_result_as_polars_dataframe():
    schema = id_email_region_schema()
    batch = id_email_region_batch(
        [1, 2, 3],
        ["a@example.com", "b@example.com", "c@example.com"],
        ["us", "eu", "us"],
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
        result = sdk.read_polars(
            catalog="analytics",
            target="test.table",
            columns=["id", "email"],
            row_filter="\"region\" = 'us'",
        )

    assert isinstance(result, pl.DataFrame)
    assert result.schema == {"id": pl.Int64, "email": pl.String}
    assert result.to_dict(as_series=False) == {"id": [1, 3], "email": ["[hidden]", "[hidden]"]}


def test_python_sdk_preserves_nested_masked_arrow_values_in_polars():
    schema = metadata_schema()
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(metadata_batch(),),
    )
    server = build_flight_service(
        table_format=table_format,
        policy_rules=[
            allow_rule(
                ["id", "metadata"],
                masks={"metadata.preferences.theme": {"type": "redact", "value": "[hidden]"}},
            )
        ],
    )

    with running_flight_client(server) as flight_client:
        sdk = DalObscuraClient.from_flight_client(
            flight_client,
            auth_token=make_jwt("user1"),
        )
        result = sdk.read_polars(
            catalog="analytics",
            target="test.table",
            columns=["id", "metadata"],
        )

    assert result.schema["metadata"] == pl.Struct
    assert result.to_dict(as_series=False)["metadata"] == [
        {
            "preferences": [
                {"name": "web", "theme": "[hidden]"},
                {"name": "mobile", "theme": "[hidden]"},
            ]
        }
    ]


def test_duckdb_reader_exposes_sdk_results_as_relation():
    schema = id_email_region_schema()
    batch = id_email_region_batch(
        [1, 2, 3],
        ["a@example.com", "b@example.com", "c@example.com"],
        ["us", "eu", "us"],
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


def test_duckdb_reader_does_not_call_materializing_table_api(monkeypatch):
    schema = id_email_region_schema()
    batch = id_email_region_batch([1], ["a@example.com"], ["us"])
    sdk = DalObscuraClient.from_flight_client(
        _StreamingFlightClient(batch),
        auth_token="token",
    )
    monkeypatch.setattr(sdk, "fetch_schema", lambda **_kwargs: schema)
    monkeypatch.setattr(
        sdk,
        "read_table",
        lambda **_kwargs: pytest.fail("DuckDB adapter materialized the result"),
    )

    relation = DuckDBDalObscuraReader(sdk).relation(
        catalog="analytics", target="users", columns=["id", "email", "region"]
    )

    assert relation.aggregate("count(*) AS rows").fetchone() == (1,)
