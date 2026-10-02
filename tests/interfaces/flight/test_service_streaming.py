import pyarrow as pa
import pyarrow.flight as flight
import pytest

from tests.support.arrow import id_region_batch, id_region_schema, metadata_batch, metadata_schema
from tests.support.flight import (
    InMemoryPolicyAuthorizer,
    StubTableFormat,
    authorization_header,
    build_flight_service,
    command_descriptor,
    flight_call_options,
    flight_request,
    make_jwt,
    running_flight_client,
)
from tests.support.flight_context import DummyContext
from tests.support.policy import allow_rule

pytestmark = pytest.mark.socket


@pytest.mark.parametrize("method", ["get_schema", "get_flight_info"])
def test_flight_invalid_descriptor_is_customer_input_error(method):
    server = build_flight_service(
        table_format=StubTableFormat(
            catalog_name="analytics",
            table_name="test.table",
            format="stub_format",
            schema=id_region_schema(),
            batches=(),
        )
    )
    with (
        running_flight_client(server) as client,
        pytest.raises(pa.ArrowInvalid, match="Invalid request"),
    ):
        getattr(client, method)(flight.FlightDescriptor.for_command(b"not-protobuf"))


def test_flight_non_utf8_ticket_is_customer_input_error():
    server = build_flight_service(
        table_format=StubTableFormat(
            catalog_name="analytics",
            table_name="test.table",
            format="stub_format",
            schema=id_region_schema(),
            batches=(),
        )
    )
    with (
        running_flight_client(server) as client,
        pytest.raises(pa.ArrowInvalid, match="Invalid ticket"),
    ):
        client.do_get(flight.Ticket(b"\xff"))


def test_flight_unknown_action_is_customer_input_error():
    server = build_flight_service(
        table_format=StubTableFormat(
            catalog_name="analytics",
            table_name="test.table",
            format="stub_format",
            schema=id_region_schema(),
            batches=(),
        )
    )
    with (
        running_flight_client(server) as client,
        pytest.raises(pa.ArrowInvalid, match="Unsupported action"),
    ):
        list(client.do_action(flight.Action("unsupported", b"")))


def test_flight_info_schema_matches_mask_output_types():
    cases = [
        (
            "hashed_id",
            pa.field("hashed_id", pa.int64()),
            pa.array([1234], type=pa.int64()),
            {"type": "hash"},
            pa.string(),
        ),
        (
            "redacted_score",
            pa.field("redacted_score", pa.int64()),
            pa.array([87], type=pa.int64()),
            {"type": "redact", "value": "***"},
            pa.string(),
        ),
        (
            "email_address",
            pa.field("email_address", pa.string()),
            pa.array(["alpha@example.com"], type=pa.string()),
            {"type": "email"},
            pa.string(),
        ),
        (
            "account_id",
            pa.field("account_id", pa.int64()),
            pa.array([123456], type=pa.int64()),
            {"type": "keep_last", "value": 2},
            pa.string(),
        ),
        (
            "default_text",
            pa.field("default_text", pa.int64()),
            pa.array([42], type=pa.int64()),
            {"type": "default", "value": "replacement"},
            pa.string(),
        ),
        (
            "default_flag",
            pa.field("default_flag", pa.string()),
            pa.array(["no"], type=pa.string()),
            {"type": "default", "value": True},
            pa.bool_(),
        ),
        (
            "default_count",
            pa.field("default_count", pa.string()),
            pa.array(["missing"], type=pa.string()),
            {"type": "default", "value": 7},
            pa.int32(),
        ),
        (
            "default_ratio",
            pa.field("default_ratio", pa.string()),
            pa.array(["missing"], type=pa.string()),
            {"type": "default", "value": 1.5},
            pa.decimal128(2, 1),
        ),
        (
            "suppressed_id",
            pa.field("suppressed_id", pa.int64()),
            pa.array([99], type=pa.int64()),
            {"type": "null"},
            pa.int64(),
        ),
    ]
    schema = pa.schema([field for _, field, _, _, _ in cases])
    batch = pa.record_batch([value for _, _, value, _, _ in cases], schema=schema)
    columns = [column for column, _, _, _, _ in cases]
    masks: dict[str, object] = {column: mask for column, _, _, mask, _ in cases}
    server = build_flight_service(
        table_format=StubTableFormat(
            catalog_name="analytics",
            table_name="test.table",
            format="stub_format",
            schema=schema,
            batches=(batch,),
        ),
        policy_rules=[allow_rule(columns, masks=masks)],
    )
    with running_flight_client(server) as client:
        info, table = flight_request(
            client,
            {"catalog": "analytics", "target": "test.table", "columns": columns},
        )
    assert table.schema.equals(info.schema)
    for column, _, _, _, expected_type in cases:
        assert info.schema.field(column).type == expected_type, column


def test_flight_service_health_action_returns_ok():
    server = build_flight_service(
        table_format=StubTableFormat(
            catalog_name="analytics",
            table_name="test.table",
            format="stub_format",
            schema=id_region_schema(),
            batches=(id_region_batch([1], ["us"]),),
        ),
        policy_rules=[allow_rule(["id", "region"])],
    )
    assert any(action.type == "healthz" for action in server.list_actions(None))
    action = flight.Action("healthz", b"")

    results = list(server.do_action(None, action))

    assert len(results) == 1
    assert results[0].body.to_pybytes() == b'{"status":"ok","service":"data-plane"}'


def test_get_schema_returns_masked_authorized_schema(tmp_path):
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("email", pa.string()),
            pa.field("region", pa.string()),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1, 2], type=pa.int64()),
            pa.array(["alpha@example.com", "beta@example.com"], type=pa.string()),
            pa.array(["us", "eu"], type=pa.string()),
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

    del tmp_path
    server = build_flight_service(
        table_format=table_format,
        policy_rules=[
            allow_rule(
                ["id", "email"],
                masks={"email": {"type": "redact", "value": "[hidden]"}},
            )
        ],
    )
    with running_flight_client(server) as client:
        descriptor = command_descriptor(
            {
                "catalog": "analytics",
                "target": "test.table",
                "columns": ["*"],
            }
        )
        options = flight_call_options("user1")

        result = client.get_schema(descriptor, options=options)

        assert result.schema.names == ["id", "email", "region"]
        assert result.schema.field("region").nullable
        assert result.schema.field("email").type == pa.string()


def test_streaming_contract_emits_multiple_batches(tmp_path, monkeypatch):
    del tmp_path
    monkeypatch.setattr(
        "dal_obscura.read.transform._DUCKDB_ARROW_OUTPUT_BATCH_SIZE",
        2,
    )
    schema = id_region_schema()
    batch1 = id_region_batch([1, 2], ["us", "eu"])
    batch2 = id_region_batch([3, 4], ["us", "us"])
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch1, batch2),
    )

    server = build_flight_service(
        table_format=table_format,
        policy_rules=[allow_rule(["id", "region"])],
    )
    with running_flight_client(server) as client:
        descriptor = command_descriptor(
            {
                "catalog": "analytics",
                "target": "test.table",
                "columns": ["id", "region"],
            }
        )
        options = flight_call_options("user1")
        info = client.get_flight_info(descriptor, options=options)
        reader = client.do_get(info.endpoints[0].ticket, options=options)
        result = reader.read_all()

        assert result.num_rows == 4
        assert result.column("id").num_chunks >= 2


def test_flight_streaming_supports_nested_projection_with_policy_and_requested_row_filters(
    tmp_path,
):
    user_type = pa.struct(
        [
            pa.field("email", pa.string()),
            pa.field(
                "address",
                pa.struct([pa.field("zip", pa.int64())]),
            ),
        ]
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("region", pa.string()),
            pa.field("active", pa.bool_()),
            pa.field("user", user_type),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3, 4], type=pa.int64()),
            pa.array(["us", "us", "eu", "us"], type=pa.string()),
            pa.array([True, False, True, True], type=pa.bool_()),
            pa.array(
                [
                    {"email": "alpha@example.com", "address": {"zip": 1011}},
                    {"email": "beta@example.com", "address": {"zip": 2022}},
                    {"email": "gamma@example.com", "address": {"zip": 3033}},
                    {"email": "delta@example.com", "address": {"zip": 4044}},
                ],
                type=user_type,
            ),
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

    del tmp_path
    server = build_flight_service(
        table_format=table_format,
        policy_rules=[
            allow_rule(
                ["id", "region", "user.address.zip"],
                principals=["group:analyst"],
                masks={"user.address.zip": {"type": "hash"}},
            ),
            allow_rule(
                ["user.email"],
                masks={"user.email": {"type": "redact", "value": "[hidden]"}},
                row_filter="active = true",
            ),
        ],
    )
    with running_flight_client(server) as client:
        descriptor = command_descriptor(
            {
                "catalog": "analytics",
                "target": "test.table",
                "columns": ["id", "user.address.zip", "user.email"],
                "row_filter": "region = 'us'",
            }
        )
        options = flight_call_options("user1", groups=["analyst"])

        info = client.get_flight_info(descriptor, options=options)
        table = client.do_get(info.endpoints[0].ticket, options=options).read_all()

        assert info.schema.names == ["id", "user"]
        user_field = info.schema.field("user")
        assert user_field.type.names == ["address", "email"]
        assert user_field.type.field("address").type.names == ["zip"]
        assert user_field.type.field("address").type.field("zip").type == pa.string()
        assert user_field.type.field("email").type == pa.string()
        assert table.num_rows == 2
        assert table.column("id").to_pylist() == [1, 4]
        users = table.column("user").to_pylist()
        assert [user["email"] for user in users] == ["[hidden]", "[hidden]"]
        assert all(len(user["address"]["zip"]) == 64 for user in users)


def test_flight_parent_projection_nulls_ungranted_nested_siblings(tmp_path):
    profile_type = pa.struct([pa.field("name", pa.string()), pa.field("ssn", pa.string())])
    schema = pa.schema([pa.field("profile", profile_type)])
    batch = pa.record_batch(
        [pa.array([{"name": "Ada", "ssn": "123-45-6789"}], type=profile_type)],
        schema=schema,
    )
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    del tmp_path
    server = build_flight_service(
        table_format=table_format,
        policy_rules=[allow_rule(["profile.name"])],
    )
    with running_flight_client(server) as client:
        descriptor = command_descriptor(
            {"catalog": "analytics", "target": "test.table", "columns": ["profile"]}
        )
        options = flight_call_options("user1")
        client_schema = client.get_schema(descriptor, options=options).schema
        info = client.get_flight_info(descriptor, options=options)
        table = client.do_get(info.endpoints[0].ticket, options=options).read_all()

    assert info.schema.field("profile").type.names == ["name", "ssn"]
    assert table.column("profile").to_pylist() == [{"name": "Ada", "ssn": None}]
    assert client_schema == info.schema


def test_flight_streaming_masks_list_of_struct_fields(tmp_path):
    schema = metadata_schema()
    batch = metadata_batch()
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    del tmp_path
    server = build_flight_service(
        table_format=table_format,
        policy_rules=[
            allow_rule(
                ["id", "metadata"],
                masks={
                    "metadata.preferences.$element.theme": {"type": "redact", "value": "[hidden]"}
                },
            )
        ],
    )
    with running_flight_client(server) as client:
        descriptor = command_descriptor(
            {
                "catalog": "analytics",
                "target": "test.table",
                "columns": ["id", "metadata"],
            }
        )
        options = flight_call_options("user1")

        info = client.get_flight_info(descriptor, options=options)
        table = client.do_get(info.endpoints[0].ticket, options=options).read_all()

        metadata_field = info.schema.field("metadata")
        preferences_field = metadata_field.type.field("preferences")
        assert preferences_field.type.value_field.type.field("theme").type == pa.string()
        preferences = table.column("metadata").to_pylist()[0]["preferences"]
        assert [item["theme"] for item in preferences] == ["[hidden]", "[hidden]"]


def test_flight_schema_matches_duckdb_list_output(tmp_path):
    del tmp_path
    schema = metadata_schema(large_list=True)
    batch = metadata_batch(large_list=True)
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    server = build_flight_service(
        table_format=table_format,
        policy_rules=[allow_rule(["id", "metadata"])],
    )
    descriptor = command_descriptor(
        {
            "catalog": "analytics",
            "target": "test.table",
            "columns": ["id", "metadata"],
        }
    )

    with running_flight_client(server) as client:
        options = flight_call_options("user1")
        schema_result = client.get_schema(descriptor, options=options)
        info = client.get_flight_info(descriptor, options=options)
        table = client.do_get(info.endpoints[0].ticket, options=options).read_all()

    schema_preferences = schema_result.schema.field("metadata").type.field("preferences")
    info_preferences = info.schema.field("metadata").type.field("preferences")

    assert pa.types.is_list(schema_preferences.type)
    assert pa.types.is_list(info_preferences.type)
    assert table.column("metadata").type.field("preferences").type == schema_preferences.type
    assert table.column("metadata").to_pylist()[0]["preferences"][0]["theme"] == "dark"


def test_do_get_rejects_principal_mismatch(tmp_path):
    del tmp_path
    schema = id_region_schema()
    batch = id_region_batch([1], ["us"])
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    server = build_flight_service(
        table_format=table_format,
        policy_rules=[allow_rule(["id", "region"])],
    )
    descriptor = command_descriptor(
        {
            "catalog": "analytics",
            "target": "test.table",
            "columns": ["id", "region"],
        }
    )
    plan_context = DummyContext(headers=[authorization_header("user1")])
    info = server.get_flight_info(plan_context, descriptor)

    do_get_context = DummyContext(headers=[authorization_header("user2")])
    with pytest.raises(flight.FlightUnauthorizedError):
        server.do_get(do_get_context, info.endpoints[0].ticket)


def test_descriptor_authorization_field_is_not_accepted(tmp_path):
    del tmp_path
    schema = id_region_schema()
    batch = id_region_batch([1], ["us"])
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    server = build_flight_service(
        table_format=table_format,
        policy_rules=[allow_rule(["id", "region"])],
    )
    descriptor = command_descriptor(
        {
            "catalog": "analytics",
            "target": "test.table",
            "columns": ["id", "region"],
            "authorization": f"Bearer {make_jwt('user1')}",
        }
    )

    with pytest.raises(flight.FlightUnauthorizedError):
        server.get_flight_info(DummyContext(headers=[]), descriptor)


def test_do_get_keeps_captured_policy_after_policy_change(tmp_path):
    del tmp_path
    schema = id_region_schema()
    batch = id_region_batch([1, 2], ["us", "eu"])
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    authorizer = InMemoryPolicyAuthorizer(
        catalog="analytics",
        target="table_a",
        rules=[allow_rule(["id", "region"])],
    )
    server = build_flight_service(
        table_format=table_format,
        authorizer=authorizer,
        max_ticket_exchanges=2,
    )
    descriptor = command_descriptor(
        {
            "catalog": "analytics",
            "target": "table_a",
            "columns": ["id", "region"],
        }
    )
    with running_flight_client(server) as client:
        options = flight_call_options("user1")
        info = client.get_flight_info(descriptor, options=options)

        authorizer.rules = [allow_rule(["id", "region"], row_filter="region = 'us'")]
        captured = client.do_get(info.endpoints[0].ticket, options=options).read_all()
        assert captured.column("region").to_pylist() == ["us", "eu"]

        updated_info = client.get_flight_info(descriptor, options=options)
        updated = client.do_get(updated_info.endpoints[0].ticket, options=options).read_all()
        assert updated.column("region").to_pylist() == ["us"]


def test_do_get_requires_authorization_header(tmp_path):
    del tmp_path
    schema = id_region_schema()
    batch = id_region_batch([1], ["us"])
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    server = build_flight_service(
        table_format=table_format,
        policy_rules=[allow_rule(["id", "region"])],
    )
    descriptor = command_descriptor(
        {
            "catalog": "analytics",
            "target": "test.table",
            "columns": ["id", "region"],
        }
    )
    plan_context = DummyContext(headers=[authorization_header("user1")])
    info = server.get_flight_info(plan_context, descriptor)

    with pytest.raises(flight.FlightUnauthorizedError):
        server.do_get(DummyContext(headers=[]), info.endpoints[0].ticket)


@pytest.mark.parametrize("after_first_batch", [False, True])
def test_stream_resource_failure_is_unavailable_and_closes_source(after_first_batch):
    from threading import Thread

    from dal_obscura.interfaces.flight.streaming import make_stream
    from dal_obscura.read.transform_contracts import StreamResourceError

    closed = []
    schema = pa.schema([pa.field("id", pa.int64())])

    def batches():
        try:
            if after_first_batch:
                yield pa.record_batch([pa.array([1])], schema=schema)
            raise StreamResourceError("Governed stream memory budget exhausted")
        finally:
            closed.append(True)

    class Server(flight.FlightServerBase):
        def do_get(self, context, ticket):
            return make_stream(schema, batches())

    server = Server("grpc://127.0.0.1:0")
    thread = Thread(target=server.serve)
    thread.start()
    try:
        with (
            flight.FlightClient(f"grpc://127.0.0.1:{server.port}") as client,
            pytest.raises(flight.FlightUnavailableError, match="memory budget exhausted"),
        ):
            client.do_get(flight.Ticket(b"test")).read_all()
        assert closed == [True]
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.parametrize("operation", ["get_schema", "get_flight_info"])
def test_provider_capacity_failure_returns_unavailable(operation):
    from dal_obscura.sources.access import AccessContextUnavailable

    class UnavailableAuthorizer:
        def authorize(self, *args, **kwargs):
            raise AccessContextUnavailable("Catalog provider capacity is unavailable; retry later")

    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub",
        schema=id_region_schema(),
        batches=(),
    )
    server = build_flight_service(table_format=table_format, authorizer=UnavailableAuthorizer())
    descriptor = command_descriptor(
        {"catalog": "analytics", "target": "test.table", "columns": ["id"]}
    )
    try:
        with pytest.raises(flight.FlightUnavailableError, match="retry later"):
            getattr(server, operation)(
                DummyContext(headers=[authorization_header("user1")]), descriptor
            )
    finally:
        server.shutdown()
