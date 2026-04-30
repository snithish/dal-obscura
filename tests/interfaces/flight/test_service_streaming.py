import pyarrow as pa
import pyarrow.flight as flight
import pytest

from dal_obscura.data_plane.interfaces.flight.contracts import parse_descriptor
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


class DummyContext:
    def __init__(self, headers):
        self.headers = headers


def _allow_rule(
    columns: list[str],
    *,
    principals: list[str] | None = None,
    masks: dict[str, object] | None = None,
    row_filter: str | None = None,
) -> dict[str, object]:
    rule: dict[str, object] = {
        "principals": principals or ["user1"],
        "columns": columns,
        "effect": "allow",
    }
    if masks:
        rule["masks"] = masks
    if row_filter:
        rule["row_filter"] = row_filter
    return rule


@pytest.mark.parametrize(
    ("column_name", "field", "value", "mask_config", "expected_type"),
    [
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
            {"type": "default", "value": "fallback"},
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
    ],
)
def test_flight_info_schema_matches_mask_output_types(
    tmp_path,
    column_name,
    field,
    value,
    mask_config,
    expected_type,
):
    schema = pa.schema([field])
    batch = pa.record_batch([value], schema=schema)
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    del tmp_path
    policy_rules = [_allow_rule([column_name], masks={column_name: mask_config})]
    server = build_flight_service(
        table_format=table_format,
        policy_rules=policy_rules,
    )

    with running_flight_client(server) as client:
        info, table = flight_request(
            client,
            {
                "catalog": "analytics",
                "target": "test.table",
                "columns": [column_name],
            },
        )

    assert info.schema.field(column_name).type == expected_type
    assert table.schema.field(column_name).type == expected_type


def test_parse_descriptor_rejects_path_descriptor():
    descriptor = flight.FlightDescriptor.for_path("analytics", "users")
    with pytest.raises(ValueError):
        parse_descriptor(descriptor)


def test_parse_descriptor_accepts_optional_row_filter():
    descriptor = command_descriptor(
        {
            "catalog": "analytics",
            "target": "test.table",
            "columns": ["id"],
            "row_filter": "region = 'us'",
        }
    )

    request = parse_descriptor(descriptor)

    assert request.row_filter is not None
    assert request.row_filter.sql == "region = 'us'"


def test_parse_descriptor_accepts_protocol_version_one():
    descriptor = command_descriptor(
        {
            "protocol_version": 1,
            "catalog": "analytics",
            "target": "test.table",
            "columns": ["id"],
        }
    )

    request = parse_descriptor(descriptor)

    assert request.catalog == "analytics"
    assert request.target == "test.table"
    assert request.columns == ["id"]


def test_parse_descriptor_rejects_unsupported_protocol_version():
    descriptor = command_descriptor(
        {
            "protocol_version": 99,
            "catalog": "analytics",
            "target": "test.table",
            "columns": ["id"],
        }
    )

    with pytest.raises(ValueError, match="Unsupported Flight protocol version: 99"):
        parse_descriptor(descriptor)


def test_parse_descriptor_rejects_invalid_row_filter_syntax():
    descriptor = command_descriptor(
        {
            "catalog": "analytics",
            "target": "test.table",
            "columns": ["id"],
            "row_filter": "region = ",
        }
    )

    with pytest.raises(ValueError, match="Invalid row filter syntax"):
        parse_descriptor(descriptor)


@pytest.mark.parametrize(
    "row_filter",
    [
        "region = 'us'; DROP TABLE input",
        "COPY input TO '/tmp/leak.csv'",
        "EXISTS(SELECT 1)",
        "id IN (SELECT 1)",
        "read_csv('/etc/passwd')",
    ],
)
def test_parse_descriptor_rejects_unsafe_row_filter_sql(row_filter):
    descriptor = command_descriptor(
        {
            "catalog": "analytics",
            "target": "test.table",
            "columns": ["id"],
            "row_filter": row_filter,
        }
    )

    with pytest.raises(ValueError, match=r"row filter|Row filter|Unsupported"):
        parse_descriptor(descriptor)


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
            _allow_rule(
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

        assert result.schema.names == ["id", "email"]
        assert result.schema.field("email").type == pa.string()


def test_streaming_contract_emits_multiple_batches(tmp_path, monkeypatch):
    del tmp_path
    monkeypatch.setattr(
        "dal_obscura.data_plane.infrastructure.adapters.duckdb_transform._DUCKDB_ARROW_OUTPUT_BATCH_SIZE",
        2,
    )
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch1 = pa.record_batch([pa.array([1, 2]), pa.array(["us", "eu"])], schema=schema)
    batch2 = pa.record_batch([pa.array([3, 4]), pa.array(["us", "us"])], schema=schema)
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch1, batch2),
    )

    server = build_flight_service(
        table_format=table_format,
        policy_rules=[_allow_rule(["id", "region"])],
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


def test_flight_streaming_supports_nested_projection_with_combined_row_filters(tmp_path):
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
            _allow_rule(
                ["id", "user.address.zip"],
                principals=["group:analyst"],
                masks={"user.address.zip": {"type": "hash"}},
                row_filter="region = 'us'",
            ),
            _allow_rule(
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
            }
        )
        options = flight_call_options("user1", groups=["analyst"])

        info = client.get_flight_info(descriptor, options=options)
        table = client.do_get(info.endpoints[0].ticket, options=options).read_all()

        assert info.schema.names == ["id", "user.address.zip", "user.email"]
        assert info.schema.field("user.address.zip").type == pa.string()
        assert info.schema.field("user.email").type == pa.string()
        assert table.num_rows == 2
        assert table.column("id").to_pylist() == [1, 4]
        assert table.column("user.email").to_pylist() == ["[hidden]", "[hidden]"]
        assert all(len(value) == 64 for value in table.column("user.address.zip").to_pylist())


def test_flight_streaming_masks_list_of_struct_fields(tmp_path):
    preference_type = pa.struct(
        [
            pa.field("name", pa.string()),
            pa.field("theme", pa.string()),
        ]
    )
    metadata_type = pa.struct(
        [
            pa.field("preferences", pa.list_(preference_type)),
        ]
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("metadata", metadata_type),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1], type=pa.int64()),
            pa.array(
                [
                    {
                        "preferences": [
                            {"name": "web", "theme": "dark"},
                            {"name": "mobile", "theme": "light"},
                        ]
                    }
                ],
                type=metadata_type,
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
            _allow_rule(
                ["id", "metadata"],
                masks={"metadata.preferences.theme": {"type": "redact", "value": "[hidden]"}},
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


def test_flight_normalizes_large_list_schema_for_clients(tmp_path):
    del tmp_path
    metadata_type = pa.struct(
        [
            pa.field(
                "preferences",
                pa.large_list(
                    pa.field(
                        "item",
                        pa.struct(
                            [
                                pa.field("name", pa.string()),
                                pa.field("theme", pa.string()),
                            ]
                        ),
                    )
                ),
            )
        ]
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("metadata", metadata_type),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1], type=pa.int64()),
            pa.array(
                [
                    {
                        "preferences": [
                            {"name": "web", "theme": "dark"},
                            {"name": "mobile", "theme": "light"},
                        ]
                    }
                ],
                type=metadata_type,
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

    server = build_flight_service(
        table_format=table_format,
        policy_rules=[_allow_rule(["id", "metadata"])],
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


def test_flight_streaming_combines_policy_and_requested_row_filters(tmp_path):
    del tmp_path
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("region", pa.string()),
            pa.field("active", pa.bool_()),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3, 4], type=pa.int64()),
            pa.array(["us", "us", "eu", "us"], type=pa.string()),
            pa.array([True, False, True, True], type=pa.bool_()),
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
    server = build_flight_service(
        table_format=table_format,
        policy_rules=[_allow_rule(["id", "region", "active"], row_filter="active = true")],
    )

    with running_flight_client(server) as client:
        info, table = flight_request(
            client,
            {
                "catalog": "analytics",
                "target": "test.table",
                "columns": ["id"],
                "row_filter": "region = 'us'",
            },
        )

    assert info.schema.names == ["id"]
    assert table.column("id").to_pylist() == [1, 4]


def test_flight_logs_include_resident_memory(tmp_path, caplog, monkeypatch):
    del tmp_path
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1, 2]), pa.array(["us", "eu"])], schema=schema)
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    monkeypatch.setattr(
        "dal_obscura.data_plane.interfaces.flight.server.get_resident_memory_bytes",
        lambda: 4_242,
    )
    server = build_flight_service(
        table_format=table_format,
        policy_rules=[_allow_rule(["id", "region"])],
    )
    descriptor = command_descriptor(
        {
            "catalog": "analytics",
            "target": "test.table",
            "columns": ["id", "region"],
        }
    )
    context = DummyContext(headers=[authorization_header("user1")])

    with caplog.at_level("INFO"):
        info = server.get_flight_info(context, descriptor)
        server.do_get(context, info.endpoints[0].ticket)

    records = [record for record in caplog.records if record.message in {"plan_request", "do_get"}]
    assert [record.message for record in records] == ["plan_request", "do_get"]
    assert all(record.resident_memory_bytes == 4_242 for record in records)


def test_flight_logs_requested_row_filter_presence(tmp_path, caplog):
    del tmp_path
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1, 2]), pa.array(["us", "eu"])], schema=schema)
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    server = build_flight_service(
        table_format=table_format,
        policy_rules=[_allow_rule(["id", "region"])],
    )
    descriptor = command_descriptor(
        {
            "catalog": "analytics",
            "target": "test.table",
            "columns": ["id"],
            "row_filter": "region = 'us'",
        }
    )
    context = DummyContext(headers=[authorization_header("user1")])

    with caplog.at_level("INFO"):
        server.get_flight_info(context, descriptor)

    record = next(item for item in caplog.records if item.msg == "plan_request")
    assert record.requested_row_filter_present is True
    assert record.requested_row_filter_dependency_count == 1


def test_do_get_rejects_principal_mismatch(tmp_path):
    del tmp_path
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1]), pa.array(["us"])], schema=schema)
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    server = build_flight_service(
        table_format=table_format,
        policy_rules=[_allow_rule(["id", "region"])],
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
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1]), pa.array(["us"])], schema=schema)
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    server = build_flight_service(
        table_format=table_format,
        policy_rules=[_allow_rule(["id", "region"])],
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


def test_policy_version_is_per_dataset(tmp_path):
    del tmp_path
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1]), pa.array(["us"])], schema=schema)
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
        rules=[_allow_rule(["id", "region"])],
    )
    server = build_flight_service(table_format=table_format, authorizer=authorizer)
    descriptor = command_descriptor(
        {
            "catalog": "analytics",
            "target": "table_a",
            "columns": ["id", "region"],
        }
    )
    plan_context = DummyContext(headers=[authorization_header("user1")])
    info = server.get_flight_info(plan_context, descriptor)
    ticket = info.endpoints[0].ticket

    authorizer.rules = [_allow_rule(["id", "region"])]
    do_get_context = DummyContext(headers=[authorization_header("user1")])
    server.do_get(do_get_context, ticket)

    authorizer.rules = [_allow_rule(["id", "region"], row_filter="region = 'us'")]
    with pytest.raises(flight.FlightUnauthorizedError):
        server.do_get(do_get_context, ticket)


def test_do_get_requires_authorization_header(tmp_path):
    del tmp_path
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1]), pa.array(["us"])], schema=schema)
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    server = build_flight_service(
        table_format=table_format,
        policy_rules=[_allow_rule(["id", "region"])],
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
