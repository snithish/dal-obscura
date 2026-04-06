import pyarrow as pa
import pyarrow.flight as flight
import pytest

from dal_obscura.interfaces.flight.contracts import parse_descriptor
from tests.support.flight import (
    StubTableFormat,
    authorization_header,
    build_flight_service,
    command_descriptor,
    flight_call_options,
    make_jwt,
    running_flight_client,
)


class DummyContext:
    def __init__(self, headers):
        self.headers = headers


def _catalog_policy(targets: str) -> str:
    return f"""
version: 1
catalogs:
  analytics:
    targets:
{targets}
"""


def test_parse_descriptor_rejects_path_descriptor():
    descriptor = flight.FlightDescriptor.for_path("analytics", "users")
    with pytest.raises(ValueError):
        parse_descriptor(descriptor)


def test_streaming_contract_emits_multiple_batches(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "dal_obscura.infrastructure.adapters.duckdb_transform._DUCKDB_ARROW_OUTPUT_BATCH_SIZE",
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

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "test.table":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
"""
        )
    )
    server = build_flight_service(table_format=table_format, policy_path=policy_path)
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

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "test.table":
        rules:
          - principals: ["group:analyst"]
            columns: ["id", "user.address.zip"]
            masks:
              "user.address.zip":
                type: "hash"
            row_filter: "region = 'us'"
          - principals: ["user1"]
            columns: ["user.email"]
            masks:
              "user.email":
                type: "redact"
                value: "[hidden]"
            row_filter: "active = true"
"""
        )
    )
    server = build_flight_service(table_format=table_format, policy_path=policy_path)
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

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "test.table":
        rules:
          - principals: ["user1"]
            columns: ["id", "metadata"]
            masks:
              "metadata.preferences.theme":
                type: "redact"
                value: "[hidden]"
"""
        )
    )
    server = build_flight_service(table_format=table_format, policy_path=policy_path)
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


def test_flight_logs_include_resident_memory(tmp_path, caplog, monkeypatch):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1, 2]), pa.array(["us", "eu"])], schema=schema)
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "test.table":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
"""
        )
    )
    monkeypatch.setattr(
        "dal_obscura.interfaces.flight.server.get_resident_memory_bytes",
        lambda: 4_242,
    )
    server = build_flight_service(table_format=table_format, policy_path=policy_path)
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


def test_do_get_rejects_principal_mismatch(tmp_path):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1]), pa.array(["us"])], schema=schema)
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "test.table":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
"""
        )
    )
    server = build_flight_service(table_format=table_format, policy_path=policy_path)
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
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1]), pa.array(["us"])], schema=schema)
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "test.table":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
"""
        )
    )
    server = build_flight_service(table_format=table_format, policy_path=policy_path)
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
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1]), pa.array(["us"])], schema=schema)
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "table_a":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
      "table_b":
        rules:
          - principals: ["user1"]
            columns: ["id"]
"""
        )
    )
    server = build_flight_service(table_format=table_format, policy_path=policy_path)
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

    policy_path.write_text(
        _catalog_policy(
            """
      "table_a":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
      "table_b":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
            row_filter: "region = 'us'"
"""
        )
    )
    do_get_context = DummyContext(headers=[authorization_header("user1")])
    server.do_get(do_get_context, ticket)

    policy_path.write_text(
        _catalog_policy(
            """
      "table_a":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
            row_filter: "region = 'us'"
      "table_b":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
"""
        )
    )
    with pytest.raises(flight.FlightUnauthorizedError):
        server.do_get(do_get_context, ticket)


def test_do_get_requires_authorization_header(tmp_path):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1]), pa.array(["us"])], schema=schema)
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "test.table":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
"""
        )
    )
    server = build_flight_service(table_format=table_format, policy_path=policy_path)
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
