import pyarrow.flight as flight
import pytest

from dal_obscura.interfaces.flight.contracts import parse_descriptor
from dal_obscura.interfaces.flight_contract import encode_plan_command
from tests.support.flight.flight import (
    command_descriptor,
)
from tests.support.policy.row_filters import FLIGHT_UNSAFE_ROW_FILTER_SMOKE_CASES


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


def test_parse_descriptor_accepts_protobuf_protocol_version_one():
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
    assert request.columns == ("id",)


def test_parse_descriptor_accepts_matching_typed_paths():
    descriptor = flight.FlightDescriptor.for_command(
        encode_plan_command(
            catalog="analytics",
            target="test.table",
            columns=['["a.b"]'],
            include_typed_paths=True,
        )
    )

    request = parse_descriptor(descriptor)

    assert request.columns == ('["a.b"]',)


def test_parse_descriptor_rejects_typed_paths_that_do_not_match_columns():
    from dal_obscura.flight.v1.read_pb2 import PlanRequest

    payload = PlanRequest(
        protocol_version=1,
        catalog="analytics",
        target="test.table",
        columns=["id"],
        column_paths=[{"version": 1, "segments": [{"kind": "FIELD", "name": "email"}]}],
    )

    with pytest.raises(ValueError, match="must match canonical"):
        parse_descriptor(flight.FlightDescriptor.for_command(payload.SerializeToString()))


def test_parse_descriptor_rejects_json_command_payload():
    descriptor = flight.FlightDescriptor.for_command(
        b'{"protocol_version":1,"catalog":"analytics","target":"test.table","columns":["id"]}'
    )

    with pytest.raises(ValueError, match="Invalid protobuf Flight descriptor command"):
        parse_descriptor(descriptor)


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


@pytest.mark.parametrize(
    "payload,error",
    [
        ({"catalog": "analytics", "columns": ["id"]}, "target is required"),
        ({"catalog": "analytics", "target": "", "columns": ["id"]}, "target is required"),
        ({"catalog": "analytics", "target": "test.table", "columns": []}, "columns"),
        (
            {
                "catalog": "analytics",
                "target": "test.table",
                "columns": ["id"],
                "row_filter": "x" * 8193,
            },
            "row_filter is too long",
        ),
    ],
)
def test_parse_descriptor_rejects_invalid_request_shapes(payload, error):
    descriptor = command_descriptor(payload)

    with pytest.raises(ValueError, match=error):
        parse_descriptor(descriptor)


def test_parse_descriptor_rejects_malformed_protobuf_command():
    descriptor = flight.FlightDescriptor.for_command(bytes([0x1A, 0xFF]))

    with pytest.raises(ValueError, match="Invalid protobuf Flight descriptor command"):
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
    FLIGHT_UNSAFE_ROW_FILTER_SMOKE_CASES,
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


@pytest.mark.parametrize(
    "name",
    ["x" * 257, "界" * 1024, "field_" + "x" * 16384],
    ids=["ascii-257", "unicode-1024", "ascii-16384"],
)
def test_flight_descriptor_preserves_long_typed_field_names(name):
    from dal_obscura_plugin_api.paths import FieldPath, FieldSegment

    column = FieldPath((FieldSegment(name),)).to_human()
    descriptor = flight.FlightDescriptor.for_command(
        encode_plan_command(
            catalog="analytics",
            target="test.table",
            columns=[column],
            include_typed_paths=True,
        )
    )
    assert parse_descriptor(descriptor).columns == (column,)


def test_flight_descriptor_still_enforces_total_wire_bytes():
    from dal_obscura.flight.v1.read_pb2 import PlanRequest
    from dal_obscura.interfaces.flight.contracts import MAX_DESCRIPTOR_BYTES

    payload = PlanRequest(
        protocol_version=1, target="test.table", columns=["x" * MAX_DESCRIPTOR_BYTES]
    )
    with pytest.raises(ValueError, match="command is too large"):
        parse_descriptor(flight.FlightDescriptor.for_command(payload.SerializeToString()))
