import pyarrow.flight as flight
import pytest

from dal_obscura.common.flight_contract import encode_plan_command
from dal_obscura.data_plane.interfaces.flight.contracts import parse_descriptor
from tests.support.flight import (
    command_descriptor,
)
from tests.support.row_filters import FLIGHT_UNSAFE_ROW_FILTER_SMOKE_CASES


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
    assert request.columns == ["id"]


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

    assert request.columns == ['["a.b"]']


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
