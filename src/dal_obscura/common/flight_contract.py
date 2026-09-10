"""Protocol v1 helpers for Arrow Flight descriptor commands.

Example:
    ```python
    command = encode_plan_command(
        catalog="analytics",
        target="default.users",
        columns=["id", "email"],
        row_filter="region = 'us'",
    )
    descriptor = flight.FlightDescriptor.for_command(command)
    ```
"""

from __future__ import annotations

from typing import Any

from google.protobuf.message import DecodeError

from dal_obscura.common.query_planning.field_paths import (
    FieldPath,
    FieldSegment,
    ListElementSegment,
    MapKeySegment,
    parse_field_path,
)
from dal_obscura.flight.v1.read_pb2 import (
    FieldPath as FieldPathProto,
)
from dal_obscura.flight.v1.read_pb2 import (
    FieldPathSegment as FieldPathSegmentProto,
)
from dal_obscura.flight.v1.read_pb2 import PlanRequest as PlanRequestProto

FLIGHT_PROTOCOL_VERSION = 1


def encode_plan_command(
    *,
    catalog: str | None,
    target: str,
    columns: list[str],
    row_filter: str | None = None,
    protocol_version: int = FLIGHT_PROTOCOL_VERSION,
    include_typed_paths: bool = False,
) -> bytes:
    """Serializes a protocol v1 plan request into Flight command bytes.

    Example:
        ```python
        payload = encode_plan_command(
            catalog="analytics",
            target="default.users",
            columns=["id"],
        )
        ```
    """
    request = PlanRequestProto(
        protocol_version=protocol_version,
        catalog=catalog or "",
        target=target,
        columns=columns,
        row_filter=row_filter or "",
    )
    if include_typed_paths and columns != ["*"]:
        for column in columns:
            request.column_paths.append(_field_path_to_proto(parse_field_path(column)))
    return request.SerializeToString()


def decode_plan_command(payload: bytes) -> PlanRequestProto:
    """Parses Flight command bytes into the generated protobuf message.

    Example:
        ```python
        request = decode_plan_command(payload)
        assert request.protocol_version == FLIGHT_PROTOCOL_VERSION
        ```
    """
    request = PlanRequestProto()
    try:
        request.ParseFromString(payload)
    except DecodeError as error:
        raise ValueError("Invalid protobuf Flight descriptor command") from error
    return request


def encode_plan_command_from_mapping(payload: dict[str, Any]) -> bytes:
    """Builds command bytes from JSON-like test or tooling input.

    Example:
        ```python
        payload = encode_plan_command_from_mapping(
            {"catalog": "analytics", "target": "default.users", "columns": ["id"]}
        )
        ```
    """
    protocol_version = payload.get("protocol_version", FLIGHT_PROTOCOL_VERSION)
    columns = payload.get("columns", [])
    if not isinstance(protocol_version, int):
        raise ValueError("protocol_version must be an integer")
    if not isinstance(columns, list):
        columns = []
    return encode_plan_command(
        protocol_version=protocol_version,
        catalog=str(payload["catalog"]) if payload.get("catalog") else None,
        target=str(payload.get("target") or ""),
        columns=[str(column) for column in columns],
        row_filter=str(payload["row_filter"]) if payload.get("row_filter") else None,
    )


def _field_path_to_proto(path: FieldPath) -> FieldPathProto:
    encoded = FieldPathProto(version=path.version)
    for segment in path.segments:
        output = encoded.segments.add()
        if isinstance(segment, FieldSegment):
            output.kind = FieldPathSegmentProto.FIELD
            output.name = segment.name
            if segment.field_id is not None:
                output.field_id = segment.field_id
        elif isinstance(segment, ListElementSegment):
            output.kind = FieldPathSegmentProto.LIST_ELEMENT
        elif isinstance(segment, MapKeySegment):
            output.kind = FieldPathSegmentProto.MAP_KEY
        else:
            output.kind = FieldPathSegmentProto.MAP_VALUE
    return encoded
