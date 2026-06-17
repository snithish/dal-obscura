from __future__ import annotations

from typing import Any

from google.protobuf.message import DecodeError

from dal_obscura.flight.v1.read_pb2 import PlanRequest as PlanRequestProto

FLIGHT_PROTOCOL_VERSION = 1


def encode_plan_command(
    *,
    catalog: str | None,
    target: str,
    columns: list[str],
    row_filter: str | None = None,
    protocol_version: int = FLIGHT_PROTOCOL_VERSION,
) -> bytes:
    request = PlanRequestProto(
        protocol_version=protocol_version,
        catalog=catalog or "",
        target=target,
        columns=columns,
        row_filter=row_filter or "",
    )
    return request.SerializeToString()


def decode_plan_command(payload: bytes) -> PlanRequestProto:
    request = PlanRequestProto()
    try:
        request.ParseFromString(payload)
    except DecodeError as error:
        raise ValueError("Invalid protobuf Flight descriptor command") from error
    return request


def encode_plan_command_from_mapping(payload: dict[str, Any]) -> bytes:
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
