from __future__ import annotations

import json
from typing import Mapping

import pyarrow.flight as flight

from dal_obscura.domain.query_planning import PlanRequest


def headers_from_context(context: flight.ServerCallContext) -> dict[str, str]:
    try:
        return normalize_headers(context.headers)
    except Exception:
        return {}


def normalize_headers(headers: Mapping[object, object]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in dict(headers).items():
        normalized[_decode_header_value(key).lower()] = _decode_header_value(value)
    return normalized


def parse_descriptor(descriptor: flight.FlightDescriptor) -> PlanRequest:
    if descriptor.command:
        raw = descriptor.command.decode("utf-8")
        data = json.loads(raw)
        auth_token = data.get("auth_token") or data.get("authorization") or data.get("api_key")
        return PlanRequest(
            table=str(data["table"]),
            columns=list(data["columns"]),
            auth_token=str(auth_token) if auth_token else None,
        )
    if descriptor.path:
        table = ".".join(descriptor.path)
        return PlanRequest(table=table, columns=["*"])
    raise ValueError("Invalid Flight descriptor")


def _decode_header_value(value: object) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    return str(value)
