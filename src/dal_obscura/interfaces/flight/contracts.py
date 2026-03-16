from __future__ import annotations

import json
from typing import Mapping

import pyarrow.flight as flight

from dal_obscura.domain.query_planning import PlanRequest


def headers_from_context(context: flight.ServerCallContext) -> dict[str, str]:
    """Reads transport headers from Flight and normalizes them for adapters."""
    try:
        return normalize_headers(context.headers)
    except Exception:
        return {}


def normalize_headers(headers: Mapping[object, object]) -> dict[str, str]:
    """Lower-cases header names and decodes byte values from Arrow Flight."""
    normalized: dict[str, str] = {}
    for key, value in dict(headers).items():
        normalized[_decode_header_value(key).lower()] = _decode_header_value(value)
    return normalized


def parse_descriptor(descriptor: flight.FlightDescriptor) -> PlanRequest:
    """Parses the JSON command payload used by `get_flight_info` requests."""
    if descriptor.command:
        raw = descriptor.command.decode("utf-8")
        data = json.loads(raw)
        auth_token = data.get("auth_token") or data.get("authorization") or data.get("api_key")
        return PlanRequest(
            catalog=str(data["catalog"]) if data.get("catalog") else None,
            target=str(data["target"]),
            columns=list(data["columns"]),
            auth_token=str(auth_token) if auth_token else None,
        )
    raise ValueError("Invalid Flight descriptor")


def _decode_header_value(value: object) -> str:
    """Converts Flight header keys and values into plain strings."""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    return str(value)
