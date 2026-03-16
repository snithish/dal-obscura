from __future__ import annotations

import json
from collections.abc import Mapping

import pyarrow.flight as flight

from dal_obscura.domain.query_planning.models import PlanRequest

REQUEST_HEADERS_MIDDLEWARE_KEY = "request_headers"


class RequestHeadersMiddleware(flight.ServerMiddleware):
    """Stores normalized request headers for later access within handlers."""

    def __init__(self, headers: Mapping[str, object]) -> None:
        self.headers = normalize_headers(
            {key: _coerce_header_value(value) for key, value in headers.items()}
        )


class RequestHeadersMiddlewareFactory(flight.ServerMiddlewareFactory):
    """Captures Flight request headers as middleware for each RPC."""

    def start_call(
        self, info: flight.CallInfo, headers: Mapping[str, object]
    ) -> RequestHeadersMiddleware:
        del info
        return RequestHeadersMiddleware(headers)


def headers_from_context(context: flight.ServerCallContext) -> dict[str, str]:
    """Reads transport headers from Flight and normalizes them for adapters."""
    try:
        middleware = context.get_middleware(REQUEST_HEADERS_MIDDLEWARE_KEY)
        if middleware is not None:
            return dict(middleware.headers)
    except Exception:
        pass
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
        return PlanRequest(
            catalog=str(data["catalog"]) if data.get("catalog") else None,
            target=str(data["target"]),
            columns=list(data["columns"]),
        )
    raise ValueError("Invalid Flight descriptor")


def _decode_header_value(value: object) -> str:
    """Converts Flight header keys and values into plain strings."""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    return str(value)


def _coerce_header_value(value: object) -> object:
    if isinstance(value, list):
        if not value:
            return ""
        return value[-1]
    return value
