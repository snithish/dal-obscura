from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

import pyarrow.flight as flight

from dal_obscura.common.access_control.filters import RowFilter, deserialize_row_filter
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.data_plane.application.ports.identity import AuthenticationRequest

REQUEST_HEADERS_MIDDLEWARE_KEY = "request_headers"
SUPPORTED_PROTOCOL_VERSION = 1
MAX_DESCRIPTOR_BYTES = 64 * 1024
MAX_TARGET_LENGTH = 512
MAX_COLUMNS = 1024
MAX_COLUMN_LENGTH = 256
MAX_ROW_FILTER_LENGTH = 8192
SECURITY_SENSITIVE_HEADERS = {
    "authorization",
    "x-api-key",
    "x-dal-obscura-proxy-secret",
}


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
    headers = getattr(context, "headers", {})
    return normalize_headers(headers)


def authentication_request_from_context(
    context: flight.ServerCallContext,
    *,
    method: str,
) -> AuthenticationRequest:
    """Builds a transport-neutral auth request from Flight call context."""
    return AuthenticationRequest(
        headers=headers_from_context(context),
        peer_identity=_safe_context_value(context, "peer_identity"),
        peer=_safe_context_value(context, "peer"),
        method=method,
    )


def normalize_headers(
    headers: Mapping[Any, Any] | Iterable[tuple[Any, Any]],
) -> dict[str, str]:
    """Lower-cases header names and decodes byte values from Arrow Flight."""
    items = list(_iter_header_items(headers))
    _reject_duplicate_security_headers(items)
    normalized: dict[str, str] = {}
    for key, value in items:
        normalized[_decode_header_value(key).lower()] = _decode_header_value(value)
    return normalized


def parse_descriptor(descriptor: flight.FlightDescriptor) -> PlanRequest:
    """Parses the JSON command payload used by `get_flight_info` requests."""
    if descriptor.command:
        if len(descriptor.command) > MAX_DESCRIPTOR_BYTES:
            raise ValueError("Flight descriptor command is too large")
        raw = descriptor.command.decode("utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("Flight descriptor command must be a JSON object")
        _validate_protocol_version(data.get("protocol_version", SUPPORTED_PROTOCOL_VERSION))
        target = _required_string(data, "target", max_length=MAX_TARGET_LENGTH)
        columns = _required_columns(data.get("columns"))
        return PlanRequest(
            catalog=str(data["catalog"]) if data.get("catalog") else None,
            target=target,
            columns=columns,
            row_filter=_optional_descriptor_row_filter(data.get("row_filter")),
        )
    raise ValueError("Invalid Flight descriptor")


def _validate_protocol_version(value: object) -> None:
    if value != SUPPORTED_PROTOCOL_VERSION:
        raise ValueError(f"Unsupported Flight protocol version: {value}")


def _optional_descriptor_row_filter(value: object) -> RowFilter | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Descriptor row_filter must be a non-empty string")
    if len(value) > MAX_ROW_FILTER_LENGTH:
        raise ValueError("Descriptor row_filter is too long")
    return deserialize_row_filter(value)


def _required_string(
    data: Mapping[str, Any],
    name: str,
    *,
    max_length: int,
) -> str:
    value = data.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Descriptor {name} is required")
    normalized = value.strip()
    if len(normalized) > max_length:
        raise ValueError(f"Descriptor {name} is too long")
    return normalized


def _required_columns(raw: object) -> list[str]:
    if not isinstance(raw, list) or not raw:
        raise ValueError("Descriptor columns must be a non-empty list of strings")
    if len(raw) > MAX_COLUMNS:
        raise ValueError("Descriptor columns list is too long")
    columns: list[str] = []
    for item in raw:
        if not isinstance(item, str) or not item.strip():
            raise ValueError("Descriptor columns must be a non-empty list of strings")
        column = item.strip()
        if len(column) > MAX_COLUMN_LENGTH:
            raise ValueError("Descriptor column name is too long")
        columns.append(column)
    return columns


def _reject_duplicate_security_headers(headers: Iterable[tuple[Any, Any]]) -> None:
    seen: set[str] = set()
    for raw_key, _value in headers:
        key = _decode_header_value(raw_key).lower()
        if key not in SECURITY_SENSITIVE_HEADERS:
            continue
        if key in seen:
            raise ValueError(f"Duplicate security-sensitive header: {key}")
        seen.add(key)


def _iter_header_items(
    headers: Mapping[Any, Any] | Iterable[tuple[Any, Any]],
) -> Iterable[tuple[Any, Any]]:
    if isinstance(headers, Mapping):
        return list(headers.items())
    return list(headers)


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


def _safe_context_value(context: flight.ServerCallContext, method_name: str) -> str:
    method = getattr(context, method_name, None)
    if not callable(method):
        return ""
    try:
        value = method()
    except Exception:
        return ""
    if value is None:
        return ""
    return _decode_header_value(value)
