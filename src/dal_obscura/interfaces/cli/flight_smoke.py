"""Authenticated Flight verification with bounded rows, batches and deadlines."""

from __future__ import annotations

import json
import os
import time
from contextlib import suppress
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


def validate_smoke(settings: dict[str, Any]) -> None:
    expected = settings["expected_rows"]
    if not isinstance(expected, list) or not expected or len(expected) > 1000:
        raise ValueError("smoke.expected_rows must contain 1-1000 synthetic non-secret rows")
    if not all(isinstance(row, dict) for row in expected):
        raise ValueError("smoke.expected_rows must be JSON objects")
    parsed = urlsplit(str(settings["uri"]))
    if parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path:
        raise ValueError("Flight smoke URI must not contain credentials, path, query or fragment")
    if not parsed.hostname:
        raise ValueError("Flight smoke URI requires a hostname")
    if parsed.scheme != "grpc+tls" and not (
        parsed.scheme == "grpc+tcp" and parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    ):
        raise ValueError("Flight smoke requires TLS except on loopback")
    columns = settings.get("columns")
    if (
        not settings.get("target")
        or not isinstance(columns, list)
        or not columns
        or not all(isinstance(column, str) and column for column in columns)
    ):
        raise ValueError("smoke requires a target and a non-empty list of column paths")
    for key in ("ca_file", "cert_file", "key_file"):
        if settings.get(key):
            Path(settings[key]).read_bytes()


def read_smoke_endpoint(
    reader: Any,
    schema: Any,
    rows: list[dict[str, Any]],
    max_rows: int,
    chunks: int,
    deadline: float,
) -> int:
    try:
        while True:
            if time.monotonic() >= deadline:
                raise ValueError("Authenticated Flight smoke deadline exceeded")
            try:
                batch = reader.read_chunk().data
            except StopIteration:
                return chunks
            chunks += 1
            if chunks > 1000:
                raise ValueError("Flight smoke exceeded batch budget")
            if batch is None:
                continue
            if len(rows) + batch.num_rows > max_rows:
                raise ValueError("Flight smoke exceeded bounded expected result")
            if not batch.schema.equals(schema, check_metadata=True):
                raise ValueError("Flight smoke batch schema differs from admitted schema")
            rows.extend(batch.to_pylist())
    finally:
        with suppress(Exception):
            reader.cancel()


def smoke_read(settings: dict[str, Any]) -> None:
    import pyarrow as pa
    import pyarrow.flight as flight

    from dal_obscura.interfaces.flight_contract import encode_plan_command

    validate_smoke(settings)
    expected = settings["expected_rows"]
    token = os.environ[settings.get("token_env", "DAL_OBSCURA_DEPLOY_FLIGHT_TOKEN")]
    options = {
        option: Path(settings[key]).read_bytes()
        for key, option in (
            ("ca_file", "tls_root_certs"),
            ("cert_file", "cert_chain"),
            ("key_file", "private_key"),
        )
        if settings.get(key)
    }
    descriptor = flight.FlightDescriptor.for_command(
        encode_plan_command(
            catalog=settings.get("catalog"),
            target=settings["target"],
            columns=settings["columns"],
            row_filter=settings.get("row_filter"),
            include_typed_paths=True,
        )
    )
    deadline = time.monotonic() + 30

    def call_options() -> flight.FlightCallOptions:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError("Authenticated Flight smoke deadline exceeded")
        return flight.FlightCallOptions(
            timeout=remaining, headers=[(b"authorization", f"Bearer {token}".encode())]
        )

    rows: list[dict[str, Any]] = []
    chunks = 0
    try:
        with flight.FlightClient(settings["uri"], **options) as transport:
            schema = transport.get_schema(descriptor, options=call_options()).schema
            plan = transport.get_flight_info(descriptor, options=call_options())
            if not plan.schema.equals(schema, check_metadata=True) or len(plan.endpoints) > 256:
                raise ValueError("Flight smoke plan/schema differs or exceeds task budget")
            for endpoint in plan.endpoints:
                reader = transport.do_get(endpoint.ticket, options=call_options())
                chunks = read_smoke_endpoint(reader, schema, rows, len(expected), chunks, deadline)
    except (flight.FlightError, pa.ArrowException) as exc:
        raise ValueError(
            "Authenticated Flight smoke failed; release was not marked verified"
        ) from exc

    def encode(row: dict[str, Any]) -> str:
        return json.dumps(row, sort_keys=True, default=str)

    if sorted(map(encode, rows)) != sorted(map(encode, expected)):
        raise ValueError("Flight smoke values differ from synthetic expected rows")
    print(f"Verified authenticated Flight read: {len(rows)} rows")
