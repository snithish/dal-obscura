from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import jwt
import pyarrow as pa
import pyarrow.flight as flight

from dal_obscura.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.application.use_cases.get_schema import GetSchemaUseCase
from dal_obscura.application.use_cases.plan_access import PlanAccessUseCase
from dal_obscura.domain.catalog.ports import TableFormat
from dal_obscura.domain.query_planning.models import PlanRequest
from dal_obscura.domain.table_format.ports import InputPartition, Plan, ScanTask
from dal_obscura.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)
from dal_obscura.infrastructure.adapters.identity_default import (
    AuthConfig,
    DefaultIdentityAdapter,
)
from dal_obscura.infrastructure.adapters.policy_file_authorizer import PolicyFileAuthorizer
from dal_obscura.infrastructure.adapters.ticket_hmac import HmacTicketCodecAdapter
from dal_obscura.interfaces.flight.server import DataAccessFlightService

TEST_JWT_SECRET = "test-jwt-secret-32-characters-long"


@dataclass(frozen=True, kw_only=True)
class StubInputPartition(InputPartition):
    payload: bytes = b"payload"


@dataclass(frozen=True, kw_only=True)
class StubTableFormat(TableFormat):
    schema: pa.Schema
    batches: tuple[pa.RecordBatch, ...]

    def get_schema(self) -> pa.Schema:
        return self.schema

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        del max_tickets
        return Plan(
            schema=self.schema,
            tasks=[
                ScanTask(
                    table_format=self,
                    schema=self.schema,
                    partition=StubInputPartition(),
                )
            ],
            full_row_filter=request.row_filter,
            backend_pushdown_row_filter=None,
            residual_row_filter=request.row_filter,
        )

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[Any]]:
        if not isinstance(partition, StubInputPartition):
            raise TypeError("StubTableFormat requires a StubInputPartition")
        return self.schema, iter(self.batches)


class StubCatalogRegistry:
    def __init__(self, table_format: TableFormat) -> None:
        self._table_format = table_format

    def describe(
        self,
        catalog: str | None,
        target: str,
        *,
        tenant_id: str = "default",
    ) -> TableFormat:
        del tenant_id
        del catalog, target
        return self._table_format


def make_jwt(
    principal_id: str = "user1",
    *,
    groups: list[str] | None = None,
    jwt_secret: str = TEST_JWT_SECRET,
) -> str:
    claims: dict[str, object] = {"sub": principal_id}
    if groups:
        claims["groups"] = list(groups)
    return jwt.encode(claims, jwt_secret, algorithm="HS256")


def authorization_header(
    principal_id: str = "user1",
    *,
    groups: list[str] | None = None,
    jwt_secret: str = TEST_JWT_SECRET,
) -> tuple[bytes, bytes]:
    return (
        b"authorization",
        f"Bearer {make_jwt(principal_id, groups=groups, jwt_secret=jwt_secret)}".encode(),
    )


def flight_call_options(
    principal_id: str = "user1",
    *,
    groups: list[str] | None = None,
    jwt_secret: str = TEST_JWT_SECRET,
) -> flight.FlightCallOptions:
    return flight.FlightCallOptions(
        headers=[authorization_header(principal_id, groups=groups, jwt_secret=jwt_secret)]
    )


def command_descriptor(payload: dict[str, object]) -> flight.FlightDescriptor:
    return flight.FlightDescriptor.for_command(json.dumps(payload).encode("utf-8"))


def build_flight_service(
    *,
    policy_path: str | Path,
    table_format: TableFormat | None = None,
    catalog_registry: Any | None = None,
    jwt_secret: str = TEST_JWT_SECRET,
    ticket_secret: str = "secret",
    ticket_ttl_seconds: int = 300,
    max_tickets: int = 1,
) -> DataAccessFlightService:
    if (table_format is None) == (catalog_registry is None):
        raise ValueError("Provide exactly one of table_format or catalog_registry")

    resolved_registry = catalog_registry
    if table_format is not None:
        resolved_registry = StubCatalogRegistry(table_format)

    identity = DefaultIdentityAdapter(AuthConfig(jwt_secret=jwt_secret))
    authorizer = PolicyFileAuthorizer(policy_path)
    masking = DefaultMaskingAdapter()
    row_transform = DuckDBRowTransformAdapter(masking)
    ticket_codec = HmacTicketCodecAdapter(ticket_secret)
    get_schema = GetSchemaUseCase(
        identity=identity,
        authorizer=authorizer,
        catalog_registry=cast(Any, resolved_registry),
        masking=masking,
    )
    plan_access = PlanAccessUseCase(
        identity=identity,
        authorizer=authorizer,
        catalog_registry=cast(Any, resolved_registry),
        masking=masking,
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=ticket_ttl_seconds,
        max_tickets=max_tickets,
    )
    fetch_stream = FetchStreamUseCase(
        identity=identity,
        authorizer=authorizer,
        masking=masking,
        row_transform=row_transform,
        ticket_codec=ticket_codec,
    )
    return DataAccessFlightService(
        location="grpc+tcp://0.0.0.0:0",
        get_schema_use_case=get_schema,
        plan_access_use_case=plan_access,
        fetch_stream_use_case=fetch_stream,
    )


def start_server(server: DataAccessFlightService) -> threading.Thread:
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    deadline = time.time() + 5
    while time.time() < deadline:
        if server.port > 0:
            return thread
        time.sleep(0.05)
    raise RuntimeError("Flight server failed to start")


@contextmanager
def running_flight_client(server: DataAccessFlightService) -> Iterator[flight.FlightClient]:
    thread = start_server(server)
    try:
        yield flight.FlightClient(f"grpc+tcp://localhost:{server.port}")
    finally:
        server.shutdown()
        thread.join(timeout=2)


def flight_info(
    client: flight.FlightClient,
    payload: dict[str, object],
    *,
    principal_id: str = "user1",
    groups: list[str] | None = None,
    jwt_secret: str = TEST_JWT_SECRET,
) -> tuple[flight.FlightInfo, flight.FlightCallOptions]:
    options = flight_call_options(principal_id, groups=groups, jwt_secret=jwt_secret)
    info = client.get_flight_info(command_descriptor(payload), options=options)
    return info, options


def read_table(
    client: flight.FlightClient,
    info: flight.FlightInfo,
    options: flight.FlightCallOptions,
) -> pa.Table:
    batches = []
    for endpoint in info.endpoints:
        reader = client.do_get(endpoint.ticket, options=options)
        batches.extend(reader.read_all().to_batches())
    return pa.Table.from_batches(batches) if batches else pa.table({})


def flight_request(
    client: flight.FlightClient,
    payload: dict[str, object],
    *,
    principal_id: str = "user1",
    groups: list[str] | None = None,
    jwt_secret: str = TEST_JWT_SECRET,
) -> tuple[flight.FlightInfo, pa.Table]:
    info, options = flight_info(
        client,
        payload,
        principal_id=principal_id,
        groups=groups,
        jwt_secret=jwt_secret,
    )
    return info, read_table(client, info, options)
