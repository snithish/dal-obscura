from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Literal, cast
from uuid import UUID

import jwt
import pyarrow as pa
import pyarrow.flight as flight
from sqlalchemy.orm import Session

from dal_obscura.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.application.use_cases.get_schema import GetSchemaUseCase
from dal_obscura.application.use_cases.plan_access import PlanAccessUseCase
from dal_obscura.domain.access_control.models import (
    AccessDecision,
    AccessRule,
    DatasetPolicy,
    MaskRule,
    Policy,
    Principal,
    PrincipalConditionValue,
)
from dal_obscura.domain.access_control.policy_resolution import dataset_version, resolve_access
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
from dal_obscura.infrastructure.adapters.published_config import (
    PublishedConfigAuthorizer,
    PublishedConfigCatalogRegistry,
    PublishedConfigStore,
)
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


class InMemoryPolicyAuthorizer:
    def __init__(
        self,
        *,
        catalog: str,
        target: str,
        rules: list[dict[str, object]],
        rules_by_dataset: dict[tuple[str | None, str], list[dict[str, object]]] | None = None,
    ) -> None:
        self.catalog = catalog
        self.target = target
        self.rules = rules
        self.rules_by_dataset = rules_by_dataset or {}

    def authorize(
        self,
        principal: Principal,
        target: str,
        catalog: str | None,
        requested_columns: Iterable[str],
    ) -> AccessDecision:
        dataset = self._dataset(target=target, catalog=catalog)
        policy = Policy(version=1, datasets=[dataset])
        allowed_columns, masks, row_filter = resolve_access(
            policy,
            principal,
            target,
            catalog,
            requested_columns,
        )
        return AccessDecision(
            allowed_columns=allowed_columns,
            masks=masks,
            row_filter=row_filter,
            policy_version=dataset_version(dataset),
        )

    def current_policy_version(
        self,
        target: str,
        catalog: str | None,
        *,
        tenant_id: str,
    ) -> int | None:
        del tenant_id
        return dataset_version(self._dataset(target=target, catalog=catalog))

    def _dataset(self, *, target: str, catalog: str | None) -> DatasetPolicy:
        return DatasetPolicy(
            target=target,
            catalog=catalog,
            rules=[
                _access_rule_from_dict(rule)
                for rule in self.rules_by_dataset.get((catalog, target), self.rules)
            ],
        )


def make_jwt(
    principal_id: str = "user1",
    *,
    groups: list[str] | None = None,
    tenant_id: str | None = None,
    jwt_secret: str = TEST_JWT_SECRET,
) -> str:
    claims: dict[str, object] = {"sub": principal_id}
    if groups:
        claims["groups"] = list(groups)
    if tenant_id is not None:
        claims["attributes"] = {"tenant_id": tenant_id}
    return jwt.encode(claims, jwt_secret, algorithm="HS256")


def authorization_header(
    principal_id: str = "user1",
    *,
    groups: list[str] | None = None,
    tenant_id: str | None = None,
    jwt_secret: str = TEST_JWT_SECRET,
) -> tuple[bytes, bytes]:
    token = make_jwt(
        principal_id,
        groups=groups,
        tenant_id=tenant_id,
        jwt_secret=jwt_secret,
    )
    return (
        b"authorization",
        f"Bearer {token}".encode(),
    )


def flight_call_options(
    principal_id: str = "user1",
    *,
    groups: list[str] | None = None,
    tenant_id: str | None = None,
    jwt_secret: str = TEST_JWT_SECRET,
) -> flight.FlightCallOptions:
    return flight.FlightCallOptions(
        headers=[
            authorization_header(
                principal_id,
                groups=groups,
                tenant_id=tenant_id,
                jwt_secret=jwt_secret,
            )
        ]
    )


def command_descriptor(payload: dict[str, object]) -> flight.FlightDescriptor:
    return flight.FlightDescriptor.for_command(json.dumps(payload).encode("utf-8"))


def build_flight_service(
    *,
    table_format: TableFormat | None = None,
    catalog_registry: Any | None = None,
    db_session: Session | None = None,
    cell_id: UUID | None = None,
    policy_rules: list[dict[str, object]] | None = None,
    policy_rules_by_dataset: dict[tuple[str | None, str], list[dict[str, object]]] | None = None,
    authorizer: Any | None = None,
    jwt_secret: str = TEST_JWT_SECRET,
    ticket_secret: str = "secret",
    ticket_ttl_seconds: int = 300,
    max_tickets: int = 1,
) -> DataAccessFlightService:
    published_config_requested = db_session is not None or cell_id is not None
    if published_config_requested and (db_session is None or cell_id is None):
        raise ValueError("Provide both db_session and cell_id for published config")
    if published_config_requested and (table_format is not None or catalog_registry is not None):
        raise ValueError("Published config tests must not provide table_format or catalog_registry")
    if not published_config_requested and (table_format is None) == (catalog_registry is None):
        raise ValueError("Provide exactly one of table_format or catalog_registry")

    if published_config_requested:
        config_store = PublishedConfigStore(cast(Session, db_session), cell_id=cast(UUID, cell_id))
        resolved_registry = PublishedConfigCatalogRegistry(config_store)
        resolved_authorizer = PublishedConfigAuthorizer(config_store)
    else:
        resolved_registry = catalog_registry
        if table_format is not None:
            resolved_registry = StubCatalogRegistry(table_format)
        resolved_authorizer = authorizer or InMemoryPolicyAuthorizer(
            catalog="analytics",
            target=getattr(table_format, "table_name", "test.table"),
            rules=policy_rules or _allow_rules(["*"]),
            rules_by_dataset=policy_rules_by_dataset,
        )

    identity = DefaultIdentityAdapter(AuthConfig(jwt_secret=jwt_secret))
    masking = DefaultMaskingAdapter()
    row_transform = DuckDBRowTransformAdapter(masking)
    ticket_codec = HmacTicketCodecAdapter(ticket_secret)
    get_schema = GetSchemaUseCase(
        identity=identity,
        authorizer=resolved_authorizer,
        catalog_registry=cast(Any, resolved_registry),
        masking=masking,
    )
    plan_access = PlanAccessUseCase(
        identity=identity,
        authorizer=resolved_authorizer,
        catalog_registry=cast(Any, resolved_registry),
        masking=masking,
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=ticket_ttl_seconds,
        max_tickets=max_tickets,
    )
    fetch_stream = FetchStreamUseCase(
        identity=identity,
        authorizer=resolved_authorizer,
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
    tenant_id: str | None = None,
    jwt_secret: str = TEST_JWT_SECRET,
) -> tuple[flight.FlightInfo, flight.FlightCallOptions]:
    options = flight_call_options(
        principal_id,
        groups=groups,
        tenant_id=tenant_id,
        jwt_secret=jwt_secret,
    )
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
    tenant_id: str | None = None,
    jwt_secret: str = TEST_JWT_SECRET,
) -> tuple[flight.FlightInfo, pa.Table]:
    info, options = flight_info(
        client,
        payload,
        principal_id=principal_id,
        groups=groups,
        tenant_id=tenant_id,
        jwt_secret=jwt_secret,
    )
    return info, read_table(client, info, options)


def _allow_rules(columns: list[str]) -> list[dict[str, object]]:
    return [{"principals": ["user1"], "columns": columns, "effect": "allow"}]


def _access_rule_from_dict(raw: dict[str, object]) -> AccessRule:
    return AccessRule(
        principals=[str(item) for item in _list(raw.get("principals"))],
        columns=[str(item) for item in _list(raw.get("columns"))],
        masks={
            str(name): MaskRule(
                type=str(_mapping(mask).get("type")),
                value=_mapping(mask).get("value"),
            )
            for name, mask in _mapping(raw.get("masks")).items()
            if isinstance(mask, dict) and _mapping(mask).get("type")
        },
        row_filter=None if raw.get("row_filter") is None else str(raw["row_filter"]),
        effect=cast(Literal["allow", "deny"], str(raw.get("effect", "allow"))),
        when=cast(dict[str, PrincipalConditionValue], _mapping(raw.get("when"))),
    )


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return cast(dict[str, Any], value)
    return {}


def _list(value: object) -> list[object]:
    if isinstance(value, list):
        return list(cast(list[object], value))
    return []
