from __future__ import annotations

import threading
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Literal, cast

import jwt
import pyarrow as pa
import pyarrow.flight as flight
from sqlalchemy.orm import Session

from dal_obscura.identity.contracts import AuthenticationRequest
from dal_obscura.interfaces.flight.server import DataAccessFlightService
from dal_obscura.interfaces.flight_contract import encode_plan_command_from_mapping
from dal_obscura.policy.models import (
    AccessDecision,
    AccessRule,
    DatasetPolicy,
    MaskRule,
    Policy,
    Principal,
    PrincipalConditionValue,
)
from dal_obscura.policy.policy_resolution import resolve_access
from dal_obscura.read.request import PlanRequest
from dal_obscura.read.signing import HmacTicketCodecAdapter
from dal_obscura.read.transform import (
    DuckDBRowTransformAdapter,
)
from dal_obscura.sources.contracts import Source
from dal_obscura.sources.planning import Plan, ScanTask
from dal_obscura.sources.published import LiveConfigCatalogRegistry
from dal_obscura.sources.task_codec import ScanTaskCodec
from dal_obscura.storage.database.db import session_factory
from dal_obscura.storage.snapshots import LiveConfigStore
from tests.support.reads import StaticAccessContext, make_read_service
from tests.support.use_cases import FakeTicketStore

TEST_JWT_SECRET = "test-jwt-secret-32-characters-long"


class TestJwtIdentity:
    """Test-only HS256 identity fixture; production accepts OIDC/JWKS only."""

    def __init__(self, secret: str) -> None:
        self._secret = secret

    def authenticate(self, request: AuthenticationRequest) -> Principal:
        header = request.header("authorization")
        if not header or not header.lower().startswith("bearer "):
            raise PermissionError("Missing token")
        try:
            payload = jwt.decode(header[7:].strip(), self._secret, algorithms=["HS256"])
        except jwt.PyJWTError as exc:
            raise PermissionError("Invalid token") from exc
        subject = payload.get("sub") or payload.get("principal")
        if not isinstance(subject, str) or not subject:
            raise PermissionError("Invalid token")
        groups = payload.get("groups", [])
        attributes = payload.get("attrs", payload.get("attributes", {}))
        if not isinstance(groups, list) or not isinstance(attributes, dict):
            raise PermissionError("Invalid token")
        return Principal(
            id=subject,
            groups=[str(group) for group in groups],
            attributes={str(key): str(value) for key, value in attributes.items()},
        )


@dataclass(frozen=True, kw_only=True)
class FixtureSource:
    catalog_name: str
    table_name: str
    format: str


@dataclass(frozen=True, kw_only=True)
class StubInputPartition:
    payload: bytes = b"payload"


@dataclass(frozen=True, kw_only=True)
class StubTableFormat(FixtureSource):
    schema: pa.Schema
    batches: tuple[pa.RecordBatch, ...]

    def get_schema(self) -> pa.Schema:
        return self.schema

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        del max_tickets
        return Plan(
            schema=self.schema,
            tasks=[ScanTask(table_format=self, schema=self.schema, partition=StubInputPartition())],
            full_row_filter=request.row_filter,
            backend_pushdown_row_filter=None,
            residual_row_filter=request.row_filter,
        )

    def execute(self, partition: object) -> tuple[pa.Schema, Iterable[Any]]:
        if not isinstance(partition, StubInputPartition):
            raise TypeError("StubTableFormat requires a StubInputPartition")
        return self.schema, iter(self.batches)


class StubCatalogRegistry:
    def __init__(self, table_format: Source) -> None:
        self._table_format = table_format

    def describe(
        self,
        catalog: str | None,
        target: str,
    ) -> Source:
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
            policy, principal, target, catalog, requested_columns
        )
        return AccessDecision(
            allowed_columns=allowed_columns,
            masks=masks,
            row_filter=row_filter,
            policy_version=1,
            asset_id="00000000-0000-4000-8000-000000000001",
        )

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
    token = make_jwt(principal_id, groups=groups, jwt_secret=jwt_secret)
    return (
        b"authorization",
        f"Bearer {token}".encode(),
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
    return flight.FlightDescriptor.for_command(encode_plan_command_from_mapping(payload))


def build_flight_service(
    *,
    table_format: Source | None = None,
    catalog_registry: Any | None = None,
    db_session: Session | None = None,
    policy_rules: list[dict[str, object]] | None = None,
    policy_rules_by_dataset: dict[tuple[str | None, str], list[dict[str, object]]] | None = None,
    authorizer: Any | None = None,
    jwt_secret: str = TEST_JWT_SECRET,
    ticket_secret: str = "secret",
    ticket_ttl_seconds: int = 300,
    max_tickets: int = 1,
    max_ticket_exchanges: int = 1,
    task_codec: ScanTaskCodec | None = None,
) -> DataAccessFlightService:
    live_config_requested = db_session is not None
    if live_config_requested and (table_format is not None or catalog_registry is not None):
        raise ValueError("Live config tests must not provide table_format or catalog_registry")
    if not live_config_requested and (table_format is None) == (catalog_registry is None):
        raise ValueError("Provide exactly one of table_format or catalog_registry")

    if live_config_requested:
        config_store = LiveConfigStore(session_factory(cast(Session, db_session).get_bind().engine))
        resolved_registry = LiveConfigCatalogRegistry(config_store)
        resolved_authorizer = None
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

    access_context = (
        resolved_registry
        if live_config_requested
        else StaticAccessContext(
            authorizer=cast(Any, resolved_authorizer), catalog_registry=cast(Any, resolved_registry)
        )
    )

    identity = TestJwtIdentity(jwt_secret)

    row_transform = DuckDBRowTransformAdapter()
    ticket_codec = HmacTicketCodecAdapter(ticket_secret)
    ticket_store = FakeTicketStore()
    reads = make_read_service(
        identity=identity,
        access_context=access_context,
        row_transform=row_transform,
        ticket_codec=ticket_codec,
        ticket_store=ticket_store,
        ticket_ttl_seconds=ticket_ttl_seconds,
        max_tickets=max_tickets,
        max_ticket_exchanges=max_ticket_exchanges,
        **({"task_codec": task_codec} if task_codec is not None else {}),
    )
    return DataAccessFlightService(location="grpc+tcp://127.0.0.1:0", reads=reads)


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
        client, payload, principal_id=principal_id, groups=groups, jwt_secret=jwt_secret
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
                exempt_principals=tuple(
                    str(item) for item in _list(_mapping(mask).get("exempt_principals"))
                ),
            )
            for name, mask in _mapping(raw.get("masks")).items()
            if isinstance(mask, dict) and _mapping(mask).get("type")
        },
        row_filter=None if raw.get("row_filter") is None else str(raw["row_filter"]),
        effect=cast(Literal["allow", "allow_all"], str(raw.get("effect", "allow"))),
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
