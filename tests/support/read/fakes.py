"""Read-service fixtures with explicit identities, passive tasks and ticket state."""

from __future__ import annotations

import base64
import json
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Any, cast

import pyarrow as pa

from dal_obscura.adapters.duckdb.compiler import DuckDBProjectionCompiler
from dal_obscura.adapters.duckdb.transform import (
    DuckDBRowTransformAdapter,
)
from dal_obscura.adapters.tickets.signing import HmacTicketCodecAdapter
from dal_obscura.application.reads.execution import ExecutionSupervisor, ManagedStream, StreamGuard
from dal_obscura.application.reads.service import ReadService, ReadSettings
from dal_obscura.application.reads.sources import AccessContext, Plan, ScanTask, Source
from dal_obscura.application.reads.tickets.contracts import StoredTicket
from dal_obscura.application.reads.tickets.payload import (
    ScanPayload,
    TicketPayload,
    TicketReference,
    ticket_payload_hash,
)
from dal_obscura.kernel.identity.authentication import AuthenticationRequest
from dal_obscura.kernel.identity.principal import Principal
from dal_obscura.kernel.plans.request import PlanRequest
from dal_obscura.kernel.policy.authorization import AuthorizationPort
from dal_obscura.kernel.policy.filters import deserialize_row_filter
from dal_obscura.kernel.policy.models import AccessDecision, MaskRule
from tests.support.arrow import id_region_batch, id_region_schema


def scan_payload() -> ScanPayload:
    return {
        "authorization_columns": ["id", "region"],
        "read_payload": "payload",
        "full_row_filter": None,
        "masks": {},
    }


@dataclass(frozen=True, kw_only=True)
class FixtureSource:
    catalog_name: str
    table_name: str
    format: str


@dataclass(frozen=True)
class StubInputPartition:
    payload: bytes


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
            tasks=[
                ScanTask(
                    table_format=self,
                    schema=self.schema,
                    partition=StubInputPartition(payload=b"payload"),
                )
            ],
            full_row_filter=request.row_filter,
            backend_pushdown_row_filter=None,
            residual_row_filter=request.row_filter,
        )

    def execute(self, partition: object, *, context) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        if not isinstance(partition, StubInputPartition):
            raise TypeError("StubTableFormat requires a StubInputPartition")
        return self.schema, iter(self.batches)


@dataclass(frozen=True, kw_only=True)
class TrackingTableFormat(FixtureSource):
    schema: pa.Schema
    planned_columns: list[list[str]]

    def get_schema(self) -> pa.Schema:
        return self.schema

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        del max_tickets
        self.planned_columns.append(list(request.columns))
        return Plan(
            schema=self.schema,
            tasks=[
                ScanTask(
                    table_format=self,
                    schema=self.schema,
                    partition=StubInputPartition(payload=b"payload"),
                )
            ],
            full_row_filter=request.row_filter,
            backend_pushdown_row_filter=None,
            residual_row_filter=request.row_filter,
        )

    def execute(self, partition: object, *, context) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        if not isinstance(partition, StubInputPartition):
            raise TypeError("TrackingTableFormat requires a StubInputPartition")
        return self.schema, iter(())


@dataclass(frozen=True, kw_only=True)
class PretendPushdownTableFormat(FixtureSource):
    schema: pa.Schema
    batches: tuple[pa.RecordBatch, ...]
    backend_pushdown_sql: str | None = None
    residual_sql: str | None = None

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
                    partition=StubInputPartition(payload=b"payload"),
                )
            ],
            full_row_filter=request.row_filter,
            backend_pushdown_row_filter=None
            if self.backend_pushdown_sql is None
            else deserialize_row_filter(self.backend_pushdown_sql),
            residual_row_filter=None
            if self.residual_sql is None
            else deserialize_row_filter(self.residual_sql),
        )

    def execute(self, partition: object, *, context) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        if not isinstance(partition, StubInputPartition):
            raise TypeError("PretendPushdownTableFormat requires a StubInputPartition")
        return self.schema, iter(self.batches)


class FakeIdentity:
    def __init__(self, principal: Principal | None) -> None:
        self._principal = principal

    def authenticate(self, request: AuthenticationRequest) -> Principal:
        del request
        if self._principal is None:
            raise PermissionError("Unauthorized")
        return self._principal


class FakeAuthorizer:
    def __init__(
        self,
        decision: AccessDecision | None,
        current_version: int | None = None,
        asset_id: str | None = "00000000-0000-4000-8000-000000000001",
    ) -> None:
        self._decision = decision
        self._current_version = current_version
        self._asset_id = asset_id
        self.last_requested_columns: list[str] | None = None

    def authorize(self, principal, target, catalog, requested_columns):
        del principal, target, catalog
        self.last_requested_columns = list(requested_columns)
        if self._decision is None:
            raise PermissionError("Unauthorized")
        if self._decision.asset_id is not None:
            return self._decision
        return replace(self._decision, asset_id=self._asset_id)


class FakeCatalogRegistry:
    def __init__(self, table_format: Source) -> None:
        self._table_format = table_format

    def resolve(
        self,
        catalog: str | None,
        target: str,
    ) -> Source:
        del catalog, target
        return self._table_format


class FakeTicketCodec:
    def __init__(self, payload: TicketPayload | None = None) -> None:
        self._payload = payload or TicketPayload(
            asset_id="00000000-0000-4000-8000-000000000001",
            catalog="catalog1",
            target="users",
            columns=["id"],
            scan=scan_payload(),
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
        )
        self.signed_payloads: list[TicketPayload] = []

    def sign_payload(self, payload: TicketPayload) -> str:
        self.signed_payloads.append(payload)
        return "signed-token"

    def verify(self, token: str) -> TicketReference:
        del token
        return TicketReference(
            ticket_id=cast(str, self._payload.ticket_id),
            expires_at=self._payload.expires_at,
            nonce=self._payload.nonce,
        )


class FakeTicketStore:
    def __init__(self) -> None:
        self.stored: list[tuple[TicketPayload, int]] = []
        self.records: dict[str, StoredTicket] = {}
        self.cleanup_calls: list[int] = []
        self.reserve_calls: list[str] = []
        self.revoked: set[str] = set()
        self.fail_store = False

    def store_many(self, payloads, *, max_exchanges: int) -> None:
        if self.fail_store:
            raise RuntimeError("store failed")
        for payload in payloads:
            if payload.ticket_id is None:
                raise AssertionError("ticket_id is required")
            self.stored.append((payload, max_exchanges))
            self.records[payload.ticket_id] = StoredTicket(
                payload=payload,
                payload_hash=ticket_payload_hash(payload),
                exchange_count=0,
                max_exchanges=max_exchanges,
                expires_at=payload.expires_at,
            )

    def load(self, ticket_id: str) -> StoredTicket:
        try:
            return self.records[ticket_id]
        except KeyError as exc:
            raise LookupError("Ticket not found") from exc

    def reserve_exchange(self, ticket_id: str, *, now: int) -> StoredTicket:
        del now
        self.reserve_calls.append(ticket_id)
        self.ensure_active(ticket_id)
        stored = self.load(ticket_id)
        if stored.exchange_count >= stored.max_exchanges:
            raise PermissionError("Ticket expired or exhausted")
        updated = StoredTicket(
            payload=stored.payload,
            payload_hash=stored.payload_hash,
            exchange_count=stored.exchange_count + 1,
            max_exchanges=stored.max_exchanges,
            expires_at=stored.expires_at,
        )
        self.records[ticket_id] = updated
        return updated

    def ensure_active(self, ticket_id: str) -> None:
        if ticket_id in self.revoked or ticket_id not in self.records:
            raise PermissionError("Ticket is revoked or unavailable")

    def cleanup_expired(self, *, now: int) -> int:
        self.cleanup_calls.append(now)
        return 0


class FakeRowTransform:
    def apply_filters_and_masks_stream(self, batches, projection, row_filter):
        del projection, row_filter
        return batches


class FixtureTaskCodec:
    """In-memory test sources use Arrow IPC; production plugins use the real codec."""

    def encode(self, task: ScanTask) -> str:
        if not isinstance(
            task.table_format, (StubTableFormat, TrackingTableFormat, PretendPushdownTableFormat)
        ):
            if task.table_format.__class__.__module__ == "tests.support.flight.flight":
                return self._fixture(task)
            return self._production().encode(task)
        return self._fixture(task)

    def _fixture(self, task: ScanTask) -> str:
        batches = getattr(task.table_format, "batches", ())
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, task.schema) as writer:
            for batch in batches:
                writer.write_batch(batch)
        return json.dumps({"fixture_ipc": base64.b64encode(sink.getvalue()).decode("ascii")})

    def decode(self, payload: str) -> ScanTask:
        try:
            raw = json.loads(payload)
            if not isinstance(raw, dict) or set(raw) != {"fixture_ipc"}:
                return self._production().decode(payload)
            reader = pa.ipc.open_stream(base64.b64decode(raw["fixture_ipc"], validate=True))
            table = StubTableFormat(
                catalog_name="fixture",
                table_name="fixture",
                format="test",
                schema=reader.schema,
                batches=tuple(reader),
            )
            return ScanTask(table, table.schema, StubInputPartition(payload=b"fixture"))
        except (ValueError, TypeError, KeyError) as exc:
            raise ValueError("Invalid read payload in ticket") from exc

    @staticmethod
    def _production():
        from dal_obscura.adapters.providers.task_codec import SourceTaskCodec
        from dal_obscura.plugins.bootstrap import (
            create_plugin_registry,
        )

        return SourceTaskCodec(create_plugin_registry())


def encode_scan_task(table_format: Source, schema: pa.Schema) -> str:
    task = ScanTask(
        table_format=table_format, schema=schema, partition=StubInputPartition(payload=b"payload")
    )
    return FixtureTaskCodec().encode(task)


class _UnusedSource:
    @contextmanager
    def open(self, catalog, target, *, context=None):
        raise AssertionError("This fetch scenario must not discover a source")
        yield


def make_read_service(**options: Any) -> ReadService:
    settings = {
        name: options.pop(name) for name in ReadSettings.__dataclass_fields__ if name in options
    }
    return ReadService(
        execution_supervisor=options.pop("execution_supervisor", ExecutionSupervisor()),
        projection_compiler=options.pop("projection_compiler", DuckDBProjectionCompiler()),
        row_transform=options.pop("row_transform", DuckDBRowTransformAdapter()),
        access_context=options.pop("access_context", _UnusedSource()),
        ticket_codec=options.pop("ticket_codec", FakeTicketCodec()),
        ticket_store=options.pop("ticket_store", FakeTicketStore()),
        settings=ReadSettings(**settings),
        task_codec=options.pop("task_codec", FixtureTaskCodec()),
        **options,
    )


@dataclass(frozen=True)
class StaticAccessContext:
    """Composition for immutable catalogs/policies, including in-memory fixtures.

    Mutable live configuration must implement AccessContextPort with a shared
    snapshot instead of independently querying these two ports.
    """

    authorizer: AuthorizationPort
    catalog_registry: Any

    @contextmanager
    def open(self, catalog: str | None, target: str, *, context=None) -> Generator[AccessContext]:
        table = self.catalog_registry.resolve(catalog, target)
        yield AccessContext(table, self.authorizer, table.get_schema())


def expiry_stream(batches, *, ticket_expires_at, identity_expires_at, stream_deadline_at, now):
    return ManagedStream(
        batches,
        guard=StreamGuard(
            ticket_expires_at,
            identity_expires_at,
            stream_deadline_at,
            now,
            lambda: None,
        ).check_disclosure,
    )


def ticket_payload(**changes: Any) -> TicketPayload:
    base = TicketPayload(
        asset_id="00000000-0000-4000-8000-000000000001",
        catalog="catalog1",
        target="users",
        columns=["id", "region"],
        scan=scan_payload(),
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="abc",
    )
    return replace(base, **changes)


AUTHORIZATION_HEADER = AuthenticationRequest(headers={"authorization": "Bearer jwt-token"})


def governed_read_service(table_format: Source, decision: AccessDecision, **options):
    ticket_codec = HmacTicketCodecAdapter("secret")
    ticket_store = FakeTicketStore()

    authorizer = FakeAuthorizer(decision=decision, current_version=decision.policy_version)
    catalog_registry = FakeCatalogRegistry(table_format)
    principal = Principal(id="user1", groups=[], attributes={})
    return make_read_service(
        identity=FakeIdentity(principal=principal),
        access_context=StaticAccessContext(
            authorizer=authorizer, catalog_registry=cast(Any, catalog_registry)
        ),
        ticket_codec=ticket_codec,
        ticket_store=ticket_store,
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
        **options,
    )


def _build_use_case_dependencies():
    schema = id_region_schema()
    batches = (id_region_batch([1], ["us"]),)
    decision = AccessDecision(
        allowed_columns=["id", "region"],
        masks={"region": MaskRule(type="redact", value="***")},
        row_filter="region = 'us'",
        policy_version=100,
    )
    table_format = StubTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        batches=batches,
    )
    return schema, decision, table_format


def _ticket_store_with(payload: TicketPayload, *, max_exchanges: int = 1) -> FakeTicketStore:
    ticket_store = FakeTicketStore()
    ticket_store.store_many([payload], max_exchanges=max_exchanges)
    return ticket_store
