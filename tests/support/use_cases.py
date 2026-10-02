from __future__ import annotations

import base64
import json
from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import cast

import pyarrow as pa

from dal_obscura.identity.contracts import AuthenticationRequest
from dal_obscura.policy.filters import deserialize_row_filter
from dal_obscura.policy.models import AccessDecision, Principal
from dal_obscura.read.request import PlanRequest
from dal_obscura.read.ticket_repository import StoredTicket
from dal_obscura.read.tickets import (
    ScanPayload,
    TicketPayload,
    TicketReference,
    ticket_payload_hash,
)
from dal_obscura.sources.contracts import TableFormat
from dal_obscura.sources.planning import InputPartition, Plan, ScanTask


def scan_payload() -> ScanPayload:
    return {
        "authorization_columns": ["id", "region"],
        "read_payload": "payload",
        "full_row_filter": None,
        "masks": {},
    }


@dataclass(frozen=True)
class StubInputPartition(InputPartition):
    payload: bytes


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
                    partition=StubInputPartition(payload=b"payload"),
                )
            ],
            full_row_filter=request.row_filter,
            backend_pushdown_row_filter=None,
            residual_row_filter=request.row_filter,
        )

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        if not isinstance(partition, StubInputPartition):
            raise TypeError("StubTableFormat requires a StubInputPartition")
        return self.schema, iter(self.batches)


@dataclass(frozen=True, kw_only=True)
class TrackingTableFormat(TableFormat):
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

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        if not isinstance(partition, StubInputPartition):
            raise TypeError("TrackingTableFormat requires a StubInputPartition")
        return self.schema, iter(())


@dataclass(frozen=True, kw_only=True)
class PretendPushdownTableFormat(TableFormat):
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

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
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
    def __init__(self, table_format: TableFormat) -> None:
        self._table_format = table_format

    def describe(
        self,
        catalog: str | None,
        target: str,
    ) -> TableFormat:
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

    def store(self, payload: TicketPayload, *, max_exchanges: int) -> None:
        if self.fail_store:
            raise RuntimeError("store failed")
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

    def store_many(self, payloads, *, max_exchanges: int) -> None:
        payloads = list(payloads)
        if self.fail_store:
            raise RuntimeError("store failed")
        for payload in payloads:
            self.store(payload, max_exchanges=max_exchanges)

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
    def apply_filters_and_masks_stream(self, batches, columns, row_filter, masks):
        del columns, row_filter, masks
        return batches


class FixtureTaskCodec:
    """In-memory test sources use Arrow IPC; production plugins use the real codec."""

    def encode(self, task: ScanTask) -> str:
        if not isinstance(
            task.table_format, (StubTableFormat, TrackingTableFormat, PretendPushdownTableFormat)
        ):
            if task.table_format.__class__.__module__ == "tests.support.flight":
                return self._fixture(task)
            return self._production().encode(public_native_scan(task))
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
        from dal_obscura.sources.builtins import (
            create_builtin_plugin_registry,
        )
        from dal_obscura.sources.task_codec import SourceTaskCodec

        return SourceTaskCodec(create_builtin_plugin_registry())


def encode_scan_task(table_format: TableFormat, schema: pa.Schema) -> str:
    task = ScanTask(
        table_format=table_format, schema=schema, partition=StubInputPartition(payload=b"payload")
    )
    return FixtureTaskCodec().encode(task)


def public_native_scan(task: ScanTask) -> ScanTask:
    """Native executor fixtures enter the same SDK envelope as production sources."""
    from dal_obscura_plugin_api import TableHandle

    from dal_obscura.sources.iceberg import IcebergInputPartition, IcebergTableFormat
    from dal_obscura.sources.iceberg_plugin import IcebergFormatPlugin
    from dal_obscura.sources.plugin_runtime import (
        PublicPluginPartition,
        PublicPluginTableFormat,
        _projected_schema,
        _table_identifier,
    )

    table, partition = task.table_format, task.partition
    if not isinstance(table, IcebergTableFormat) or not isinstance(
        partition, IcebergInputPartition
    ):
        return task
    handle = TableHandle(
        catalog_plugin_id="iceberg.sql",
        catalog_instance_id=table.catalog_name,
        catalog_revision=0,
        identifier=_table_identifier(table.table_name),
        format_plugin_id="iceberg",
        handle_version=1,
        metadata={"metadata_location": table.metadata_location, "io_options": table.io_options},
    )
    source = PublicPluginTableFormat(
        catalog_name=table.catalog_name,
        table_name=table.table_name,
        format="iceberg",
        handle=handle,
        format_factory=IcebergFormatPlugin,
        path_roots=() if table.path_enforcer is None else table.path_enforcer.roots,
    )
    return ScanTask(
        source,
        task.schema,
        PublicPluginPartition(
            task=IcebergFormatPlugin._encode_partition(partition),
            handle=handle,
            format_factory=IcebergFormatPlugin,
            schema=_projected_schema(task.schema, partition.columns),
        ),
    )
