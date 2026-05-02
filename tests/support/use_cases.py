from __future__ import annotations

import base64
import pickle
from collections.abc import Iterable
from dataclasses import dataclass

import pyarrow as pa

from dal_obscura.common.access_control.filters import deserialize_row_filter
from dal_obscura.common.access_control.models import AccessDecision, Principal
from dal_obscura.common.catalog.ports import TableFormat
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.table_format.ports import InputPartition, Plan, ScanTask
from dal_obscura.common.ticket_delivery.models import ScanPayload, TicketPayload
from dal_obscura.data_plane.application.ports.identity import AuthenticationRequest


def scan_payload() -> ScanPayload:
    return {"read_payload": "payload", "full_row_filter": None, "masks": {}}


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
    def __init__(self, decision: AccessDecision | None, current_version: int | None = None) -> None:
        self._decision = decision
        self._current_version = current_version
        self.last_requested_columns: list[str] | None = None
        self.last_current_version_tenant_id: str | None = None

    def authorize(self, principal, target, catalog, requested_columns):
        del principal, target, catalog
        self.last_requested_columns = list(requested_columns)
        if self._decision is None:
            raise PermissionError("Unauthorized")
        return self._decision

    def current_policy_version(self, target, catalog, *, tenant_id):
        del target, catalog
        self.last_current_version_tenant_id = tenant_id
        return self._current_version


class FakeCatalogRegistry:
    def __init__(self, table_format: TableFormat) -> None:
        self._table_format = table_format
        self.last_tenant_id: str | None = None

    def describe(self, catalog: str | None, target: str, *, tenant_id: str) -> TableFormat:
        del catalog, target
        self.last_tenant_id = tenant_id
        return self._table_format


class FakeMasking:
    def apply(self, base_schema, columns, masks):
        del base_schema, columns, masks

    def masked_schema(self, base_schema, columns, masks):
        del columns, masks
        return base_schema


class FakeTicketCodec:
    def __init__(self, payload: TicketPayload | None = None) -> None:
        self._payload = payload or TicketPayload(
            catalog="catalog1",
            target="users",
            tenant_id="default",
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

    def verify(self, token: str) -> TicketPayload:
        del token
        return self._payload


class FakeRowTransform:
    def apply_filters_and_masks_stream(self, batches, columns, row_filter, masks):
        del columns, row_filter, masks
        return batches


def encode_scan_task(table_format: TableFormat, schema: pa.Schema) -> str:
    task = ScanTask(
        table_format=table_format,
        schema=schema,
        partition=StubInputPartition(payload=b"payload"),
    )
    return base64.b64encode(pickle.dumps(task)).decode("utf-8")
