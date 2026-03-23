import base64
import pickle
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, cast

import pyarrow as pa
import pytest

from dal_obscura.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.application.use_cases.plan_access import PlanAccessUseCase
from dal_obscura.domain.access_control.models import AccessDecision, MaskRule, Principal
from dal_obscura.domain.catalog.ports import ResolvedTable
from dal_obscura.domain.format_handler.ports import InputPartition, Plan, ScanTask
from dal_obscura.domain.query_planning.models import PlanRequest
from dal_obscura.domain.ticket_delivery.models import ScanPayload, TicketPayload

AUTHORIZATION_HEADER = {"authorization": "Bearer jwt-token"}


def _scan_payload() -> ScanPayload:
    return {"read_payload": "payload", "row_filter": None, "masks": {}}


@dataclass(frozen=True, kw_only=True)
class StubResolvedTable(ResolvedTable):
    pass


@dataclass(frozen=True)
class StubInputPartition(InputPartition):
    payload: bytes
    _table: ResolvedTable | None = None

    @property
    def table(self) -> ResolvedTable:
        return self._table or StubResolvedTable(
            catalog_name="",
            table_name="test",
            format="test",
        )


class FakeIdentity:
    def __init__(self, principal: Principal | None) -> None:
        self._principal = principal

    def authenticate(self, headers):
        if self._principal is None:
            raise PermissionError("Unauthorized")
        return self._principal


class FakeAuthorizer:
    def __init__(self, decision: AccessDecision | None, current_version: int | None = None) -> None:
        self._decision = decision
        self._current_version = current_version
        self.last_requested_columns: list[str] | None = None

    def authorize(self, principal, target, catalog, requested_columns):
        self.last_requested_columns = list(requested_columns)
        if self._decision is None:
            raise PermissionError("Unauthorized")
        return self._decision

    def current_policy_version(self, target, catalog):
        return self._current_version


class FakeCatalogRegistry:
    def __init__(self, table: ResolvedTable) -> None:
        self._table = table

    def describe(self, catalog: str | None, target: str) -> ResolvedTable:
        return self._table


class FakeFormatHandler:
    def __init__(self, schema: pa.Schema, plan: Plan) -> None:
        self._schema = schema
        self._plan = plan

    @property
    def supported_format(self):
        return "fake_format"

    def get_schema(self, table: ResolvedTable) -> pa.Schema:
        return self._schema

    def plan(self, table: ResolvedTable, request: PlanRequest, max_tickets: int) -> Plan:
        return self._plan

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        return self._schema, iter(
            [
                pa.record_batch(
                    [pa.array([1]), pa.array(["us"])],
                    schema=pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())]),
                )
            ]
        )


class FakeFormatRegistry:
    def __init__(self, handler: FakeFormatHandler):
        self._handler = handler

    def get_handler(self, format: str):
        return self._handler


class FakeMasking:
    def apply(self, columns, masks):
        return None

    def masked_schema(self, base_schema, columns, masks):
        return base_schema


class FakeTicketCodec:
    def __init__(self, payload: TicketPayload) -> None:
        self._payload = payload
        self.signed_payloads: list[TicketPayload] = []

    def sign_payload(self, payload: TicketPayload) -> str:
        self.signed_payloads.append(payload)
        return "signed-token"

    def verify(self, token: str) -> TicketPayload:
        return self._payload


class FakeRowTransform:
    def apply_filters_and_masks_stream(self, batches, columns, row_filter, masks):
        return batches


def _build_use_case_dependencies():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    read_payload = StubInputPartition(payload=b"payload")
    plan = Plan(
        schema=schema, tasks=[ScanTask(format="fake_format", schema=schema, partition=read_payload)]
    )
    decision = AccessDecision(
        allowed_columns=["id", "region"],
        masks={"region": MaskRule(type="redact", value="***")},
        row_filter="region = 'us'",
        policy_version=100,
    )
    resolved_table = StubResolvedTable(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
    )
    return schema, plan, decision, resolved_table


def test_plan_access_auth_failure():
    schema, plan, decision, resolved_table = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(resolved_table)
    format_registry = FakeFormatRegistry(FakeFormatHandler(schema, plan))
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            catalog="catalog1",
            target="users",
            columns=["id", "region"],
            scan=_scan_payload(),
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
            format="fake_format",
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=None),
        authorizer=authorizer,
        catalog_registry=cast(Any, catalog_registry),
        format_registry=cast(Any, format_registry),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    with pytest.raises(PermissionError):
        use_case.execute(PlanRequest(catalog="catalog1", target="users", columns=["id"]), {})


def test_plan_access_authz_failure():
    schema, plan, _, resolved_table = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(resolved_table)
    format_registry = FakeFormatRegistry(FakeFormatHandler(schema, plan))
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            catalog="catalog1",
            target="users",
            columns=["id", "region"],
            scan=_scan_payload(),
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
            format="fake_format",
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=None),
        catalog_registry=cast(Any, catalog_registry),
        format_registry=cast(Any, format_registry),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    with pytest.raises(PermissionError):
        use_case.execute(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]),
            AUTHORIZATION_HEADER,
        )


def test_plan_access_expands_wildcard_columns():
    schema, plan, decision, resolved_table = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(resolved_table)
    format_registry = FakeFormatRegistry(FakeFormatHandler(schema, plan))
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            catalog="catalog1",
            target="users",
            columns=["id", "region"],
            scan=_scan_payload(),
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
            format="fake_format",
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=authorizer,
        catalog_registry=cast(Any, catalog_registry),
        format_registry=cast(Any, format_registry),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
    )
    use_case.execute(
        PlanRequest(catalog="catalog1", target="users", columns=["*"]),
        AUTHORIZATION_HEADER,
    )

    assert authorizer.last_requested_columns == ["id", "region"]


def test_fetch_stream_policy_version_mismatch():
    schema, plan, decision, _ = _build_use_case_dependencies()
    format_registry = FakeFormatRegistry(FakeFormatHandler(schema, plan))
    payload = TicketPayload(
        catalog="catalog1",
        target="users",
        columns=["id", "region"],
        scan={
            "read_payload": base64.b64encode(pickle.dumps(StubInputPartition(b"payload"))).decode(
                "utf-8"
            ),
            "row_filter": None,
            "masks": {},
        },
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="abc",
        format="fake_format",
    )
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, current_version=101),
        format_registry=cast(Any, format_registry),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(PermissionError):
        use_case.execute("token", AUTHORIZATION_HEADER)


def test_fetch_stream_principal_mismatch():
    schema, plan, decision, _ = _build_use_case_dependencies()
    format_registry = FakeFormatRegistry(FakeFormatHandler(schema, plan))
    payload = TicketPayload(
        catalog="catalog1",
        target="users",
        columns=["id", "region"],
        scan={
            "read_payload": base64.b64encode(pickle.dumps(StubInputPartition(b"payload"))).decode(
                "utf-8"
            ),
            "row_filter": None,
            "masks": {},
        },
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="abc",
        format="fake_format",
    )
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user2", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, current_version=100),
        format_registry=cast(Any, format_registry),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(PermissionError):
        use_case.execute("token", AUTHORIZATION_HEADER)


def test_fetch_stream_rejects_invalid_mask_payload():
    schema, plan, decision, _ = _build_use_case_dependencies()
    format_registry = FakeFormatRegistry(FakeFormatHandler(schema, plan))
    payload = TicketPayload(
        catalog="catalog1",
        target="users",
        columns=["id", "region"],
        scan=cast(
            ScanPayload,
            {
                "read_payload": base64.b64encode(
                    pickle.dumps(StubInputPartition(b"payload"))
                ).decode("utf-8"),
                "row_filter": None,
                "masks": {"region": "not-an-object"},
            },
        ),
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="abc",
        format="fake_format",
    )
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, current_version=100),
        format_registry=cast(Any, format_registry),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(ValueError, match="Invalid mask payload"):
        use_case.execute("token", AUTHORIZATION_HEADER)
