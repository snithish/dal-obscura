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
from dal_obscura.domain.catalog.ports import TableFormat
from dal_obscura.domain.query_planning.models import PlanRequest
from dal_obscura.domain.table_format.ports import InputPartition, Plan, ScanTask
from dal_obscura.domain.ticket_delivery.models import ScanPayload, TicketPayload

AUTHORIZATION_HEADER = {"authorization": "Bearer jwt-token"}


def _scan_payload() -> ScanPayload:
    return {"read_payload": "payload", "row_filter": None, "masks": {}}


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
        del request, max_tickets
        return Plan(
            schema=self.schema,
            tasks=[
                ScanTask(
                    table_format=self,
                    schema=self.schema,
                    partition=StubInputPartition(payload=b"payload"),
                )
            ],
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
        )

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        if not isinstance(partition, StubInputPartition):
            raise TypeError("TrackingTableFormat requires a StubInputPartition")
        return self.schema, iter(())


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
    def __init__(self, table_format: TableFormat) -> None:
        self._table_format = table_format

    def describe(self, catalog: str | None, target: str) -> TableFormat:
        del catalog, target
        return self._table_format


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
        del token
        return self._payload


class FakeRowTransform:
    def apply_filters_and_masks_stream(self, batches, columns, row_filter, masks):
        return batches


def _build_use_case_dependencies():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batches = (
        pa.record_batch(
            [pa.array([1]), pa.array(["us"])],
            schema=pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())]),
        ),
    )
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


def _encode_scan_task(table_format: TableFormat, schema: pa.Schema) -> str:
    task = ScanTask(
        table_format=table_format,
        schema=schema,
        partition=StubInputPartition(payload=b"payload"),
    )
    return base64.b64encode(pickle.dumps(task)).decode("utf-8")


def test_plan_access_auth_failure():
    _schema, decision, table_format = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(table_format)
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
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=None),
        authorizer=authorizer,
        catalog_registry=cast(Any, catalog_registry),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    with pytest.raises(PermissionError):
        use_case.execute(PlanRequest(catalog="catalog1", target="users", columns=["id"]), {})


def test_plan_access_authz_failure():
    _schema, _, table_format = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(table_format)
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
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=None),
        catalog_registry=cast(Any, catalog_registry),
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
    _schema, decision, table_format = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(table_format)
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
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=authorizer,
        catalog_registry=cast(Any, catalog_registry),
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


def test_plan_access_accepts_nested_requested_columns():
    schema = pa.schema(
        [
            pa.field(
                "user",
                pa.struct(
                    [
                        pa.field(
                            "address",
                            pa.struct([pa.field("zip", pa.int64())]),
                        )
                    ]
                ),
            )
        ]
    )
    table_format = StubTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        batches=(),
    )
    decision = AccessDecision(
        allowed_columns=["user.address.zip"],
        masks={"user.address.zip": MaskRule(type="hash")},
        row_filter=None,
        policy_version=100,
    )
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            catalog="catalog1",
            target="users",
            columns=["user.address.zip"],
            scan=_scan_payload(),
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=authorizer,
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    use_case.execute(
        PlanRequest(catalog="catalog1", target="users", columns=["user.address.zip"]),
        AUTHORIZATION_HEADER,
    )

    assert authorizer.last_requested_columns == ["user.address.zip"]


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Phase 1: planner still uses only the visible client projection and omits "
        "hidden policy dependencies"
    ),
)
def test_plan_access_includes_hidden_row_filter_dependency_columns_in_execution_plan():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    planned_columns: list[list[str]] = []
    table_format = TrackingTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        planned_columns=planned_columns,
    )
    decision = AccessDecision(
        allowed_columns=["id"],
        masks={},
        row_filter="region = 'us'",
        policy_version=100,
    )
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            catalog="catalog1",
            target="users",
            columns=["id"],
            scan=_scan_payload(),
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=authorizer,
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    result = use_case.execute(
        PlanRequest(catalog="catalog1", target="users", columns=["id"]),
        AUTHORIZATION_HEADER,
    )

    assert authorizer.last_requested_columns == ["id"]
    assert result.columns == ["id"]
    assert planned_columns == [["id", "region"]]


def test_fetch_stream_policy_version_mismatch():
    schema, decision, table_format = _build_use_case_dependencies()
    payload = TicketPayload(
        catalog="catalog1",
        target="users",
        columns=["id", "region"],
        scan={
            "read_payload": _encode_scan_task(table_format, schema),
            "row_filter": None,
            "masks": {},
        },
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="abc",
    )
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, current_version=101),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(PermissionError):
        use_case.execute("token", AUTHORIZATION_HEADER)


def test_fetch_stream_principal_mismatch():
    schema, decision, table_format = _build_use_case_dependencies()
    payload = TicketPayload(
        catalog="catalog1",
        target="users",
        columns=["id", "region"],
        scan={
            "read_payload": _encode_scan_task(table_format, schema),
            "row_filter": None,
            "masks": {},
        },
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="abc",
    )
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user2", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, current_version=100),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(PermissionError):
        use_case.execute("token", AUTHORIZATION_HEADER)


def test_fetch_stream_rejects_invalid_mask_payload():
    schema, decision, table_format = _build_use_case_dependencies()
    payload = TicketPayload(
        catalog="catalog1",
        target="users",
        columns=["id", "region"],
        scan=cast(
            ScanPayload,
            {
                "read_payload": _encode_scan_task(table_format, schema),
                "row_filter": None,
                "masks": {"region": "not-an-object"},
            },
        ),
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="abc",
    )
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, current_version=100),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(ValueError, match="Invalid mask payload"):
        use_case.execute("token", AUTHORIZATION_HEADER)


def test_fetch_stream_rejects_legacy_partition_payload():
    _, decision, _ = _build_use_case_dependencies()
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
    )
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, current_version=100),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(ValueError, match="Invalid read payload"):
        use_case.execute("token", AUTHORIZATION_HEADER)
