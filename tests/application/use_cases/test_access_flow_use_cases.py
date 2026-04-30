import base64
import pickle
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, cast

import pyarrow as pa
import pytest

from dal_obscura.common.access_control.filters import deserialize_row_filter, row_filter_to_sql
from dal_obscura.common.access_control.models import AccessDecision, MaskRule, Principal
from dal_obscura.common.catalog.ports import TableFormat
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.table_format.ports import InputPartition, Plan, ScanTask
from dal_obscura.common.ticket_delivery.models import ScanPayload, TicketPayload
from dal_obscura.data_plane.application.ports.identity import AuthenticationRequest
from dal_obscura.data_plane.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.data_plane.application.use_cases.plan_access import PlanAccessUseCase
from dal_obscura.data_plane.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)
from dal_obscura.data_plane.infrastructure.adapters.ticket_hmac import HmacTicketCodecAdapter

AUTHORIZATION_HEADER = AuthenticationRequest(headers={"authorization": "Bearer jwt-token"})


def _scan_payload() -> ScanPayload:
    return {"read_payload": "payload", "full_row_filter": None, "masks": {}}


def test_ticket_payload_from_dict_keeps_string_full_row_filter():
    payload = TicketPayload.from_dict(
        {
            "catalog": "catalog1",
            "target": "users",
            "columns": ["id"],
            "scan": {
                "read_payload": "payload",
                "full_row_filter": "LOWER(region) = 'us'",
                "masks": {},
            },
            "policy_version": 100,
            "principal_id": "user1",
            "expires_at": 9999999999,
            "nonce": "abc",
        }
    )

    assert payload.scan["full_row_filter"] == "LOWER(region) = 'us'"


def test_ticket_payload_from_dict_rejects_non_string_full_row_filter():
    payload = TicketPayload.from_dict(
        {
            "catalog": "catalog1",
            "target": "users",
            "columns": ["id"],
            "scan": {
                "read_payload": "payload",
                "full_row_filter": {"type": "comparison"},
                "masks": {},
            },
            "policy_version": 100,
            "principal_id": "user1",
            "expires_at": 9999999999,
            "nonce": "abc",
        }
    )

    assert payload.scan["full_row_filter"] is None


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
        return None

    def masked_schema(self, base_schema, columns, masks):
        return base_schema


class FakeTicketCodec:
    def __init__(self, payload: TicketPayload | None = None) -> None:
        self._payload = payload or TicketPayload(
            catalog="catalog1",
            target="users",
            tenant_id="default",
            columns=["id"],
            scan=_scan_payload(),
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
        return batches


def _build_end_to_end_access_flow(table_format: TableFormat, decision: AccessDecision):
    ticket_codec = HmacTicketCodecAdapter("secret")
    masking = DefaultMaskingAdapter()
    authorizer = FakeAuthorizer(decision=decision, current_version=decision.policy_version)
    catalog_registry = FakeCatalogRegistry(table_format)
    principal = Principal(id="user1", groups=[], attributes={})
    plan_access = PlanAccessUseCase(
        identity=FakeIdentity(principal=principal),
        authorizer=authorizer,
        catalog_registry=cast(Any, catalog_registry),
        masking=masking,
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
    )
    fetch_stream = FetchStreamUseCase(
        identity=FakeIdentity(principal=principal),
        authorizer=authorizer,
        masking=masking,
        row_transform=DuckDBRowTransformAdapter(masking),
        ticket_codec=ticket_codec,
    )
    return plan_access, fetch_stream


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


def test_plan_access_resolves_catalog_with_tenant_from_principal_attributes():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    table_format = TrackingTableFormat(
        catalog_name="analytics",
        table_name="default.users",
        format="fake_format",
        schema=schema,
        planned_columns=[],
    )
    catalog_registry = FakeCatalogRegistry(table_format)
    authorizer = FakeAuthorizer(
        decision=AccessDecision(
            allowed_columns=["id", "region"],
            masks={},
            row_filter=None,
            policy_version=100,
        ),
        current_version=100,
    )
    ticket_codec = FakeTicketCodec()
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(
            principal=Principal(id="user1", groups=[], attributes={"tenant_id": "tenant-a"})
        ),
        authorizer=authorizer,
        catalog_registry=cast(Any, catalog_registry),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
        now=lambda: 1000,
        nonce_factory=lambda: "nonce",
    )

    use_case.execute(
        PlanRequest(catalog="analytics", target="default.users", columns=["id"]),
        AUTHORIZATION_HEADER,
    )

    assert catalog_registry.last_tenant_id == "tenant-a"
    assert ticket_codec.signed_payloads[0].tenant_id == "tenant-a"


def test_fetch_stream_checks_policy_version_for_ticket_tenant():
    schema, _, table_format = _build_use_case_dependencies()
    payload = TicketPayload(
        catalog="analytics",
        target="default.users",
        tenant_id="tenant-a",
        columns=["id"],
        scan={
            "read_payload": _encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="nonce",
    )
    authorizer = FakeAuthorizer(decision=None, current_version=101)
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(
            principal=Principal(id="user1", groups=[], attributes={"tenant_id": "tenant-a"})
        ),
        authorizer=authorizer,
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(PermissionError):
        use_case.execute("ticket", AUTHORIZATION_HEADER)

    assert authorizer.last_current_version_tenant_id == "tenant-a"


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
        use_case.execute(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]),
            AuthenticationRequest(),
        )


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
    assert ticket_codec.signed_payloads[0].scan["full_row_filter"] == "region = 'us'"
    assert ticket_codec.signed_payloads[0].scan["full_row_filter"] is not None


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


def test_plan_access_rejects_unknown_requested_columns():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "user",
                pa.struct([pa.field("address", pa.struct([pa.field("zip", pa.int64())]))]),
            ),
        ]
    )
    table_format = StubTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        batches=(),
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id", "user.address.zip"],
                masks={},
                row_filter=None,
                policy_version=100,
            )
        ),
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(
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
        ),
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    for requested_columns in (["missing"], ["id.value"], ["user.missing"]):
        with pytest.raises(ValueError, match="Unknown columns requested"):
            use_case.execute(
                PlanRequest(catalog="catalog1", target="users", columns=requested_columns),
                AUTHORIZATION_HEADER,
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


def test_plan_access_rejects_requested_row_filter_for_masked_column():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id", "region"],
                masks={"region": MaskRule(type="redact", value="***")},
                row_filter=None,
                policy_version=100,
            )
        ),
        catalog_registry=cast(
            Any,
            FakeCatalogRegistry(
                TrackingTableFormat(
                    catalog_name="catalog1",
                    table_name="users",
                    format="fake_format",
                    schema=schema,
                    planned_columns=[],
                )
            ),
        ),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(
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
        ),
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    with pytest.raises(
        PermissionError,
        match="Requested row filter may not reference masked columns: region",
    ):
        use_case.execute(
            PlanRequest(
                catalog="catalog1",
                target="users",
                columns=["id"],
                row_filter=deserialize_row_filter("region = 'us'"),
            ),
            AUTHORIZATION_HEADER,
        )


def test_plan_access_rejects_requested_row_filter_for_non_visible_column():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id"],
                masks={},
                row_filter=None,
                policy_version=100,
            )
        ),
        catalog_registry=cast(
            Any,
            FakeCatalogRegistry(
                TrackingTableFormat(
                    catalog_name="catalog1",
                    table_name="users",
                    format="fake_format",
                    schema=schema,
                    planned_columns=[],
                )
            ),
        ),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(
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
        ),
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    with pytest.raises(
        PermissionError,
        match="Requested row filter may only reference visible unmasked columns: region",
    ):
        use_case.execute(
            PlanRequest(
                catalog="catalog1",
                target="users",
                columns=["id"],
                row_filter=deserialize_row_filter("region = 'us'"),
            ),
            AUTHORIZATION_HEADER,
        )


def test_plan_access_allows_requested_row_filter_on_visible_unmasked_hidden_dependency():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    planned_columns: list[list[str]] = []
    table_format = TrackingTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        planned_columns=planned_columns,
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id", "region"],
                masks={},
                row_filter=None,
                policy_version=100,
            )
        ),
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(
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
        ),
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    result = use_case.execute(
        PlanRequest(
            catalog="catalog1",
            target="users",
            columns=["id"],
            row_filter=deserialize_row_filter("region = 'us'"),
        ),
        AUTHORIZATION_HEADER,
    )

    assert result.columns == ["id"]
    assert planned_columns == [["id", "region"]]


def test_plan_access_combines_policy_and_requested_row_filters_before_ticketing():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("region", pa.string()),
            pa.field("active", pa.bool_()),
        ]
    )
    planned_columns: list[list[str]] = []
    table_format = TrackingTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        planned_columns=planned_columns,
    )
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
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id", "region", "active"],
                masks={},
                row_filter="active = true",
                policy_version=100,
            )
        ),
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    use_case.execute(
        PlanRequest(
            catalog="catalog1",
            target="users",
            columns=["id"],
            row_filter=deserialize_row_filter("region = 'us'"),
        ),
        AUTHORIZATION_HEADER,
    )

    payload_filter = ticket_codec.signed_payloads[0].scan["full_row_filter"]
    assert payload_filter is not None
    assert (
        row_filter_to_sql(deserialize_row_filter(payload_filter))
        == "active = TRUE AND region = 'us'"
    )
    assert planned_columns == [["id", "active", "region"]]


def test_fetch_stream_reapplies_fully_pushed_row_filter_after_backend_execution():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batches = (
        pa.record_batch(
            [
                pa.array([1, 2], type=pa.int64()),
                pa.array(["us", "eu"], type=pa.string()),
            ],
            schema=schema,
        ),
    )
    table_format = PretendPushdownTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="pretend_pushdown",
        schema=schema,
        batches=batches,
        backend_pushdown_sql="region = 'us'",
        residual_sql=None,
    )
    decision = AccessDecision(
        allowed_columns=["id", "region"],
        masks={},
        row_filter=None,
        policy_version=100,
    )
    plan_access, fetch_stream = _build_end_to_end_access_flow(table_format, decision)

    plan_result = plan_access.execute(
        PlanRequest(
            catalog="catalog1",
            target="users",
            columns=["id"],
            row_filter=deserialize_row_filter("region = 'us'"),
        ),
        AUTHORIZATION_HEADER,
    )
    fetch_result = fetch_stream.execute(plan_result.ticket_tokens[0], AUTHORIZATION_HEADER)
    table = pa.Table.from_batches(
        list(fetch_result.result_batches), schema=fetch_result.output_schema
    )

    assert table.schema.names == ["id"]
    assert table.column("id").to_pylist() == [1]


def test_fetch_stream_reapplies_full_policy_and_requested_filter_after_partial_pushdown():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("region", pa.string()),
            pa.field("status", pa.string()),
        ]
    )
    batches = (
        pa.record_batch(
            [
                pa.array([1, 2, 3], type=pa.int64()),
                pa.array(["us", "eu", "us"], type=pa.string()),
                pa.array(["active", "active", "inactive"], type=pa.string()),
            ],
            schema=schema,
        ),
    )
    table_format = PretendPushdownTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="pretend_pushdown",
        schema=schema,
        batches=batches,
        backend_pushdown_sql="region = 'us'",
        residual_sql="LOWER(status) = 'active'",
    )
    decision = AccessDecision(
        allowed_columns=["id", "region", "status"],
        masks={},
        row_filter="LOWER(status) = 'active'",
        policy_version=100,
    )
    plan_access, fetch_stream = _build_end_to_end_access_flow(table_format, decision)

    plan_result = plan_access.execute(
        PlanRequest(
            catalog="catalog1",
            target="users",
            columns=["id"],
            row_filter=deserialize_row_filter("region = 'us'"),
        ),
        AUTHORIZATION_HEADER,
    )
    fetch_result = fetch_stream.execute(plan_result.ticket_tokens[0], AUTHORIZATION_HEADER)
    table = pa.Table.from_batches(
        list(fetch_result.result_batches), schema=fetch_result.output_schema
    )

    assert table.schema.names == ["id"]
    assert table.column("id").to_pylist() == [1]


def test_plan_access_revalidates_requested_row_filter_against_base_schema():
    schema = pa.schema([pa.field("id", pa.int64())])
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id"],
                masks={},
                row_filter=None,
                policy_version=100,
            )
        ),
        catalog_registry=cast(
            Any,
            FakeCatalogRegistry(
                TrackingTableFormat(
                    catalog_name="catalog1",
                    table_name="users",
                    format="fake_format",
                    schema=schema,
                    planned_columns=[],
                )
            ),
        ),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(
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
        ),
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    with pytest.raises(ValueError, match="Unknown column in row filter: missing"):
        use_case.execute(
            PlanRequest(
                catalog="catalog1",
                target="users",
                columns=["id"],
                row_filter=deserialize_row_filter("missing = 1"),
            ),
            AUTHORIZATION_HEADER,
        )


def test_plan_access_excludes_denied_requested_columns_from_execution_plan():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("secret", pa.string()),
        ]
    )
    planned_columns: list[list[str]] = []
    table_format = TrackingTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        planned_columns=planned_columns,
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id"],
                masks={},
                row_filter=None,
                policy_version=100,
            )
        ),
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(
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
        ),
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    result = use_case.execute(
        PlanRequest(catalog="catalog1", target="users", columns=["id", "secret"]),
        AUTHORIZATION_HEADER,
    )

    assert result.columns == ["id"]
    assert planned_columns == [["id"]]


def test_fetch_stream_policy_version_mismatch():
    schema, decision, table_format = _build_use_case_dependencies()
    payload = TicketPayload(
        catalog="catalog1",
        target="users",
        columns=["id", "region"],
        scan={
            "read_payload": _encode_scan_task(table_format, schema),
            "full_row_filter": None,
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
            "full_row_filter": None,
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


def test_fetch_stream_rejects_invalid_scan_payloads():
    schema, decision, table_format = _build_use_case_dependencies()
    cases = [
        (
            {
                "read_payload": "",
                "full_row_filter": None,
                "masks": {},
            },
            "Missing read payload",
        ),
        (
            {
                "read_payload": _encode_scan_task(table_format, schema),
                "full_row_filter": None,
                "masks": {"region": "not-an-object"},
            },
            "Invalid mask payload",
        ),
        (
            {
                "read_payload": _encode_scan_task(table_format, schema),
                "full_row_filter": None,
                "masks": {"region": {"value": "***"}},
            },
            "Invalid mask payload",
        ),
        (
            {
                "read_payload": _encode_scan_task(table_format, schema),
                "full_row_filter": {"type": "comparison", "field": "region"},
                "masks": {},
            },
            "Invalid row filter payload",
        ),
    ]

    for scan, error_match in cases:
        payload = TicketPayload(
            catalog="catalog1",
            target="users",
            columns=["id", "region"],
            scan=cast(ScanPayload, scan),
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

        with pytest.raises(ValueError, match=error_match):
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
            "full_row_filter": None,
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
