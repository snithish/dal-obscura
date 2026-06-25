import pyarrow as pa
import pytest

from dal_obscura.common.access_control.filters import deserialize_row_filter
from dal_obscura.common.access_control.models import AccessDecision, Principal
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.ticket_delivery.models import TicketPayload
from dal_obscura.data_plane.application.use_cases.fetch_stream import FetchStreamUseCase
from tests.application.access_flow.helpers import (
    AUTHORIZATION_HEADER,
    _build_end_to_end_access_flow,
    _build_use_case_dependencies,
    _ticket_store_with,
)
from tests.support.use_cases import (
    FakeAuthorizer,
    FakeIdentity,
    FakeMasking,
    FakeRowTransform,
    FakeTicketCodec,
    PretendPushdownTableFormat,
    encode_scan_task,
)


def test_fetch_stream_accepts_matching_current_policy_version():
    schema, _, table_format = _build_use_case_dependencies()
    payload = TicketPayload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        catalog="analytics",
        target="default.users",
        tenant_id="tenant-a",
        columns=["id"],
        scan={
            "read_payload": encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="nonce",
    )
    authorizer = FakeAuthorizer(decision=None, current_version=100)
    ticket_store = _ticket_store_with(payload)
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(
            principal=Principal(id="user1", groups=[], attributes={"tenant_id": "tenant-a"})
        ),
        authorizer=authorizer,
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
    )

    result = use_case.execute("ticket", AUTHORIZATION_HEADER)

    assert result.target == "default.users"
    assert authorizer.last_current_version_tenant_id == "tenant-a"


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


def test_fetch_stream_principal_mismatch():
    schema, decision, table_format = _build_use_case_dependencies()
    payload = TicketPayload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        catalog="catalog1",
        target="users",
        columns=["id", "region"],
        scan={
            "read_payload": encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="abc",
    )
    ticket_store = _ticket_store_with(payload)
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user2", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, current_version=100),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
    )

    with pytest.raises(PermissionError):
        use_case.execute("token", AUTHORIZATION_HEADER)
