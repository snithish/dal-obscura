from typing import Any, cast

import pyarrow as pa
import pytest

from dal_obscura.common.access_control.filters import deserialize_row_filter
from dal_obscura.common.access_control.models import AccessDecision, Principal
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.ticket_delivery.models import TicketPayload
from dal_obscura.data_plane.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.data_plane.application.use_cases.plan_access import PlanAccessUseCase
from dal_obscura.data_plane.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)
from dal_obscura.data_plane.infrastructure.adapters.ticket_hmac import HmacTicketCodecAdapter
from tests.application.access_flow.helpers import (
    AUTHORIZATION_HEADER,
    _build_end_to_end_access_flow,
    _build_use_case_dependencies,
    _ticket_store_with,
)
from tests.support.flight import InMemoryPolicyAuthorizer
from tests.support.use_cases import (
    FakeAuthorizer,
    FakeCatalogRegistry,
    FakeIdentity,
    FakeMasking,
    FakeRowTransform,
    FakeTicketCodec,
    FakeTicketStore,
    PretendPushdownTableFormat,
    encode_scan_task,
)


def test_fetch_stream_does_not_recheck_policy_version():
    schema, _, table_format = _build_use_case_dependencies()
    payload = TicketPayload(
        asset_id="00000000-0000-4000-8000-000000000001",
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
    authorizer = FakeAuthorizer(
        decision=AccessDecision(
            allowed_columns=["id"],
            masks={},
            row_filter=None,
            policy_version=100,
        ),
        current_version=100,
    )
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
    assert authorizer.last_current_version_tenant_id is None


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
        asset_id="00000000-0000-4000-8000-000000000001",
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


def test_fetch_stream_rejects_ticket_when_granting_group_is_removed():
    schema = pa.schema([pa.field("id", pa.int64())])
    table_format = PretendPushdownTableFormat(
        catalog_name="analytics",
        table_name="users",
        format="test",
        schema=schema,
        batches=(pa.record_batch([pa.array([1], type=pa.int64())], schema=schema),),
    )
    identity = FakeIdentity(principal=Principal(id="user1", groups=["analyst"], attributes={}))
    authorizer = InMemoryPolicyAuthorizer(
        catalog="analytics",
        target="users",
        rules=[{"principals": ["group:analyst"], "columns": ["id"]}],
    )
    masking = DefaultMaskingAdapter()
    ticket_codec = HmacTicketCodecAdapter("secret")
    ticket_store = FakeTicketStore()
    plan_access = PlanAccessUseCase(
        identity=identity,
        authorizer=authorizer,
        catalog_registry=FakeCatalogRegistry(table_format),
        masking=masking,
        ticket_codec=ticket_codec,
        ticket_store=ticket_store,
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )
    fetch_stream = FetchStreamUseCase(
        identity=identity,
        authorizer=authorizer,
        masking=masking,
        row_transform=DuckDBRowTransformAdapter(masking),
        ticket_codec=ticket_codec,
        ticket_store=ticket_store,
    )

    planned = plan_access.execute(
        PlanRequest(catalog="analytics", target="users", columns=["id"]),
        AUTHORIZATION_HEADER,
    )
    identity._principal = Principal(id="user1", groups=[], attributes={})

    with pytest.raises(PermissionError):
        fetch_stream.execute(planned.ticket_tokens[0], AUTHORIZATION_HEADER)


def test_fetch_stream_rejects_matching_subject_from_another_issuer():
    schema, decision, table_format = _build_use_case_dependencies()
    payload = TicketPayload(
        asset_id="00000000-0000-4000-8000-000000000001",
        ticket_id="00000000-0000-0000-0000-000000000001",
        catalog="catalog1",
        target="users",
        tenant_id="tenant-a",
        columns=["id", "region"],
        scan={
            "read_payload": encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
        policy_version=100,
        principal_id="user1",
        issuer="https://issuer-a.example",
        expires_at=9999999999,
        nonce="abc",
    )
    ticket_store = _ticket_store_with(payload)
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(
            principal=Principal(
                id="user1",
                groups=[],
                attributes={"tenant_id": "tenant-a"},
                issuer="https://issuer-b.example",
            )
        ),
        authorizer=FakeAuthorizer(decision=decision, current_version=100),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
    )

    with pytest.raises(PermissionError, match="Unauthorized"):
        use_case.execute("token", AUTHORIZATION_HEADER)

    assert ticket_store.reserve_calls == []


def test_fetch_stream_stops_before_emitting_batches_after_identity_expiry():
    schema = pa.schema([pa.field("id", pa.int64())])
    table_format = PretendPushdownTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="test",
        schema=schema,
        batches=(
            pa.record_batch([pa.array([1])], schema=schema),
            pa.record_batch([pa.array([2])], schema=schema),
        ),
    )
    payload = TicketPayload(
        asset_id="00000000-0000-4000-8000-000000000001",
        ticket_id="00000000-0000-0000-0000-000000000001",
        catalog="catalog1",
        target="users",
        tenant_id="tenant-a",
        columns=["id"],
        scan={
            "read_payload": encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
        policy_version=100,
        principal_id="user1",
        expires_at=1001,
        nonce="nonce",
    )
    clock = [999]
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(
            principal=Principal(
                id="user1",
                groups=[],
                attributes={"tenant_id": "tenant-a"},
                expires_at=1000,
            )
        ),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id"], masks={}, row_filter=None, policy_version=100
            ),
            current_version=100,
        ),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=_ticket_store_with(payload),
        now=lambda: clock[0],
    )

    result = use_case.execute("ticket", AUTHORIZATION_HEADER)
    batches = iter(result.result_batches)
    assert next(batches).column("id").to_pylist() == [1]
    clock[0] = 1000
    with pytest.raises(PermissionError, match="Identity expired"):
        next(batches)


def test_fetch_stream_keeps_captured_policy_after_policy_version_changes():
    schema = pa.schema([pa.field("id", pa.int64())])
    table_format = PretendPushdownTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="test",
        schema=schema,
        batches=(
            pa.record_batch([pa.array([1])], schema=schema),
            pa.record_batch([pa.array([2])], schema=schema),
        ),
    )
    payload = TicketPayload(
        asset_id="00000000-0000-4000-8000-000000000001",
        ticket_id="00000000-0000-0000-0000-000000000001",
        catalog="catalog1",
        target="users",
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
    authorizer = FakeAuthorizer(
        decision=AccessDecision(
            allowed_columns=["id"], masks={}, row_filter=None, policy_version=100
        ),
        current_version=100,
    )
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(
            principal=Principal(id="user1", groups=[], attributes={"tenant_id": "tenant-a"})
        ),
        authorizer=authorizer,
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=_ticket_store_with(payload),
    )

    batches = iter(use_case.execute("ticket", AUTHORIZATION_HEADER).result_batches)
    assert next(batches).column("id").to_pylist() == [1]
    authorizer._current_version = 101

    assert next(batches).column("id").to_pylist() == [2]


def test_fetch_stream_stops_after_ticket_is_revoked_between_batches():
    schema = pa.schema([pa.field("id", pa.int64())])
    table_format = PretendPushdownTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="test",
        schema=schema,
        batches=(
            pa.record_batch([pa.array([1])], schema=schema),
            pa.record_batch([pa.array([2])], schema=schema),
        ),
    )
    payload = TicketPayload(
        asset_id="00000000-0000-4000-8000-000000000001",
        ticket_id="00000000-0000-0000-0000-000000000002",
        catalog="catalog1",
        target="users",
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
    ticket_store = _ticket_store_with(payload)
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(
            principal=Principal(id="user1", groups=[], attributes={"tenant_id": "tenant-a"})
        ),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id"], masks={}, row_filter=None, policy_version=100
            ),
            current_version=100,
        ),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
    )

    batches = iter(use_case.execute("ticket", AUTHORIZATION_HEADER).result_batches)
    assert next(batches).column("id").to_pylist() == [1]
    assert payload.ticket_id is not None
    ticket_store.revoked.add(payload.ticket_id)
    with pytest.raises(PermissionError, match="revoked"):
        next(batches)


def test_stream_expiry_guard_rejects_ticket_expiry_before_identity_expiry():
    from dal_obscura.data_plane.application.use_cases.fetch_stream import _guard_stream_expiry

    batch = pa.record_batch([pa.array([1])], names=["id"])
    guarded = _guard_stream_expiry(
        [batch],
        ticket_expires_at=1000,
        identity_expires_at=2000,
        stream_deadline_at=2000,
        now=lambda: 1000,
    )

    with pytest.raises(PermissionError, match="Ticket expired"):
        next(guarded)


def test_stream_deadline_guard_stops_before_emitting_a_late_batch():
    from dal_obscura.data_plane.application.use_cases.fetch_stream import _guard_stream_expiry

    batch = pa.record_batch([pa.array([1])], names=["id"])
    guarded = _guard_stream_expiry(
        [batch],
        ticket_expires_at=1000,
        identity_expires_at=None,
        stream_deadline_at=100,
        now=lambda: 100,
    )

    with pytest.raises(TimeoutError, match="Stream deadline exceeded"):
        next(guarded)


def test_stream_guard_closes_upstream_when_consumer_stops_early() -> None:
    from dal_obscura.data_plane.application.use_cases.fetch_stream import _guard_stream_expiry

    class ClosableBatches:
        def __init__(self) -> None:
            self.closed = False

        def __iter__(self):
            yield pa.record_batch([pa.array([1])], names=["id"])
            yield pa.record_batch([pa.array([2])], names=["id"])

        def close(self) -> None:
            self.closed = True

    upstream = ClosableBatches()
    guarded = _guard_stream_expiry(
        upstream,
        ticket_expires_at=9999,
        identity_expires_at=None,
        stream_deadline_at=9999,
        now=lambda: 1,
    )
    next(guarded)
    cast(Any, guarded).close()

    assert upstream.closed is True
