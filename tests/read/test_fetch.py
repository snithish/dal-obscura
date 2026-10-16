import pyarrow as pa
import pytest

from dal_obscura.adapters.duckdb.transform import (
    DuckDBRowTransformAdapter,
)
from dal_obscura.adapters.tickets.signing import HmacTicketCodecAdapter
from dal_obscura.kernel.identity.principal import Principal
from dal_obscura.kernel.plans.request import PlanRequest
from tests.support.flight.flight import InMemoryPolicyAuthorizer
from tests.support.read.fakes import (
    AUTHORIZATION_HEADER,
    FakeCatalogRegistry,
    FakeIdentity,
    FakeRowTransform,
    FakeTicketCodec,
    FakeTicketStore,
    PretendPushdownTableFormat,
    StaticAccessContext,
    _build_use_case_dependencies,
    _ticket_store_with,
    encode_scan_task,
    make_read_service,
    ticket_payload,
)


def test_fetch_stream_principal_mismatch():
    schema, _decision, table_format = _build_use_case_dependencies()
    payload = ticket_payload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        columns=["id", "region"],
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
    )
    ticket_store = _ticket_store_with(payload)
    use_case = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user2", groups=[], attributes={})),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
    )

    with pytest.raises(PermissionError):
        use_case.fetch("token", AUTHORIZATION_HEADER)


@pytest.mark.parametrize(
    ("before", "after"),
    [
        (
            Principal(id="user1", groups=["analyst"], attributes={}),
            Principal(id="user1", groups=[], attributes={}),
        ),
        (
            Principal(id="user1", groups=["analyst"], attributes={"department": "research"}),
            Principal(id="user1", groups=["analyst"], attributes={"department": "sales"}),
        ),
    ],
)
def test_fetch_stream_rejects_ticket_when_identity_context_changes(before, after):
    schema = pa.schema([pa.field("id", pa.int64())])
    table_format = PretendPushdownTableFormat(
        catalog_name="analytics",
        table_name="users",
        format="test",
        schema=schema,
        batches=(pa.record_batch([pa.array([1], type=pa.int64())], schema=schema),),
    )
    identity = FakeIdentity(principal=before)
    authorizer = InMemoryPolicyAuthorizer(
        catalog="analytics",
        target="users",
        rules=[{"principals": ["group:analyst"], "columns": ["id"]}],
    )

    ticket_codec = HmacTicketCodecAdapter("secret")
    ticket_store = FakeTicketStore()
    plan_access = make_read_service(
        identity=identity,
        access_context=StaticAccessContext(
            authorizer=authorizer, catalog_registry=FakeCatalogRegistry(table_format)
        ),
        ticket_codec=ticket_codec,
        ticket_store=ticket_store,
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )
    fetch_stream = make_read_service(
        identity=identity,
        row_transform=DuckDBRowTransformAdapter(),
        ticket_codec=ticket_codec,
        ticket_store=ticket_store,
    )

    planned = plan_access.plan(
        PlanRequest(catalog="analytics", target="users", columns=["id"]), AUTHORIZATION_HEADER
    )
    identity._principal = after

    with pytest.raises(PermissionError):
        fetch_stream.fetch(planned.ticket_tokens[0], AUTHORIZATION_HEADER)


def test_fetch_stream_rejects_matching_subject_from_another_issuer():
    schema, _decision, table_format = _build_use_case_dependencies()
    payload = ticket_payload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        columns=["id", "region"],
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
        issuer="https://issuer-a.example",
    )
    ticket_store = _ticket_store_with(payload)
    use_case = make_read_service(
        identity=FakeIdentity(
            principal=Principal(
                id="user1", groups=[], attributes={}, issuer="https://issuer-b.example"
            )
        ),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
    )

    with pytest.raises(PermissionError, match="Unauthorized"):
        use_case.fetch("token", AUTHORIZATION_HEADER)

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
    payload = ticket_payload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        columns=["id"],
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
        expires_at=1001,
        nonce="nonce",
    )
    clock = [999]
    use_case = make_read_service(
        identity=FakeIdentity(
            principal=Principal(id="user1", groups=[], attributes={}, expires_at=1000)
        ),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=_ticket_store_with(payload),
        now=lambda: clock[0],
    )

    result = use_case.fetch("ticket", AUTHORIZATION_HEADER)
    batches = iter(result.result_batches)
    assert next(batches).column("id").to_pylist() == [1]
    clock[0] = 1000
    with pytest.raises(PermissionError, match="Identity expired"):
        next(batches)


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
    payload = ticket_payload(
        ticket_id="00000000-0000-0000-0000-000000000002",
        columns=["id"],
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
        nonce="nonce",
    )
    ticket_store = _ticket_store_with(payload)
    use_case = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
    )

    batches = iter(use_case.fetch("ticket", AUTHORIZATION_HEADER).result_batches)
    assert next(batches).column("id").to_pylist() == [1]
    assert payload.ticket_id is not None
    ticket_store.revoked.add(payload.ticket_id)
    with pytest.raises(PermissionError, match="revoked"):
        next(batches)
