import base64
import pickle
from typing import Any, cast

import pytest

from dal_obscura.common.access_control.models import Principal
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.ticket_delivery.models import ScanPayload, TicketPayload
from dal_obscura.data_plane.application.ports.ticket_store import StoredTicket
from dal_obscura.data_plane.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.data_plane.application.use_cases.plan_access import PlanAccessUseCase
from tests.application.access_flow.helpers import (
    AUTHORIZATION_HEADER,
    _build_use_case_dependencies,
    _ticket_store_with,
)
from tests.support.use_cases import (
    FakeAuthorizer,
    FakeCatalogRegistry,
    FakeIdentity,
    FakeMasking,
    FakeRowTransform,
    FakeTicketCodec,
    FakeTicketStore,
    StubInputPartition,
    encode_scan_task,
)


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


def test_fetch_stream_rejects_stale_policy_version_before_decoding(monkeypatch):
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
    authorizer = FakeAuthorizer(decision=None, current_version=101)
    ticket_store = _ticket_store_with(payload)
    monkeypatch.setattr(pickle, "loads", lambda _: pytest.fail("pickle.loads was reached"))
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(
            principal=Principal(id="user1", groups=[], attributes={"tenant_id": "tenant-a"})
        ),
        authorizer=authorizer,
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
        now=lambda: 1000,
    )

    with pytest.raises(PermissionError, match="stale policy version"):
        use_case.execute("ticket", AUTHORIZATION_HEADER)

    assert authorizer.last_current_version_tenant_id == "tenant-a"
    assert ticket_store.reserve_calls == []


def test_fetch_stream_rejects_legacy_ticket_without_ticket_id_before_decoding(monkeypatch):
    schema, _, table_format = _build_use_case_dependencies()
    payload = TicketPayload(
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
    ticket_store = FakeTicketStore()
    monkeypatch.setattr(pickle, "loads", lambda _: pytest.fail("pickle.loads was reached"))
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(
            principal=Principal(id="user1", groups=[], attributes={"tenant_id": "tenant-a"})
        ),
        authorizer=FakeAuthorizer(decision=None, current_version=100),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
        now=lambda: 1000,
    )

    with pytest.raises(PermissionError):
        use_case.execute("token", AUTHORIZATION_HEADER)

    assert ticket_store.reserve_calls == []


def test_fetch_stream_rejects_missing_db_ticket_before_decoding(monkeypatch):
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
    ticket_store = FakeTicketStore()
    monkeypatch.setattr(pickle, "loads", lambda _: pytest.fail("pickle.loads was reached"))
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(
            principal=Principal(id="user1", groups=[], attributes={"tenant_id": "tenant-a"})
        ),
        authorizer=FakeAuthorizer(decision=None, current_version=100),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
        now=lambda: 1000,
    )

    with pytest.raises(PermissionError):
        use_case.execute("token", AUTHORIZATION_HEADER)

    assert ticket_store.reserve_calls == []


def test_fetch_stream_rejects_hash_mismatch_before_reserving_or_decoding(monkeypatch):
    schema, _, table_format = _build_use_case_dependencies()
    ticket_id = "00000000-0000-0000-0000-000000000001"
    signed_payload = TicketPayload(
        ticket_id=ticket_id,
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
    ticket_store = FakeTicketStore()
    ticket_store.records[ticket_id] = StoredTicket(
        payload=signed_payload,
        payload_hash="0" * 64,
        exchange_count=0,
        max_exchanges=1,
        expires_at=signed_payload.expires_at,
    )
    monkeypatch.setattr(pickle, "loads", lambda _: pytest.fail("pickle.loads was reached"))
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(
            principal=Principal(id="user1", groups=[], attributes={"tenant_id": "tenant-a"})
        ),
        authorizer=FakeAuthorizer(decision=None, current_version=100),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(signed_payload),
        ticket_store=ticket_store,
        now=lambda: 1000,
    )

    with pytest.raises(PermissionError):
        use_case.execute("token", AUTHORIZATION_HEADER)

    assert ticket_store.reserve_calls == []


def test_fetch_stream_reserves_exchange_before_scan_execution():
    schema, _, table_format = _build_use_case_dependencies()
    ticket_id = "00000000-0000-0000-0000-000000000001"
    payload = TicketPayload(
        ticket_id=ticket_id,
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
    ticket_store = FakeTicketStore()
    ticket_store.store(payload, max_exchanges=1)
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(
            principal=Principal(id="user1", groups=[], attributes={"tenant_id": "tenant-a"})
        ),
        authorizer=FakeAuthorizer(decision=None, current_version=100),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
        now=lambda: 1000,
    )

    first = use_case.execute("token", AUTHORIZATION_HEADER)
    assert first.columns == ["id"]

    with pytest.raises(PermissionError):
        use_case.execute("token", AUTHORIZATION_HEADER)


def test_plan_access_persists_ticket_with_id_before_returning_signed_token():
    _schema, decision, table_format = _build_use_case_dependencies()
    ticket_codec = FakeTicketCodec()
    ticket_store = FakeTicketStore()
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision),
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_store=ticket_store,
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=2,
        now=lambda: 1000,
        nonce_factory=lambda: "nonce",
        ticket_id_factory=lambda: "00000000-0000-0000-0000-000000000001",
    )

    result = use_case.execute(
        PlanRequest(catalog="catalog1", target="users", columns=["id"]),
        AUTHORIZATION_HEADER,
    )

    assert result.ticket_tokens == ["signed-token"]
    assert ticket_codec.signed_payloads[0].ticket_id == ("00000000-0000-0000-0000-000000000001")
    assert ticket_store.stored[0][0] == ticket_codec.signed_payloads[0]
    assert ticket_store.stored[0][1] == 2


def test_plan_access_does_not_sign_ticket_when_persistence_fails():
    _schema, decision, table_format = _build_use_case_dependencies()
    ticket_codec = FakeTicketCodec()
    ticket_store = FakeTicketStore()
    ticket_store.fail_store = True
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision),
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_store=ticket_store,
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
        now=lambda: 1000,
        nonce_factory=lambda: "nonce",
        ticket_id_factory=lambda: "00000000-0000-0000-0000-000000000001",
    )

    with pytest.raises(RuntimeError, match="store failed"):
        use_case.execute(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]),
            AUTHORIZATION_HEADER,
        )

    assert ticket_codec.signed_payloads == []


def test_fetch_stream_rejects_ticket_policy_version_after_policy_changes():
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
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, current_version=101),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
    )

    with pytest.raises(PermissionError, match="stale policy version"):
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
                "read_payload": encode_scan_task(table_format, schema),
                "full_row_filter": None,
                "masks": {"region": "not-an-object"},
            },
            "Invalid mask payload",
        ),
        (
            {
                "read_payload": encode_scan_task(table_format, schema),
                "full_row_filter": None,
                "masks": {"region": {"value": "***"}},
            },
            "Invalid mask payload",
        ),
        (
            {
                "read_payload": encode_scan_task(table_format, schema),
                "full_row_filter": {"type": "comparison", "field": "region"},
                "masks": {},
            },
            "Invalid row filter payload",
        ),
    ]

    for index, (scan, error_match) in enumerate(cases, start=1):
        payload = TicketPayload(
            ticket_id=f"00000000-0000-0000-0000-{index:012d}",
            catalog="catalog1",
            target="users",
            columns=["id", "region"],
            scan=cast(ScanPayload, scan),
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
        )
        ticket_store = _ticket_store_with(payload)
        use_case = FetchStreamUseCase(
            identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
            authorizer=FakeAuthorizer(decision=decision, current_version=100),
            masking=FakeMasking(),
            row_transform=FakeRowTransform(),
            ticket_codec=FakeTicketCodec(payload),
            ticket_store=ticket_store,
        )

        with pytest.raises(ValueError, match=error_match):
            use_case.execute("token", AUTHORIZATION_HEADER)


def test_fetch_stream_rejects_legacy_partition_payload():
    _, decision, _ = _build_use_case_dependencies()
    payload = TicketPayload(
        ticket_id="00000000-0000-0000-0000-000000000001",
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
    ticket_store = _ticket_store_with(payload)
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, current_version=100),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
    )

    with pytest.raises(ValueError, match="Invalid read payload"):
        use_case.execute("token", AUTHORIZATION_HEADER)
