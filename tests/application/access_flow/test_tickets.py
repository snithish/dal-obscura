import base64
import pickle
from typing import Any, cast

import pytest

from dal_obscura.common.access_control.models import Principal
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.ticket_delivery.models import ScanPayload
from dal_obscura.data_plane.application.ports.access_context import StaticAccessContext
from dal_obscura.data_plane.application.ports.ticket_store import StoredTicket
from dal_obscura.data_plane.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.data_plane.application.use_cases.plan_access import PlanAccessUseCase
from tests.application.access_flow.helpers import (
    AUTHORIZATION_HEADER,
    _build_use_case_dependencies,
    _ticket_store_with,
)
from tests.support.tickets import ticket_payload
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


def test_fetch_stream_rejects_legacy_ticket_without_ticket_id_before_decoding(monkeypatch):
    schema, _, table_format = _build_use_case_dependencies()
    payload = ticket_payload(
        catalog="analytics",
        target="default.users",
        columns=["id"],
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
        nonce="nonce",
    )
    ticket_store = FakeTicketStore()
    monkeypatch.setattr(pickle, "loads", lambda _: pytest.fail("pickle.loads was reached"))
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
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
    payload = ticket_payload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        catalog="analytics",
        target="default.users",
        columns=["id"],
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
        nonce="nonce",
    )
    ticket_store = FakeTicketStore()
    monkeypatch.setattr(pickle, "loads", lambda _: pytest.fail("pickle.loads was reached"))
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
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
    signed_payload = ticket_payload(
        ticket_id=ticket_id,
        catalog="analytics",
        target="default.users",
        columns=["id"],
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
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
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
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
    payload = ticket_payload(
        ticket_id=ticket_id,
        catalog="analytics",
        target="default.users",
        columns=["id"],
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
        nonce="nonce",
    )
    ticket_store = FakeTicketStore()
    ticket_store.store(payload, max_exchanges=1)
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
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
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(decision=decision),
            catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        ),
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
        PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
    )

    assert result.ticket_tokens == ["signed-token"]
    assert ticket_codec.signed_payloads[0].ticket_id == ("00000000-0000-0000-0000-000000000001")
    assert ticket_store.stored[0][0] == ticket_codec.signed_payloads[0]
    assert ticket_store.stored[0][1] == 2
    assert ticket_codec.signed_payloads[0].identity_context
    assert ticket_codec.signed_payloads[0].decision_digest


def test_plan_access_does_not_sign_ticket_when_persistence_fails():
    _schema, decision, table_format = _build_use_case_dependencies()
    ticket_codec = FakeTicketCodec()
    ticket_store = FakeTicketStore()
    ticket_store.fail_store = True
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(decision=decision),
            catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        ),
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
            PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
        )

    assert ticket_codec.signed_payloads == []


@pytest.mark.parametrize(
    "change,error_match",
    [
        pytest.param({"read_payload": ""}, "Missing read payload", id="missing-read-payload"),
        pytest.param(
            {"masks": {"region": "not-an-object"}}, "Invalid mask payload", id="non-object-mask"
        ),
        pytest.param(
            {"masks": {"region": {"value": "***"}}}, "Invalid mask payload", id="missing-mask-type"
        ),
        pytest.param(
            {"full_row_filter": {"type": "comparison", "field": "region"}},
            "Invalid row filter payload",
            id="non-sql-filter",
        ),
    ],
)
def test_fetch_stream_rejects_invalid_scan_payloads(change, error_match):
    schema, _decision, table_format = _build_use_case_dependencies()
    scan = {
        "authorization_columns": ["id", "region"],
        "read_payload": encode_scan_task(table_format, schema),
        "full_row_filter": None,
        "masks": {},
        **change,
    }
    payload = ticket_payload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        scan=cast(ScanPayload, scan),
    )
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=_ticket_store_with(payload),
    )

    with pytest.raises(ValueError, match=error_match):
        use_case.execute("token", AUTHORIZATION_HEADER)


def test_fetch_stream_rejects_legacy_partition_payload():
    payload = ticket_payload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        columns=["id", "region"],
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": base64.b64encode(pickle.dumps(StubInputPartition(b"payload"))).decode(
                "utf-8"
            ),
            "full_row_filter": None,
            "masks": {},
        },
    )
    ticket_store = _ticket_store_with(payload)
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
    )

    with pytest.raises(ValueError, match="Invalid read payload"):
        use_case.execute("token", AUTHORIZATION_HEADER)


def test_scan_decoder_rejects_missing_authorization_columns():
    from dal_obscura.data_plane.application.use_cases.fetch_stream import _decode_scan

    with pytest.raises(ValueError, match="Invalid authorization columns"):
        _decode_scan({"read_payload": "cGF5bG9hZA==", "masks": {}, "full_row_filter": None})
