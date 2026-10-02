from __future__ import annotations

from dataclasses import replace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select

from dal_obscura.common.config_store.db import (
    session_factory,
)
from dal_obscura.common.config_store.orm import DataPlaneTicketRecord
from dal_obscura.common.ticket_delivery.models import TicketPayload
from dal_obscura.data_plane.infrastructure.adapters.ticket_store_sqlalchemy import (
    SqlAlchemyTicketStore,
)
from tests.support.tickets import ticket_payload


def _payload(ticket_id: str, *, expires_at: int = 2000) -> TicketPayload:
    return ticket_payload(
        ticket_id=ticket_id,
        catalog="analytics",
        target="default.users",
        columns=["id"],
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": "payload",
            "full_row_filter": None,
            "masks": {},
        },
        expires_at=expires_at,
        nonce="nonce-a",
    )


def test_ticket_store_reserves_exchanges_until_limit(db_engine):
    session_maker = session_factory(db_engine)
    store = SqlAlchemyTicketStore(session_maker)
    ticket_id = "00000000-0000-0000-0000-000000000001"

    store.store(_payload(ticket_id), max_exchanges=2)

    assert store.reserve_exchange(ticket_id, now=1000).exchange_count == 1
    assert store.reserve_exchange(ticket_id, now=1001).exchange_count == 2
    with pytest.raises(PermissionError):
        store.reserve_exchange(ticket_id, now=1002)


def test_ticket_store_persists_many_tickets_in_one_transaction(db_engine):
    session_maker = session_factory(db_engine)
    store = SqlAlchemyTicketStore(session_maker)

    store.store_many(
        [
            _payload("00000000-0000-0000-0000-000000000001"),
            _payload("00000000-0000-0000-0000-000000000002"),
        ],
        max_exchanges=1,
    )

    assert store.load("00000000-0000-0000-0000-000000000001").exchange_count == 0
    assert store.load("00000000-0000-0000-0000-000000000002").exchange_count == 0


def test_ticket_store_rejects_load_and_exchange_after_owner_revocation(db_engine):
    session_maker = session_factory(db_engine)
    asset_id = uuid4()
    store = SqlAlchemyTicketStore(session_maker)
    ticket_id = "00000000-0000-0000-0000-000000000003"
    payload = replace(_payload(ticket_id), asset_id=str(asset_id))
    store.store(payload, max_exchanges=1)

    with session_maker() as session:
        record = session.scalar(
            select(DataPlaneTicketRecord).where(DataPlaneTicketRecord.ticket_id == UUID(ticket_id))
        )
        assert record is not None
        record.revoked_at = record.created_at
        session.commit()

    with pytest.raises(LookupError):
        store.load(ticket_id)
    with pytest.raises(PermissionError, match="revoked"):
        store.reserve_exchange(ticket_id, now=1000)
    with pytest.raises(PermissionError, match="revoked"):
        store.ensure_active(ticket_id)


def test_ticket_cleanup_deletes_only_expired_rows_globally(db_engine):
    session_maker = session_factory(db_engine)

    store_a = SqlAlchemyTicketStore(session_maker)
    store_b = SqlAlchemyTicketStore(session_maker)

    store_a.store(_payload("00000000-0000-0000-0000-000000000001", expires_at=900), max_exchanges=1)
    store_a.store(
        _payload("00000000-0000-0000-0000-000000000002", expires_at=2000), max_exchanges=1
    )
    store_a.reserve_exchange("00000000-0000-0000-0000-000000000002", now=1000)
    store_a.store(
        _payload("00000000-0000-0000-0000-000000000003", expires_at=2000), max_exchanges=2
    )
    store_b.store(_payload("00000000-0000-0000-0000-000000000004", expires_at=900), max_exchanges=1)

    assert store_a.cleanup_expired(now=1000) == 2

    with session_maker() as session:
        remaining = {
            str(record.ticket_id) for record in session.scalars(select(DataPlaneTicketRecord))
        }
    assert remaining == {
        "00000000-0000-0000-0000-000000000002",
        "00000000-0000-0000-0000-000000000003",
    }


def test_cleanup_preserves_last_reserved_stream_until_expiry(db_engine):
    session_maker = session_factory(db_engine)
    store = SqlAlchemyTicketStore(session_maker)
    ticket_id = "00000000-0000-0000-0000-000000000005"
    store.store(_payload(ticket_id, expires_at=2000), max_exchanges=1)
    store.reserve_exchange(ticket_id, now=1000)
    store.cleanup_expired(now=1001)
    store.ensure_active(ticket_id)
    with pytest.raises(PermissionError):
        store.reserve_exchange(ticket_id, now=1001)
    assert store.cleanup_expired(now=2000) == 1


def test_ticket_exchange_rejects_exact_expiry_boundary(db_engine):
    session_maker = session_factory(db_engine)
    store = SqlAlchemyTicketStore(session_maker)
    ticket_id = "00000000-0000-0000-0000-000000000006"
    store.store(_payload(ticket_id, expires_at=2000), max_exchanges=1)
    with pytest.raises(PermissionError):
        store.reserve_exchange(ticket_id, now=2000)
