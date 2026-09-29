from __future__ import annotations

from dataclasses import replace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.common.config_store.orm import CellRecord, DataPlaneTicketRecord
from dal_obscura.common.ticket_delivery.models import TicketPayload
from dal_obscura.data_plane.infrastructure.adapters.ticket_store_sqlalchemy import (
    SqlAlchemyTicketStore,
)


def _session_maker() -> sessionmaker[Session]:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    return session_factory(engine)


def _create_cell(session_maker: sessionmaker[Session], cell_id) -> None:
    with session_maker() as session:
        session.add(CellRecord(id=cell_id, name=f"cell-{cell_id}", region="local"))
        session.commit()


def _payload(ticket_id: str, *, expires_at: int = 2000) -> TicketPayload:
    return TicketPayload(
        asset_id="00000000-0000-4000-8000-000000000001",
        ticket_id=ticket_id,
        catalog="analytics",
        target="default.users",
        tenant_id="tenant-a",
        columns=["id"],
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": "payload",
            "full_row_filter": None,
            "masks": {},
        },
        policy_version=100,
        principal_id="user1",
        expires_at=expires_at,
        nonce="nonce-a",
    )


def test_ticket_store_reserves_exchanges_until_limit():
    session_maker = _session_maker()
    cell_id = uuid4()
    _create_cell(session_maker, cell_id)
    store = SqlAlchemyTicketStore(session_maker, cell_id=cell_id)
    ticket_id = "00000000-0000-0000-0000-000000000001"

    store.store(_payload(ticket_id), max_exchanges=2)

    assert store.reserve_exchange(ticket_id, now=1000).exchange_count == 1
    assert store.reserve_exchange(ticket_id, now=1001).exchange_count == 2
    with pytest.raises(PermissionError):
        store.reserve_exchange(ticket_id, now=1002)


def test_ticket_store_persists_many_tickets_in_one_transaction():
    session_maker = _session_maker()
    cell_id = uuid4()
    _create_cell(session_maker, cell_id)
    store = SqlAlchemyTicketStore(session_maker, cell_id=cell_id)

    store.store_many(
        [
            _payload("00000000-0000-0000-0000-000000000001"),
            _payload("00000000-0000-0000-0000-000000000002"),
        ],
        max_exchanges=1,
    )

    assert store.load("00000000-0000-0000-0000-000000000001").exchange_count == 0
    assert store.load("00000000-0000-0000-0000-000000000002").exchange_count == 0


def test_ticket_store_rejects_load_and_exchange_after_owner_revocation():
    session_maker = _session_maker()
    cell_id = uuid4()
    asset_id = uuid4()
    _create_cell(session_maker, cell_id)
    store = SqlAlchemyTicketStore(session_maker, cell_id=cell_id)
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


def test_ticket_store_lookup_is_scoped_to_cell():
    session_maker = _session_maker()
    owner_cell = uuid4()
    other_cell = uuid4()
    _create_cell(session_maker, owner_cell)
    _create_cell(session_maker, other_cell)
    ticket_id = "00000000-0000-0000-0000-000000000001"

    SqlAlchemyTicketStore(session_maker, cell_id=owner_cell).store(
        _payload(ticket_id), max_exchanges=1
    )

    with pytest.raises(LookupError):
        SqlAlchemyTicketStore(session_maker, cell_id=other_cell).load(ticket_id)


def test_ticket_cleanup_deletes_only_expired_and_exhausted_rows_for_current_cell():
    session_maker = _session_maker()
    cell_a = uuid4()
    cell_b = uuid4()
    _create_cell(session_maker, cell_a)
    _create_cell(session_maker, cell_b)
    store_a = SqlAlchemyTicketStore(session_maker, cell_id=cell_a)
    store_b = SqlAlchemyTicketStore(session_maker, cell_id=cell_b)

    store_a.store(
        _payload("00000000-0000-0000-0000-000000000001", expires_at=900),
        max_exchanges=1,
    )
    store_a.store(
        _payload("00000000-0000-0000-0000-000000000002", expires_at=2000),
        max_exchanges=1,
    )
    store_a.reserve_exchange("00000000-0000-0000-0000-000000000002", now=1000)
    store_a.store(
        _payload("00000000-0000-0000-0000-000000000003", expires_at=2000),
        max_exchanges=2,
    )
    store_b.store(
        _payload("00000000-0000-0000-0000-000000000004", expires_at=900),
        max_exchanges=1,
    )

    assert store_a.cleanup_expired_and_exhausted(now=1000) == 2

    with session_maker() as session:
        remaining = {
            str(record.ticket_id) for record in session.scalars(select(DataPlaneTicketRecord))
        }
    assert remaining == {
        "00000000-0000-0000-0000-000000000003",
        "00000000-0000-0000-0000-000000000004",
    }
