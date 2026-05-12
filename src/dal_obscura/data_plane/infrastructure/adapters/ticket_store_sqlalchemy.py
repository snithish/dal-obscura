from __future__ import annotations

import hmac
from datetime import datetime, timezone
from typing import cast
from uuid import UUID

from sqlalchemy import delete, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.common.config_store.orm import DataPlaneTicketRecord
from dal_obscura.common.ticket_delivery.models import TicketPayload, ticket_payload_hash
from dal_obscura.data_plane.application.ports.ticket_store import StoredTicket


class SqlAlchemyTicketStore:
    def __init__(self, session_maker: sessionmaker[Session], *, cell_id: UUID) -> None:
        self._session_maker = session_maker
        self._cell_id = cell_id

    def store(self, payload: TicketPayload, *, max_exchanges: int) -> None:
        if payload.ticket_id is None:
            raise ValueError("ticket_id is required")
        ticket_uuid = _ticket_uuid(payload.ticket_id)
        with self._session_maker() as session:
            session.add(
                DataPlaneTicketRecord(
                    ticket_id=ticket_uuid,
                    cell_id=self._cell_id,
                    tenant_id=payload.tenant_id,
                    catalog=payload.catalog,
                    target=payload.target,
                    principal_id=payload.principal_id,
                    policy_version=payload.policy_version,
                    expires_at=payload.expires_at,
                    max_exchanges=max_exchanges,
                    exchange_count=0,
                    payload_hash=ticket_payload_hash(payload),
                    payload_json=payload.to_dict(),
                )
            )
            session.commit()

    def load(self, ticket_id: str) -> StoredTicket:
        with self._session_maker() as session:
            record = self._record(session, ticket_id)
            return _stored_ticket(record)

    def reserve_exchange(self, ticket_id: str, *, now: int) -> StoredTicket:
        ticket_uuid = _ticket_uuid(ticket_id)
        with self._session_maker() as session:
            result = cast(
                CursorResult,
                session.execute(
                    update(DataPlaneTicketRecord)
                    .where(DataPlaneTicketRecord.cell_id == self._cell_id)
                    .where(DataPlaneTicketRecord.ticket_id == ticket_uuid)
                    .where(DataPlaneTicketRecord.expires_at >= now)
                    .where(
                        DataPlaneTicketRecord.exchange_count < DataPlaneTicketRecord.max_exchanges
                    )
                    .values(
                        exchange_count=DataPlaneTicketRecord.exchange_count + 1,
                        last_exchanged_at=datetime.now(timezone.utc),
                    )
                ),
            )
            if result.rowcount != 1:
                session.rollback()
                raise PermissionError("Ticket expired or exhausted")
            record = self._record(session, ticket_id)
            stored = _stored_ticket(record)
            session.commit()
            return stored

    def cleanup_expired_and_exhausted(self, *, now: int) -> int:
        with self._session_maker() as session:
            result = cast(
                CursorResult,
                session.execute(
                    delete(DataPlaneTicketRecord)
                    .where(DataPlaneTicketRecord.cell_id == self._cell_id)
                    .where(
                        (DataPlaneTicketRecord.expires_at < now)
                        | (
                            DataPlaneTicketRecord.exchange_count
                            >= DataPlaneTicketRecord.max_exchanges
                        )
                    )
                ),
            )
            session.commit()
            return int(result.rowcount or 0)

    def _record(self, session: Session, ticket_id: str) -> DataPlaneTicketRecord:
        record = session.scalar(
            select(DataPlaneTicketRecord)
            .where(DataPlaneTicketRecord.cell_id == self._cell_id)
            .where(DataPlaneTicketRecord.ticket_id == _ticket_uuid(ticket_id))
        )
        if record is None:
            raise LookupError("Ticket not found")
        return record


def _stored_ticket(record: DataPlaneTicketRecord) -> StoredTicket:
    payload = TicketPayload.from_dict(record.payload_json)
    actual_hash = ticket_payload_hash(payload)
    if not hmac.compare_digest(actual_hash, record.payload_hash):
        raise PermissionError("Stored ticket payload hash mismatch")
    return StoredTicket(
        payload=payload,
        payload_hash=record.payload_hash,
        exchange_count=record.exchange_count,
        max_exchanges=record.max_exchanges,
        expires_at=record.expires_at,
    )


def _ticket_uuid(ticket_id: str) -> UUID:
    try:
        return UUID(ticket_id)
    except ValueError as exc:
        raise LookupError("Ticket not found") from exc
