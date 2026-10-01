from __future__ import annotations

import hmac
from collections.abc import Iterable
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
    """Durable SQLAlchemy-backed ticket exchange store.

    Example:
        ```python
        store = SqlAlchemyTicketStore(session_factory)
        store.store(payload, max_exchanges=1)
        stored = store.reserve_exchange(payload.ticket_id, now=payload.expires_at - 1)
        ```
    """

    def __init__(
        self,
        session_maker: sessionmaker[Session],
    ) -> None:
        self._session_maker = session_maker

    def store(self, payload: TicketPayload, *, max_exchanges: int) -> None:
        self.store_many([payload], max_exchanges=max_exchanges)

    def store_many(self, payloads: Iterable[TicketPayload], *, max_exchanges: int) -> None:
        records = [_ticket_record(payload, max_exchanges) for payload in payloads]
        if not records:
            return
        with self._session_maker() as session:
            session.add_all(records)
            session.commit()

    def load(self, ticket_id: str) -> StoredTicket:
        with self._session_maker() as session:
            record = self._record(session, ticket_id, require_active=True)
            return _stored_ticket(record)

    def reserve_exchange(self, ticket_id: str, *, now: int) -> StoredTicket:
        ticket_uuid = _ticket_uuid(ticket_id)
        with self._session_maker() as session:
            result = cast(
                CursorResult,
                session.execute(
                    update(DataPlaneTicketRecord)
                    .where(DataPlaneTicketRecord.ticket_id == ticket_uuid)
                    .where(DataPlaneTicketRecord.revoked_at.is_(None))
                    .where(DataPlaneTicketRecord.expires_at > now)
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
                raise PermissionError("Ticket is revoked, expired, or exhausted")
            record = self._record(session, ticket_id)
            stored = _stored_ticket(record)
            session.commit()
            return stored

    def ensure_active(self, ticket_id: str) -> None:
        """Fails closed when a ticket is absent or has been revoked."""

        with self._session_maker() as session:
            active_id = session.scalar(
                select(DataPlaneTicketRecord.ticket_id)
                .where(DataPlaneTicketRecord.ticket_id == _ticket_uuid(ticket_id))
                .where(DataPlaneTicketRecord.revoked_at.is_(None))
            )
            if active_id is None:
                raise PermissionError("Ticket is revoked or unavailable")

    def cleanup_expired(self, *, now: int) -> int:
        """Retain exhausted tickets until expiry for active-stream revocation checks.

        Exhaustion prevents new exchanges but does not terminate the last
        reserved exchange.
        """
        with self._session_maker() as session:
            result = cast(
                CursorResult,
                session.execute(
                    delete(DataPlaneTicketRecord).where(DataPlaneTicketRecord.expires_at <= now)
                ),
            )
            session.commit()
            return int(result.rowcount or 0)

    def _record(
        self,
        session: Session,
        ticket_id: str,
        *,
        require_active: bool = False,
    ) -> DataPlaneTicketRecord:
        query = select(DataPlaneTicketRecord).where(
            DataPlaneTicketRecord.ticket_id == _ticket_uuid(ticket_id)
        )
        if require_active:
            query = query.where(DataPlaneTicketRecord.revoked_at.is_(None))
        record = session.scalar(query)
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


def _ticket_record(payload: TicketPayload, max_exchanges: int) -> DataPlaneTicketRecord:
    if payload.ticket_id is None:
        raise ValueError("ticket_id is required")
    return DataPlaneTicketRecord(
        ticket_id=_ticket_uuid(payload.ticket_id),
        asset_id=_asset_uuid(payload.asset_id),
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


def _asset_uuid(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError as exc:
        raise ValueError("Ticket asset_id must be a UUID") from exc


def _ticket_uuid(ticket_id: str) -> UUID:
    try:
        return UUID(ticket_id)
    except ValueError as exc:
        raise LookupError("Ticket not found") from exc
