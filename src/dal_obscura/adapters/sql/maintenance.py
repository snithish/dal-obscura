"""Atomic operational invalidation of persisted access artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from sqlalchemy import delete, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.adapters.sql.database.orm import (
    BrowserSessionRecord,
    DataPlaneTicketRecord,
    LoginTransactionRecord,
    utcnow,
)


@dataclass(frozen=True)
class InvalidationCounts:
    """Rows invalidated by one restore/emergency operation."""

    sessions: int
    login_transactions: int
    tickets: int


def invalidate_access(
    session_maker: sessionmaker[Session],
) -> InvalidationCounts:
    """Revokes browser credentials and removes replayable access artifacts."""

    now = utcnow()
    with session_maker() as session:
        session_count = (
            cast(
                CursorResult,
                session.execute(
                    update(BrowserSessionRecord)
                    .where(BrowserSessionRecord.revoked_at.is_(None))
                    .values(revoked_at=now)
                ),
            ).rowcount
            or 0
        )
        login_count = (
            cast(CursorResult, session.execute(delete(LoginTransactionRecord))).rowcount or 0
        )
        ticket_delete = delete(DataPlaneTicketRecord)
        ticket_count = cast(CursorResult, session.execute(ticket_delete)).rowcount or 0
        session.commit()
    return InvalidationCounts(
        sessions=int(session_count), login_transactions=int(login_count), tickets=int(ticket_count)
    )
