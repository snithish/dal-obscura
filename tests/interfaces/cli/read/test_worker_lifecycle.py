from threading import Event
from threading import enumerate as active_threads

from dal_obscura.adapters.sql.database.db import session_factory
from dal_obscura.adapters.sql.tickets import SqlAlchemyTicketStore
from dal_obscura.bootstrap.read import TicketCleanupWorker


def test_ticket_cleanup_worker_has_explicit_idempotent_shutdown(db_engine):
    cleaned = Event()

    class Store(SqlAlchemyTicketStore):
        def cleanup_expired(self, *, now):
            assert now > 0
            deleted = super().cleanup_expired(now=now)
            cleaned.set()
            return deleted

    worker = TicketCleanupWorker(Store(session_factory(db_engine)), interval_seconds=1)
    worker.start()
    try:
        assert cleaned.wait(5), "cleanup worker did not perform its scheduled work"
    finally:
        worker.close()
    worker.close()
    assert not any(thread.name == "dal-obscura-ticket-cleanup" for thread in active_threads())
