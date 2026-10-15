"""Own database resources for operational access invalidation."""

from dal_obscura.adapters.sql.database.db import create_engine_from_url, session_factory
from dal_obscura.adapters.sql.maintenance import InvalidationCounts, invalidate_access


def invalidate_database_access(database_url: str) -> InvalidationCounts:
    engine = create_engine_from_url(database_url)
    try:
        return invalidate_access(session_factory(engine))
    finally:
        engine.dispose()
