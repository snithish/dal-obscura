from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from sqlalchemy import Engine, create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dal_obscura.common.config_store.orm import Base


def create_engine_from_url(database_url: str) -> Engine:
    """Creates an SQLAlchemy engine with SQLite thread settings for tests/dev."""
    if database_url.startswith("sqlite"):
        kwargs: dict[str, Any] = {"connect_args": {"check_same_thread": False}}
        if database_url.endswith(":memory:"):
            kwargs["poolclass"] = StaticPool
        return create_engine(database_url, future=True, **kwargs)
    return create_engine(database_url, future=True)


def session_factory(engine: Engine) -> sessionmaker[Session]:
    """Builds the session factory shared by control-plane routes and tests."""
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)


def session_scope(session_maker: sessionmaker[Session]) -> Iterator[Session]:
    """Yields one transaction-scoped session."""
    session = session_maker()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def ensure_config_store_schema(engine: Engine) -> None:
    """Creates current config-store tables and applies idempotent schema upgrades."""
    Base.metadata.create_all(engine)
    _ensure_runtime_max_ticket_exchanges(engine)


def _ensure_runtime_max_ticket_exchanges(engine: Engine) -> None:
    inspector = inspect(engine)
    if not inspector.has_table("cell_runtime_settings"):
        return
    columns = {column["name"] for column in inspector.get_columns("cell_runtime_settings")}
    if "max_ticket_exchanges" in columns:
        return

    with engine.begin() as connection:
        connection.execute(
            text(
                "ALTER TABLE cell_runtime_settings "
                "ADD COLUMN max_ticket_exchanges INTEGER NOT NULL DEFAULT 1"
            )
        )
