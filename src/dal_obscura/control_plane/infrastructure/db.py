from __future__ import annotations

from collections.abc import Iterator

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker


def create_engine_from_url(database_url: str) -> Engine:
    """Creates an SQLAlchemy engine with SQLite thread settings for tests/dev."""
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    return create_engine(database_url, future=True, connect_args=connect_args)


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
