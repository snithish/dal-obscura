from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool


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
    """Apply packaged config-store migrations to the configured database."""
    from alembic import command
    from alembic.config import Config

    config = Config()
    config.set_main_option("script_location", _migration_script_location())
    config.set_main_option("sqlalchemy.url", str(engine.url))

    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "head")


def _migration_script_location() -> str:
    from importlib import resources

    return str(resources.files("dal_obscura.common.config_store").joinpath("migrations"))
