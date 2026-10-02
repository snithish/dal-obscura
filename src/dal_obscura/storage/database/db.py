from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from sqlalchemy import Engine, create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

DB_DRIVER_INSTALL_HINTS = {
    "postgresql+psycopg": "pip install 'dal-obscura[postgres]'",
    "postgresql": "pip install 'dal-obscura[postgres]'",
    "sqlite+pysqlite": "pip install 'dal-obscura[sqlite]'",
    "sqlite": "pip install 'dal-obscura[sqlite]'",
}


class ConfigStoreSchemaError(RuntimeError):
    """Base error for config-store schema state failures."""


class ConfigStoreMigrationRequired(ConfigStoreSchemaError):
    """Raised when the config-store database is not at the packaged Alembic head."""


@dataclass(frozen=True)
class ConfigStoreMigrationStatus:
    """Current and expected Alembic heads for the config store.

    Example:
        ```python
        status = check_config_store_schema(engine)
        assert status.is_current
        ```
    """

    current_heads: tuple[str, ...]
    expected_heads: tuple[str, ...]

    @property
    def is_current(self) -> bool:
        return set(self.current_heads) == set(self.expected_heads)


def create_engine_from_url(database_url: str) -> Engine:
    """Creates an SQLAlchemy engine with SQLite thread settings for tests/dev."""
    url = make_url(database_url)
    try:
        if url.drivername.startswith("sqlite"):
            kwargs: dict[str, Any] = {"connect_args": {"check_same_thread": False}}
            if database_url.endswith(":memory:"):
                kwargs["poolclass"] = StaticPool
            return create_engine(database_url, future=True, **kwargs)
        return create_engine(database_url, future=True)
    except (ModuleNotFoundError, NoSuchModuleError) as exc:
        hint = DB_DRIVER_INSTALL_HINTS.get(url.drivername)
        if hint is None:
            hint = "install the SQLAlchemy driver for this database URL"
        raise RuntimeError(f"Missing database driver for {url.drivername!r}; {hint}.") from exc


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


def migrate_config_store(engine: Engine, revision: str = "head") -> None:
    """Apply packaged config-store migrations to the configured database."""
    from alembic import command

    config = _alembic_config(engine)

    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, revision)


def check_config_store_schema(engine: Engine) -> ConfigStoreMigrationStatus:
    """Require that the database is already at the packaged Alembic head."""
    from alembic.runtime.migration import MigrationContext
    from alembic.script import ScriptDirectory

    config = _alembic_config(engine)
    script = ScriptDirectory.from_config(config)
    expected_heads = tuple(sorted(script.get_heads()))
    with engine.connect() as connection:
        context = MigrationContext.configure(connection)
        current_heads = tuple(sorted(context.get_current_heads()))

    status = ConfigStoreMigrationStatus(
        current_heads=current_heads,
        expected_heads=expected_heads,
    )
    if not status.is_current:
        current = ", ".join(status.current_heads) if status.current_heads else "none"
        expected = ", ".join(status.expected_heads) if status.expected_heads else "none"
        raise ConfigStoreMigrationRequired(
            "Config-store database schema is not current "
            f"(current: {current}; expected: {expected}). "
            "Run `dal-obscura-migrate upgrade` before starting services."
        )
    return status


def _alembic_config(engine: Engine):
    from alembic.config import Config

    config = Config()
    config.set_main_option("script_location", _migration_script_location())
    config.set_main_option("sqlalchemy.url", str(engine.url).replace("%", "%%"))
    return config


def _migration_script_location() -> str:
    from importlib import resources

    return str(resources.files("dal_obscura.storage.database").joinpath("migrations"))
