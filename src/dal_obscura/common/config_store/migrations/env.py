from __future__ import annotations

from logging.config import fileConfig

from alembic import context

from dal_obscura.common.config_store.orm import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_online() -> None:
    connectable = config.attributes.get("connection")
    if connectable is None:
        raise RuntimeError("Config-store migrations require an existing SQLAlchemy connection")

    context.configure(
        connection=connectable,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


run_migrations_online()
