"""Opt-in PostgreSQL tests own and clean up an isolated schema."""

import os
from collections.abc import Iterator
from contextlib import contextmanager
from uuid import uuid4

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.storage.database.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)


@contextmanager
def isolated_postgres_sessions() -> Iterator[sessionmaker[Session]]:
    database_url = os.getenv("DAL_OBSCURA_POSTGRES_TEST_URL", "").strip()
    if not database_url:
        pytest.skip("set DAL_OBSCURA_POSTGRES_TEST_URL for PostgreSQL evidence")
    url = make_url(database_url)
    if url.get_backend_name() != "postgresql":
        pytest.fail("DAL_OBSCURA_POSTGRES_TEST_URL must point to PostgreSQL")
    schema = "config_test_" + uuid4().hex
    admin = create_engine_from_url(database_url)
    with admin.begin() as conn:
        conn.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    isolated_url = url.update_query_dict({"options": f"-csearch_path={schema}"})
    engine = create_engine_from_url(isolated_url.render_as_string(hide_password=False))
    try:
        migrate_config_store(engine)
        yield session_factory(engine)
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.exec_driver_sql(f'DROP SCHEMA "{schema}" CASCADE')
        admin.dispose()
