from __future__ import annotations

import sys
from contextlib import ExitStack
from pathlib import Path

import pytest

from dal_obscura.storage.database.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)

REPO_ROOT = Path(__file__).resolve().parent.parent

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture
def db_engine():
    """A fresh migrated database per test; never share mutable records."""
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    try:
        migrate_config_store(engine)
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def db_session(db_engine):
    with session_factory(db_engine)() as session:
        yield session


@pytest.fixture
def http_client():
    """Manage clients even when a test needs custom app or transport options."""
    from fastapi.testclient import TestClient

    with ExitStack() as resources:

        def create(app, **client_options):
            return resources.enter_context(TestClient(app, **client_options))

        yield create


@pytest.fixture
def client_factory(db_engine, http_client):
    """Compose real HTTP routes with explicit app overrides."""
    from dal_obscura.interfaces.http.app import create_app

    def create(*, base_url="http://testserver", **app_options):
        factory = session_factory(db_engine)
        app = create_app(factory, admin_token="test-admin", **app_options)
        app.state.test_session_factory = factory
        return http_client(app, base_url=base_url)

    return create
