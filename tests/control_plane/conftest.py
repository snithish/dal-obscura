from __future__ import annotations

import pytest

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)


@pytest.fixture
def db_session():
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    session_maker = session_factory(engine)
    with session_maker() as session:
        yield session
