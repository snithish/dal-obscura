from __future__ import annotations

import pytest

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import Base


@pytest.fixture
def db_session():
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_maker = session_factory(engine)
    with session_maker() as session:
        yield session
