from __future__ import annotations

import os

import uvicorn

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import Base
from dal_obscura.control_plane.interfaces.api import create_app


def main() -> None:
    database_url = os.environ["DAL_OBSCURA_DATABASE_URL"]
    admin_token = os.environ["DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN"]
    host = os.getenv("DAL_OBSCURA_CONTROL_PLANE_HOST", "0.0.0.0")
    port = int(os.getenv("DAL_OBSCURA_CONTROL_PLANE_PORT", "8820"))
    engine = create_engine_from_url(database_url)
    Base.metadata.create_all(engine)
    app = create_app(session_factory(engine), admin_token=admin_token)
    uvicorn.run(app, host=host, port=port)
