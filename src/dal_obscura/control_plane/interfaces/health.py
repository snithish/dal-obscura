from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker


def install_health_routes(app: FastAPI, session_maker: sessionmaker[Session]) -> None:
    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", response_model=None)
    def readyz() -> dict[str, Any] | JSONResponse:
        try:
            with session_maker() as session:
                session.execute(text("SELECT 1"))
        except Exception:
            return JSONResponse(
                status_code=503,
                content={"status": "not_ready", "checks": {"database": "failed"}},
            )
        return {"status": "ready", "checks": {"database": "ok"}}
