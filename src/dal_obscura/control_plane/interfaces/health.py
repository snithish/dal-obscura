"""Control-plane HTTP health and readiness routes.

Example:
    ```python
    install_health_routes(app, session_factory(engine))
    ```
"""

from __future__ import annotations

from typing import Any, Literal

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.control_plane.infrastructure.request_context import current_request_id
from dal_obscura.control_plane.interfaces.routes.schemas import ApiError


class ReadinessResponse(BaseModel):
    """Probe state with optional correlated failure evidence."""

    status: Literal["ready", "not_ready"]
    checks: dict[str, str]
    error: ApiError | None = None


def install_health_routes(app: FastAPI, session_maker: sessionmaker[Session]) -> None:
    """Installs `/healthz` and `/readyz` routes on the control-plane app.

    Example:
        ```python
        install_health_routes(app, session_maker)
        ```
    """

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get(
        "/readyz",
        response_model=ReadinessResponse,
        response_model_exclude_none=True,
        responses={503: {"model": ReadinessResponse}},
    )
    def readyz() -> dict[str, Any] | JSONResponse:
        try:
            with session_maker() as session:
                session.execute(text("SELECT 1"))
        except Exception:
            request_id = current_request_id()
            return JSONResponse(
                status_code=503,
                content={
                    "status": "not_ready",
                    "checks": {"database": "failed"},
                    "error": {
                        "code": "not_ready",
                        "message": "Database readiness check failed",
                        "request_id": request_id,
                    },
                },
                headers={"x-request-id": request_id} if request_id else None,
            )
        return {"status": "ready", "checks": {"database": "ok"}}
