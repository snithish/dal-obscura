"""FastAPI control-plane application factory."""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Mapping
from typing import cast
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi import HTTPException as FastAPIHTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from starlette.exceptions import HTTPException
from starlette.types import Message

from dal_obscura import __version__
from dal_obscura.application.governance.errors import (
    AuthorizationFailure,
    ConfigurationConflictError,
    PolicyValidationFailure,
    ValidationFailure,
)
from dal_obscura.interfaces.http.health import install_health_routes
from dal_obscura.interfaces.http.request_context import (
    current_request_id,
    reset_request_id,
    set_request_id,
)
from dal_obscura.interfaces.http.routes import (
    assets as asset_routes,
)
from dal_obscura.interfaces.http.routes import (
    audit as audit_routes,
)
from dal_obscura.interfaces.http.routes import (
    catalogs as catalog_routes,
)
from dal_obscura.interfaces.http.routes import (
    plugins as plugin_routes,
)
from dal_obscura.interfaces.http.routes import (
    policies as policy_routes,
)
from dal_obscura.interfaces.http.routes import (
    session as session_routes,
)
from dal_obscura.interfaces.http.routes import (
    settings as settings_routes,
)
from dal_obscura.interfaces.http.routes import (
    workspace as workspace_routes,
)
from dal_obscura.interfaces.http.routes.deps import (
    ControlPlaneDeps,
    RevisionConflictResponse,
)
from dal_obscura.interfaces.http.routes.schemas import ApiErrorResponse


class _RequestBodyTooLarge(HTTPException):
    """Raised by the receive wrapper when a streamed request exceeds its bound."""

    def __init__(self) -> None:
        # FastAPI body parsing preserves HTTP exceptions; an ordinary exception
        # would be converted into a misleading 400 before middleware sees it.
        super().__init__(status_code=413, detail="Request body too large")


_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def _http_error_code(status_code: int) -> str:
    """Maps transport status to a stable, non-sensitive client code."""

    return {
        400: "validation_error",
        401: "authentication_required",
        403: "forbidden",
        404: "not_found",
        405: "method_not_allowed",
        409: "revision_conflict",
        413: "request_too_large",
        422: "validation_error",
        428: "revision_precondition_required",
        429: "rate_limited",
        503: "not_ready",
    }.get(status_code, "request_rejected")


def _structured_error_response(status_code: int, detail: str) -> JSONResponse:
    """Build a correlated error response for middleware-level rejections."""

    request_id = current_request_id() or uuid4().hex
    response = JSONResponse(
        status_code=status_code,
        content={
            "detail": detail,
            "error": {
                "code": _http_error_code(status_code),
                "message": detail,
                "request_id": request_id,
            },
        },
    )
    response.headers["x-request-id"] = request_id
    return response


def _field_errors_from_detail(detail: object) -> list[dict[str, str]]:
    """Normalize Pydantic locations without returning submitted values."""

    if not isinstance(detail, list):
        return []
    field_errors: list[dict[str, str]] = []
    for item in detail:
        if not isinstance(item, Mapping):
            continue
        error = cast(Mapping[str, object], item)
        location = error.get("loc", ())
        if not isinstance(location, (list, tuple)):
            location = ()
        field = ".".join(str(part) for part in location if part != "body")
        message = error.get("msg", "Invalid value")
        error_type = error.get("type", "value_error")
        field_errors.append({"field": field, "message": str(message), "type": str(error_type)})
    return field_errors


def create_app(  # noqa: C901
    deps: ControlPlaneDeps,
    *,
    readiness: Callable[[], None],
    cors_origins: tuple[str, ...] = (),
    max_request_bytes: int = 1_048_576,
) -> FastAPI:
    """Build HTTP transport around already constructed application dependencies."""
    if max_request_bytes <= 0:
        raise ValueError("max_request_bytes must be positive")
    app = FastAPI(
        title="dal-obscura control-plane API",
        summary="Configuration, catalog, asset, policy, and session API for dal-obscura.",
        version=__version__,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        responses={
            status: {"model": ApiErrorResponse}
            for status in (400, 401, 403, 404, 405, 409, 413, 422, 428, 429, 503)
        },
    )

    @app.exception_handler(HTTPException)
    async def structured_http_error(request: Request, exc: HTTPException) -> JSONResponse:
        """Return one safe, correlated error shape for every API failure."""

        del request
        detail = exc.detail if isinstance(exc.detail, (str, list, dict)) else "Request rejected"
        field_errors = _field_errors_from_detail(detail)
        message = (
            detail
            if isinstance(detail, str)
            else ("Request validation failed" if field_errors else "Request rejected")
        )
        code = _http_error_code(exc.status_code)
        error: dict[str, object] = {
            "code": code,
            "message": message,
            "request_id": current_request_id(),
        }
        if (current_revision := getattr(exc, "current_revision", None)) is not None:
            error["current_revision"] = current_revision
        if field_errors:
            error["field_errors"] = field_errors
        headers = dict(exc.headers or {})
        if exc.status_code == 401:
            headers.setdefault("WWW-Authenticate", "Bearer")
        return JSONResponse(
            status_code=exc.status_code,
            headers=headers,
            content={
                "detail": detail,
                "error": error,
            },
        )

    @app.exception_handler(PolicyValidationFailure)
    @app.exception_handler(ValidationFailure)
    @app.exception_handler(AuthorizationFailure)
    @app.exception_handler(ConfigurationConflictError)
    @app.exception_handler(LookupError)
    async def governance_error(request: Request, exc: Exception) -> JSONResponse:
        if isinstance(exc, PolicyValidationFailure):
            error = FastAPIHTTPException(
                status_code=422,
                detail=[
                    {
                        "loc": ["body", "rules", exc.rule_index],
                        "msg": str(exc),
                        "type": "value_error",
                    }
                ],
            )
        elif isinstance(exc, ValidationFailure):
            error = HTTPException(status_code=400, detail=str(exc))
        elif isinstance(exc, AuthorizationFailure):
            error = HTTPException(status_code=403, detail=str(exc))
        elif isinstance(exc, ConfigurationConflictError):
            error = RevisionConflictResponse(exc)
        else:
            error = HTTPException(status_code=404, detail=str(exc))
        return await structured_http_error(request, error)

    @app.exception_handler(RequestValidationError)
    async def structured_request_validation_error(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        """Keep framework-level query/path validation on the public error contract."""

        del request
        request_id = current_request_id() or uuid4().hex
        field_errors = [
            {
                "field": ".".join(str(part) for part in error.get("loc", ()) if part != "body"),
                "message": str(error.get("msg", "Invalid value")),
                "type": str(error.get("type", "value_error")),
            }
            for error in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content={
                "detail": "Request validation failed",
                "error": {
                    "code": "validation_error",
                    "message": "Request validation failed",
                    "request_id": request_id,
                    "field_errors": field_errors,
                },
            },
            headers={"x-request-id": request_id},
        )

    @app.middleware("http")
    async def security_headers(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        response = await call_next(request)
        if request.url.path.startswith(("/v1/", "/auth/")):
            response.headers["cache-control"] = "no-store"
            response.headers["pragma"] = "no-cache"
        response.headers.setdefault("x-content-type-options", "nosniff")
        response.headers.setdefault("referrer-policy", "no-referrer")
        response.headers.setdefault(
            "content-security-policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; "
            "base-uri 'self'; form-action 'self'",
        )
        if request.url.path in {"/docs", "/redoc"}:
            # FastAPI's static documentation shells load their renderer from
            # jsDelivr and contain inline initialization. Keep this allowance
            # restricted to these shells; application/API responses stay strict.
            response.headers["content-security-policy"] = (
                "default-src 'self'; script-src 'self' https://cdn.jsdelivr.net 'unsafe-inline'; "
                "style-src 'self' https://cdn.jsdelivr.net https://fonts.googleapis.com "
                "'unsafe-inline'; font-src 'self' https://fonts.gstatic.com; "
                "img-src 'self' data: https://fastapi.tiangolo.com; "
                "connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'"
            )
        response.headers.setdefault("x-frame-options", "DENY")
        response.headers.setdefault("cross-origin-opener-policy", "same-origin")
        response.headers.setdefault(
            "permissions-policy",
            "camera=(), geolocation=(), microphone=(), payment=(), usb=()",
        )
        if request.url.scheme == "https":
            response.headers.setdefault(
                "strict-transport-security", "max-age=31536000; includeSubDomains"
            )
        return response

    async def request_correlation(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        supplied = request.headers.get("x-request-id", "").strip()
        request_id = supplied if _REQUEST_ID.fullmatch(supplied) else uuid4().hex
        token = set_request_id(request_id)
        try:
            response = await call_next(request)
            response.headers["x-request-id"] = request_id
            return response
        finally:
            reset_request_id(token)

    @app.middleware("http")
    async def request_size_limit(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        raw_length = request.headers.get("content-length")
        if raw_length:
            try:
                content_length = int(raw_length)
            except ValueError:
                return _structured_error_response(400, "Invalid content length")
            if content_length > max_request_bytes:
                return _structured_error_response(413, "Request body too large")
        received = 0
        original_receive = request.receive

        async def limited_receive() -> Message:
            nonlocal received
            message = await original_receive()
            if message.get("type") == "http.request":
                received += len(message.get("body", b""))
                if received > max_request_bytes:
                    raise _RequestBodyTooLarge
            return message

        # Starlette's ``call_next`` rebuilds the downstream Request from the
        # scope, so replace the receive callable on the original request rather
        # than passing a second Request instance.
        request._receive = limited_receive
        try:
            response = await call_next(request)
        except _RequestBodyTooLarge:
            return _structured_error_response(413, "Request body too large")
        # A downstream body parser can turn receive failures (including task
        # exception groups) into a 400. The measured byte count is authoritative.
        if received > max_request_bytes:
            return _structured_error_response(413, "Request body too large")
        return response

    # Correlation must wrap size checks, including failures before route dispatch.
    app.middleware("http")(request_correlation)

    if cors_origins:
        app.add_middleware(
            CORSMiddleware,  # ty: ignore[invalid-argument-type]
            allow_origins=list(cors_origins),
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "PATCH", "OPTIONS"],
            allow_headers=[
                "authorization",
                "content-type",
                "accept",
                "x-csrf-token",
                "x-request-id",
            ],
            expose_headers=["x-request-id", "retry-after"],
        )
    install_health_routes(app, readiness)

    for route in (
        session_routes.router,
        workspace_routes.router,
        catalog_routes.router,
        policy_routes.router,
        audit_routes.router,
        asset_routes.router,
        settings_routes.router,
        plugin_routes.router,
    ):
        app.include_router(route(deps))
    return app
