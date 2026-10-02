"""Request-scoped metadata shared by control-plane adapters."""

from __future__ import annotations

from contextvars import ContextVar, Token

_request_id: ContextVar[str | None] = ContextVar("dal_obscura_request_id", default=None)


def set_request_id(value: str) -> Token[str | None]:
    """Sets the current request correlation identifier."""

    return _request_id.set(value)


def reset_request_id(token: Token[str | None]) -> None:
    """Restores the parent request context after an ASGI request."""

    _request_id.reset(token)


def current_request_id() -> str | None:
    """Returns the current request correlation identifier, when present."""

    return _request_id.get()
