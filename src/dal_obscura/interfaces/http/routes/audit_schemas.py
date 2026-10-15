"""HTTP audit request and response contracts."""

from __future__ import annotations

from typing import Any

from pydantic import (
    BaseModel,
)


class AuditEventResponse(BaseModel):
    """Redacted audit event safe for management clients."""

    id: str
    actor: str
    action: str
    resource_type: str
    resource_id: str
    outcome: str
    details: dict[str, Any]
    correlation_id: str | None = None
    created_at: str


class AuditEventPageResponse(BaseModel):
    """Keyset-paginated redacted audit events."""

    items: list[AuditEventResponse]
    next_cursor: str | None = None
