from __future__ import annotations

from datetime import datetime


def _escape_like(value: str) -> str:
    """Escapes SQL LIKE metacharacters so search remains literal and bounded."""

    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _isoformat(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")
