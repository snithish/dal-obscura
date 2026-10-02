from __future__ import annotations


def _escape_like(value: str) -> str:
    """Escapes SQL LIKE metacharacters so search remains literal and bounded."""

    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _isoformat(value) -> str:
    return value.isoformat().replace("+00:00", "Z")
