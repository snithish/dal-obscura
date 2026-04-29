from __future__ import annotations


class ValidationFailure(ValueError):
    """Raised when draft control-plane state cannot be published."""
