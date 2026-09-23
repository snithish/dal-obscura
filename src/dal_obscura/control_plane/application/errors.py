from __future__ import annotations


class ValidationFailure(ValueError):
    """Raised when a live configuration mutation is invalid."""


class AuthorizationFailure(PermissionError):
    """Raised when a control-plane actor cannot mutate a protected resource."""


class ConfigurationConflictError(RuntimeError):
    """Raised when an optimistic live-configuration update is stale."""


class RevisionPreconditionRequired(ConfigurationConflictError):
    """Raised when an existing mutable resource is written without its revision."""
