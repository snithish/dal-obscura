from __future__ import annotations


class ValidationFailure(ValueError):
    """Raised when draft control-plane state cannot be published."""


class AuthorizationFailure(PermissionError):
    """Raised when a control-plane actor cannot mutate a protected resource."""


class PublicationConflictError(RuntimeError):
    """Raised when a publication activation loses its generation compare-and-swap."""


class RevisionPreconditionRequired(PublicationConflictError):
    """Raised when an existing mutable resource is written without its revision."""
