from __future__ import annotations

from .base import Authenticator, AuthResult
from .default import AuthConfig, DefaultAuthenticator, issue_api_key

__all__ = ["AuthConfig", "AuthResult", "Authenticator", "DefaultAuthenticator", "issue_api_key"]


def authenticate(headers: dict[str, str], config: AuthConfig) -> AuthResult:
    return DefaultAuthenticator(config).authenticate(headers)
