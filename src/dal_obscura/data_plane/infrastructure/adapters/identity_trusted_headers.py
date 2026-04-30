from __future__ import annotations

import hmac

from dal_obscura.common.access_control.models import Principal
from dal_obscura.data_plane.application.ports.identity import (
    AuthenticationRequest,
    InvalidCredentialsError,
    MissingCredentialsError,
)


class TrustedHeaderIdentityProvider:
    """Authenticates identities asserted by a trusted gateway."""

    def __init__(
        self,
        *,
        shared_secret: str,
        secret_header: str = "x-dal-obscura-proxy-secret",
        subject_header: str = "x-auth-request-user",
        groups_header: str = "x-auth-request-groups",
        attribute_header_prefix: str = "x-auth-request-attr-",
    ) -> None:
        if not shared_secret:
            raise ValueError("TrustedHeaderIdentityProvider requires shared_secret")
        self._shared_secret = shared_secret
        self._secret_header = secret_header.lower()
        self._subject_header = subject_header.lower()
        self._groups_header = groups_header.lower()
        self._attribute_header_prefix = attribute_header_prefix.lower()

    def authenticate(self, request: AuthenticationRequest) -> Principal:
        supplied_secret = request.header(self._secret_header)
        if not supplied_secret:
            raise MissingCredentialsError("Missing trusted proxy secret")
        if not hmac.compare_digest(supplied_secret, self._shared_secret):
            raise InvalidCredentialsError("Invalid trusted proxy secret")

        subject = (request.header(self._subject_header) or "").strip()
        if not subject:
            raise InvalidCredentialsError("Missing trusted subject")

        return Principal(
            id=subject,
            groups=_split_groups(request.header(self._groups_header)),
            attributes=_attributes_from_headers(request, self._attribute_header_prefix),
        )


def _split_groups(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _attributes_from_headers(
    request: AuthenticationRequest,
    prefix: str,
) -> dict[str, str]:
    attributes: dict[str, str] = {}
    for header, value in request.headers.items():
        if not header.startswith(prefix):
            continue
        attribute_name = header[len(prefix) :].strip()
        if not attribute_name:
            continue
        normalized = value.strip()
        if normalized:
            attributes[attribute_name] = normalized
    return attributes
