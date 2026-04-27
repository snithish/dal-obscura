from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dal_obscura.application.ports.identity import (
    AuthenticationRequest,
    InvalidCredentialsError,
    MissingCredentialsError,
)
from dal_obscura.domain.access_control.models import Principal


@dataclass(frozen=True)
class _MtlsIdentityRecord:
    peer_identity: str
    id: str
    groups: list[str]
    attributes: dict[str, str]


class MtlsIdentityProvider:
    """Authenticates callers from the verified Flight peer identity."""

    def __init__(self, *, identities: list[dict[str, Any]] | None = None) -> None:
        self._identities = {
            record.peer_identity: record
            for record in [_record_from_config(item) for item in identities or []]
        }

    def authenticate(self, request: AuthenticationRequest) -> Principal:
        peer_identity = request.peer_identity.strip()
        if not peer_identity:
            raise MissingCredentialsError("Missing peer identity")
        if not self._identities:
            return Principal(
                id=peer_identity,
                groups=[],
                attributes={"peer_identity": peer_identity},
            )
        record = self._identities.get(peer_identity)
        if record is None:
            raise InvalidCredentialsError("Unrecognized peer identity")
        return Principal(id=record.id, groups=record.groups, attributes=record.attributes)


def _record_from_config(raw: dict[str, Any]) -> _MtlsIdentityRecord:
    peer_identity = str(raw.get("peer_identity") or "").strip()
    principal_id = str(raw.get("id") or "").strip()
    if not peer_identity:
        raise ValueError("mTLS identity entry requires peer_identity")
    if not principal_id:
        raise ValueError(f"mTLS identity entry {peer_identity!r} requires id")
    raw_attributes = raw.get("attributes", {})
    if not isinstance(raw_attributes, dict):
        raise ValueError(f"mTLS identity entry {peer_identity!r} attributes must be an object")
    return _MtlsIdentityRecord(
        peer_identity=peer_identity,
        id=principal_id,
        groups=[str(group) for group in raw.get("groups", [])],
        attributes={str(key): str(value) for key, value in raw_attributes.items()},
    )
