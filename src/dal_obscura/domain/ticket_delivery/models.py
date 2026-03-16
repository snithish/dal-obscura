from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TicketPayload:
    """Serialized contents of a signed Flight ticket."""

    target: str
    columns: list[str]
    scan: dict[str, Any]
    policy_version: int
    principal_id: str
    expires_at: int
    nonce: str
    backend_id: str
    backend_generation: int
    catalog: str | None = None
    auth_header: str | None = None
    auth_value: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Produces a JSON-friendly representation used by the ticket codec."""
        payload: dict[str, Any] = {
            "target": self.target,
            "columns": self.columns,
            "scan": self.scan,
            "policy_version": self.policy_version,
            "principal_id": self.principal_id,
            "expires_at": self.expires_at,
            "nonce": self.nonce,
            "backend_id": self.backend_id,
            "backend_generation": self.backend_generation,
        }
        if self.catalog is not None:
            payload["catalog"] = self.catalog
        if self.auth_header is not None:
            payload["auth_header"] = self.auth_header
        if self.auth_value is not None:
            payload["auth_value"] = self.auth_value
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TicketPayload":
        """Restores a payload after signature verification and JSON parsing."""
        return cls(
            target=str(payload["target"]),
            columns=list(payload.get("columns", [])),
            scan=dict(payload.get("scan", {})),
            policy_version=int(payload.get("policy_version", 0)),
            principal_id=str(payload.get("principal_id", "")),
            expires_at=int(payload.get("expires_at", 0)),
            nonce=str(payload.get("nonce", "")),
            backend_id=str(payload.get("backend_id", "")),
            backend_generation=int(payload.get("backend_generation", 0)),
            catalog=payload.get("catalog"),
            auth_header=payload.get("auth_header"),
            auth_value=payload.get("auth_value"),
        )
