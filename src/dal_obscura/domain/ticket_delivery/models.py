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
    format: str
    catalog: str | None = None

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
            "format": self.format,
        }
        if self.catalog is not None:
            payload["catalog"] = self.catalog
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> TicketPayload:
        """Restores a payload after signature verification and JSON parsing."""
        return cls(
            target=str(payload["target"]),
            columns=list(payload.get("columns", [])),
            scan=dict(payload.get("scan", {})),
            policy_version=int(payload.get("policy_version", 0)),
            principal_id=str(payload.get("principal_id", "")),
            expires_at=int(payload.get("expires_at", 0)),
            nonce=str(payload.get("nonce", "")),
            format=str(payload.get("format", "")),
            catalog=payload.get("catalog"),
        )
