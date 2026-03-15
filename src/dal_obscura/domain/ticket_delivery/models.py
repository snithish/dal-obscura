from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TicketPayload:
    table: str
    columns: list[str]
    scan: dict[str, Any]
    policy_version: int
    principal_id: str
    expires_at: int
    nonce: str
    auth_header: str | None = None
    auth_value: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "table": self.table,
            "columns": self.columns,
            "scan": self.scan,
            "policy_version": self.policy_version,
            "principal_id": self.principal_id,
            "expires_at": self.expires_at,
            "nonce": self.nonce,
        }
        if self.auth_header is not None:
            payload["auth_header"] = self.auth_header
        if self.auth_value is not None:
            payload["auth_value"] = self.auth_value
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TicketPayload":
        return cls(
            table=str(payload["table"]),
            columns=list(payload.get("columns", [])),
            scan=dict(payload.get("scan", {})),
            policy_version=int(payload.get("policy_version", 0)),
            principal_id=str(payload.get("principal_id", "")),
            expires_at=int(payload.get("expires_at", 0)),
            nonce=str(payload.get("nonce", "")),
            auth_header=payload.get("auth_header"),
            auth_value=payload.get("auth_value"),
        )
