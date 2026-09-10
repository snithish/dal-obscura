"""Ticket payload value objects and canonical hashing helpers.

Example:
    ```python
    payload_hash = ticket_payload_hash(payload)
    encoded = payload.to_dict()
    restored = TicketPayload.from_dict(encoded)
    ```
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TypedDict, cast


class MaskPayload(TypedDict):
    """JSON shape for one mask stored inside a ticket scan payload."""

    type: str
    value: object | None


class ScanPayloadBase(TypedDict):
    """JSON shape for server-stored scan context referenced by a ticket."""

    read_payload: str
    full_row_filter: str | None
    masks: dict[str, MaskPayload]


class ScanPayload(ScanPayloadBase, total=False):
    """Scan payload with optional fields supported by prior ticket records."""

    authorization_columns: list[str]


@dataclass(frozen=True)
class TicketPayload:
    """Serialized contents of a signed Flight ticket.

    Example:
        ```python
        payload = TicketPayload(
            ticket_id="ticket-1",
            catalog="analytics",
            target="default.users",
            columns=["id"],
            scan={"read_payload": "...", "full_row_filter": None, "masks": {}},
            policy_version=1,
            principal_id="user:alice",
            expires_at=1_900_000_000,
            nonce="nonce",
            tenant_id="default",
        )
        ```
    """

    target: str
    columns: list[str]
    scan: ScanPayload
    policy_version: int
    principal_id: str
    expires_at: int
    nonce: str
    tenant_id: str = "default"
    issuer: str = ""
    identity_context: str = ""
    decision_digest: str = ""
    catalog: str | None = None
    ticket_id: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Produces a JSON-friendly representation used by the ticket codec."""
        payload: dict[str, object] = {
            "target": self.target,
            "columns": self.columns,
            "scan": self.scan,
            "policy_version": self.policy_version,
            "principal_id": self.principal_id,
            "expires_at": self.expires_at,
            "nonce": self.nonce,
            "tenant_id": self.tenant_id,
            "issuer": self.issuer,
            "identity_context": self.identity_context,
            "decision_digest": self.decision_digest,
        }
        if self.catalog is not None:
            payload["catalog"] = self.catalog
        if self.ticket_id is not None:
            payload["ticket_id"] = self.ticket_id
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> TicketPayload:
        """Restores one complete, strictly-shaped stored ticket payload.

        Stored tickets are security inputs.  A malformed record must not be
        coerced into an apparently valid permissive read.
        """
        _reject_unknown_fields(
            payload,
            {
                "target",
                "columns",
                "scan",
                "policy_version",
                "principal_id",
                "expires_at",
                "nonce",
                "tenant_id",
                "issuer",
                "identity_context",
                "decision_digest",
                "catalog",
                "ticket_id",
            },
        )
        return cls(
            target=_required_string(payload, "target"),
            columns=_strict_columns(payload.get("columns")),
            scan=_strict_scan_payload(payload.get("scan")),
            policy_version=_strict_int(payload, "policy_version", minimum=0),
            principal_id=_required_string(payload, "principal_id"),
            expires_at=_strict_int(payload, "expires_at", minimum=0),
            nonce=_required_string(payload, "nonce"),
            tenant_id=_required_string(payload, "tenant_id"),
            issuer=_required_string(payload, "issuer", allow_empty=True),
            identity_context=_required_string(payload, "identity_context", allow_empty=True),
            decision_digest=_required_string(payload, "decision_digest", allow_empty=True),
            catalog=_optional_string(payload, "catalog"),
            ticket_id=_optional_string(payload, "ticket_id"),
        )


def _reject_unknown_fields(payload: Mapping[str, object], allowed: set[str]) -> None:
    unknown = set(payload).difference(allowed)
    if unknown:
        raise ValueError("Unknown ticket payload fields")


def _required_string(payload: Mapping[str, object], key: str, *, allow_empty: bool = False) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or (not allow_empty and not value):
        raise ValueError(f"Ticket payload {key} must be a non-empty string")
    return value


def _optional_string(payload: Mapping[str, object], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"Ticket payload {key} must be a non-empty string or null")
    return value


def _strict_int(payload: Mapping[str, object], key: str, *, minimum: int) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"Ticket payload {key} must be an integer")
    return value


def _strict_columns(raw: object) -> list[str]:
    if (
        not isinstance(raw, list)
        or not raw
        or any(not isinstance(item, str) or not item for item in raw)
    ):
        raise ValueError("Ticket payload columns must be a non-empty string list")
    if len(raw) != len(set(raw)):
        raise ValueError("Ticket payload columns must not contain duplicates")
    return cast(list[str], raw)


def _strict_scan_payload(raw: object) -> ScanPayload:
    if not isinstance(raw, Mapping):
        raise ValueError("Ticket payload scan must be an object")
    raw_mapping = cast(Mapping[str, object], raw)
    _reject_unknown_fields(
        raw_mapping,
        {"read_payload", "full_row_filter", "masks", "authorization_columns"},
    )
    read_payload = raw_mapping.get("read_payload")
    full_row_filter = raw_mapping.get("full_row_filter")
    if not isinstance(read_payload, str) or not read_payload:
        raise ValueError("Ticket payload read_payload must be a non-empty string")
    if full_row_filter is not None and (
        not isinstance(full_row_filter, str) or not full_row_filter
    ):
        raise ValueError("Ticket payload full_row_filter must be a string or null")
    payload: ScanPayload = {
        "read_payload": read_payload,
        "full_row_filter": full_row_filter,
        "masks": _strict_masks(raw_mapping.get("masks")),
    }
    if "authorization_columns" in raw_mapping:
        payload["authorization_columns"] = _strict_columns(
            raw_mapping.get("authorization_columns")
        )
    return payload


def _strict_masks(raw: object) -> dict[str, MaskPayload]:
    if not isinstance(raw, Mapping):
        raise ValueError("Ticket payload masks must be an object")
    masks: dict[str, MaskPayload] = {}
    raw_mapping = cast(Mapping[str, object], raw)
    for key, value in raw_mapping.items():
        if not isinstance(key, str) or not isinstance(value, Mapping):
            raise ValueError("Ticket payload masks must map strings to objects")
        mask_mapping = cast(Mapping[str, object], value)
        _reject_unknown_fields(mask_mapping, {"type", "value"})
        mask_type = mask_mapping.get("type")
        if not isinstance(mask_type, str) or not mask_type:
            raise ValueError("Ticket payload mask type must be a non-empty string")
        masks[key] = {"type": mask_type, "value": mask_mapping.get("value")}
    return masks


def canonical_ticket_payload_bytes(payload: TicketPayload) -> bytes:
    """Returns stable JSON bytes used for signing and integrity checks.

    Example:
        ```python
        signed_bytes = canonical_ticket_payload_bytes(payload)
        ```
    """
    return json.dumps(
        payload.to_dict(),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def ticket_payload_hash(payload: TicketPayload) -> str:
    """Returns SHA-256 hex digest of the canonical ticket payload.

    Example:
        ```python
        digest = ticket_payload_hash(payload)
        ```
    """
    return hashlib.sha256(canonical_ticket_payload_bytes(payload)).hexdigest()


def canonical_context_digest(value: Mapping[str, object]) -> str:
    """Hashes normalized authorization or identity context for ticket binding."""
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()
