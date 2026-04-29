from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TypedDict, cast


class MaskPayload(TypedDict):
    type: str
    value: object | None


class ScanPayload(TypedDict):
    read_payload: str
    full_row_filter: str | None
    masks: dict[str, MaskPayload]


@dataclass(frozen=True)
class TicketPayload:
    """Serialized contents of a signed Flight ticket."""

    target: str
    columns: list[str]
    scan: ScanPayload
    policy_version: int
    principal_id: str
    expires_at: int
    nonce: str
    tenant_id: str = "default"
    catalog: str | None = None

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
        }
        if self.catalog is not None:
            payload["catalog"] = self.catalog
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> TicketPayload:
        """Restores a payload after signature verification and JSON parsing."""
        return cls(
            target=str(payload["target"]),
            columns=_coerce_columns(payload.get("columns")),
            scan=_coerce_scan_payload(payload.get("scan")),
            policy_version=_coerce_int(payload.get("policy_version")),
            principal_id=str(payload.get("principal_id", "")),
            expires_at=_coerce_int(payload.get("expires_at")),
            nonce=str(payload.get("nonce", "")),
            tenant_id=str(payload.get("tenant_id", "default") or "default"),
            catalog=_coerce_optional_str(payload.get("catalog")),
        )


def _coerce_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, (str, bytes, bytearray)):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _coerce_optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _coerce_columns(raw: object) -> list[str]:
    if not isinstance(raw, list):
        return []
    return [str(item) for item in raw]


def _coerce_scan_payload(raw: object) -> ScanPayload:
    if not isinstance(raw, Mapping):
        return {"read_payload": "", "full_row_filter": None, "masks": {}}
    raw_mapping = cast(Mapping[str, object], raw)
    read_payload = raw_mapping.get("read_payload")
    full_row_filter = raw_mapping.get("full_row_filter")
    return {
        "read_payload": "" if read_payload is None else str(read_payload),
        "full_row_filter": _coerce_row_filter(full_row_filter),
        "masks": _coerce_masks(raw_mapping.get("masks")),
    }


def _coerce_masks(raw: object) -> dict[str, MaskPayload]:
    if not isinstance(raw, Mapping):
        return {}
    masks: dict[str, MaskPayload] = {}
    raw_mapping = cast(Mapping[str, object], raw)
    for key, value in raw_mapping.items():
        if not isinstance(key, str) or not isinstance(value, Mapping):
            continue
        mask_mapping = cast(Mapping[str, object], value)
        mask_type = mask_mapping.get("type")
        if mask_type is None:
            continue
        masks[key] = {"type": str(mask_type), "value": mask_mapping.get("value")}
    return masks


def _coerce_row_filter(raw: object) -> str | None:
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw:
        return None
    return raw
