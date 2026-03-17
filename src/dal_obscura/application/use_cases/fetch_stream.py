from __future__ import annotations

import base64
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, cast

from dal_obscura.application.ports.authorization import AuthorizationPort
from dal_obscura.application.ports.identity import IdentityPort
from dal_obscura.application.ports.masking import MaskingPort
from dal_obscura.application.ports.row_transform import RowTransformPort
from dal_obscura.application.ports.ticket_codec import TicketCodecPort
from dal_obscura.domain.access_control.models import MaskRule
from dal_obscura.domain.query_planning.models import BackendReference, DatasetSelector
from dal_obscura.infrastructure.adapters.backend_registry import DynamicBackendRegistry


@dataclass(frozen=True)
class FetchStreamResult:
    """Information needed to construct the Flight `do_get` response stream."""

    output_schema: Any
    result_batches: Iterable[Any]
    target: str
    principal_id: str
    columns: list[str]
    catalog: str | None = None


@dataclass(frozen=True)
class DecodedScan:
    """Scan instructions restored from a ticket's serialized payload."""

    read_payload: bytes
    row_filter: str | None
    masks: dict[str, MaskRule]


class FetchStreamUseCase:
    """Verifies a ticket, re-checks authn/authz freshness, and streams masked rows."""

    def __init__(
        self,
        identity: IdentityPort,
        authorizer: AuthorizationPort,
        backend_registry: DynamicBackendRegistry,
        masking: MaskingPort,
        row_transform: RowTransformPort,
        ticket_codec: TicketCodecPort,
    ) -> None:
        self._identity = identity
        self._authorizer = authorizer
        self._backend_registry = backend_registry
        self._masking = masking
        self._row_transform = row_transform
        self._ticket_codec = ticket_codec

    def execute(self, ticket: str, headers: Mapping[str, str]) -> FetchStreamResult:
        """Executes the second half of the Flight flow for a previously planned ticket."""
        payload = self._ticket_codec.verify(ticket)
        principal = self._identity.authenticate(headers)
        if principal.id != payload.principal_id:
            raise PermissionError("Unauthorized")

        selector = DatasetSelector(target=payload.target, catalog=payload.catalog)
        current_version = self._authorizer.current_policy_version(selector)
        if current_version is not None and payload.policy_version != current_version:
            raise PermissionError("Unauthorized")

        backend_reference = BackendReference(
            backend_id=payload.backend_id,
            generation=payload.backend_generation,
        )
        scan = _decode_scan(payload.scan)
        spec = self._backend_registry.read_spec_for(backend_reference, scan.read_payload)
        if spec.dataset != selector or spec.columns != payload.columns:
            raise ValueError("Ticket payload mismatch")

        # Data is fetched first, then row filters and masks are enforced over the
        # backend output so the fetch path remains backend-agnostic.
        batches = self._backend_registry.read_stream_for(backend_reference, scan.read_payload)
        result_batches = self._row_transform.apply_filters_and_masks_stream(
            batches, payload.columns, scan.row_filter, scan.masks
        )
        output_schema = self._masking.masked_schema(spec.schema, payload.columns, scan.masks)
        return FetchStreamResult(
            output_schema=output_schema,
            result_batches=result_batches,
            target=payload.target,
            principal_id=payload.principal_id,
            columns=payload.columns,
            catalog=payload.catalog,
        )


def _decode_scan(scan_info: Mapping[str, object]) -> DecodedScan:
    """Parses the backend scan payload and mask metadata embedded in a ticket."""
    read_payload = scan_info.get("read_payload")
    if not read_payload:
        raise ValueError("Missing read payload in ticket")
    masks_raw = scan_info.get("masks", {})
    if not isinstance(masks_raw, Mapping):
        raise ValueError("Invalid mask payload in ticket")

    parsed_masks: dict[str, MaskRule] = {}
    for name, raw_mask in masks_raw.items():
        if not isinstance(name, str) or not isinstance(raw_mask, dict):
            raise ValueError("Invalid mask payload in ticket")
        mask_data = cast(dict[str, object], raw_mask)
        mask_type = mask_data.get("type")
        if mask_type is None:
            raise ValueError("Invalid mask payload in ticket")
        parsed_masks[name] = MaskRule(
            type=str(mask_type),
            value=mask_data.get("value"),
        )

    return DecodedScan(
        read_payload=base64.b64decode(str(read_payload).encode("utf-8")),
        row_filter=_optional_string(scan_info.get("row_filter")),
        masks=parsed_masks,
    )


def _optional_string(value: object) -> str | None:
    """Normalizes optional scalar values from JSON-like ticket payloads."""
    if value is None:
        return None
    return str(value)
