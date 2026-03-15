from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, cast

from dal_obscura.application.ports import (
    AuthorizationPort,
    IdentityPort,
    MaskingPort,
    QueryBackendPort,
    RowTransformPort,
    TicketCodecPort,
)
from dal_obscura.domain.access_control import MaskRule, Principal
from dal_obscura.domain.query_planning import BackendReference, DatasetSelector


@dataclass(frozen=True)
class FetchStreamResult:
    output_schema: Any
    result_batches: Iterable[Any]
    target: str
    principal_id: str
    columns: list[str]
    catalog: str | None = None


@dataclass(frozen=True)
class DecodedScan:
    read_payload: bytes
    row_filter: str | None
    masks: dict[str, MaskRule]


class FetchStreamUseCase:
    def __init__(
        self,
        identity: IdentityPort,
        authorizer: AuthorizationPort,
        backend: QueryBackendPort,
        masking: MaskingPort,
        row_transform: RowTransformPort,
        ticket_codec: TicketCodecPort,
    ) -> None:
        self._identity = identity
        self._authorizer = authorizer
        self._backend = backend
        self._masking = masking
        self._row_transform = row_transform
        self._ticket_codec = ticket_codec

    def execute(self, ticket: str, headers: Mapping[str, str]) -> FetchStreamResult:
        payload = self._ticket_codec.verify(ticket)
        principal = _authenticate_request(
            self._identity, headers, payload.auth_header, payload.auth_value
        )
        if principal is None:
            raise PermissionError("Unauthorized")
        if principal.id != payload.principal_id:
            raise PermissionError("Unauthorized")

        selector = DatasetSelector(target=payload.target, catalog=payload.catalog)
        current_version = self._authorizer.current_policy_version(selector)
        if current_version is not None and payload.policy_version != current_version:
            raise PermissionError("Unauthorized")

        backend = BackendReference(
            backend_id=payload.backend_id,
            generation=payload.backend_generation,
        )
        scan = _decode_scan(payload.scan)
        spec = self._backend.read_spec(backend, scan.read_payload)
        if spec.dataset != selector or spec.columns != payload.columns:
            raise ValueError("Ticket payload mismatch")

        batches = self._backend.read_stream(backend, scan.read_payload)
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


def _authenticate_request(
    identity: IdentityPort,
    headers: Mapping[str, str],
    ticket_auth_header: str | None,
    ticket_auth_value: str | None,
) -> Principal | None:
    if headers:
        try:
            return identity.authenticate(headers)
        except PermissionError:
            return None
    if ticket_auth_header and ticket_auth_value:
        try:
            return identity.authenticate({ticket_auth_header: ticket_auth_value})
        except PermissionError:
            return None
    return None


def _decode_scan(scan_info: Mapping[str, object]) -> DecodedScan:
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
    if value is None:
        return None
    return str(value)
