from __future__ import annotations

import base64
import hmac
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import cast

import pyarrow as pa

from dal_obscura.common.access_control.filters import RowFilter, deserialize_row_filter
from dal_obscura.common.access_control.models import MaskRule
from dal_obscura.common.table_format.ports import ScanTask
from dal_obscura.common.ticket_delivery.models import ticket_payload_hash
from dal_obscura.data_plane.application.access_flow import AccessFlow
from dal_obscura.data_plane.application.ports.authorization import AuthorizationPort
from dal_obscura.data_plane.application.ports.identity import AuthenticationRequest, IdentityPort
from dal_obscura.data_plane.application.ports.masking import MaskingPort
from dal_obscura.data_plane.application.ports.row_transform import RowTransformPort
from dal_obscura.data_plane.application.ports.ticket_codec import TicketCodecPort
from dal_obscura.data_plane.application.ports.ticket_store import TicketStorePort
from dal_obscura.data_plane.application.use_cases.plan_access import _tenant_id


@dataclass(frozen=True)
class FetchStreamResult:
    """Information needed to construct the Flight `do_get` response stream."""

    output_schema: pa.Schema
    result_batches: Iterable[pa.RecordBatch]
    target: str
    principal_id: str
    columns: list[str]
    catalog: str | None = None


@dataclass(frozen=True)
class DecodedScan:
    """Scan instructions restored from a ticket's serialized payload."""

    read_payload: bytes
    full_row_filter: RowFilter | None
    masks: dict[str, MaskRule]


class FetchStreamUseCase:
    """Verifies a ticket, re-authenticates the caller, and streams masked rows."""

    def __init__(
        self,
        identity: IdentityPort,
        authorizer: AuthorizationPort,
        masking: MaskingPort,
        row_transform: RowTransformPort,
        ticket_codec: TicketCodecPort,
        ticket_store: TicketStorePort,
        now: Callable[[], int] | None = None,
        access_flow: AccessFlow | None = None,
    ) -> None:
        self._flow = access_flow or AccessFlow(
            identity=identity,
            authorizer=authorizer,
            catalog_registry=_UnavailableCatalogRegistry(),
            masking=masking,
            row_transform=row_transform,
            ticket_codec=ticket_codec,
            ticket_store=ticket_store,
            ticket_ttl_seconds=0,
            max_tickets=0,
            max_ticket_exchanges=0,
            now=now or _epoch_seconds,
        )

    def execute(self, ticket: str, auth_request: AuthenticationRequest) -> FetchStreamResult:
        """Executes the second half of the Flight flow for a previously planned ticket."""
        return fetch_read(self._flow, ticket, auth_request)


def fetch_read(
    flow: AccessFlow,
    ticket: str,
    auth_request: AuthenticationRequest,
) -> FetchStreamResult:
    client_payload = flow.ticket_codec.verify(ticket)
    if client_payload.ticket_id is None:
        raise PermissionError("Unauthorized")

    principal = flow.identity.authenticate(auth_request)

    try:
        stored = flow.ticket_store.load(client_payload.ticket_id)
    except (LookupError, PermissionError) as exc:
        raise PermissionError("Unauthorized") from exc

    payload = stored.payload
    if not hmac.compare_digest(ticket_payload_hash(payload), stored.payload_hash):
        raise PermissionError("Unauthorized")
    if client_payload.expires_at != payload.expires_at:
        raise PermissionError("Unauthorized")
    if not hmac.compare_digest(client_payload.nonce, payload.nonce):
        raise PermissionError("Unauthorized")
    if principal.id != payload.principal_id:
        raise PermissionError("Unauthorized")

    tenant_id = _tenant_id(principal)
    if tenant_id != payload.tenant_id:
        raise PermissionError("Unauthorized")

    current_policy_version = flow.authorizer.current_policy_version(
        payload.target,
        payload.catalog,
        tenant_id=tenant_id,
    )
    if current_policy_version != payload.policy_version:
        raise PermissionError("stale policy version")

    now = flow.now()
    try:
        flow.ticket_store.reserve_exchange(client_payload.ticket_id, now=now)
    except PermissionError:
        flow.ticket_store.cleanup_expired_and_exhausted(now=now)
        raise

    scan = _decode_scan(payload.scan)

    import pickle

    task = pickle.loads(scan.read_payload)
    if not isinstance(task, ScanTask):
        raise ValueError("Invalid read payload in ticket")

    original_schema, batches = task.table_format.execute(task.partition)
    if flow.row_transform is None:
        raise ValueError("Fetch flow requires a row transform")

    result_batches = flow.row_transform.apply_filters_and_masks_stream(
        batches, payload.columns, scan.full_row_filter, scan.masks
    )

    output_schema = flow.masking.masked_schema(original_schema, payload.columns, scan.masks)

    return FetchStreamResult(
        output_schema=output_schema,
        result_batches=result_batches,
        target=payload.target,
        principal_id=payload.principal_id,
        columns=payload.columns,
        catalog=payload.catalog,
    )


def _decode_scan(scan_info: Mapping[str, object]) -> DecodedScan:
    """Parses the format scan payload and mask metadata embedded in a ticket."""
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
        full_row_filter=_optional_row_filter(scan_info.get("full_row_filter")),
        masks=parsed_masks,
    )


def _optional_row_filter(value: object) -> RowFilter | None:
    """Normalizes an optional validated row filter from the ticket payload."""
    if value is None:
        return None
    return deserialize_row_filter(value)


def _epoch_seconds() -> int:
    return int(time.time())


class _UnavailableCatalogRegistry:
    def describe(self, catalog: str | None, target: str, *, tenant_id: str):
        del catalog, target, tenant_id
        raise RuntimeError("Fetch flow does not resolve catalogs")
