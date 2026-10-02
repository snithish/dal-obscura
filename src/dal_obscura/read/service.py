from __future__ import annotations

import hmac
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, fields
from typing import cast
from uuid import uuid4

from dal_obscura.identity.contracts import AuthenticationRequest, IdentityPort
from dal_obscura.policy.admission import (
    _authorize_requested_row_filter,
    _build_authorization_columns,
    _build_execution_projection,
    _extract_filter_dependencies,
    _validate_policy_row_filter,
    _validate_requested_row_filter,
    _visible_columns,
)
from dal_obscura.policy.filters import (
    RowFilter,
    combine_row_filters,
    deserialize_row_filter,
    serialize_row_filter,
)
from dal_obscura.policy.models import AccessDecision, MaskRule, Principal
from dal_obscura.policy.projection import compile_projection
from dal_obscura.policy.schema_index import SchemaIndex
from dal_obscura.read.contracts import (
    AuthorizedRead,
    DecodedScan,
    FetchStreamResult,
    GetSchemaResult,
    PlanAccessResult,
)
from dal_obscura.read.request import PlanRequest
from dal_obscura.read.signing_contract import TicketCodecPort
from dal_obscura.read.stream import ManagedStream, StreamGuard
from dal_obscura.read.ticket_repository import TicketStorePort
from dal_obscura.read.tickets import (
    TicketPayload,
    canonical_context_digest,
    ticket_payload_hash,
)
from dal_obscura.read.transform_contracts import RowTransformPort
from dal_obscura.sources.access import AccessContext, AccessContextPort
from dal_obscura.sources.task_codec import ScanTaskCodec


def _epoch_seconds() -> int:
    return int(time.time())


def _nonce() -> str:
    return os.urandom(16).hex()


def _ticket_id() -> str:
    return str(uuid4())


@dataclass(frozen=True)
class ReadSettings:
    ticket_ttl_seconds: int = 300
    max_tickets: int = 1
    max_ticket_exchanges: int = 1
    max_ticket_payload_bytes: int = 16 * 1024 * 1024
    max_stream_seconds: int = 300

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            if type(value) is not int or value < 1:
                raise ValueError(f"{field.name} must be a positive integer")


@dataclass(frozen=True)
class ReadService:
    """The complete governed read API; dependencies always form an executable service."""

    identity: IdentityPort
    access_context: AccessContextPort
    row_transform: RowTransformPort
    ticket_codec: TicketCodecPort
    ticket_store: TicketStorePort
    task_codec: ScanTaskCodec
    settings: ReadSettings = ReadSettings()
    now: Callable[[], int] = _epoch_seconds
    nonce_factory: Callable[[], str] = _nonce
    ticket_id_factory: Callable[[], str] = _ticket_id

    def schema(self, request: PlanRequest, auth_request: AuthenticationRequest) -> GetSchemaResult:
        principal = self.identity.authenticate(auth_request)
        with self.access_context.open(request.catalog, request.target) as context:
            admitted = self._authorize(context, principal, request)
            return GetSchemaResult(
                output_schema=admitted.output_schema,
                target=request.target,
                columns=list(admitted.projection.visible_columns),
                principal_id=principal.id,
                policy_version=admitted.decision.policy_version,
                catalog=request.catalog,
            )

    def _authorize(
        self, context: AccessContext, principal: Principal, request: PlanRequest
    ) -> AuthorizedRead:
        base_schema = context.schema

        requested_columns = SchemaIndex(base_schema).expand(request.columns)
        requested_row_filter = _validate_requested_row_filter(base_schema, request.row_filter)

        authorization_columns = _build_authorization_columns(
            requested_columns, requested_row_filter
        )
        decision = context.authorizer.authorize(
            principal=principal,
            target=request.target,
            catalog=request.catalog,
            requested_columns=authorization_columns,
        )
        asset_id = decision.asset_id
        if asset_id is None:
            raise PermissionError("Authorization did not resolve a governed asset identity")

        visible_columns = _visible_columns(
            requested_columns, decision, wildcard_requested=tuple(request.columns) == ("*",)
        )
        _authorize_requested_row_filter(requested_row_filter, decision)

        policy_row_filter = _validate_policy_row_filter(base_schema, decision.row_filter)
        effective_row_filter = combine_row_filters(policy_row_filter, requested_row_filter)
        execution_projection = _build_execution_projection(visible_columns, effective_row_filter)
        output_schema = compile_projection(
            base_schema, visible_columns, decision.masks
        ).output_schema
        return AuthorizedRead(
            base_schema,
            decision,
            execution_projection,
            tuple(authorization_columns),
            requested_row_filter,
            effective_row_filter,
            output_schema,
        )

    def plan(self, request: PlanRequest, auth_request: AuthenticationRequest) -> PlanAccessResult:
        principal = self.identity.authenticate(auth_request)
        with self.access_context.open(request.catalog, request.target) as context:
            admitted = self._authorize(context, principal, request)
            table_format = context.table_format
            base_schema, decision = admitted.schema, admitted.decision
            execution_projection = admitted.projection
            effective_row_filter = admitted.effective_filter
            requested_row_filter = admitted.requested_filter
            requested_filter_dependencies = _extract_filter_dependencies(requested_row_filter)
            authorization_columns = list(admitted.authorization_columns)
            asset_id = cast(str, decision.asset_id)
            execution_request = PlanRequest(
                catalog=request.catalog,
                target=request.target,
                columns=execution_projection.execution_columns,
                row_filter=effective_row_filter,
            )
            plan = table_format.plan(execution_request, self.settings.max_tickets)
            if not plan.schema.equals(base_schema, check_metadata=True) or any(
                not task.schema.equals(base_schema, check_metadata=True) for task in plan.tasks
            ):
                raise ValueError("Backend schema changed after authorization")

            now = self.now()
            identity_context = _identity_context_digest(principal)
            decision_digest = _decision_digest(decision)
            expiry = _ticket_expiry(principal.expires_at, now, self.settings.ticket_ttl_seconds)
            payloads: list[TicketPayload] = []
            for task in plan.tasks:
                # Each ticket carries enough context to re-validate authz later without
                # trusting the client to resubmit the original plan request faithfully.
                serialized_task = self.task_codec.encode(task)
                if len(serialized_task.encode("utf-8")) > self.settings.max_ticket_payload_bytes:
                    raise ValueError("Planned scan payload exceeds configured ticket byte limit")
                payload = TicketPayload(
                    ticket_id=self.ticket_id_factory(),
                    catalog=request.catalog,
                    target=request.target,
                    columns=list(execution_projection.visible_columns),
                    scan={
                        "read_payload": serialized_task,
                        "full_row_filter": None
                        if effective_row_filter is None
                        else serialize_row_filter(effective_row_filter),
                        "masks": {
                            key: {"type": value.type, "value": value.value}
                            for key, value in decision.masks.items()
                        },
                        "authorization_columns": authorization_columns,
                    },
                    policy_version=decision.policy_version,
                    principal_id=principal.id,
                    expires_at=expiry,
                    nonce=self.nonce_factory(),
                    issuer=principal.issuer,
                    identity_context=identity_context,
                    decision_digest=decision_digest,
                    asset_id=asset_id,
                )
                payloads.append(payload)
            self.ticket_store.store_many(payloads, max_exchanges=self.settings.max_ticket_exchanges)
            ticket_tokens = [self.ticket_codec.sign_payload(payload) for payload in payloads]

            output_schema = admitted.output_schema
            return PlanAccessResult(
                output_schema=output_schema,
                ticket_tokens=ticket_tokens,
                target=request.target,
                columns=list(execution_projection.visible_columns),
                principal_id=principal.id,
                policy_version=decision.policy_version,
                catalog=request.catalog,
                requested_row_filter_present=requested_row_filter is not None,
                requested_row_filter_dependency_count=len(requested_filter_dependencies),
                full_row_filter_present=effective_row_filter is not None,
                backend_pushdown_row_filter_present=plan.backend_pushdown_row_filter is not None,
                residual_row_filter_present=plan.residual_row_filter is not None,
                visible_column_count=len(execution_projection.visible_columns),
                execution_column_count=len(execution_projection.execution_columns),
            )

    def fetch(self, ticket: str, auth_request: AuthenticationRequest) -> FetchStreamResult:
        client_payload = self.ticket_codec.verify(ticket)
        if client_payload.ticket_id is None:
            raise PermissionError("Unauthorized")

        principal = self.identity.authenticate(auth_request)

        try:
            stored = self.ticket_store.load(client_payload.ticket_id)
        except (LookupError, PermissionError) as exc:
            raise PermissionError("Unauthorized") from exc

        payload = stored.payload
        if not hmac.compare_digest(ticket_payload_hash(payload), stored.payload_hash):
            raise PermissionError("Unauthorized")
        if client_payload.expires_at != payload.expires_at:
            raise PermissionError("Unauthorized")
        if not hmac.compare_digest(client_payload.nonce, payload.nonce):
            raise PermissionError("Unauthorized")
        _require_ticket_identity(principal, payload)
        if payload.identity_context and not hmac.compare_digest(
            payload.identity_context, _identity_context_digest(principal)
        ):
            raise PermissionError("Unauthorized")

        scan = _decode_scan(payload.scan)

        task = self.task_codec.decode(scan.read_payload)

        output_schema = compile_projection(task.schema, payload.columns, scan.masks).output_schema
        now = self.now()
        try:
            self.ticket_store.reserve_exchange(client_payload.ticket_id, now=now)
        except PermissionError:
            self.ticket_store.cleanup_expired(now=now)
            raise

        _, batches = task.table_format.execute(task.partition)
        scanner = ManagedStream(batches)
        try:
            transformed = self.row_transform.apply_filters_and_masks_stream(
                scanner, payload.columns, scan.full_row_filter, scan.masks
            )
            result_batches = ManagedStream(
                transformed,
                resources=(scanner,),
                guard=StreamGuard(
                    ticket_expires_at=payload.expires_at,
                    identity_expires_at=principal.expires_at,
                    stream_deadline_at=self.now() + self.settings.max_stream_seconds,
                    now=self.now,
                    check_revocation=lambda: self.ticket_store.ensure_active(
                        client_payload.ticket_id
                    ),
                ),
            )
        except BaseException:
            scanner.close(suppress_errors=True)
            raise

        return FetchStreamResult(
            output_schema=output_schema,
            result_batches=result_batches,
            target=payload.target,
            principal_id=payload.principal_id,
            columns=payload.columns,
            catalog=payload.catalog,
        )


def _ticket_expiry(identity_expiry: int | None, now: int, ttl_seconds: int) -> int:
    """Caps ticket validity at the authenticated identity's validated expiry."""
    configured_expiry = now + ttl_seconds
    if identity_expiry is None:
        return configured_expiry
    if identity_expiry <= now:
        raise PermissionError("Expired identity")
    return min(configured_expiry, identity_expiry)


def _identity_context_digest(
    principal: Principal,
) -> str:
    """Returns stable ticket-bound identity context without carrying raw claims."""
    return canonical_context_digest(
        {
            "issuer": principal.issuer,
            "subject": principal.id,
            "groups": sorted(set(principal.groups)),
            "attributes": dict(sorted(principal.attributes.items())),
        }
    )


def _decision_digest(decision: AccessDecision) -> str:
    """Returns a stable summary of the effective authorization decision."""
    return canonical_context_digest(
        {
            "allowed_columns": sorted(decision.allowed_columns),
            "masks": {
                path: {"type": mask.type, "value": mask.value}
                for path, mask in sorted(decision.masks.items())
            },
            "policy_version": decision.policy_version,
            "row_filter": decision.row_filter,
        }
    )


def _require_ticket_identity(principal: Principal, payload: TicketPayload) -> None:
    """Reject tickets whose authenticated issuer or subject no longer matches."""
    if principal.id != payload.principal_id or principal.issuer != payload.issuer:
        raise PermissionError("Unauthorized")


def _decode_scan(scan_info: Mapping[str, object]) -> DecodedScan:
    """Parses the format scan payload and mask metadata embedded in a ticket."""
    read_payload = scan_info.get("read_payload")
    if not isinstance(read_payload, str) or not read_payload:
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
        parsed_masks[name] = MaskRule(type=str(mask_type), value=mask_data.get("value"))

    authorization_columns = scan_info.get("authorization_columns")
    if (
        not isinstance(authorization_columns, list)
        or not authorization_columns
        or not all(isinstance(column, str) and column for column in authorization_columns)
        or len(authorization_columns) != len(set(authorization_columns))
    ):
        raise ValueError("Invalid authorization columns in ticket")

    return DecodedScan(
        read_payload=cast(str, read_payload),
        full_row_filter=_optional_row_filter(scan_info.get("full_row_filter")),
        masks=parsed_masks,
        authorization_columns=cast(list[str], authorization_columns),
    )


def _optional_row_filter(value: object) -> RowFilter | None:
    """Normalizes an optional validated row filter from the ticket payload."""
    if value is None:
        return None
    return deserialize_row_filter(value)
