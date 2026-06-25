from __future__ import annotations

import base64
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from uuid import uuid4

import pyarrow as pa

from dal_obscura.common.access_control.filters import (
    RowFilter,
    combine_row_filters,
    extract_row_filter_dependencies,
    parse_row_filter,
    serialize_row_filter,
    validate_row_filter_against_schema,
)
from dal_obscura.common.access_control.models import AccessDecision
from dal_obscura.common.query_planning.models import ExecutionProjection, PlanRequest
from dal_obscura.common.ticket_delivery.models import TicketPayload
from dal_obscura.data_plane.application.access_flow import AccessFlow
from dal_obscura.data_plane.application.ports.authorization import AuthorizationPort
from dal_obscura.data_plane.application.ports.catalog import CatalogRegistryPort
from dal_obscura.data_plane.application.ports.identity import AuthenticationRequest, IdentityPort
from dal_obscura.data_plane.application.ports.masking import MaskingPort
from dal_obscura.data_plane.application.ports.row_transform import RowTransformPort
from dal_obscura.data_plane.application.ports.ticket_codec import TicketCodecPort
from dal_obscura.data_plane.application.ports.ticket_store import TicketStorePort


@dataclass(frozen=True)
class PlanAccessResult:
    """Material returned to Flight `get_flight_info` after authorization succeeds."""

    output_schema: pa.Schema
    ticket_tokens: list[str]
    target: str
    columns: list[str]
    principal_id: str
    policy_version: int
    catalog: str | None = None
    requested_row_filter_present: bool = False
    requested_row_filter_dependency_count: int = 0
    full_row_filter_present: bool = False
    backend_pushdown_row_filter_present: bool = False
    residual_row_filter_present: bool = False
    visible_column_count: int = 0
    execution_column_count: int = 0


class PlanAccessUseCase:
    """Authenticates the caller, authorizes columns, and mints signed read tickets."""

    def __init__(
        self,
        identity: IdentityPort,
        authorizer: AuthorizationPort,
        catalog_registry: CatalogRegistryPort,
        masking: MaskingPort,
        ticket_codec: TicketCodecPort,
        ticket_store: TicketStorePort,
        ticket_ttl_seconds: int,
        max_tickets: int,
        max_ticket_exchanges: int,
        now: Callable[[], int] | None = None,
        nonce_factory: Callable[[], str] | None = None,
        ticket_id_factory: Callable[[], str] | None = None,
        row_transform: RowTransformPort | None = None,
        access_flow: AccessFlow | None = None,
    ) -> None:
        self._flow = access_flow or AccessFlow(
            identity=identity,
            authorizer=authorizer,
            catalog_registry=catalog_registry,
            masking=masking,
            row_transform=row_transform,
            ticket_codec=ticket_codec,
            ticket_store=ticket_store,
            ticket_ttl_seconds=ticket_ttl_seconds,
            max_tickets=max_tickets,
            max_ticket_exchanges=max_ticket_exchanges,
            now=now or _epoch_seconds,
            nonce_factory=nonce_factory or _nonce,
            ticket_id_factory=ticket_id_factory or _ticket_id,
        )

    def execute(
        self, request: PlanRequest, auth_request: AuthenticationRequest
    ) -> PlanAccessResult:
        """Builds a plan for the requested dataset and returns one signed ticket per task."""
        return plan_read(self._flow, request, auth_request)


def plan_read(
    flow: AccessFlow,
    request: PlanRequest,
    auth_request: AuthenticationRequest,
) -> PlanAccessResult:
    principal = flow.identity.authenticate(auth_request)
    tenant_id = _tenant_id(principal)

    # Phase 1: Discovery via Catalog
    table_format = flow.catalog_registry.describe(
        request.catalog,
        request.target,
        tenant_id=tenant_id,
    )
    base_schema = table_format.get_schema()

    requested_columns = _expand_requested_columns(base_schema, request.columns)
    requested_row_filter = _validate_requested_row_filter(base_schema, request.row_filter)
    requested_filter_dependencies = _extract_filter_dependencies(requested_row_filter)

    decision = flow.authorizer.authorize(
        principal=principal,
        target=request.target,
        catalog=request.catalog,
        requested_columns=_build_authorization_columns(requested_columns, requested_row_filter),
    )

    visible_columns = _visible_columns(requested_columns, decision)
    _authorize_requested_row_filter(requested_row_filter, decision)

    policy_row_filter = _validate_policy_row_filter(base_schema, decision.row_filter)
    effective_row_filter = combine_row_filters(policy_row_filter, requested_row_filter)
    execution_projection = _build_execution_projection(visible_columns, effective_row_filter)
    execution_request = PlanRequest(
        catalog=request.catalog,
        target=request.target,
        columns=execution_projection.execution_columns,
        row_filter=effective_row_filter,
    )
    plan = table_format.plan(execution_request, flow.max_tickets)

    now = flow.now()
    flow.ticket_store.cleanup_expired_and_exhausted(now=now)
    ticket_tokens: list[str] = []
    for task in plan.tasks:
        # Each ticket carries enough context to re-validate authz later without
        # trusting the client to resubmit the original plan request faithfully.
        import pickle

        payload = TicketPayload(
            ticket_id=flow.ticket_id_factory(),
            catalog=request.catalog,
            target=request.target,
            columns=execution_projection.visible_columns,
            scan={
                "read_payload": base64.b64encode(pickle.dumps(task)).decode("utf-8"),
                "full_row_filter": None
                if plan.full_row_filter is None
                else serialize_row_filter(plan.full_row_filter),
                "masks": {
                    key: {"type": value.type, "value": value.value}
                    for key, value in decision.masks.items()
                },
            },
            policy_version=decision.policy_version,
            principal_id=principal.id,
            expires_at=now + flow.ticket_ttl_seconds,
            nonce=flow.nonce_factory(),
            tenant_id=tenant_id,
        )
        flow.ticket_store.store(payload, max_exchanges=flow.max_ticket_exchanges)
        ticket_tokens.append(flow.ticket_codec.sign_payload(payload))

    output_schema = flow.masking.masked_schema(
        base_schema, execution_projection.visible_columns, decision.masks
    )
    return PlanAccessResult(
        output_schema=output_schema,
        ticket_tokens=ticket_tokens,
        target=request.target,
        columns=execution_projection.visible_columns,
        principal_id=principal.id,
        policy_version=decision.policy_version,
        catalog=request.catalog,
        requested_row_filter_present=requested_row_filter is not None,
        requested_row_filter_dependency_count=len(requested_filter_dependencies),
        full_row_filter_present=plan.full_row_filter is not None,
        backend_pushdown_row_filter_present=plan.backend_pushdown_row_filter is not None,
        residual_row_filter_present=plan.residual_row_filter is not None,
        visible_column_count=len(execution_projection.visible_columns),
        execution_column_count=len(execution_projection.execution_columns),
    )


def _build_execution_projection(
    visible_columns: list[str],
    row_filter: RowFilter | None,
) -> ExecutionProjection:
    """Builds the visible and internal columns needed to enforce policy safely."""
    internal_dependency_columns = [
        column
        for column in _extract_filter_dependencies(row_filter)
        if column not in visible_columns
    ]
    return ExecutionProjection(
        visible_columns=visible_columns,
        internal_dependency_columns=internal_dependency_columns,
        execution_columns=[*visible_columns, *internal_dependency_columns],
    )


def _expand_requested_columns(
    base_schema: pa.Schema,
    columns: list[str],
) -> list[str]:
    """Expands `*` into concrete column names so downstream authz remains explicit."""
    requested = list(columns)
    if "*" not in requested:
        _validate_requested_columns(base_schema, requested)
        return requested
    return [field.name for field in base_schema]


def _validate_requested_columns(schema: pa.Schema, requested: list[str]) -> None:
    """Fails fast when the client projects columns that are not in the dataset schema."""
    missing = [column for column in requested if not _schema_has_path(schema, column)]
    if missing:
        raise ValueError(f"Unknown columns requested: {', '.join(missing)}")


def _schema_has_path(schema: pa.Schema, column: str) -> bool:
    """Returns whether a top-level or nested struct field path exists in the schema."""
    parts = column.split(".")
    current_type: pa.DataType | None = None

    for index, part in enumerate(parts):
        if index == 0:
            try:
                current_type = schema.field(part).type
            except KeyError:
                return False
            continue

        if current_type is None or not pa.types.is_struct(current_type):
            return False
        struct_type = current_type
        try:
            current_type = struct_type.field(part).type
        except KeyError:
            return False

    return True


def _extract_filter_dependencies(row_filter: RowFilter | None) -> list[str]:
    if not row_filter:
        return []
    return extract_row_filter_dependencies(row_filter)


def _build_authorization_columns(
    requested_columns: list[str],
    row_filter: RowFilter | None,
) -> list[str]:
    authorization_columns = list(requested_columns)
    for dependency in _extract_filter_dependencies(row_filter):
        if dependency not in authorization_columns:
            authorization_columns.append(dependency)
    return authorization_columns


def _visible_columns(
    requested_columns: list[str],
    decision: AccessDecision,
) -> list[str]:
    visible_columns = [column for column in requested_columns if column in decision.allowed_columns]
    if not visible_columns:
        raise PermissionError("No allowed columns for principal")
    return visible_columns


def _authorize_requested_row_filter(
    row_filter: RowFilter | None,
    decision: AccessDecision,
) -> None:
    if row_filter is None:
        return

    dependencies = _extract_filter_dependencies(row_filter)

    masked = [column for column in dependencies if column in decision.masks]
    if masked:
        raise PermissionError(
            "Requested row filter may not reference masked columns: " + ", ".join(masked)
        )

    invisible = [column for column in dependencies if column not in decision.allowed_columns]
    if invisible:
        raise PermissionError(
            "Requested row filter may only reference visible unmasked columns: "
            + ", ".join(invisible)
        )


def _validate_policy_row_filter(schema: pa.Schema, row_filter: str | None) -> RowFilter | None:
    if row_filter is None:
        return None
    return parse_row_filter(row_filter, schema)


def _validate_requested_row_filter(
    schema: pa.Schema,
    row_filter: RowFilter | None,
) -> RowFilter | None:
    if row_filter is None:
        return None
    return validate_row_filter_against_schema(row_filter, schema)


def _epoch_seconds() -> int:
    return int(time.time())


def _tenant_id(principal) -> str:
    return str(
        principal.attributes.get("tenant_id") or principal.attributes.get("tenant") or "default"
    )


def _nonce() -> str:
    return base64.urlsafe_b64encode(os.urandom(12)).decode("utf-8")


def _ticket_id() -> str:
    return str(uuid4())
