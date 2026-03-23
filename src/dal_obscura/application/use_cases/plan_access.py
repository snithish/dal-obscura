from __future__ import annotations

import base64
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass

import pyarrow as pa

from dal_obscura.application.ports.authorization import AuthorizationPort
from dal_obscura.application.ports.identity import IdentityPort
from dal_obscura.application.ports.masking import MaskingPort
from dal_obscura.application.ports.ticket_codec import TicketCodecPort
from dal_obscura.domain.query_planning.models import PlanRequest
from dal_obscura.domain.ticket_delivery.models import TicketPayload
from dal_obscura.infrastructure.adapters.catalog_registry import DynamicCatalogRegistry


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


class PlanAccessUseCase:
    """Authenticates the caller, authorizes columns, and mints signed read tickets."""

    def __init__(
        self,
        identity: IdentityPort,
        authorizer: AuthorizationPort,
        catalog_registry: DynamicCatalogRegistry,
        masking: MaskingPort,
        ticket_codec: TicketCodecPort,
        ticket_ttl_seconds: int,
        max_tickets: int,
        now: Callable[[], int] | None = None,
        nonce_factory: Callable[[], str] | None = None,
    ) -> None:
        self._identity = identity
        self._authorizer = authorizer
        self._catalog_registry = catalog_registry
        self._masking = masking
        self._ticket_codec = ticket_codec
        self._ticket_ttl_seconds = ticket_ttl_seconds
        self._max_tickets = max_tickets
        self._now = now or _epoch_seconds
        self._nonce_factory = nonce_factory or _nonce

    def execute(self, request: PlanRequest, headers: Mapping[str, str]) -> PlanAccessResult:
        """Builds a plan for the requested dataset and returns one signed ticket per task."""
        principal = self._identity.authenticate(headers)

        # Phase 1: Discovery via Catalog
        table_format = self._catalog_registry.describe(request.catalog, request.target)
        base_schema = table_format.get_schema()

        requested_columns = _expand_requested_columns(base_schema, request.columns)

        decision = self._authorizer.authorize(
            principal=principal,
            target=request.target,
            catalog=request.catalog,
            requested_columns=requested_columns,
        )

        plan = table_format.plan(request, self._max_tickets)

        ticket_tokens: list[str] = []
        for task in plan.tasks:
            # Each ticket carries enough context to re-validate authz later without
            # trusting the client to resubmit the original plan request faithfully.
            import pickle

            payload = TicketPayload(
                catalog=request.catalog,
                target=request.target,
                columns=decision.allowed_columns,
                scan={
                    "read_payload": base64.b64encode(pickle.dumps(task)).decode("utf-8"),
                    "row_filter": decision.row_filter,
                    "masks": {
                        key: {"type": value.type, "value": value.value}
                        for key, value in decision.masks.items()
                    },
                },
                policy_version=decision.policy_version,
                principal_id=principal.id,
                expires_at=self._now() + self._ticket_ttl_seconds,
                nonce=self._nonce_factory(),
            )
            ticket_tokens.append(self._ticket_codec.sign_payload(payload))

        output_schema = self._masking.masked_schema(
            plan.schema, decision.allowed_columns, decision.masks
        )
        return PlanAccessResult(
            output_schema=output_schema,
            ticket_tokens=ticket_tokens,
            target=request.target,
            columns=decision.allowed_columns,
            principal_id=principal.id,
            policy_version=decision.policy_version,
            catalog=request.catalog,
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
    schema_names = {field.name for field in schema}
    missing = [column for column in requested if column not in schema_names]
    if missing:
        raise ValueError(f"Unknown columns requested: {', '.join(missing)}")


def _epoch_seconds() -> int:
    return int(time.time())


def _nonce() -> str:
    return base64.urlsafe_b64encode(os.urandom(12)).decode("utf-8")
