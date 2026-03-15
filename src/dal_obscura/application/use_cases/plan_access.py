from __future__ import annotations

import base64
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from dal_obscura.application.ports import (
    AuthorizationPort,
    IdentityPort,
    MaskingPort,
    PlanningBackendPort,
    TicketCodecPort,
)
from dal_obscura.domain.query_planning import PlanRequest
from dal_obscura.domain.ticket_delivery import TicketPayload


@dataclass(frozen=True)
class PlanAccessResult:
    output_schema: Any
    ticket_tokens: list[str]
    table: str
    columns: list[str]
    principal_id: str
    policy_version: int


class PlanAccessUseCase:
    def __init__(
        self,
        identity: IdentityPort,
        authorizer: AuthorizationPort,
        planning_backend: PlanningBackendPort,
        masking: MaskingPort,
        ticket_codec: TicketCodecPort,
        ticket_ttl_seconds: int,
        max_tickets: int,
        now: Callable[[], int] | None = None,
        nonce_factory: Callable[[], str] | None = None,
    ) -> None:
        self._identity = identity
        self._authorizer = authorizer
        self._planning_backend = planning_backend
        self._masking = masking
        self._ticket_codec = ticket_codec
        self._ticket_ttl_seconds = ticket_ttl_seconds
        self._max_tickets = max_tickets
        self._now = now or _epoch_seconds
        self._nonce_factory = nonce_factory or _nonce

    def execute(self, request: PlanRequest, headers: Mapping[str, str]) -> PlanAccessResult:
        normalized_headers = dict(headers)
        if not normalized_headers and request.auth_token:
            normalized_headers = {"authorization": request.auth_token}

        principal = self._identity.authenticate(normalized_headers)
        requested_columns = _expand_requested_columns(
            self._planning_backend, request.table, request.columns
        )
        decision = self._authorizer.authorize(
            principal=principal,
            table_identifier=request.table,
            requested_columns=requested_columns,
        )
        plan = self._planning_backend.plan(
            request.table, decision.allowed_columns, self._max_tickets
        )
        auth_header, auth_value = _auth_binding(normalized_headers, request.auth_token)

        ticket_tokens: list[str] = []
        for task in plan.tasks:
            payload = TicketPayload(
                table=request.table,
                columns=decision.allowed_columns,
                scan={
                    "read_payload": base64.b64encode(task.payload).decode("utf-8"),
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
                auth_header=auth_header,
                auth_value=auth_value,
            )
            ticket_tokens.append(self._ticket_codec.sign_payload(payload))

        output_schema = self._masking.masked_schema(
            plan.schema, decision.allowed_columns, decision.masks
        )
        return PlanAccessResult(
            output_schema=output_schema,
            ticket_tokens=ticket_tokens,
            table=request.table,
            columns=decision.allowed_columns,
            principal_id=principal.id,
            policy_version=decision.policy_version,
        )


def _auth_binding(
    headers: Mapping[str, str], fallback_token: str | None
) -> tuple[str | None, str | None]:
    if "authorization" in headers:
        return "authorization", headers["authorization"]
    if "x-api-key" in headers:
        return "x-api-key", headers["x-api-key"]
    if fallback_token:
        return "authorization", fallback_token
    return None, None


def _expand_requested_columns(
    backend: PlanningBackendPort, table: str, columns: list[str]
) -> list[str]:
    requested = list(columns)
    if "*" not in requested:
        return requested
    schema = backend.get_schema(table)
    return [field.name for field in schema]


def _epoch_seconds() -> int:
    return int(time.time())


def _nonce() -> str:
    return base64.urlsafe_b64encode(os.urandom(12)).decode("utf-8")
