from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List

import duckdb
import pyarrow as pa
import pyarrow.flight as flight

from dal_obscura.auth import AuthConfig, authenticate
from dal_obscura.backend.base import Backend, ScanTask
from dal_obscura.masking import build_select_list
from dal_obscura.policy import MaskRule, Principal, load_policy, resolve_access
from dal_obscura.tickets import TicketSigner, new_ticket_payload


@dataclass(frozen=True)
class ServerConfig:
    policy_path: str
    ticket_secret: str
    ticket_ttl_seconds: int
    auth: AuthConfig


class DataAccessFlightService(flight.FlightServerBase):
    def __init__(self, location: str, backend: Backend, config: ServerConfig) -> None:
        super().__init__(location)
        self._backend = backend
        self._config = config
        self._signer = TicketSigner(config.ticket_secret)
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_flight_info(
        self, context: flight.ServerCallContext, descriptor: flight.FlightDescriptor
    ) -> flight.FlightInfo:
        headers = _headers_from_context(context)
        request = _parse_descriptor(descriptor)
        if not headers and request.auth_token:
            headers = {"authorization": request.auth_token}
        auth_result = authenticate(headers, self._config.auth)
        principal = Principal(
            id=auth_result.principal_id,
            groups=auth_result.groups,
            attributes=auth_result.attributes,
        )
        policy = load_policy(self._config.policy_path)
        allowed_columns, masks, row_filter = resolve_access(
            policy=policy,
            principal=principal,
            table_identifier=request.table,
            requested_columns=request.columns,
        )
        self._logger.info(
            "plan_request",
            extra={
                "table": request.table,
                "principal": principal.id,
                "columns": allowed_columns,
                "policy_version": policy.version,
            },
        )
        plan = self._backend.plan(request.table, allowed_columns)

        tickets: List[flight.Ticket] = []
        for task in plan.tasks:
            scan = {
                "task": task.descriptor,
                "row_filter": row_filter,
                "masks": {k: v.__dict__ for k, v in masks.items()},
            }
            payload = new_ticket_payload(
                table=request.table,
                snapshot=plan.snapshot,
                columns=allowed_columns,
                scan=scan,
                policy_version=policy.version,
                principal_id=principal.id,
                ttl_seconds=self._config.ticket_ttl_seconds,
            )
            ticket = self._signer.sign_payload(payload)
            tickets.append(flight.Ticket(ticket.encode().encode("utf-8")))

        schema = pa.schema([])
        endpoints = [flight.FlightEndpoint(ticket, []) for ticket in tickets]
        return flight.FlightInfo(schema, descriptor, endpoints, -1, -1)

    def do_get(
        self, context: flight.ServerCallContext, ticket: flight.Ticket
    ) -> flight.RecordBatchStream:
        policy = load_policy(self._config.policy_path)
        payload = self._signer.verify(ticket.ticket.decode("utf-8"))
        if payload.policy_version != policy.version:
            raise flight.FlightUnauthorizedError("Policy version changed")

        scan_info = payload.scan
        task_descriptor = scan_info.get("task", {})
        row_filter = scan_info.get("row_filter")
        masks_raw = scan_info.get("masks", {})
        masks = {k: MaskRule(**v) for k, v in masks_raw.items()}

        table = self._backend.read(
            payload.table, payload.snapshot, task=ScanTask(descriptor=task_descriptor)
        )
        result = _apply_filters_and_masks(table, payload.columns, row_filter, masks)
        self._logger.info(
            "do_get",
            extra={
                "table": payload.table,
                "principal": payload.principal_id,
                "columns": payload.columns,
            },
        )
        return flight.RecordBatchStream(result)


def _headers_from_context(context: flight.ServerCallContext) -> Dict[str, str]:
    try:
        headers = dict(context.headers)
        normalized: Dict[str, str] = {}
        for k, v in headers.items():
            key = k.decode("utf-8") if isinstance(k, (bytes, bytearray)) else str(k)
            val = v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else str(v)
            normalized[key.lower()] = val
        return normalized
    except Exception:
        return {}


def _apply_filters_and_masks(
    table: pa.Table,
    columns: Iterable[str],
    row_filter: str | None,
    masks: Dict[str, MaskRule],
) -> pa.Table:
    selection = build_select_list(columns, masks)
    select_sql = ", ".join(selection.select_list)
    query = f"SELECT {select_sql} FROM input"
    if row_filter:
        query += f" WHERE {row_filter}"

    con = duckdb.connect()
    con.register("input", table)
    return con.execute(query).to_arrow_table()


@dataclass(frozen=True)
class PlanRequest:
    table: str
    columns: List[str]
    auth_token: str | None = None


def _parse_descriptor(descriptor: flight.FlightDescriptor) -> PlanRequest:
    if descriptor.command:
        raw = descriptor.command.decode("utf-8")
        data = json.loads(raw)
        auth_token = data.get("auth_token") or data.get("authorization") or data.get("api_key")
        return PlanRequest(
            table=str(data["table"]),
            columns=list(data["columns"]),
            auth_token=str(auth_token) if auth_token else None,
        )
    if descriptor.path:
        table = ".".join(descriptor.path)
        return PlanRequest(table=table, columns=["*"])
    raise ValueError("Invalid Flight descriptor")
