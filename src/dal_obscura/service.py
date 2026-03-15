from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight as flight

from dal_obscura.auth.base import Authenticator
from dal_obscura.authorization import Authorizer
from dal_obscura.backend.base import Backend
from dal_obscura.masking import MaskApplier
from dal_obscura.policy import MaskRule, Principal
from dal_obscura.tickets import TicketSigner, new_ticket_payload


@dataclass(frozen=True)
class ServerConfig:
    ticket_secret: str
    ticket_ttl_seconds: int
    max_tickets: int
    authenticator: Authenticator
    authorizer: Authorizer
    mask_applier: MaskApplier


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
        try:
            auth_result = self._config.authenticator.authenticate(headers)
        except PermissionError as exc:
            self._logger.warning("auth_failed", extra={"table": request.table})
            raise flight.FlightUnauthorizedError("Unauthorized") from exc
        principal = Principal(
            id=auth_result.principal_id,
            groups=auth_result.groups,
            attributes=auth_result.attributes,
        )
        requested_columns = _expand_requested_columns(self._backend, request.table, request.columns)
        try:
            decision = self._config.authorizer.authorize(
                principal=principal,
                table_identifier=request.table,
                requested_columns=requested_columns,
            )
        except PermissionError as exc:
            self._logger.warning(
                "authz_failed", extra={"table": request.table, "principal": principal.id}
            )
            raise flight.FlightUnauthorizedError("Unauthorized") from exc
        self._logger.info(
            "plan_request",
            extra={
                "table": request.table,
                "principal": principal.id,
                "columns": decision.allowed_columns,
                "policy_version": decision.policy_version,
            },
        )
        plan = self._backend.plan(request.table, decision.allowed_columns, self._config.max_tickets)

        tickets: List[flight.Ticket] = []
        for read_payload in plan.tasks:
            scan = {
                "read_payload": base64.b64encode(read_payload.payload).decode("utf-8"),
                "row_filter": decision.row_filter,
                "masks": {k: v.__dict__ for k, v in decision.masks.items()},
            }
            auth_header, auth_value = _auth_binding(headers, request.auth_token)
            payload = new_ticket_payload(
                table=request.table,
                columns=decision.allowed_columns,
                scan=scan,
                policy_version=decision.policy_version,
                principal_id=principal.id,
                ttl_seconds=self._config.ticket_ttl_seconds,
                auth_header=auth_header,
                auth_value=auth_value,
            )
            ticket = self._signer.sign_payload(payload)
            tickets.append(flight.Ticket(ticket.encode().encode("utf-8")))

        output_schema = self._config.mask_applier.masked_schema(
            plan.schema, decision.allowed_columns, decision.masks
        )
        endpoints = [flight.FlightEndpoint(ticket, []) for ticket in tickets]
        return flight.FlightInfo(output_schema, descriptor, endpoints, -1, -1)

    def do_get(
        self, context: flight.ServerCallContext, ticket: flight.Ticket
    ) -> flight.RecordBatchStream:
        try:
            payload = self._signer.verify(ticket.ticket.decode("utf-8"))
        except PermissionError as exc:
            self._logger.warning("ticket_invalid")
            raise flight.FlightUnauthorizedError("Unauthorized") from exc

        headers = _headers_from_context(context)
        auth_result = _authenticate_request(
            self._config.authenticator, headers, payload.auth_header, payload.auth_value
        )
        if not auth_result:
            self._logger.warning("auth_failed", extra={"table": payload.table})
            raise flight.FlightUnauthorizedError("Unauthorized")
        if auth_result.principal_id != payload.principal_id:
            self._logger.warning(
                "principal_mismatch",
                extra={"expected": payload.principal_id, "actual": auth_result.principal_id},
            )
            raise flight.FlightUnauthorizedError("Unauthorized")

        current_version = self._config.authorizer.current_policy_version(payload.table)
        if current_version is not None and payload.policy_version != current_version:
            self._logger.warning(
                "policy_version_mismatch",
                extra={
                    "table": payload.table,
                    "ticket": payload.policy_version,
                    "current": current_version,
                },
            )
            raise flight.FlightUnauthorizedError("Unauthorized")

        scan_info = payload.scan
        read_payload = scan_info.get("read_payload")
        row_filter = scan_info.get("row_filter")
        masks_raw = scan_info.get("masks", {})
        masks = {k: MaskRule(**v) for k, v in masks_raw.items()}

        if not read_payload:
            raise flight.FlightInternalError("Missing read payload in ticket")
        read_payload_bytes = base64.b64decode(read_payload.encode("utf-8"))
        spec = self._backend.read_spec(read_payload_bytes)
        if spec.table != payload.table or spec.columns != payload.columns:
            self._logger.error(
                "read_spec_mismatch",
                extra={"table": payload.table, "spec_table": spec.table},
            )
            raise flight.FlightInternalError("Ticket payload mismatch")

        batches = self._backend.read_stream(read_payload_bytes)
        result_batches = _apply_filters_and_masks_stream(
            batches, payload.columns, row_filter, masks, self._config.mask_applier
        )
        output_schema = self._config.mask_applier.masked_schema(
            self._backend.get_schema(payload.table), payload.columns, masks
        )
        self._logger.info(
            "do_get",
            extra={
                "table": payload.table,
                "principal": payload.principal_id,
                "columns": payload.columns,
            },
        )
        return _make_stream(output_schema, result_batches)


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
    mask_applier: MaskApplier,
) -> pa.Table:
    selection = mask_applier.apply(columns, masks)
    select_sql = ", ".join(selection.select_list)
    query = f"SELECT {select_sql} FROM input"
    if row_filter:
        query += f" WHERE {row_filter}"

    con = duckdb.connect()
    con.register("input", table)
    return con.execute(query).to_arrow_table()


def _apply_filters_and_masks_stream(
    batches: Iterable[pa.RecordBatch],
    columns: Iterable[str],
    row_filter: str | None,
    masks: Dict[str, MaskRule],
    mask_applier: MaskApplier,
) -> Iterator[pa.RecordBatch]:
    selection = mask_applier.apply(columns, masks)
    select_sql = ", ".join(selection.select_list)
    query = f"SELECT {select_sql} FROM input"
    if row_filter:
        query += f" WHERE {row_filter}"

    con = duckdb.connect()
    try:
        for batch in batches:
            con.register("input", batch)
            try:
                reader = con.execute(query).to_arrow_reader()
                for output_batch in reader:
                    yield output_batch
            finally:
                con.unregister("input")
    finally:
        con.close()


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


def _auth_binding(
    headers: Dict[str, str], fallback_token: str | None
) -> tuple[str | None, str | None]:
    if "authorization" in headers:
        return "authorization", headers["authorization"]
    if "x-api-key" in headers:
        return "x-api-key", headers["x-api-key"]
    if fallback_token:
        return "authorization", fallback_token
    return None, None


def _authenticate_request(
    authenticator: Authenticator,
    headers: Dict[str, str],
    ticket_auth_header: str | None,
    ticket_auth_value: str | None,
):
    if headers:
        try:
            return authenticator.authenticate(headers)
        except PermissionError:
            return None
    if ticket_auth_header and ticket_auth_value:
        try:
            return authenticator.authenticate({ticket_auth_header: ticket_auth_value})
        except PermissionError:
            return None
    return None


def _expand_requested_columns(backend: Backend, table: str, columns: Iterable[str]) -> List[str]:
    requested = list(columns)
    if "*" not in requested:
        return requested
    schema = backend.get_schema(table)
    return [field.name for field in schema]


def _make_stream(schema: pa.Schema, batches: Iterable[pa.RecordBatch]) -> flight.RecordBatchStream:
    batches = _coerce_batches_to_schema(schema, batches)
    if hasattr(flight, "GeneratorStream"):
        return flight.GeneratorStream(schema, batches)
    batch_list = list(batches)
    reader = pa.RecordBatchReader.from_batches(schema, batch_list)
    return flight.RecordBatchStream(reader)


def _coerce_batches_to_schema(
    schema: pa.Schema, batches: Iterable[pa.RecordBatch]
) -> Iterator[pa.RecordBatch]:
    for batch in batches:
        arrays = []
        for field in schema:
            array = batch.column(batch.schema.get_field_index(field.name))
            if not array.type.equals(field.type):
                array = pc.cast(array, target_type=field.type, safe=False)
            arrays.append(array)
        yield pa.RecordBatch.from_arrays(arrays, schema=schema)
