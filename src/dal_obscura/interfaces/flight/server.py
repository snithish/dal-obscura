from __future__ import annotations

import logging

import pyarrow.flight as flight

from dal_obscura.application.use_cases import FetchStreamUseCase, PlanAccessUseCase
from dal_obscura.observability import get_resident_memory_bytes

from .contracts import headers_from_context, parse_descriptor
from .streaming import make_stream


class DataAccessFlightService(flight.FlightServerBase):
    def __init__(
        self,
        location: str,
        plan_access_use_case: PlanAccessUseCase,
        fetch_stream_use_case: FetchStreamUseCase,
    ) -> None:
        super().__init__(location)
        self._plan_access_use_case = plan_access_use_case
        self._fetch_stream_use_case = fetch_stream_use_case
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_flight_info(
        self, context: flight.ServerCallContext, descriptor: flight.FlightDescriptor
    ) -> flight.FlightInfo:
        headers = headers_from_context(context)
        request = parse_descriptor(descriptor)
        try:
            result = self._plan_access_use_case.execute(request, headers)
        except PermissionError as exc:
            self._logger.warning("auth_or_authz_failed", extra=self._log_extra(table=request.table))
            raise flight.FlightUnauthorizedError("Unauthorized") from exc

        self._logger.info(
            "plan_request",
            extra=self._log_extra(
                table=result.table,
                principal=result.principal_id,
                columns=result.columns,
                policy_version=result.policy_version,
            ),
        )
        endpoints = [
            flight.FlightEndpoint(flight.Ticket(token.encode("utf-8")), [])
            for token in result.ticket_tokens
        ]
        return flight.FlightInfo(result.output_schema, descriptor, endpoints, -1, -1)

    def do_get(
        self, context: flight.ServerCallContext, ticket: flight.Ticket
    ) -> flight.RecordBatchStream:
        headers = headers_from_context(context)
        token = ticket.ticket.decode("utf-8")
        try:
            result = self._fetch_stream_use_case.execute(token, headers)
        except PermissionError as exc:
            self._logger.warning("unauthorized", extra=self._log_extra())
            raise flight.FlightUnauthorizedError("Unauthorized") from exc
        except ValueError as exc:
            self._logger.error("ticket_payload_mismatch", extra=self._log_extra())
            raise flight.FlightInternalError("Ticket payload mismatch") from exc

        self._logger.info(
            "do_get",
            extra=self._log_extra(
                table=result.table,
                principal=result.principal_id,
                columns=result.columns,
            ),
        )
        return make_stream(result.output_schema, result.result_batches)

    def _log_extra(self, **extra: object) -> dict[str, object]:
        return {"resident_memory_bytes": get_resident_memory_bytes(), **extra}
