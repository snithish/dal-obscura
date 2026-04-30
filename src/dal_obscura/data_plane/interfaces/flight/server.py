from __future__ import annotations

import logging

import pyarrow.flight as flight

from dal_obscura.data_plane.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.data_plane.application.use_cases.get_schema import GetSchemaUseCase
from dal_obscura.data_plane.application.use_cases.plan_access import PlanAccessUseCase
from dal_obscura.data_plane.interfaces.flight.contracts import (
    REQUEST_HEADERS_MIDDLEWARE_KEY,
    RequestHeadersMiddlewareFactory,
    authentication_request_from_context,
    parse_descriptor,
)
from dal_obscura.data_plane.interfaces.flight.streaming import (
    make_stream,
    normalize_schema_for_flight,
)
from dal_obscura.observability import get_resident_memory_bytes


class DataAccessFlightService(flight.FlightServerBase):
    """Arrow Flight transport adapter for the plan-then-fetch data access flow."""

    def __init__(
        self,
        location: str,
        get_schema_use_case: GetSchemaUseCase,
        plan_access_use_case: PlanAccessUseCase,
        fetch_stream_use_case: FetchStreamUseCase,
        *,
        tls_certificates: list[flight.CertKeyPair] | None = None,
        verify_client: bool = False,
        root_certificates: bytes | None = None,
    ) -> None:
        super().__init__(
            location,
            tls_certificates=tls_certificates,
            verify_client=verify_client,
            root_certificates=root_certificates,
            middleware={REQUEST_HEADERS_MIDDLEWARE_KEY: RequestHeadersMiddlewareFactory()},
        )
        self._get_schema_use_case = get_schema_use_case
        self._plan_access_use_case = plan_access_use_case
        self._fetch_stream_use_case = fetch_stream_use_case
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_schema(
        self, context: flight.ServerCallContext, descriptor: flight.FlightDescriptor
    ) -> flight.SchemaResult:
        """Returns the caller-visible masked schema without minting read tickets."""
        auth_request = authentication_request_from_context(context, method="get_schema")
        request = parse_descriptor(descriptor)
        try:
            result = self._get_schema_use_case.execute(request, auth_request)
        except PermissionError as exc:
            self._logger.warning(
                "auth_or_authz_failed", extra=self._log_extra(target=request.target)
            )
            raise flight.FlightUnauthorizedError("Unauthorized") from exc
        except ValueError as exc:
            raise flight.FlightInternalError(str(exc)) from exc

        self._logger.info(
            "schema_request",
            extra=self._log_extra(
                target=result.target,
                catalog=result.catalog,
                principal=result.principal_id,
                columns=result.columns,
                policy_version=result.policy_version,
            ),
        )
        return flight.SchemaResult(normalize_schema_for_flight(result.output_schema))

    def get_flight_info(
        self, context: flight.ServerCallContext, descriptor: flight.FlightDescriptor
    ) -> flight.FlightInfo:
        """Plans a dataset read and returns one endpoint per signed ticket."""
        auth_request = authentication_request_from_context(context, method="get_flight_info")
        request = parse_descriptor(descriptor)
        try:
            result = self._plan_access_use_case.execute(request, auth_request)
        except PermissionError as exc:
            self._logger.warning(
                "auth_or_authz_failed", extra=self._log_extra(target=request.target)
            )
            raise flight.FlightUnauthorizedError("Unauthorized") from exc
        except ValueError as exc:
            raise flight.FlightInternalError(str(exc)) from exc

        self._logger.info(
            "plan_request",
            extra=self._log_extra(
                target=result.target,
                catalog=result.catalog,
                principal=result.principal_id,
                columns=result.columns,
                policy_version=result.policy_version,
                requested_row_filter_present=result.requested_row_filter_present,
                requested_row_filter_dependency_count=result.requested_row_filter_dependency_count,
            ),
        )
        endpoints = [
            flight.FlightEndpoint(flight.Ticket(token.encode("utf-8")), [])
            for token in result.ticket_tokens
        ]
        return flight.FlightInfo(
            normalize_schema_for_flight(result.output_schema), descriptor, endpoints, -1, -1
        )

    def do_get(
        self, context: flight.ServerCallContext, ticket: flight.Ticket
    ) -> flight.RecordBatchStream:
        """Executes a previously planned read and streams the masked result batches."""
        auth_request = authentication_request_from_context(context, method="do_get")
        token = ticket.ticket.decode("utf-8")
        try:
            result = self._fetch_stream_use_case.execute(token, auth_request)
        except PermissionError as exc:
            self._logger.warning("unauthorized", extra=self._log_extra())
            raise flight.FlightUnauthorizedError("Unauthorized") from exc
        except ValueError as exc:
            self._logger.error("ticket_payload_mismatch", extra=self._log_extra())
            raise flight.FlightInternalError("Ticket payload mismatch") from exc

        self._logger.info(
            "do_get",
            extra=self._log_extra(
                target=result.target,
                catalog=result.catalog,
                principal=result.principal_id,
                columns=result.columns,
            ),
        )
        return make_stream(result.output_schema, result.result_batches)

    def _log_extra(self, **extra: object) -> dict[str, object]:
        """Adds process-level telemetry to every structured log line."""
        return {"resident_memory_bytes": get_resident_memory_bytes(), **extra}
