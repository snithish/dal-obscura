from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping

import pyarrow as pa
import pyarrow.flight as flight

from dal_obscura.interfaces.flight.contracts import (
    REQUEST_HEADERS_MIDDLEWARE_KEY,
    RequestHeadersMiddlewareFactory,
    authentication_request_from_context,
    parse_descriptor,
)
from dal_obscura.interfaces.flight.streaming import make_stream
from dal_obscura.observability import ServiceMetrics, get_resident_memory_bytes
from dal_obscura.read.service import ReadService
from dal_obscura.sources.access import AccessContextUnavailable

HEALTH_ACTION = "healthz"


class DataAccessFlightService(flight.FlightServerBase):
    """Arrow Flight transport adapter for the plan-then-fetch data access flow."""

    def __init__(
        self,
        location: str,
        reads: ReadService,
        *,
        tls_certificates: list[flight.CertKeyPair] | None = None,
        verify_client: bool = False,
        root_certificates: bytes | None = None,
        health_check: Callable[[], Mapping[str, object]] | None = None,
        metrics: ServiceMetrics | None = None,
    ) -> None:
        super().__init__(
            location,
            tls_certificates=tls_certificates,
            verify_client=verify_client,
            root_certificates=root_certificates,
            middleware={REQUEST_HEADERS_MIDDLEWARE_KEY: RequestHeadersMiddlewareFactory()},
        )
        self._reads = reads
        self._health_check = health_check
        self._metrics = metrics or ServiceMetrics()
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def metrics(self) -> ServiceMetrics:
        """Returns the bounded process-local metrics collector."""

        return self._metrics

    def list_actions(self, context: flight.ServerCallContext) -> list[flight.ActionType]:
        del context
        return [flight.ActionType(HEALTH_ACTION, "Data plane health check")]

    def do_action(
        self,
        context: flight.ServerCallContext,
        action: flight.Action,
    ) -> list[flight.Result]:
        del context
        if action.type != HEALTH_ACTION:
            raise pa.ArrowInvalid("Unsupported action")
        payload = _health_payload(self._health_check, self._logger, metrics=self._metrics)
        body = json.dumps(payload, separators=(",", ":"))
        return [flight.Result(body.encode("utf-8"))]

    def get_schema(
        self, context: flight.ServerCallContext, descriptor: flight.FlightDescriptor
    ) -> flight.SchemaResult:
        """Returns the caller-visible masked schema without minting read tickets."""
        with self._metrics.measure("flight.get_schema"):
            try:
                auth_request = authentication_request_from_context(context, method="get_schema")
                request = parse_descriptor(descriptor)
                result = self._reads.schema(request, auth_request)
            except AccessContextUnavailable as exc:
                raise flight.FlightUnavailableError(str(exc)) from exc
            except PermissionError as exc:
                self._logger.warning("auth_or_authz_failed", extra=self._log_extra())
                raise flight.FlightUnauthorizedError("Unauthorized") from exc
            except ValueError as exc:
                self._logger.warning("invalid_request", extra=self._log_extra())
                raise pa.ArrowInvalid("Invalid request") from exc

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
            return flight.SchemaResult(result.output_schema)

    def get_flight_info(
        self, context: flight.ServerCallContext, descriptor: flight.FlightDescriptor
    ) -> flight.FlightInfo:
        """Plans a dataset read and returns one endpoint per signed ticket."""
        with self._metrics.measure("flight.get_flight_info"):
            try:
                auth_request = authentication_request_from_context(
                    context, method="get_flight_info"
                )
                request = parse_descriptor(descriptor)
                result = self._reads.plan(request, auth_request)
            except AccessContextUnavailable as exc:
                raise flight.FlightUnavailableError(str(exc)) from exc
            except PermissionError as exc:
                self._logger.warning("auth_or_authz_failed", extra=self._log_extra())
                raise flight.FlightUnauthorizedError("Unauthorized") from exc
            except ValueError as exc:
                self._logger.warning("invalid_request", extra=self._log_extra())
                raise pa.ArrowInvalid("Invalid request") from exc

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
                    full_row_filter_present=result.full_row_filter_present,
                    backend_pushdown_row_filter_present=result.backend_pushdown_row_filter_present,
                    residual_row_filter_present=result.residual_row_filter_present,
                    visible_column_count=result.visible_column_count,
                    execution_column_count=result.execution_column_count,
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
        """Executes a previously planned read and streams the masked result batches."""
        with self._metrics.measure("flight.do_get"):
            auth_request = authentication_request_from_context(context, method="do_get")
            try:
                token = ticket.ticket.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise pa.ArrowInvalid("Invalid ticket") from exc
            try:
                result = self._reads.fetch(token, auth_request)
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


def _health_payload(
    health_check: Callable[[], Mapping[str, object]] | None,
    logger: logging.Logger,
    *,
    metrics: ServiceMetrics | None = None,
) -> dict[str, object]:
    """Builds the Flight health response and fails closed on readiness errors."""

    payload: dict[str, object] = {"status": "ok", "service": "data-plane"}
    if health_check is None:
        return payload
    try:
        observed = dict(health_check())
    except Exception as exc:
        logger.warning("flight_health_check_failed")
        raise flight.FlightUnavailableError("Data plane is not ready") from exc
    if observed.get("status") != "ready":
        logger.warning("flight_health_check_not_ready", extra={"health": observed})
        raise flight.FlightUnavailableError("Data plane is not ready")
    payload["checks"] = observed.get("checks", {})
    if metrics is not None:
        snapshot = metrics.snapshot()
        if snapshot:
            payload["metrics"] = snapshot
    return payload
