"""Bounded Flight failures with diagnostics that never include provider messages."""

import logging
from typing import Literal

import pyarrow.flight as flight

_LOGGER = logging.getLogger(__name__)


def internal_error(
    error: Exception,
    *,
    phase: Literal["schema", "plan", "fetch", "stream"],
    correlation_id: str,
) -> flight.FlightInternalError:
    """Keep arbitrary exception text and traceback chains outside RPC metadata."""
    error_type = type(error).__name__
    if len(error_type) > 96 or not error_type.isidentifier():
        error_type = "Exception"
    _LOGGER.error(
        "flight_operation_failed",
        extra={"phase": phase, "error_type": error_type, "correlation_id": correlation_id},
    )
    return flight.FlightInternalError("Read operation failed")
