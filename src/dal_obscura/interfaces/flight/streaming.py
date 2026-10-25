from __future__ import annotations

from collections.abc import Iterable, Iterator
from uuid import uuid4

import pyarrow as pa
import pyarrow.flight as flight
from dal_obscura_plugin_api.execution import close_preserving_error

from dal_obscura.application.reads.execution import (
    SourceInitializationValueError,
    StreamCancelledError,
    StreamResourceError,
)
from dal_obscura.interfaces.flight.errors import internal_error


def make_stream(
    schema: pa.Schema, batches: Iterable[pa.RecordBatch], *, correlation_id: str | None = None
) -> flight.GeneratorStream:
    """Builds a Flight stream from the exact schema produced by the use case."""
    return flight.GeneratorStream(schema, _resource_guard(batches, correlation_id or uuid4().hex))


def _resource_guard(
    batches: Iterable[pa.RecordBatch], correlation_id: str
) -> Iterator[pa.RecordBatch]:
    try:
        try:
            yield from batches
        finally:
            close_preserving_error(batches)
    except StreamCancelledError, InterruptedError:
        raise flight.FlightCancelledError("Stream cancelled") from None
    except TimeoutError:
        raise flight.FlightTimedOutError("Read deadline exceeded") from None
    except PermissionError:
        raise flight.FlightUnauthorizedError("Unauthorized") from None
    except SourceInitializationValueError:
        raise flight.FlightInternalError("Ticket payload mismatch") from None
    except StreamResourceError as exc:
        raise flight.FlightUnavailableError(str(exc)) from exc
    except Exception as exc:
        raise internal_error(exc, phase="stream", correlation_id=correlation_id) from None
