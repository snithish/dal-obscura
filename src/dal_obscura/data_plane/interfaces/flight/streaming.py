from __future__ import annotations

from collections.abc import Iterable, Iterator

import pyarrow as pa
import pyarrow.flight as flight

from dal_obscura.data_plane.application.ports.row_transform import StreamResourceError


def make_stream(schema: pa.Schema, batches: Iterable[pa.RecordBatch]) -> flight.GeneratorStream:
    """Builds a Flight stream from the exact schema produced by the use case."""
    return flight.GeneratorStream(schema, _resource_guard(batches))


def _resource_guard(batches: Iterable[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    try:
        yield from batches
    except StreamResourceError as exc:
        raise flight.FlightUnavailableError(str(exc)) from exc
    finally:
        close = getattr(batches, "close", None)
        if callable(close):
            close()
