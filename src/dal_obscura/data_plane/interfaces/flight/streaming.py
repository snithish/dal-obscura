from __future__ import annotations

from collections.abc import Iterable

import pyarrow as pa
import pyarrow.flight as flight


def make_stream(schema: pa.Schema, batches: Iterable[pa.RecordBatch]) -> flight.RecordBatchStream:
    """Builds a Flight stream from the exact schema produced by the use case."""
    return flight.GeneratorStream(schema, batches)
