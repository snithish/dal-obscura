from __future__ import annotations

from typing import Iterable, Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight as flight


def make_stream(schema: pa.Schema, batches: Iterable[pa.RecordBatch]) -> flight.RecordBatchStream:
    coerced_batches = coerce_batches_to_schema(schema, batches)
    if hasattr(flight, "GeneratorStream"):
        return flight.GeneratorStream(schema, coerced_batches)
    batch_list = list(coerced_batches)
    reader = pa.RecordBatchReader.from_batches(schema, batch_list)
    return flight.RecordBatchStream(reader)


def coerce_batches_to_schema(
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
