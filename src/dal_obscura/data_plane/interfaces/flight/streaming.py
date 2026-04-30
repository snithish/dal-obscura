from __future__ import annotations

from collections.abc import Iterable, Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight as flight


def make_stream(schema: pa.Schema, batches: Iterable[pa.RecordBatch]) -> flight.RecordBatchStream:
    """Builds a Flight stream while keeping batch field types aligned with the schema."""
    normalized_schema = normalize_schema_for_flight(schema)
    coerced_batches = coerce_batches_to_schema(normalized_schema, batches)
    if hasattr(flight, "GeneratorStream"):
        return flight.GeneratorStream(normalized_schema, coerced_batches)
    batch_list = list(coerced_batches)
    reader = pa.RecordBatchReader.from_batches(normalized_schema, batch_list)
    return flight.RecordBatchStream(reader)


def coerce_batches_to_schema(
    schema: pa.Schema, batches: Iterable[pa.RecordBatch]
) -> Iterator[pa.RecordBatch]:
    """Casts each batch to the advertised schema before it reaches Flight clients."""
    for batch in batches:
        arrays = []
        for field in schema:
            array = batch.column(batch.schema.get_field_index(field.name))
            if not array.type.equals(field.type):
                array = pc.cast(array, target_type=field.type, safe=False)
            arrays.append(array)
        yield pa.RecordBatch.from_arrays(arrays, schema=schema)


def normalize_schema_for_flight(schema: pa.Schema) -> pa.Schema:
    """Rewrites Arrow list widths to the Flight compatibility shape exposed to clients."""
    return pa.schema([_normalize_field_for_flight(field) for field in schema])


def _normalize_field_for_flight(field: pa.Field) -> pa.Field:
    field_type = field.type

    if pa.types.is_struct(field_type):
        return pa.field(
            field.name,
            pa.struct([_normalize_field_for_flight(child) for child in field_type]),
            nullable=field.nullable,
            metadata=field.metadata,
        )

    if pa.types.is_list(field_type) or pa.types.is_large_list(field_type):
        normalized_value = _normalize_field_for_flight(field_type.value_field)
        return pa.field(
            field.name,
            pa.list_(normalized_value),
            nullable=field.nullable,
            metadata=field.metadata,
        )

    if pa.types.is_map(field_type):
        key_field = _normalize_field_for_flight(field_type.key_field)
        item_field = _normalize_field_for_flight(field_type.item_field)
        return pa.field(
            field.name,
            pa.map_(
                key_field.type,
                item_field,
                keys_sorted=field_type.keys_sorted,
            ),
            nullable=field.nullable,
            metadata=field.metadata,
        )

    return field
