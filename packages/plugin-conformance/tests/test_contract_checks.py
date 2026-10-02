from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest
from dal_obscura_plugin_api import (
    DiscoveryPage,
    ExecutionContext,
    SchemaDescriptor,
)
from dal_obscura_plugin_conformance import (
    check_capabilities,
    check_discovery_page,
    check_record_batches,
    check_schema_descriptor,
)


def test_capability_negative_case_fails_closed():
    with pytest.raises(ValueError, match="missing required capabilities"):
        check_capabilities({"nested_schema"}, {"snapshot_reads"})


def test_record_batches_reject_schema_mutation():
    schema = pa.schema([pa.field("id", pa.int64())])
    bad = pa.RecordBatch.from_arrays([pa.array([1])], names=["unexpected"])

    with pytest.raises(ValueError, match="schema differs"):
        check_record_batches(schema, [bad])


def test_record_batch_validation_stops_unbounded_output_generators():
    schema = pa.schema([pa.field("id", pa.int64())])
    batch = pa.RecordBatch.from_pylist([{"id": 1}], schema=schema)

    def endless_batches():
        while True:
            yield batch

    with pytest.raises(ValueError, match="more than 2 output batches"):
        check_record_batches(schema, endless_batches(), max_batches=2)


def test_record_batch_validation_closes_provider_output_on_failure() -> None:
    schema = pa.schema([pa.field("id", pa.int64())])
    batch = pa.RecordBatch.from_pylist([{"id": 1}], schema=schema)

    class ClosableBatches:
        closed = False

        def __iter__(self):
            yield batch
            yield batch

        def close(self) -> None:
            self.closed = True

    batches = ClosableBatches()
    with pytest.raises(ValueError, match="more than 1 output batches"):
        check_record_batches(schema, batches, max_batches=1)

    assert batches.closed is True


def test_record_batch_validation_enforces_per_batch_byte_budget():
    schema = pa.schema([pa.field("payload", pa.binary())])
    batch = pa.RecordBatch.from_pylist([{"payload": b"secret"}], schema=schema)

    with pytest.raises(ValueError, match="batch byte budget"):
        check_record_batches(schema, [batch], max_batch_bytes=1)


def test_schema_validation_enforces_nested_depth_budget():
    nested = pa.field("root", pa.struct([pa.field("child", pa.string())]))
    schema = SchemaDescriptor(arrow_schema=pa.schema([nested]))

    with pytest.raises(ValueError, match="nesting levels"):
        check_schema_descriptor(schema, max_depth=1)


def test_record_batch_validation_rejects_expired_deadline():
    schema = pa.schema([pa.field("id", pa.int64())])
    batch = pa.RecordBatch.from_pylist([{"id": 1}], schema=schema)

    with pytest.raises(TimeoutError, match="deadline"):
        check_record_batches(
            schema,
            [batch],
            context=ExecutionContext(datetime.now(timezone.utc) - timedelta(seconds=1), "expired"),
        )


def test_schema_descriptor_rejects_oversized_field_count():
    schema = SchemaDescriptor(
        arrow_schema=pa.schema([pa.field(f"field_{index}", pa.string()) for index in range(3)])
    )

    with pytest.raises(ValueError, match="more than 2 fields"):
        check_schema_descriptor(schema, max_fields=2)


def test_check_discovery_page_rejects_invalid_continuation():
    with pytest.raises(ValueError, match="continuation"):
        check_discovery_page(DiscoveryPage((), continuation=""))
