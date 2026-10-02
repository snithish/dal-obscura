from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from contextlib import ExitStack
from itertools import chain
from threading import BoundedSemaphore

import duckdb
import pyarrow as pa

from dal_obscura.policy.filters import RowFilter, row_filter_to_sql
from dal_obscura.policy.models import MaskRule
from dal_obscura.policy.projection import compile_projection
from dal_obscura.read.duckdb_connection import TRANSFORM_CONFIG, connect
from dal_obscura.read.memory_limits import validate_memory_limit
from dal_obscura.read.transform_contracts import StreamResourceError

_DUCKDB_ARROW_OUTPUT_BATCH_SIZE = 8_192
_DEFAULT_DUCKDB_MEMORY_LIMIT = "512MB"
_DEFAULT_MAX_ACTIVE_STREAMS = 16
_DEFAULT_MAX_INPUT_BATCH_BYTES = 64 * 1024 * 1024
_DEFAULT_MAX_OUTPUT_BATCH_BYTES = 64 * 1024 * 1024


class StreamAdmissionError(StreamResourceError):
    """Raised when the configured governed-stream capacity is exhausted."""


class InputBatchLimitError(ValueError, StreamResourceError):
    """Raised when an Arrow input batch exceeds the configured byte budget."""


class OutputBatchLimitError(ValueError, StreamResourceError):
    """Raised when an Arrow output batch exceeds the configured byte budget."""


class StreamMemoryLimitError(StreamResourceError):
    """DuckDB or Arrow could not allocate memory for the governed stream."""


class DuckDBRowTransformAdapter:
    """Applies row filters and masks to streamed Arrow batches via DuckDB SQL."""

    def __init__(
        self,
        *,
        max_active_streams: int = _DEFAULT_MAX_ACTIVE_STREAMS,
        duckdb_memory_limit: str = _DEFAULT_DUCKDB_MEMORY_LIMIT,
        max_input_batch_bytes: int = _DEFAULT_MAX_INPUT_BATCH_BYTES,
        max_output_batch_bytes: int = _DEFAULT_MAX_OUTPUT_BATCH_BYTES,
    ) -> None:
        if max_active_streams < 1:
            raise ValueError("max_active_streams must be positive")
        duckdb_memory_limit = validate_memory_limit(duckdb_memory_limit)
        if max_input_batch_bytes < 1:
            raise ValueError("max_input_batch_bytes must be positive")
        if max_output_batch_bytes < 1:
            raise ValueError("max_output_batch_bytes must be positive")
        self._stream_slots = BoundedSemaphore(max_active_streams)
        self._duckdb_config = {
            **TRANSFORM_CONFIG,
            "memory_limit": duckdb_memory_limit,
        }
        self._max_input_batch_bytes = max_input_batch_bytes
        self._max_output_batch_bytes = max_output_batch_bytes

    def apply_filters_and_masks_stream(
        self,
        batches: Iterable[pa.RecordBatch],
        columns: Iterable[str],
        row_filter: RowFilter | None,
        masks: Mapping[str, MaskRule],
    ) -> Iterable[pa.RecordBatch]:
        """Builds a transient DuckDB query and streams transformed record batches."""
        if not self._stream_slots.acquire(blocking=False):
            _close_if_possible(batches)
            raise StreamAdmissionError("Governed stream capacity is exhausted; retry later")
        with ExitStack() as resources:
            resources.callback(self._stream_slots.release)
            resources.callback(_close_if_possible, batches)
            batch_iter = iter(batches)
            if batch_iter is not batches:
                resources.callback(_close_if_possible, batch_iter)
            try:
                first_batch = next(batch_iter)
            except StopIteration:
                return
            except MemoryError as exc:
                raise StreamMemoryLimitError(
                    "Arrow input allocation exhausted the stream memory budget"
                ) from exc
            try:
                _require_batch_size(first_batch, self._max_input_batch_bytes)
                schema = first_batch.schema
                query = _build_query(schema, columns, row_filter, masks)
                con = connect(self._duckdb_config)
                resources.callback(con.close)
                # Policy expressions are row-local. Bound each query to one input
                # batch: DuckDB may eagerly consume an Arrow reader before yielding.
                input_batches = chain((first_batch,), batch_iter)
                del first_batch
                for batch in input_batches:
                    if not batch.schema.equals(schema):
                        raise ValueError("Arrow input schema changed during the governed stream")
                    _require_batch_size(batch, self._max_input_batch_bytes)
                    result_reader = (
                        con.from_arrow(batch)
                        .query("input", query)
                        .to_arrow_reader(batch_size=_DUCKDB_ARROW_OUTPUT_BATCH_SIZE)
                    )
                    try:
                        yield from _bounded_output_batches(
                            result_reader,
                            max_output_batch_bytes=self._max_output_batch_bytes,
                        )
                    finally:
                        _close_if_possible(result_reader)
                    del batch
            except (duckdb.OutOfMemoryException, MemoryError) as exc:
                raise StreamMemoryLimitError(
                    "Governed stream memory budget exhausted; reduce batch size or concurrent reads"
                ) from exc


def _close_if_possible(value: object) -> None:
    close = getattr(value, "close", None)
    if callable(close):
        close()


def _require_batch_size(batch: pa.RecordBatch, maximum: int) -> None:
    retained = batch.get_total_buffer_size()
    if max(batch.nbytes, retained) > maximum:
        raise InputBatchLimitError(
            f"Arrow input batch uses {batch.nbytes} logical / {retained} retained bytes; "
            f"limit is {maximum} bytes"
        )


def _bounded_output_batches(
    batches: Iterable[pa.RecordBatch], *, max_output_batch_bytes: int
) -> Iterator[pa.RecordBatch]:
    for batch in batches:
        retained = batch.get_total_buffer_size()
        if max(batch.nbytes, retained) > max_output_batch_bytes:
            raise OutputBatchLimitError(
                f"Arrow output batch uses {batch.nbytes} logical / {retained} retained bytes; "
                f"limit is {max_output_batch_bytes} bytes"
            )
        yield batch


def _build_query(
    base_schema: pa.Schema,
    columns: Iterable[str],
    row_filter: RowFilter | None,
    masks: Mapping[str, MaskRule],
) -> str:
    """Builds the SQL statement used to apply projection, masks, and filters."""
    selection = compile_projection(base_schema, columns, masks)
    query = f"SELECT {', '.join(selection.select_list)} FROM input"
    if row_filter:
        query += f" WHERE {row_filter_to_sql(row_filter)}"
    return query
