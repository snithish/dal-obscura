"""A scan has one projected Arrow schema and one bounded work budget."""

from dataclasses import dataclass

import pyarrow as pa


@dataclass(frozen=True, slots=True)
class ScanRequest:
    """Exact top-level fields in output order; nested fields remain complete.

    row_filter is an optional DuckDB SQL optimization hint. Core owns full
    enforcement and only supplies hints to formats declaring filter_pushdown.
    """

    schema: pa.Schema
    max_tasks: int
    row_filter: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.schema, pa.Schema):
            raise ValueError("Scan request requires an Arrow schema")
        if len(self.schema.names) != len(set(self.schema.names)):
            raise ValueError("Scan request fields must have unique names")
        if type(self.max_tasks) is not int or self.max_tasks <= 0:
            raise ValueError("max_tasks must be a positive integer")
        if self.row_filter is not None and (
            not isinstance(self.row_filter, str)
            or not self.row_filter.strip()
            or len(self.row_filter.encode("utf-8")) > 65_536
        ):
            raise ValueError("Scan row filter must be bounded non-empty SQL text")

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(self.schema.names)
