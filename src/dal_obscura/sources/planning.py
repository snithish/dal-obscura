"""Execution-planning value objects shared by table format implementations.

Example:
    ```python
    partition = MyPartition(path="s3://warehouse/users/part-000.parquet")
    task = ScanTask(table_format=format, schema=schema, partition=partition)
    plan = Plan(schema=schema, tasks=[task])
    ```
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from dal_obscura.policy.filters import RowFilter
from dal_obscura.sources.contracts import Source


@dataclass(frozen=True)
class ScanTask:
    """A planned unit of work that carries executable table format context.

    Example:
        ```python
        task = ScanTask(table_format=format, schema=schema, partition=partition)
        ```
    """

    table_format: Source
    schema: pa.Schema
    partition: object


@dataclass(frozen=True)
class Plan:
    """The complete execution plan covering all split scan tasks.

    Example:
        ```python
        plan = Plan(schema=schema, tasks=[task], residual_row_filter=row_filter)
        ```
    """

    schema: pa.Schema
    tasks: list[ScanTask]
    full_row_filter: RowFilter | None = None
    backend_pushdown_row_filter: RowFilter | None = None
    residual_row_filter: RowFilter | None = None
