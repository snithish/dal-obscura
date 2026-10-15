from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import pyarrow as pa

from dal_obscura.kernel.policy.filters import RowFilter
from dal_obscura.kernel.policy.models import MaskRule


@dataclass(frozen=True)
class PlanAccessResult:
    """Material returned to Flight `get_flight_info` after authorization succeeds."""

    output_schema: pa.Schema
    ticket_tokens: list[str]
    target: str
    columns: list[str]
    principal_id: str
    policy_version: int
    catalog: str | None = None
    requested_row_filter_present: bool = False
    requested_row_filter_dependency_count: int = 0
    full_row_filter_present: bool = False
    backend_pushdown_row_filter_present: bool = False
    residual_row_filter_present: bool = False
    visible_column_count: int = 0
    execution_column_count: int = 0


@dataclass(frozen=True)
class FetchStreamResult:
    """Information needed to construct the Flight `do_get` response stream."""

    output_schema: pa.Schema
    result_batches: Iterable[pa.RecordBatch]
    target: str
    principal_id: str
    columns: list[str]
    catalog: str | None = None


@dataclass(frozen=True)
class DecodedScan:
    """Scan instructions restored from a ticket's serialized payload."""

    read_payload: str
    full_row_filter: RowFilter | None
    masks: dict[str, MaskRule]
    authorization_columns: list[str]


@dataclass(frozen=True)
class GetSchemaResult:
    """Authorized schema returned by ReadService.schema."""

    output_schema: pa.Schema
    target: str
    columns: list[str]
    principal_id: str
    policy_version: int
    catalog: str | None = None
