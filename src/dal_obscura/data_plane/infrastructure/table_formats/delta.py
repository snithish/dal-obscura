from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from dal_obscura.common.catalog.ports import TableFormat
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.table_format.ports import InputPartition, Plan, ScanTask
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer


@dataclass(frozen=True, kw_only=True)
class DeltaInputPartition(InputPartition):
    columns: list[str]
    version: int | None = None


@dataclass(frozen=True, kw_only=True)
class DeltaTableFormat(TableFormat):
    format: str = "delta"
    table_uri: str
    storage_options: dict[str, str] = field(default_factory=dict)
    path_enforcer: PathRuleEnforcer | None = None

    def get_schema(self) -> pa.Schema:
        table = self._load_table()
        schema = table.schema().to_arrow()
        if isinstance(schema, pa.Schema):
            return schema
        return pa.schema(schema)

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        del max_tickets
        table = self._load_table()
        version = table.version()
        schema = self.get_schema()
        task = ScanTask(
            table_format=self,
            schema=schema,
            partition=DeltaInputPartition(columns=list(request.columns), version=version),
        )
        return Plan(
            schema=schema,
            tasks=[task],
            full_row_filter=request.row_filter,
            backend_pushdown_row_filter=None,
            residual_row_filter=request.row_filter,
        )

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        if not isinstance(partition, DeltaInputPartition):
            raise TypeError("DeltaTableFormat requires a DeltaInputPartition")
        table = self._load_table(version=partition.version)
        arrow_table = table.to_pyarrow_table(columns=partition.columns or None)
        return arrow_table.schema, arrow_table.to_batches()

    def _load_table(self, *, version: int | None = None) -> Any:
        _check_path(self.table_uri, self.path_enforcer)
        from deltalake import DeltaTable

        return DeltaTable(
            self.table_uri,
            version=version,
            storage_options=dict(self.storage_options),
        )


def _check_path(path: str, enforcer: PathRuleEnforcer | None) -> None:
    if enforcer is not None:
        enforcer.check(path)
