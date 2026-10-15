from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from dal_obscura.kernel.plans.projection import CompiledProjection, ExecutionProjection
from dal_obscura.kernel.policy.filters import RowFilter
from dal_obscura.kernel.policy.models import AccessDecision


@dataclass(frozen=True)
class GovernedReadPlan:
    schema: pa.Schema
    decision: AccessDecision
    projection: ExecutionProjection
    authorization_columns: tuple[str, ...]
    requested_filter: RowFilter | None
    effective_filter: RowFilter | None
    compiled_projection: CompiledProjection

    @property
    def output_schema(self) -> pa.Schema:
        return self.compiled_projection.output_schema
