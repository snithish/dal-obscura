from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa


@dataclass(frozen=True, kw_only=True)
class _FakeProjectedSchema:
    schema: pa.Schema

    def as_arrow(self) -> pa.Schema:
        return self.schema

    def select(self, *columns: str) -> _FakeProjectedSchema:
        return _FakeProjectedSchema(
            schema=pa.schema([self.schema.field(column) for column in columns])
        )
