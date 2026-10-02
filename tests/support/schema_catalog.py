"""Schema-only catalog double shared by HTTP evaluation scenarios."""

from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType


class EvaluationTable:
    def schema(self) -> Schema:
        return Schema(
            NestedField(field_id=1, name="id", field_type=LongType()),
            NestedField(field_id=2, name="email", field_type=StringType()),
            NestedField(field_id=3, name="region", field_type=StringType()),
        )


class EvaluationCatalog:
    def load_table(self, identifier: str) -> EvaluationTable:
        assert identifier == "prod.users"
        return EvaluationTable()
