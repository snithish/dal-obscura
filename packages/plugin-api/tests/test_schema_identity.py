"""Provider identity normalization through public SDK contracts."""

import pyarrow as pa
import pytest
from dal_obscura_plugin_api.schema.identity import schema_field_id, schema_scope_digest


@pytest.mark.parametrize(
    "metadata",
    [
        {b"PARQUET:field_id": b"bad\x00id"},
        {b"iceberg.field.id": b"x" * 129},
        {b"PARQUET:field_id": b"\xff"},
        {b"PARQUET:field_id": b"synthetic:forged"},
    ],
)
def test_schema_identity_rejects_unbounded_or_malformed_provider_ids(metadata: dict[bytes, bytes]):
    field_id, stable = schema_field_id(pa.field("id", pa.int64(), metadata=metadata), ("id",))
    assert field_id.startswith("synthetic:")
    assert stable is False


def test_runtime_schema_identities_reject_oversized_canonical_metadata():
    schema = pa.schema([pa.field("email", pa.string())], metadata={b"key": b"x" * 1_600_000})
    with pytest.raises(ValueError, match="Schema encoding exceeds"):
        schema_scope_digest(schema)
