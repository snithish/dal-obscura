from __future__ import annotations

import pyarrow as pa
import pytest

from dal_obscura.data_plane.infrastructure.adapters.live_config import (
    LiveAsset,
    _schema_identities,
    _validate_schema_admission,
)


def _asset_for(schema: pa.Schema) -> LiveAsset:
    identities = _schema_identities(schema)
    return LiveAsset(
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        compiled_config={
            "schema": {
                "fields": [
                    {"path": list(path), "field_id": field_id, "type": field_type}
                    for (path, field_id), field_type in identities.items()
                ]
            }
        },
        policy_version=1,
    )


def test_schema_evolution_accepts_admitted_identity_and_rejects_a_rename() -> None:
    admitted = pa.schema([pa.field("profile", pa.struct([pa.field("email", pa.string())]))])
    asset = _asset_for(admitted)

    _validate_schema_admission(asset, admitted)
    renamed = pa.schema([pa.field("profile", pa.struct([pa.field("address", pa.string())]))])
    with pytest.raises(ValueError, match="schema admission"):
        _validate_schema_admission(asset, renamed)


def test_schema_evolution_rejects_a_type_change_even_when_the_path_survives() -> None:
    admitted = pa.schema([pa.field("id", pa.int64())])
    asset = _asset_for(admitted)

    with pytest.raises(ValueError, match="schema admission"):
        _validate_schema_admission(asset, pa.schema([pa.field("id", pa.string())]))
