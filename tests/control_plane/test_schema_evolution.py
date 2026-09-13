from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
import pytest

from dal_obscura.data_plane.infrastructure.adapters.published_config import (
    PublishedAsset,
    _schema_identities,
    _validate_schema_admission,
)


def _asset_for(schema: pa.Schema) -> PublishedAsset:
    identities = _schema_identities(schema)
    return PublishedAsset(
        publication_id=uuid4(),
        tenant_id=uuid4(),
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


def test_schema_evolution_accepts_the_reviewed_identity_and_rejects_a_rename() -> None:
    reviewed = pa.schema([pa.field("profile", pa.struct([pa.field("email", pa.string())]))])
    asset = _asset_for(reviewed)

    _validate_schema_admission(asset, reviewed)
    renamed = pa.schema([pa.field("profile", pa.struct([pa.field("address", pa.string())]))])
    with pytest.raises(ValueError, match="review again"):
        _validate_schema_admission(asset, renamed)


def test_schema_evolution_rejects_a_type_change_even_when_the_path_survives() -> None:
    reviewed = pa.schema([pa.field("id", pa.int64())])
    asset = _asset_for(reviewed)

    with pytest.raises(ValueError, match="review again"):
        _validate_schema_admission(asset, pa.schema([pa.field("id", pa.string())]))
