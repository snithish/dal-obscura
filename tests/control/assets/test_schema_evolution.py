from __future__ import annotations

import pyarrow as pa
import pytest

from dal_obscura.adapters.providers.published import _schema_identities, _validate_schema_admission
from dal_obscura.kernel.plans.snapshot import (
    PublishedAssetSnapshot,
    PublishedCatalog,
    PublishedSchema,
    PublishedSource,
)
from dal_obscura.kernel.policy.models import AssetPolicy


def _asset_for(schema: pa.Schema) -> PublishedAssetSnapshot:
    identities = _schema_identities(schema)
    return PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", "", 0, {}),
        target="default.users",
        source=PublishedSource("iceberg", None, {}),
        policy=AssetPolicy(1, "analytics", "default.users", []),
        admitted_schema=PublishedSchema(
            1,
            tuple(
                [
                    {"path": list(path), "field_id": field_id, "type": field_type}
                    for (path, field_id), field_type in identities.items()
                ]
            ),
            False,
            None,
        ),
        asset_id=None,
    )


def test_schema_evolution_rejects_a_type_change_even_when_the_path_survives() -> None:
    admitted = pa.schema([pa.field("id", pa.int64())])
    asset = _asset_for(admitted)

    with pytest.raises(ValueError, match="schema admission"):
        _validate_schema_admission(asset, pa.schema([pa.field("id", pa.string())]))
