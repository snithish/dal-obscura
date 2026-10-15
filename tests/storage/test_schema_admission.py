from uuid import uuid4

import pytest

from dal_obscura.adapters.sql import assets
from dal_obscura.adapters.sql.database.orm import AssetRecord, CatalogRecord


@pytest.mark.parametrize("overload", ["bytes", "fields"])
def test_schema_admission_budgets_reject_before_replacing_existing_fields(db_session, overload):
    catalog_id, asset_id = uuid4(), uuid4()
    db_session.add(CatalogRecord(id=catalog_id, name="catalog", plugin_id="iceberg.catalog"))
    db_session.flush()
    db_session.add(
        AssetRecord(id=asset_id, catalog_id=catalog_id, target="table", backend="iceberg")
    )
    db_session.flush()
    original = [{"name": "id", "path": ["id"], "field_id": "1", "type": "int64", "nullable": False}]
    saved = assets.replace_asset_schema_fields(
        db_session, asset_id=asset_id, fields=original, expected_revision=0
    )
    if overload == "bytes":
        name = "界" * 350000
        fields = [{"name": name, "path": [name], "field_id": "2"}]
        expected_error = "encoded-byte budget"
    else:
        fields = original * 5001
        expected_error = "field-count budget"
    with pytest.raises(ValueError, match=expected_error):
        assets.replace_asset_schema_fields(
            db_session, asset_id=asset_id, fields=fields, expected_revision=1
        )
    assert assets.list_asset_schema_fields(db_session, asset_id) == saved
    assert db_session.get(AssetRecord, asset_id).revision == 1
