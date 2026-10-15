from __future__ import annotations

from typing import Any, cast

from sqlalchemy.orm import Session

from dal_obscura.adapters.providers.published import PublishedPolicy
from dal_obscura.adapters.sql.database.db import (
    session_factory,
)
from dal_obscura.adapters.sql.database.orm import (
    AssetRecord,
    CatalogRecord,
    PolicyRuleRecord,
)
from dal_obscura.adapters.sql.snapshots import LiveConfigStore
from dal_obscura.kernel.identity.principal import Principal
from tests.support.sources.live_config import seed_live_config

ICEBERG_CATALOG_ID = "iceberg.catalog"


def test_published_options_detach_nested_caller_containers():
    from dal_obscura.kernel.plans.snapshot import PublishedCatalog, PublishedSource

    options = {"nested": {"values": [1, True, None, "original"]}}
    catalog = PublishedCatalog("analytics", "iceberg.catalog", 3, options)
    source = PublishedSource("iceberg", "default.users", options)
    options["nested"]["values"].append("later")

    expected = {"nested": {"values": (1, True, None, "original")}}
    assert catalog.options == expected
    assert source.options == expected
    import pytest

    with pytest.raises(TypeError):
        cast(dict[str, Any], catalog.options["nested"])["values"] = ()
    with pytest.raises(ValueError, match="JSON"):
        PublishedCatalog("analytics", "iceberg.catalog", 3, {"unsupported": {1, 2}})


def test_published_serialization_returns_detached_json_containers():
    from dal_obscura.kernel.plans.snapshot import PublishedCatalog, PublishedSchema

    catalog = PublishedCatalog("analytics", "iceberg.catalog", 3, {"nested": [1, {"flag": True}]})
    schema = PublishedSchema(1, ({"path": ["id"], "field_id": "iceberg:1"},), True, "digest")
    encoded = catalog.to_json()
    encoded["options"]["nested"][1]["flag"] = False
    fields = schema.to_json()
    fields["fields"][0]["path"].append("changed")

    assert catalog.to_json()["options"] == {"nested": [1, {"flag": True}]}
    assert schema.to_json()["fields"] == [{"path": ["id"], "field_id": "iceberg:1"}]


def test_live_store_reads_current_policy_from_canonical_records(db_session: Session):
    (asset_id,) = seed_live_config(db_session)
    store = LiveConfigStore(session_factory(db_session.get_bind().engine))
    authorizer = PublishedPolicy(
        store.get_asset_and_catalog(catalog="analytics", target="default.users")
    )
    principal = Principal(id="user1", groups=[], attributes={})

    first = authorizer.authorize(principal, "default.users", "analytics", ["id"])
    assert first.allowed_columns == ("id",)
    assert first.asset_id == str(asset_id)

    rule = db_session.query(PolicyRuleRecord).filter_by(asset_id=asset_id).one()
    rule.columns_json = ["email"]
    asset = db_session.get(AssetRecord, asset_id)
    assert asset is not None
    asset.policy_revision += 1
    db_session.commit()

    authorizer = PublishedPolicy(
        store.get_asset_and_catalog(catalog="analytics", target="default.users")
    )
    second = authorizer.authorize(principal, "default.users", "analytics", ["email"])
    assert second.allowed_columns == ("email",)
    assert second.policy_version != first.policy_version


def test_live_runtime_settings_and_catalogs_need_no_snapshot_table(db_session: Session):
    (_,) = seed_live_config(db_session)
    store = LiveConfigStore(session_factory(db_session.get_bind().engine))

    runtime = store.get_runtime()
    _asset = store.get_asset_and_catalog(catalog="analytics", target="default.users")

    assert runtime.ticket == {"ttl_seconds": 600, "max_tickets": 12, "max_exchanges": 2}
    assert runtime.auth_chain["providers"] == [
        {
            "ordinal": 1,
            "module": "example.IdentityProvider",
            "args": {"issuer": "https://issuer.example"},
            "enabled": True,
        }
    ]
    assert _asset.catalog.plugin_id == "iceberg.catalog"
    assert _asset.catalog.revision == 3


def test_live_store_reads_catalog_edit_without_global_invalidation(db_session: Session):
    (_,) = seed_live_config(db_session)
    store = LiveConfigStore(session_factory(db_session.get_bind().engine))
    store.get_asset_and_catalog(catalog="analytics", target="default.users")

    catalog = db_session.query(CatalogRecord).filter_by(name="analytics").one()
    catalog.options_json = {"type": "sql", "uri": "sqlite:///changed.db"}
    catalog.revision += 1
    db_session.commit()

    second = store.get_asset_and_catalog(catalog="analytics", target="default.users")
    assert second.catalog.options["uri"] == "sqlite:///changed.db"
