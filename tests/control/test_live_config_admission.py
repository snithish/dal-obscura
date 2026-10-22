from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import pyarrow as pa
import pytest
from dal_obscura_plugin_api.storage import PathRuleEnforcer
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from dal_obscura.adapters.providers.published import (
    PublishedPolicy,
)
from dal_obscura.adapters.sql.database.db import (
    session_factory,
)
from dal_obscura.adapters.sql.database.orm import (
    AssetRecord,
    CatalogRecord,
    PolicyRuleRecord,
    RuntimeSettingsRecord,
    WorkspaceRecord,
)
from dal_obscura.adapters.sql.snapshots import LiveConfigStore
from dal_obscura.kernel.identity.principal import Principal
from dal_obscura.kernel.plans.snapshot import (
    PublishedAssetSnapshot,
    PublishedCatalog,
    PublishedSchema,
    PublishedSource,
)
from dal_obscura.kernel.policy.models import AccessRule, AssetPolicy
from tests.control.live_admission_cases import (
    AdmissionPlugins,
    admission_identities,
    admit_snapshot,
    capture_catalog,
)

ICEBERG_CATALOG_ID = "iceberg.catalog"


def test_live_authorizer_resolves_policy_from_active_asset(db_session: Session):
    _seed_live_asset(db_session, policy_version=123)
    authorizer = PublishedPolicy(
        LiveConfigStore(session_factory(db_session.get_bind().engine)).get_asset_and_catalog(
            catalog="analytics", target="default.users"
        )
    )

    decision = authorizer.authorize(
        principal=Principal(id="user1", groups=[], attributes={}),
        target="default.users",
        catalog="analytics",
        requested_columns=["id", "email"],
    )

    assert decision.allowed_columns == ("id", "email")
    assert decision.masks["email"].type == "email"
    assert decision.row_filter == "(region = 'us')"
    assert decision.policy_version == 123


@pytest.mark.parametrize("plugin_id", ["untrusted.catalog", "iceberg.catalog"])
def test_live_config_rejects_tampered_plugin_binding(plugin_id):
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", plugin_id, 0, {}),
        target="default.users",
        source=PublishedSource("iceberg", "default.users", {}),
        policy=AssetPolicy(1, "analytics", "default.users", []),
        admitted_schema=None,
        asset_id=None,
    )
    catalog = PublishedCatalog("analytics", "", 0, {})

    with pytest.raises(ValueError, match="unsupported without an admitted registry"):
        capture_catalog(replace(asset, catalog=replace(catalog, plugin_id=asset.catalog.plugin_id)))


def test_live_config_passes_runtime_path_enforcer_to_catalog():
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", "iceberg.catalog", 0, {}),
        target="default.users",
        source=PublishedSource("iceberg", "default.users", {}),
        policy=AssetPolicy(1, "analytics", "default.users", []),
        admitted_schema=None,
        asset_id=None,
    )
    catalog = PublishedCatalog("analytics", "", 0, {})
    enforcer = PathRuleEnforcer([{"root": "s3://warehouse"}])

    resolved = capture_catalog(
        replace(asset, catalog=replace(catalog, plugin_id=asset.catalog.plugin_id)),
        path_enforcer=enforcer,
        plugin_registry=AdmissionPlugins(
            ("catalog", "iceberg.catalog"), ("table_format", "iceberg")
        ),
    )

    assert resolved.plugin_id == "iceberg.catalog"
    with pytest.raises(PermissionError):
        capture_catalog(
            asset,
            path_enforcer=enforcer,
            location="s3://outside/table",
            plugin_registry=AdmissionPlugins(
                ("catalog", "iceberg.catalog"), ("table_format", "iceberg")
            ),
        )


def test_live_config_requires_both_plugin_identities_in_admitted_snapshot():
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", "iceberg.catalog", 0, {}),
        target="default.users",
        source=PublishedSource("iceberg", "default.users", {}),
        policy=AssetPolicy(1, "analytics", "default.users", []),
        admitted_schema=None,
        asset_id=None,
    )
    catalog = PublishedCatalog("analytics", "", 0, {})

    with pytest.raises(ValueError, match="plugin binding is not admitted"):
        capture_catalog(
            replace(asset, catalog=replace(catalog, plugin_id=asset.catalog.plugin_id)),
            plugin_registry=AdmissionPlugins(("catalog", "iceberg.catalog")),
        )

    resolved = capture_catalog(
        replace(asset, catalog=replace(catalog, plugin_id=asset.catalog.plugin_id)),
        plugin_registry=AdmissionPlugins(
            ("catalog", "iceberg.catalog"), ("table_format", "iceberg")
        ),
    )
    assert resolved.plugin_id == "iceberg.catalog"

    retired = replace(
        asset,
        catalog=replace(asset.catalog, plugin_id="dal_obscura.sources.catalogs.IcebergCatalog"),
    )
    with pytest.raises(ValueError, match="plugin binding is not admitted"):
        capture_catalog(
            replace(retired, catalog=replace(catalog, plugin_id=retired.catalog.plugin_id)),
            plugin_registry=AdmissionPlugins(
                ("catalog", "iceberg.catalog"), ("table_format", "iceberg")
            ),
        )


def test_live_config_preserves_external_plugin_identity():
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", "manifest", 0, {}),
        target="default.users",
        source=PublishedSource("parquet.dataset", "default.users", {}),
        policy=AssetPolicy(1, "analytics", "default.users", []),
        admitted_schema=None,
        asset_id=None,
    )
    catalog = PublishedCatalog("analytics", "manifest", 17, {"root": "/srv/data"})
    resolved = capture_catalog(
        replace(asset, catalog=replace(catalog, plugin_id=asset.catalog.plugin_id)),
        plugin_registry=AdmissionPlugins(
            ("catalog", "manifest"), ("table_format", "parquet.dataset")
        ),
    )
    assert resolved.plugin_id == "manifest"
    assert resolved.options == {"root": "/srv/data"}
    assert resolved.revision == 17


def test_schema_without_provider_ids_uses_schema_scoped_nested_synthetic_ids():
    schema = pa.schema(
        [
            pa.field("email", pa.string()),
            pa.field("profile", pa.struct([pa.field("phone", pa.string())])),
            pa.field("tags", pa.list_(pa.field("item", pa.string()))),
            pa.field("labels", pa.map_(pa.string(), pa.string())),
        ]
    )
    identities = admission_identities(schema)

    admission = {
        "fields": [
            {"path": list(path), "field_id": field_id, "type": field_type}
            for (path, field_id), field_type in identities.items()
        ]
    }
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", "", 0, {}),
        target="default.users",
        source=PublishedSource("iceberg", None, {}),
        policy=AssetPolicy(1, "analytics", "default.users", []),
        admitted_schema=PublishedSchema(1, tuple(admission["fields"]), False, None),
        asset_id=None,
    )

    admit_snapshot(asset, schema)
    changed = pa.schema(
        [
            pa.field("email", pa.string()),
            pa.field("profile", pa.struct([pa.field("mobile", pa.string())])),
            pa.field("tags", pa.list_(pa.field("item", pa.string()))),
            pa.field("labels", pa.map_(pa.string(), pa.string())),
        ]
    )
    with pytest.raises(ValueError, match="schema admission"):
        admit_snapshot(asset, changed)


def test_schema_admission_rejects_unstable_live_schema_for_stable_ids():
    schema = pa.schema([pa.field("email", pa.string())])
    field_id = "iceberg:1"
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", "", 0, {}),
        target="default.users",
        source=PublishedSource("iceberg", None, {}),
        policy=AssetPolicy(1, "analytics", "default.users", []),
        admitted_schema=PublishedSchema(
            1, ({"path": ["email"], "field_id": field_id, "type": "string"},), True, None
        ),
        asset_id=None,
    )

    with pytest.raises(ValueError, match="stable provider field IDs"):
        admit_snapshot(asset, schema)


def test_schema_admission_rejects_binary_metadata_replaced_with_escape_text():
    original = pa.schema([pa.field("email", pa.string(), metadata={b"key": b"\xff"})])
    identities = admission_identities(original)
    asset = PublishedAssetSnapshot(
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
    changed = pa.schema([pa.field("email", pa.string(), metadata={b"key": b"\\xff"})])

    admit_snapshot(asset, original)
    with pytest.raises(ValueError, match="schema admission"):
        admit_snapshot(asset, changed)


def test_legacy_wildcard_policy_requires_schema_admission():
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", "", 0, {}),
        target="default.users",
        source=PublishedSource("iceberg", None, {}),
        policy=AssetPolicy(
            1,
            "analytics",
            "default.users",
            [AccessRule(principals=["*"], columns=["*"], masks={}, effect="allow")],
        ),
        admitted_schema=None,
        asset_id=None,
    )

    with pytest.raises(ValueError, match="requires schema admission"):
        admit_snapshot(asset, pa.schema([pa.field("id", pa.int64())]))


def test_legacy_parent_policy_requires_schema_admission():
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", "", 0, {}),
        target="default.users",
        source=PublishedSource("iceberg", None, {}),
        policy=AssetPolicy(
            1,
            "analytics",
            "default.users",
            [AccessRule(principals=["*"], columns=["profile"], masks={}, effect="allow")],
        ),
        admitted_schema=None,
        asset_id=None,
    )
    schema = pa.schema([pa.field("profile", pa.struct([pa.field("email", pa.string())]))])

    with pytest.raises(ValueError, match="requires schema admission"):
        admit_snapshot(asset, schema)


def test_live_store_fails_closed_by_default_after_transient_failure(
    db_session: Session,
):
    _seed_live_asset(db_session, policy_version=123)
    config_store = LiveConfigStore(session_factory(db_session.get_bind().engine))

    config_store.get_asset_and_catalog(catalog="analytics", target="default.users")

    from sqlalchemy import event

    engine = db_session.get_bind().engine

    def unavailable(*args, **kwargs):
        raise RuntimeError("database unavailable")

    event.listen(engine, "before_cursor_execute", unavailable)
    try:
        with pytest.raises(RuntimeError, match="database unavailable"):
            config_store.get_asset_and_catalog(catalog="analytics", target="default.users")
    finally:
        event.remove(engine, "before_cursor_execute", unavailable)


def test_live_store_rejects_directly_removed_assets(db_session: Session):
    _seed_live_asset(db_session, policy_version=123)
    config_store = LiveConfigStore(session_factory(db_session.get_bind().engine))
    config_store.get_asset_and_catalog(catalog="analytics", target="default.users")

    asset = db_session.scalar(select(AssetRecord).where(AssetRecord.target == "default.users"))
    assert asset is not None
    db_session.delete(asset)
    db_session.commit()

    with pytest.raises(LookupError, match="No live asset"):
        config_store.get_asset_and_catalog(catalog="analytics", target="default.users")


def test_live_authorizer_rejects_corrupt_mask_instead_of_dropping_it(
    db_session: Session,
):
    _seed_live_asset(db_session, policy_version=123)
    record = db_session.scalar(select(PolicyRuleRecord))
    assert record is not None
    record.masks_json["email"] = {}
    flag_modified(record, "masks_json")
    db_session.commit()
    with pytest.raises(ValueError, match=r"mask\.type"):
        authorizer = PublishedPolicy(
            LiveConfigStore(session_factory(db_session.get_bind().engine)).get_asset_and_catalog(
                catalog="analytics", target="default.users"
            )
        )

        authorizer.authorize(
            principal=Principal(id="user1", groups=[], attributes={}),
            target="default.users",
            catalog="analytics",
            requested_columns=["email"],
        )


def test_live_schema_admission_rejects_rebound_or_added_field():
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", "", 0, {}),
        target="default.users",
        source=PublishedSource("iceberg", None, {}),
        policy=AssetPolicy(1, "analytics", "default.users", []),
        admitted_schema=PublishedSchema(
            1,
            (
                {
                    "name": "profile.email",
                    "field_id": "iceberg:3",
                    "path": ["profile", "email"],
                    "type": "string",
                    "nullable": True,
                },
            ),
            False,
            None,
        ),
        asset_id=None,
    )
    schema = pa.schema(
        [
            pa.field(
                "profile",
                pa.struct([pa.field("email", pa.string(), metadata={b"PARQUET:field_id": b"99"})]),
                metadata={b"PARQUET:field_id": b"iceberg:2"},
            )
        ]
    )

    with pytest.raises(ValueError, match="no longer matches"):
        admit_snapshot(asset, schema)


def test_live_schema_admission_requires_refresh_after_renamed_field():
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", "", 0, {}),
        target="default.users",
        source=PublishedSource("iceberg", None, {}),
        policy=AssetPolicy(1, "analytics", "default.users", []),
        admitted_schema=PublishedSchema(
            1,
            (
                {
                    "name": "profile.email",
                    "field_id": "iceberg:3",
                    "path": ["profile", "email"],
                    "type": "string",
                    "nullable": True,
                },
            ),
            False,
            None,
        ),
        asset_id=None,
    )
    schema = pa.schema(
        [
            pa.field(
                "profile",
                pa.struct(
                    [pa.field("contact_email", pa.string(), metadata={b"PARQUET:field_id": b"3"})]
                ),
                metadata={b"PARQUET:field_id": b"2"},
            )
        ]
    )

    with pytest.raises(ValueError, match="no longer matches"):
        admit_snapshot(asset, schema)


def test_live_schema_admission_accepts_iceberg_numeric_metadata_and_aliases():
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", "", 0, {}),
        target="default.users",
        source=PublishedSource("iceberg", None, {}),
        policy=AssetPolicy(1, "analytics", "default.users", []),
        admitted_schema=PublishedSchema(
            1,
            (
                {
                    "name": "id",
                    "field_id": "iceberg:1",
                    "path": ["id"],
                    "type": "long",
                    "nullable": False,
                },
                {
                    "name": "email",
                    "field_id": "iceberg:2",
                    "path": ["email"],
                    "type": "string",
                    "nullable": True,
                },
                {
                    "name": "profile",
                    "field_id": "iceberg:3",
                    "path": ["profile"],
                    "type": "struct<name: string>",
                    "nullable": True,
                },
                {
                    "name": "name",
                    "field_id": "iceberg:4",
                    "path": ["profile", "name"],
                    "type": "string",
                    "nullable": True,
                },
            ),
            False,
            None,
        ),
        asset_id=None,
    )

    admit_snapshot(
        asset,
        pa.schema(
            [
                pa.field("id", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
                pa.field("email", pa.large_string(), metadata={b"PARQUET:field_id": b"2"}),
                pa.field(
                    "profile",
                    pa.struct(
                        [pa.field("name", pa.large_string(), metadata={b"PARQUET:field_id": b"4"})]
                    ),
                    metadata={b"PARQUET:field_id": b"3"},
                ),
            ]
        ),
    )


def test_live_schema_admission_tracks_collection_element_and_map_value_paths():
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", "", 0, {}),
        target="default.nested",
        source=PublishedSource("iceberg", None, {}),
        policy=AssetPolicy(1, "analytics", "default.nested", []),
        admitted_schema=PublishedSchema(
            1,
            (
                {
                    "name": "tags.$element",
                    "field_id": "iceberg:11",
                    "path": ["tags", "$element"],
                    "type": "string",
                    "nullable": True,
                },
                {
                    "name": "attributes.$key",
                    "field_id": "iceberg:12",
                    "path": ["attributes", "$key"],
                    "type": "string",
                    "nullable": False,
                },
                {
                    "name": "attributes.$value.label",
                    "field_id": "iceberg:14",
                    "path": ["attributes", "$value", "label"],
                    "type": "string",
                    "nullable": True,
                },
            ),
            False,
            None,
        ),
        asset_id=None,
    )
    schema = pa.schema(
        [
            pa.field(
                "tags",
                pa.list_(pa.field("element", pa.string(), metadata={b"PARQUET:field_id": b"11"})),
                metadata={b"PARQUET:field_id": b"10"},
            ),
            pa.field(
                "attributes",
                pa.map_(
                    pa.field(
                        "key", pa.string(), nullable=False, metadata={b"PARQUET:field_id": b"12"}
                    ),
                    pa.field(
                        "value",
                        pa.struct(
                            [pa.field("label", pa.string(), metadata={b"PARQUET:field_id": b"14"})]
                        ),
                        metadata={b"PARQUET:field_id": b"13"},
                    ),
                ),
                metadata={b"PARQUET:field_id": b"9"},
            ),
        ]
    )

    admit_snapshot(asset, schema)


def test_live_schema_admission_rejects_collection_identity_drift():
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", "", 0, {}),
        target="default.nested",
        source=PublishedSource("iceberg", None, {}),
        policy=AssetPolicy(1, "analytics", "default.nested", []),
        admitted_schema=PublishedSchema(
            1,
            (
                {
                    "name": "tags.$element",
                    "field_id": "iceberg:11",
                    "path": ["tags", "$element"],
                    "type": "string",
                    "nullable": True,
                },
            ),
            False,
            None,
        ),
        asset_id=None,
    )
    schema = pa.schema(
        [
            pa.field(
                "tags",
                pa.list_(pa.field("element", pa.string(), metadata={b"PARQUET:field_id": b"99"})),
                metadata={b"PARQUET:field_id": b"10"},
            )
        ]
    )

    with pytest.raises(ValueError, match="no longer matches"):
        admit_snapshot(asset, schema)


def test_live_schema_admission_rejects_duplicate_live_field_ids():
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", "", 0, {}),
        target="default.duplicate",
        source=PublishedSource("iceberg", None, {}),
        policy=AssetPolicy(1, "analytics", "default.duplicate", []),
        admitted_schema=PublishedSchema(
            1,
            (
                {
                    "name": "id",
                    "field_id": "iceberg:1",
                    "path": ["id"],
                    "type": "int64",
                    "nullable": False,
                },
            ),
            False,
            None,
        ),
        asset_id=None,
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
            pa.field("renamed", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
        ]
    )

    with pytest.raises(ValueError, match="duplicate field identity"):
        admit_snapshot(asset, schema)


def test_live_schema_admission_rejects_tampered_admission_digest():
    fields = [
        {"name": "id", "field_id": "iceberg:1", "path": ["id"], "type": "int64", "nullable": False}
    ]
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", "", 0, {}),
        target="default.tampered",
        source=PublishedSource("iceberg", None, {}),
        policy=AssetPolicy(1, "analytics", "default.tampered", []),
        admitted_schema=PublishedSchema(1, tuple(fields), False, "0" * 64),
        asset_id=None,
    )

    with pytest.raises(ValueError, match="admission digest"):
        admit_snapshot(
            asset, pa.schema([pa.field("id", pa.int64(), metadata={b"PARQUET:field_id": b"1"})])
        )


def _seed_live_asset(
    session: Session,
    *,
    policy_version: int,
    backend: str = "iceberg",
    table: str = "prod.users",
    plugin_id: str = ICEBERG_CATALOG_ID,
    catalog_options: dict[str, object] | None = None,
    target_options: dict[str, object] | None = None,
) -> None:
    catalog_id = uuid4()
    asset_id = uuid4()
    session.add_all(
        [
            WorkspaceRecord(id=1),
            RuntimeSettingsRecord(
                ticket_ttl_seconds=300,
                max_tickets=32,
                max_ticket_exchanges=1,
                revision=0,
                path_rules_json=[],
                id=1,
            ),
            CatalogRecord(
                id=catalog_id,
                name="analytics",
                plugin_id=plugin_id,
                options_json=dict(catalog_options or {}),
                revision=0,
            ),
            AssetRecord(
                id=asset_id,
                catalog_id=catalog_id,
                target="default.users",
                backend=backend,
                table_identifier=table,
                options_json=dict(target_options or {}),
                revision=0,
                policy_revision=policy_version,
            ),
            PolicyRuleRecord(
                id=uuid4(),
                asset_id=asset_id,
                ordinal=10,
                effect="allow",
                principals_json=["user1"],
                when_json={},
                columns_json=["id", "email"],
                masks_json={"email": {"type": "email"}},
                row_filter_sql="region = 'us'",
            ),
        ]
    )
    session.commit()


def test_explicit_allow_all_does_not_require_a_frozen_column_selection():
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog("analytics", "", 0, {}),
        target="default.users",
        source=PublishedSource("iceberg", None, {}),
        policy=AssetPolicy(
            1,
            "analytics",
            "default.users",
            [AccessRule(principals=["*"], columns=["*"], masks={}, effect="allow_all")],
        ),
        admitted_schema=None,
        asset_id=None,
    )
    admit_snapshot(asset, pa.schema([pa.field("new_column", pa.string())]))
