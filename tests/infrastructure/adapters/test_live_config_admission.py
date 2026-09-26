from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from typing import cast
from uuid import UUID, uuid4

import pyarrow as pa
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from dal_obscura.common.access_control.models import Principal
from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.common.config_store.orm import (
    AssetRecord,
    CatalogRecord,
    CellRecord,
    CellRuntimeSettingsRecord,
    CellTenantRecord,
    PolicyRuleRecord,
    TenantRecord,
)
from dal_obscura.data_plane.infrastructure.adapters.live_config import (
    CatalogRegistry,
    LiveAsset,
    LiveCatalog,
    LiveConfigAuthorizer,
    LiveConfigCatalogRegistry,
    LiveConfigStore,
    _catalog_config_for_asset,
    _schema_identities,
    _validate_schema_admission,
)
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer

ICEBERG_CATALOG_ID = "iceberg.sql"


def test_catalog_registry_close_attempts_all_cached_instances_when_one_fails() -> None:
    closed: list[str] = []

    class FakeRegistry:
        def __init__(self, name: str) -> None:
            self.name = name

        def close(self) -> None:
            closed.append(self.name)
            if self.name == "first":
                raise RuntimeError("first generation close failed")

    registry = LiveConfigCatalogRegistry(cast(LiveConfigStore, object()))
    registry._registry_cache = cast(
        dict[tuple[UUID, UUID, str, str], CatalogRegistry],
        {
            (uuid4(), uuid4(), "analytics", "first"): FakeRegistry("first"),
            (uuid4(), uuid4(), "analytics", "second"): FakeRegistry("second"),
        },
    )

    with pytest.raises(RuntimeError, match="first generation close failed"):
        registry.close()

    assert closed == ["first", "second"]
    assert registry._registry_cache == {}


@pytest.fixture
def db_session() -> Iterator[Session]:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    session_maker = session_factory(engine)
    with session_maker() as session:
        yield session


def test_live_authorizer_resolves_policy_from_active_asset(db_session: Session):
    cell_id = uuid4()
    tenant_id = uuid4()
    _seed_live_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    authorizer = LiveConfigAuthorizer(LiveConfigStore(db_session, cell_id=cell_id))

    decision = authorizer.authorize(
        principal=Principal(id="user1", groups=[], attributes={"tenant_id": str(tenant_id)}),
        target="default.users",
        catalog="analytics",
        requested_columns=["id", "email"],
    )

    assert decision.allowed_columns == ["id", "email"]
    assert decision.masks["email"].type == "email"
    assert decision.row_filter == "(region = 'us')"
    assert decision.policy_version != 123


def test_live_authorizer_accepts_tenant_slug_attribute(db_session: Session):
    cell_id = uuid4()
    tenant_id = uuid4()
    _seed_live_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    authorizer = LiveConfigAuthorizer(LiveConfigStore(db_session, cell_id=cell_id))

    decision = authorizer.authorize(
        principal=Principal(id="user1", groups=[], attributes={"tenant_id": f"tenant-{tenant_id}"}),
        target="default.users",
        catalog="analytics",
        requested_columns=["id"],
    )

    assert decision.allowed_columns == ["id"]
    assert decision.policy_version != 123


def test_live_store_loads_asset_and_catalog_from_one_generation(db_session: Session):
    cell_id = uuid4()
    tenant_id = uuid4()
    _seed_live_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    store = LiveConfigStore(db_session, cell_id=cell_id)

    asset, catalog = store.get_asset_and_catalog(
        tenant_id=str(tenant_id),
        catalog="analytics",
        target="default.users",
    )

    assert asset.config_revision == catalog.config_revision
    assert asset.catalog == catalog.catalog == "analytics"


def test_live_config_rejects_tampered_plugin_binding():
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        compiled_config={
            "plugins": {"catalog": "untrusted.catalog", "table_format": "iceberg"},
            "target": {"backend": "iceberg", "table": "default.users"},
        },
        policy_version=1,
    )
    catalog = LiveCatalog(
        config_revision=asset.config_revision,
        tenant_id=asset.tenant_id,
        catalog="analytics",
        config={"type": "iceberg", "options": {}},
    )

    with pytest.raises(ValueError, match="unsupported without an admitted registry"):
        _catalog_config_for_asset(catalog, asset)


def test_live_config_rejects_legacy_plugin_id_shape():
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        compiled_config={
            "plugins": {"catalog": "iceberg.sql", "table_format": "iceberg"},
            "target": {"backend": "iceberg", "table": "default.users"},
        },
        policy_version=1,
    )
    catalog = LiveCatalog(
        config_revision=asset.config_revision,
        tenant_id=asset.tenant_id,
        catalog="analytics",
        config={"module": ICEBERG_CATALOG_ID, "options": {}},
    )

    with pytest.raises(ValueError, match="retired module identity"):
        _catalog_config_for_asset(catalog, asset)


def test_live_config_rejects_retired_provider_modules_option():
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        compiled_config={
            "plugins": {"catalog": "iceberg.sql", "table_format": "iceberg"},
            "target": {"backend": "iceberg", "table": "default.users"},
        },
        policy_version=1,
    )
    catalog = LiveCatalog(
        config_revision=asset.config_revision,
        tenant_id=asset.tenant_id,
        catalog="analytics",
        config={"type": "iceberg", "options": {"provider_modules": ["old.provider"]}},
    )

    with pytest.raises(ValueError, match="retired provider_modules option"):
        _catalog_config_for_asset(catalog, asset)


def test_live_config_passes_runtime_path_enforcer_to_catalog():
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        compiled_config={
            "plugins": {"catalog": "iceberg.sql", "table_format": "iceberg"},
            "target": {"backend": "iceberg", "table": "default.users"},
        },
        policy_version=1,
    )
    catalog = LiveCatalog(
        config_revision=asset.config_revision,
        tenant_id=asset.tenant_id,
        catalog="analytics",
        config={"type": "iceberg", "options": {}},
    )
    enforcer = PathRuleEnforcer([{"root": "s3://warehouse"}])

    resolved = _catalog_config_for_asset(catalog, asset, path_enforcer=enforcer)

    assert resolved.path_enforcer is enforcer


class _AdmittedPluginSnapshot:
    def __init__(self, *keys: tuple[str, str]) -> None:
        self._keys = set(keys)

    def admitted(self) -> dict[tuple[str, str], object]:
        return {key: object() for key in self._keys}


def test_live_config_requires_both_plugin_identities_in_admitted_snapshot():
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        compiled_config={
            "plugins": {
                "catalog": "iceberg.sql",
                "table_format": "iceberg",
            },
            "target": {"backend": "iceberg", "table": "default.users"},
        },
        policy_version=1,
    )
    catalog = LiveCatalog(
        config_revision=asset.config_revision,
        tenant_id=asset.tenant_id,
        catalog="analytics",
        config={"type": "iceberg", "options": {}},
    )

    with pytest.raises(ValueError, match="plugin binding is not admitted"):
        _catalog_config_for_asset(
            catalog,
            asset,
            plugin_registry=_AdmittedPluginSnapshot(("catalog", "iceberg.sql")),
        )

    resolved = _catalog_config_for_asset(
        catalog,
        asset,
        plugin_registry=_AdmittedPluginSnapshot(
            ("catalog", "iceberg.sql"),
            ("table_format", "iceberg"),
        ),
    )
    assert resolved.type == "iceberg"

    retired = replace(
        asset,
        compiled_config={
            **asset.compiled_config,
            "plugins": {
                "catalog": (
                    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
                ),
                "table_format": "iceberg",
            },
        },
    )
    with pytest.raises(ValueError, match="plugin binding is not admitted"):
        _catalog_config_for_asset(
            catalog,
            retired,
            plugin_registry=_AdmittedPluginSnapshot(
                ("catalog", "iceberg.sql"),
                ("table_format", "iceberg"),
            ),
        )


def test_live_config_preserves_external_plugin_identity():
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.users",
        backend="parquet.dataset",
        compiled_config={
            "plugins": {"catalog": "manifest", "table_format": "parquet.dataset"},
            "target": {"backend": "parquet.dataset", "table": "default.users"},
        },
        policy_version=1,
    )
    catalog = LiveCatalog(
        config_revision=asset.config_revision,
        tenant_id=asset.tenant_id,
        catalog="analytics",
        config={
            "type": "plugin",
            "plugin_id": "manifest",
            "options": {"root": "/srv/data"},
        },
    )
    resolved = _catalog_config_for_asset(
        catalog,
        asset,
        plugin_registry=_AdmittedPluginSnapshot(
            ("catalog", "manifest"), ("table_format", "parquet.dataset")
        ),
    )
    assert resolved.plugin_id == "manifest"
    assert resolved.options == {"root": "/srv/data"}


def test_live_config_preserves_catalog_plugin_revision():
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.users",
        backend="parquet.dataset",
        compiled_config={
            "plugins": {"catalog": "manifest", "table_format": "parquet.dataset"},
            "target": {"backend": "parquet.dataset", "table": "default.users"},
        },
        policy_version=1,
    )
    catalog = LiveCatalog(
        config_revision=asset.config_revision,
        tenant_id=asset.tenant_id,
        catalog="analytics",
        config={"type": "iceberg", "options": {"root": "/srv/data"}},
        plugin_id="manifest",
        plugin_revision=17,
    )

    resolved = _catalog_config_for_asset(
        catalog,
        asset,
        plugin_registry=_AdmittedPluginSnapshot(
            ("catalog", "manifest"), ("table_format", "parquet.dataset")
        ),
    )

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
    identities = _schema_identities(schema)

    assert len(identities) == 8
    assert all(field_id.startswith("synthetic:") for _, field_id in identities)
    assert any(path == ("profile", "phone") for path, _ in identities)

    admission = {
        "fields": [
            {"path": list(path), "field_id": field_id, "type": field_type}
            for (path, field_id), field_type in identities.items()
        ]
    }
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        compiled_config={"schema": admission},
        policy_version=1,
    )

    _validate_schema_admission(asset, schema)
    changed = pa.schema(
        [
            pa.field("email", pa.string()),
            pa.field("profile", pa.struct([pa.field("mobile", pa.string())])),
            pa.field("tags", pa.list_(pa.field("item", pa.string()))),
            pa.field("labels", pa.map_(pa.string(), pa.string())),
        ]
    )
    with pytest.raises(ValueError, match="schema admission"):
        _validate_schema_admission(asset, changed)


def test_schema_admission_rejects_unstable_live_schema_for_stable_ids():
    schema = pa.schema([pa.field("email", pa.string())])
    field_id = "iceberg:1"
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        compiled_config={
            "schema": {
                "stable_ids": True,
                "fields": [{"path": ["email"], "field_id": field_id, "type": "string"}],
            }
        },
        policy_version=1,
    )

    with pytest.raises(ValueError, match="stable provider field IDs"):
        _validate_schema_admission(asset, schema)


def test_legacy_wildcard_policy_requires_schema_admission():
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        compiled_config={"policy": {"rules": [{"columns": ["*"], "masks": {}}]}},
        policy_version=1,
    )

    with pytest.raises(ValueError, match="requires schema admission"):
        _validate_schema_admission(asset, pa.schema([pa.field("id", pa.int64())]))


def test_legacy_parent_policy_requires_schema_admission():
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        compiled_config={"policy": {"rules": [{"columns": ["profile"], "masks": {}}]}},
        policy_version=1,
    )
    schema = pa.schema([pa.field("profile", pa.struct([pa.field("email", pa.string())]))])

    with pytest.raises(ValueError, match="requires schema admission"):
        _validate_schema_admission(asset, schema)


def test_live_store_fails_closed_by_default_after_transient_failure(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
):
    cell_id = uuid4()
    tenant_id = uuid4()
    _seed_live_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    config_store = LiveConfigStore(db_session, cell_id=cell_id)

    config_store.get_asset(
        tenant_id=str(tenant_id),
        catalog="analytics",
        target="default.users",
    )

    def fail_get(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(db_session, "get", fail_get)

    with pytest.raises(RuntimeError, match="database unavailable"):
        config_store.get_asset(
            tenant_id=str(tenant_id),
            catalog="analytics",
            target="default.users",
        )


def test_live_store_rejects_directly_removed_assets_and_flushes_cache(db_session: Session):
    cell_id = uuid4()
    tenant_id = uuid4()
    _seed_live_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    config_store = LiveConfigStore(db_session, cell_id=cell_id)
    config_store.get_asset(tenant_id=str(tenant_id), catalog="analytics", target="default.users")

    asset = db_session.scalar(select(AssetRecord).where(AssetRecord.target == "default.users"))
    assert asset is not None
    db_session.delete(asset)
    cell = db_session.get(CellRecord, cell_id)
    assert cell is not None
    cell.configuration_revision += 1
    db_session.commit()

    with pytest.raises(LookupError, match="No live asset"):
        config_store.get_asset(
            tenant_id=str(tenant_id), catalog="analytics", target="default.users"
        )
    assert config_store._asset_cache == {}


def test_live_authorizer_rejects_corrupt_mask_instead_of_dropping_it(
    db_session: Session,
):
    cell_id = uuid4()
    tenant_id = uuid4()
    _seed_live_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    record = db_session.scalar(select(PolicyRuleRecord))
    assert record is not None
    record.masks_json["email"] = {}
    flag_modified(record, "masks_json")
    db_session.commit()
    authorizer = LiveConfigAuthorizer(LiveConfigStore(db_session, cell_id=cell_id))

    with pytest.raises(ValueError, match=r"mask\.type"):
        authorizer.authorize(
            principal=Principal(id="user1", groups=[], attributes={"tenant_id": str(tenant_id)}),
            target="default.users",
            catalog="analytics",
            requested_columns=["email"],
        )


def test_live_schema_admission_rejects_rebound_or_added_field():
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        compiled_config={
            "schema": {
                "encoding": 1,
                "fields": [
                    {
                        "name": "profile.email",
                        "field_id": "iceberg:3",
                        "path": ["profile", "email"],
                        "type": "string",
                        "nullable": True,
                    }
                ],
            }
        },
        policy_version=1,
    )
    schema = pa.schema(
        [
            pa.field(
                "profile",
                pa.struct(
                    [
                        pa.field(
                            "email",
                            pa.string(),
                            metadata={b"PARQUET:field_id": b"99"},
                        )
                    ]
                ),
                metadata={b"PARQUET:field_id": b"iceberg:2"},
            )
        ]
    )

    with pytest.raises(ValueError, match="no longer matches"):
        _validate_schema_admission(asset, schema)


def test_live_schema_admission_requires_refresh_after_renamed_field():
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        compiled_config={
            "schema": {
                "encoding": 1,
                "fields": [
                    {
                        "name": "profile.email",
                        "field_id": "iceberg:3",
                        "path": ["profile", "email"],
                        "type": "string",
                        "nullable": True,
                    }
                ],
            }
        },
        policy_version=1,
    )
    schema = pa.schema(
        [
            pa.field(
                "profile",
                pa.struct(
                    [
                        pa.field(
                            "contact_email",
                            pa.string(),
                            metadata={b"PARQUET:field_id": b"3"},
                        )
                    ]
                ),
                metadata={b"PARQUET:field_id": b"2"},
            )
        ]
    )

    with pytest.raises(ValueError, match="no longer matches"):
        _validate_schema_admission(asset, schema)


def test_live_schema_admission_accepts_iceberg_numeric_metadata_and_aliases():
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        compiled_config={
            "schema": {
                "encoding": 1,
                "fields": [
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
                ],
            }
        },
        policy_version=1,
    )

    _validate_schema_admission(
        asset,
        pa.schema(
            [
                pa.field("id", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
                pa.field("email", pa.large_string(), metadata={b"PARQUET:field_id": b"2"}),
                pa.field(
                    "profile",
                    pa.struct(
                        [
                            pa.field(
                                "name",
                                pa.large_string(),
                                metadata={b"PARQUET:field_id": b"4"},
                            )
                        ]
                    ),
                    metadata={b"PARQUET:field_id": b"3"},
                ),
            ]
        ),
    )


@pytest.mark.parametrize(
    "metadata",
    [
        {b"PARQUET:field_id": b"bad\x00id"},
        {b"iceberg.field.id": b"x" * 129},
        {b"PARQUET:field_id": b"\xff"},
        {b"PARQUET:field_id": b"synthetic:forged"},
        {b"iceberg.field.id": b"legacy:forged"},
    ],
)
def test_schema_identity_rejects_unbounded_or_malformed_provider_ids(metadata: dict[bytes, bytes]):
    identities = _schema_identities(pa.schema([pa.field("id", pa.int64(), metadata=metadata)]))

    [(path, field_id)] = list(identities)
    assert path == ("id",)
    assert field_id.startswith("synthetic:")


def test_live_schema_admission_tracks_collection_element_and_map_value_paths():
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.nested",
        backend="iceberg",
        compiled_config={
            "schema": {
                "encoding": 1,
                "fields": [
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
                ],
            }
        },
        policy_version=1,
    )
    schema = pa.schema(
        [
            pa.field(
                "tags",
                pa.list_(
                    pa.field(
                        "element",
                        pa.string(),
                        metadata={b"PARQUET:field_id": b"11"},
                    )
                ),
                metadata={b"PARQUET:field_id": b"10"},
            ),
            pa.field(
                "attributes",
                pa.map_(
                    pa.field(
                        "key",
                        pa.string(),
                        nullable=False,
                        metadata={b"PARQUET:field_id": b"12"},
                    ),
                    pa.field(
                        "value",
                        pa.struct(
                            [
                                pa.field(
                                    "label",
                                    pa.string(),
                                    metadata={b"PARQUET:field_id": b"14"},
                                )
                            ]
                        ),
                        metadata={b"PARQUET:field_id": b"13"},
                    ),
                ),
                metadata={b"PARQUET:field_id": b"9"},
            ),
        ]
    )

    _validate_schema_admission(asset, schema)


def test_live_schema_admission_rejects_collection_identity_drift():
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.nested",
        backend="iceberg",
        compiled_config={
            "schema": {
                "encoding": 1,
                "fields": [
                    {
                        "name": "tags.$element",
                        "field_id": "iceberg:11",
                        "path": ["tags", "$element"],
                        "type": "string",
                        "nullable": True,
                    }
                ],
            }
        },
        policy_version=1,
    )
    schema = pa.schema(
        [
            pa.field(
                "tags",
                pa.list_(
                    pa.field(
                        "element",
                        pa.string(),
                        metadata={b"PARQUET:field_id": b"99"},
                    )
                ),
                metadata={b"PARQUET:field_id": b"10"},
            )
        ]
    )

    with pytest.raises(ValueError, match="no longer matches"):
        _validate_schema_admission(asset, schema)


def test_live_schema_admission_rejects_duplicate_live_field_ids():
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.duplicate",
        backend="iceberg",
        compiled_config={
            "schema": {
                "encoding": 1,
                "fields": [
                    {
                        "name": "id",
                        "field_id": "iceberg:1",
                        "path": ["id"],
                        "type": "int64",
                        "nullable": False,
                    }
                ],
            }
        },
        policy_version=1,
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
            pa.field("renamed", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
        ]
    )

    with pytest.raises(ValueError, match="duplicate field identity"):
        _validate_schema_admission(asset, schema)


def test_live_schema_admission_rejects_tampered_admission_digest():
    fields = [
        {
            "name": "id",
            "field_id": "iceberg:1",
            "path": ["id"],
            "type": "int64",
            "nullable": False,
        }
    ]
    asset = LiveAsset(
        config_revision=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.tampered",
        backend="iceberg",
        compiled_config={"schema": {"encoding": 1, "fields": fields, "digest": "0" * 64}},
        policy_version=1,
    )

    with pytest.raises(ValueError, match="admission digest"):
        _validate_schema_admission(
            asset,
            pa.schema([pa.field("id", pa.int64(), metadata={b"PARQUET:field_id": b"1"})]),
        )


def _seed_live_asset(
    session: Session,
    *,
    cell_id,
    tenant_id,
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
            CellRecord(id=cell_id, name=f"cell-{cell_id}", region="local"),
            TenantRecord(id=tenant_id, slug=f"tenant-{tenant_id}", display_name="Default"),
            CellTenantRecord(cell_id=cell_id, tenant_id=tenant_id, shard_key="default"),
            CellRuntimeSettingsRecord(
                cell_id=cell_id,
                ticket_ttl_seconds=300,
                max_tickets=32,
                max_ticket_exchanges=1,
                revision=0,
                path_rules_json=[],
            ),
            CatalogRecord(
                id=catalog_id,
                cell_id=cell_id,
                tenant_id=tenant_id,
                name="analytics",
                plugin_id=plugin_id,
                options_json=dict(catalog_options or {}),
                revision=0,
            ),
            AssetRecord(
                id=asset_id,
                cell_id=cell_id,
                tenant_id=tenant_id,
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


def test_live_authorizer_changes_effective_version_after_policy_edit(db_session: Session):
    cell_id = uuid4()
    tenant_id = uuid4()
    _seed_live_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    authorizer = LiveConfigAuthorizer(LiveConfigStore(db_session, cell_id=cell_id))
    initial_version = authorizer.current_policy_version(
        "default.users", "analytics", tenant_id=str(tenant_id)
    )
    asset = db_session.scalar(select(AssetRecord).where(AssetRecord.target == "default.users"))
    rule = db_session.scalar(select(PolicyRuleRecord))
    assert asset is not None and rule is not None

    rule.columns_json = ["email"]
    asset.policy_revision += 1
    cell = db_session.get(CellRecord, cell_id)
    assert cell is not None
    cell.configuration_revision += 1
    db_session.commit()

    current_version = authorizer.current_policy_version(
        "default.users", "analytics", tenant_id=str(tenant_id)
    )
    assert current_version != initial_version


@pytest.mark.parametrize("attributes", [{}, {"tenant": "default"}])
def test_live_authorizer_requires_canonical_tenant_id_claim(
    db_session: Session, attributes: dict[str, str]
) -> None:
    authorizer = LiveConfigAuthorizer(LiveConfigStore(db_session, cell_id=uuid4()))

    with pytest.raises(PermissionError, match="tenant_id"):
        authorizer.authorize(
            Principal(id="user1", groups=[], attributes=attributes),
            target="default.users",
            catalog="analytics",
            requested_columns=["id"],
        )
