from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from uuid import uuid4

import pyarrow as pa
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dal_obscura.common.access_control.models import Principal
from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.common.config_store.orm import (
    PublishedAssetRecord,
    PublishedCatalogRecord,
    PublishedCellRuntimeRecord,
)
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore
from dal_obscura.data_plane.infrastructure.adapters.published_config import (
    PublishedAsset,
    PublishedCatalog,
    PublishedConfigAuthorizer,
    PublishedConfigStore,
    _catalog_config_for_asset,
    _schema_identities,
    _validate_schema_admission,
)

ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)


@pytest.fixture
def db_session() -> Iterator[Session]:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    session_maker = session_factory(engine)
    with session_maker() as session:
        yield session


def test_published_authorizer_resolves_policy_from_active_asset(db_session: Session):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    authorizer = PublishedConfigAuthorizer(PublishedConfigStore(db_session, cell_id=cell_id))

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


def test_published_authorizer_accepts_tenant_slug_attribute(db_session: Session):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    authorizer = PublishedConfigAuthorizer(PublishedConfigStore(db_session, cell_id=cell_id))

    decision = authorizer.authorize(
        principal=Principal(id="user1", groups=[], attributes={"tenant_id": f"tenant-{tenant_id}"}),
        target="default.users",
        catalog="analytics",
        requested_columns=["id"],
    )

    assert decision.allowed_columns == ["id"]
    assert decision.policy_version != 123


def test_published_store_loads_asset_and_catalog_from_one_generation(db_session: Session):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    store = PublishedConfigStore(db_session, cell_id=cell_id)

    asset, catalog = store.get_asset_and_catalog(
        tenant_id=str(tenant_id),
        catalog="analytics",
        target="default.users",
    )

    assert asset.publication_id == catalog.publication_id
    assert asset.catalog == catalog.catalog == "analytics"


def test_published_config_rejects_tampered_plugin_binding():
    asset = PublishedAsset(
        publication_id=uuid4(),
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
    catalog = PublishedCatalog(
        publication_id=asset.publication_id,
        tenant_id=asset.tenant_id,
        catalog="analytics",
        config={"type": "iceberg", "options": {}},
    )

    with pytest.raises(ValueError, match="plugin binding is unsupported"):
        _catalog_config_for_asset(catalog, asset)


def test_published_config_rejects_legacy_catalog_module_shape():
    asset = PublishedAsset(
        publication_id=uuid4(),
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
    catalog = PublishedCatalog(
        publication_id=asset.publication_id,
        tenant_id=asset.tenant_id,
        catalog="analytics",
        config={"module": ICEBERG_CATALOG_MODULE, "options": {}},
    )

    with pytest.raises(ValueError, match="retired module identity"):
        _catalog_config_for_asset(catalog, asset)


class _AdmittedPluginSnapshot:
    def __init__(self, *keys: tuple[str, str]) -> None:
        self._keys = set(keys)

    def admitted(self) -> dict[tuple[str, str], object]:
        return {key: object() for key in self._keys}


def test_published_config_requires_both_plugin_identities_in_admitted_snapshot():
    asset = PublishedAsset(
        publication_id=uuid4(),
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
    catalog = PublishedCatalog(
        publication_id=asset.publication_id,
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
            "plugins": {"catalog": ICEBERG_CATALOG_MODULE, "table_format": "iceberg"},
        },
    )
    with pytest.raises(ValueError, match="retired module identity"):
        _catalog_config_for_asset(
            catalog,
            retired,
            plugin_registry=_AdmittedPluginSnapshot(
                ("catalog", "iceberg.sql"),
                ("table_format", "iceberg"),
            ),
        )


def test_published_config_preserves_external_plugin_identity():
    asset = PublishedAsset(
        publication_id=uuid4(),
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
    catalog = PublishedCatalog(
        publication_id=asset.publication_id,
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


def test_published_config_preserves_catalog_plugin_revision():
    asset = PublishedAsset(
        publication_id=uuid4(),
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
    catalog = PublishedCatalog(
        publication_id=asset.publication_id,
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
    asset = PublishedAsset(
        publication_id=uuid4(),
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
    with pytest.raises(ValueError, match="review again"):
        _validate_schema_admission(asset, changed)


def test_schema_admission_rejects_unstable_live_schema_for_stable_publication():
    schema = pa.schema([pa.field("email", pa.string())])
    field_id = "iceberg:1"
    asset = PublishedAsset(
        publication_id=uuid4(),
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
    asset = PublishedAsset(
        publication_id=uuid4(),
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
    asset = PublishedAsset(
        publication_id=uuid4(),
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


def test_published_store_fails_closed_by_default_after_transient_failure(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    config_store = PublishedConfigStore(db_session, cell_id=cell_id)

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


def test_published_store_rejects_assets_removed_by_new_generation(
    db_session: Session,
):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    config_store = PublishedConfigStore(db_session, cell_id=cell_id)

    config_store.get_asset(
        tenant_id=str(tenant_id),
        catalog="analytics",
        target="default.users",
    )
    removed_asset_publication_id = uuid4()
    store = PublicationStore(db_session)
    store.insert_publication(
        cell_id=cell_id,
        publication_id=removed_asset_publication_id,
        manifest_hash="c" * 64,
    )
    db_session.add(
        PublishedCellRuntimeRecord(
            publication_id=removed_asset_publication_id,
            auth_chain_json={"providers": []},
            ticket_json={},
            path_rules_json=[],
        )
    )
    store.activate_publication(cell_id=cell_id, publication_id=removed_asset_publication_id)
    db_session.commit()

    with pytest.raises(LookupError, match="No published asset"):
        config_store.get_asset(
            tenant_id=str(tenant_id),
            catalog="analytics",
            target="default.users",
        )
    assert config_store._asset_cache == {}


def test_published_authorizer_rejects_corrupt_mask_instead_of_dropping_it(
    db_session: Session,
):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    record = db_session.scalar(select(PublishedAssetRecord))
    assert record is not None
    record.compiled_config_json["policy"]["rules"][0]["masks"] = {"email": {}}
    db_session.commit()
    authorizer = PublishedConfigAuthorizer(PublishedConfigStore(db_session, cell_id=cell_id))

    with pytest.raises(ValueError, match=r"mask\.type"):
        authorizer.authorize(
            principal=Principal(id="user1", groups=[], attributes={"tenant_id": str(tenant_id)}),
            target="default.users",
            catalog="analytics",
            requested_columns=["email"],
        )


def test_published_schema_admission_rejects_rebound_or_added_field():
    asset = PublishedAsset(
        publication_id=uuid4(),
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


def test_published_schema_admission_requires_reapproval_for_renamed_field():
    asset = PublishedAsset(
        publication_id=uuid4(),
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


def test_published_schema_admission_accepts_iceberg_numeric_metadata_and_aliases():
    asset = PublishedAsset(
        publication_id=uuid4(),
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
                    }
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


def test_published_schema_admission_tracks_collection_element_and_map_value_paths():
    asset = PublishedAsset(
        publication_id=uuid4(),
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


def test_published_schema_admission_rejects_collection_identity_drift():
    asset = PublishedAsset(
        publication_id=uuid4(),
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


def test_published_schema_admission_rejects_duplicate_live_field_ids():
    asset = PublishedAsset(
        publication_id=uuid4(),
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


def test_published_schema_admission_rejects_tampered_admission_digest():
    fields = [
        {
            "name": "id",
            "field_id": "iceberg:1",
            "path": ["id"],
            "type": "int64",
            "nullable": False,
        }
    ]
    asset = PublishedAsset(
        publication_id=uuid4(),
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


def _publish_asset(
    session: Session,
    *,
    cell_id,
    tenant_id,
    policy_version: int,
    backend: str = "iceberg",
    table: str = "prod.users",
    catalog_module: str = ICEBERG_CATALOG_MODULE,
    catalog_options: dict[str, object] | None = None,
    target_options: dict[str, object] | None = None,
) -> None:
    publication_id = uuid4()
    store = PublicationStore(session)
    store.create_cell(cell_id=cell_id, name=f"cell-{cell_id}", region="local")
    store.create_tenant(
        tenant_id=tenant_id,
        slug=f"tenant-{tenant_id}",
        display_name="Default",
    )
    store.assign_tenant_to_cell(cell_id=cell_id, tenant_id=tenant_id, shard_key="default")
    store.insert_publication(
        cell_id=cell_id,
        publication_id=publication_id,
        manifest_hash="b" * 64,
    )
    session.add(
        PublishedCellRuntimeRecord(
            publication_id=publication_id,
            auth_chain_json={"providers": []},
            ticket_json={},
            path_rules_json=[],
        )
    )
    session.add(
        PublishedCatalogRecord(
            publication_id=publication_id,
            tenant_id=tenant_id,
            catalog="analytics",
            config_json={
                "module": catalog_module,
                "options": dict(catalog_options or {}),
            },
        )
    )
    store.insert_published_asset(
        publication_id=publication_id,
        tenant_id=tenant_id,
        catalog="analytics",
        target="default.users",
        backend=backend,
        compiled_config={
            "catalog": {
                "module": ICEBERG_CATALOG_MODULE,
                "options": {},
            },
            "target": {
                "backend": backend,
                "table": table,
                "options": dict(target_options or {}),
            },
            "policy": {
                "rules": [
                    {
                        "principals": ["user1"],
                        "columns": ["id", "email"],
                        "effect": "allow",
                        "when": {},
                        "masks": {"email": {"type": "email"}},
                        "row_filter": "region = 'us'",
                    }
                ]
            },
        },
        policy_version=policy_version,
    )
    store.activate_publication(cell_id=cell_id, publication_id=publication_id)
    session.commit()


def test_published_authorizer_changes_effective_version_for_new_generation(db_session: Session):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    authorizer = PublishedConfigAuthorizer(PublishedConfigStore(db_session, cell_id=cell_id))
    principal = Principal(id="user1", groups=[], attributes={"tenant_id": str(tenant_id)})
    initial_version = authorizer.authorize(
        principal=principal,
        target="default.users",
        catalog="analytics",
        requested_columns=["id"],
    ).policy_version
    asset = db_session.scalar(select(PublishedAssetRecord))
    catalog = db_session.scalar(select(PublishedCatalogRecord))
    runtime = db_session.scalar(select(PublishedCellRuntimeRecord))
    assert asset is not None and catalog is not None and runtime is not None

    new_publication_id = uuid4()
    store = PublicationStore(db_session)
    store.insert_publication(
        cell_id=cell_id,
        publication_id=new_publication_id,
        manifest_hash="d" * 64,
    )
    db_session.add(
        PublishedCellRuntimeRecord(
            publication_id=new_publication_id,
            auth_chain_json=dict(runtime.auth_chain_json),
            ticket_json=dict(runtime.ticket_json),
            path_rules_json=list(runtime.path_rules_json),
        )
    )
    db_session.add(
        PublishedCatalogRecord(
            publication_id=new_publication_id,
            tenant_id=tenant_id,
            catalog=catalog.catalog,
            config_json=dict(catalog.config_json),
        )
    )
    store.insert_published_asset(
        publication_id=new_publication_id,
        tenant_id=tenant_id,
        catalog=asset.catalog,
        target=asset.target,
        backend=asset.backend,
        compiled_config=dict(asset.compiled_config_json),
        policy_version=asset.policy_version,
    )
    store.activate_publication(cell_id=cell_id, publication_id=new_publication_id)
    db_session.commit()

    current_version = authorizer.current_policy_version(
        "default.users", "analytics", tenant_id=str(tenant_id)
    )
    assert current_version != initial_version
