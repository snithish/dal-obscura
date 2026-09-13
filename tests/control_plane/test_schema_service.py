from __future__ import annotations

from dataclasses import replace
from typing import Any, cast
from uuid import UUID, uuid4

import pyarrow as pa
import pytest
from dal_obscura_plugin_api import PluginDescriptor, SchemaDescriptor, TableHandle, TableIdentifier
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    ListType,
    MapType,
    NestedField,
    StringType,
    StructType,
)

from dal_obscura.common.schema_identity import schema_scope_digest
from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.control_plane.application.schema_service import (
    MAX_SCHEMA_DEPTH,
    MAX_SCHEMA_NODES,
    _arrow_field_id,
    _load_legacy_iceberg_schema,
    get_asset_schema,
    schema_fingerprint,
)


class _FakeTable:
    def __init__(self, schema: Schema) -> None:
        self._schema = schema

    def schema(self) -> Schema:
        return self._schema


class _FakeCatalog:
    def __init__(self, table: _FakeTable) -> None:
        self._table = table
        self.loaded_identifier: str | None = None

    def load_table(self, identifier: str) -> _FakeTable:
        self.loaded_identifier = identifier
        return self._table


class _FakeStore:
    def __init__(self, asset_id: UUID) -> None:
        self.asset_id = asset_id

    def get_workspace_asset(self, asset_id: UUID) -> dict[str, object]:
        assert asset_id == self.asset_id
        return {
            "catalog": "analytics",
            "name": "default.events",
            "table_identifier": "default.events",
        }

    def get_default_workspace_context(self) -> object:
        return object()

    def get_workspace_catalog(self, context: object, name: str) -> dict[str, object]:
        assert name == "analytics"
        return {
            "name": "analytics",
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        }

    def list_asset_owners(self, asset_id: UUID) -> list[str]:
        return []

    def list_asset_grants(self, asset_id: UUID) -> list[dict[str, str]]:
        return []


def _nested_schema() -> Schema:
    return Schema(
        NestedField(
            field_id=1,
            name="profile",
            field_type=StructType(
                NestedField(field_id=2, name="email", field_type=StringType()),
                NestedField(
                    field_id=3,
                    name="labels",
                    field_type=ListType(
                        element_id=4,
                        element_type=StringType(),
                        element_required=False,
                    ),
                ),
            ),
        ),
        NestedField(
            field_id=5,
            name="attributes",
            field_type=MapType(
                key_id=6,
                key_type=StringType(),
                value_id=7,
                value_type=IntegerType(),
                value_required=False,
            ),
        ),
    )


def test_get_asset_schema_returns_typed_nested_paths() -> None:
    asset_id = uuid4()
    store = _FakeStore(asset_id)
    catalog = _FakeCatalog(_FakeTable(_nested_schema()))
    calls: list[tuple[str, dict[str, Any]]] = []

    def load_catalog(name: str, **options: Any) -> _FakeCatalog:
        calls.append((name, options))
        return catalog

    result = get_asset_schema(
        store,  # type: ignore[arg-type]
        asset_id,
        ControlPlaneActor.for_platform_admin("admin"),
        load_catalog_fn=load_catalog,
    )

    assert calls == [("analytics", {"type": "sql", "uri": "sqlite:///catalog.db"})]
    assert catalog.loaded_identifier == "default.events"
    assert result["schema_version"] == 1
    assert isinstance(result["schema_fingerprint"], str)
    assert len(result["schema_fingerprint"]) == 64
    fields = cast(list[dict[str, object]], result["fields"])
    profile = fields[0]
    assert profile["kind"] == "struct"
    assert profile["path"] == {
        "version": 1,
        "segments": [{"kind": "field", "name": "profile", "field_id": 1}],
    }
    children = cast(list[dict[str, object]], profile["children"])
    assert children[0]["human_path"] == "profile.email"
    assert children[1]["human_path"] == "profile.labels"
    labels_element = cast(list[dict[str, object]], children[1]["children"])[0]
    assert labels_element["human_path"] == "profile.labels.$element"
    attributes = fields[1]
    assert attributes["kind"] == "map"
    attribute_children = cast(list[dict[str, object]], attributes["children"])
    assert [child["human_path"] for child in attribute_children] == [
        "attributes.$key",
        "attributes.$value",
    ]


def test_arrow_synthetic_field_ids_are_schema_scoped() -> None:
    integer_schema = pa.schema([pa.field("value", pa.int64())])
    string_schema = pa.schema([pa.field("value", pa.string())])

    integer_id = _arrow_field_id(
        integer_schema.field("value"),
        ("value",),
        schema_scope_digest(integer_schema),
    )
    string_id = _arrow_field_id(
        string_schema.field("value"),
        ("value",),
        schema_scope_digest(string_schema),
    )

    assert integer_id != string_id
    assert 0 <= integer_id <= 2**31 - 1
    assert 0 <= string_id <= 2**31 - 1


def test_get_asset_schema_routes_admitted_catalog_and_format_plugins() -> None:  # noqa: C901
    asset_id = uuid4()
    schema = pa.schema(
        [
            pa.field(
                "profile",
                pa.struct([pa.field("email", pa.string())]),
                metadata={b"iceberg.field.id": b"2"},
            )
        ]
    )
    closed: list[str] = []

    class PublicStore(_FakeStore):
        def get_workspace_asset(self, asset_id: UUID) -> dict[str, object]:
            assert asset_id == self.asset_id
            return {
                "id": str(self.asset_id),
                "catalog": "analytics",
                "name": "default.events",
                "table_identifier": "default.events",
                "backend": "fixture.format",
            }

        def get_workspace_catalog(self, context: object, name: str) -> dict[str, object]:
            assert name == "analytics"
            return {
                "name": name,
                "module": "fixture.catalog",
                "revision": 3,
                "options": {"uri": "https://catalog.example"},
            }

    identifier = TableIdentifier(namespace=("default",), name="events")
    handle = TableHandle(
        catalog_plugin_id="fixture.catalog",
        catalog_instance_id="analytics",
        catalog_revision=3,
        identifier=identifier,
        format_plugin_id="fixture.format",
        handle_version=1,
    )

    class PublicCatalog:
        descriptor = PluginDescriptor(
            kind="catalog",
            plugin_id="fixture.catalog",
            api_version="1",
            config_version=1,
            distribution="fixture",
            version="1.0.0",
        )

        def validate_config(self, context):
            del context

        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return (("default",),)

        def resolve_table(self, value, context):
            del context
            assert value == identifier
            return handle

        def close(self):
            closed.append("catalog")

    class PublicFormat:
        descriptor = PluginDescriptor(
            kind="table_format",
            plugin_id="fixture.format",
            api_version="1",
            config_version=1,
            distribution="fixture",
            version="1.0.0",
        )

        def schema(self, value, context):
            del context
            assert value == handle
            return SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=schema)

        def close(self):
            closed.append("format")

    class Registry:
        def admitted(self):
            return {
                ("catalog", "fixture.catalog"): PublicCatalog.descriptor,
            }

        def load(self, kind, plugin_id):
            if (kind, plugin_id) == ("catalog", "fixture.catalog"):
                return lambda config, context: PublicCatalog()
            if (kind, plugin_id) == ("table_format", "fixture.format"):
                return lambda value, context: PublicFormat()
            raise AssertionError((kind, plugin_id))

    result = get_asset_schema(
        PublicStore(asset_id),  # type: ignore[arg-type]
        asset_id,
        ControlPlaneActor.for_platform_admin("admin"),
        plugin_registry=Registry(),
    )

    assert result["catalog"] == "analytics"
    assert result["target"] == "default.events"
    assert result["stable_field_ids"] is False
    assert cast(list[dict[str, object]], result["fields"])[0]["name"] == "profile"
    assert closed == ["format", "catalog"]


    class ForgedHandleCatalog(PublicCatalog):
        def resolve_table(self, value, context):
            del value, context
            return replace(handle, catalog_revision=4)

    class ForgedHandleRegistry(Registry):
        def load(self, kind, plugin_id):
            if (kind, plugin_id) == ("catalog", "fixture.catalog"):
                return lambda config, context: ForgedHandleCatalog()
            return super().load(kind, plugin_id)

    with pytest.raises(ValidationFailure, match="table handle identity"):
        get_asset_schema(
            PublicStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            plugin_registry=ForgedHandleRegistry(),
        )

    class ForgedFormat(PublicFormat):
        descriptor = PluginDescriptor(
            kind="table_format",
            plugin_id="other.format",
            api_version="1",
            config_version=1,
            distribution="fixture",
            version="1.0.0",
        )

    class ForgedRegistry(Registry):
        def load(self, kind, plugin_id):
            if (kind, plugin_id) == ("table_format", "fixture.format"):
                return lambda value, context: ForgedFormat()
            return super().load(kind, plugin_id)

    with pytest.raises(ValidationFailure, match="identity"):
        get_asset_schema(
            PublicStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            plugin_registry=ForgedRegistry(),
        )


def test_get_asset_schema_rejects_persisted_unknown_catalog_option_before_factory() -> None:
    asset_id = uuid4()
    store = _FakeStore(asset_id)
    cast(Any, store).get_workspace_catalog = lambda context, name: {
        "name": name,
        "module": "fixture.catalog",
        "revision": 1,
        "options": {"uri": "https://catalog.example", "debug": True},
    }
    descriptor = PluginDescriptor(
        kind="catalog",
        plugin_id="fixture.catalog",
        api_version="1",
        config_version=1,
        distribution="fixture",
        version="1.0.0",
        config_schema={"fields": [{"name": "uri", "required": True}]},
    )
    loaded = False

    class Registry:
        def admitted(self):
            return {("catalog", "fixture.catalog"): descriptor}

        def load(self, kind, plugin_id):
            nonlocal loaded
            loaded = True
            raise AssertionError("unknown options must fail before factory loading")

    with pytest.raises(ValidationFailure, match="unsupported fields"):
        get_asset_schema(
            store,  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            plugin_registry=Registry(),
        )
    assert loaded is False


def test_legacy_iceberg_format_bridge_loads_external_catalog_handle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = pa.schema([pa.field("id", pa.int64())])
    identifier = TableIdentifier(namespace=("default",), name="events")
    handle = TableHandle(
        catalog_plugin_id="iceberg.rest",
        catalog_instance_id="analytics",
        catalog_revision=2,
        identifier=identifier,
        format_plugin_id="iceberg",
        handle_version=1,
        metadata={"metadata_location": "https://catalog.example/metadata.json"},
    )
    received: dict[str, object] = {}

    class FakeIcebergFormat:
        def __init__(self, **kwargs):
            received.update(kwargs)

        def get_schema(self):
            return schema

    monkeypatch.setattr(
        "dal_obscura.data_plane.infrastructure.table_formats.iceberg.IcebergTableFormat",
        FakeIcebergFormat,
    )

    result = _load_legacy_iceberg_schema(
        handle=handle,
        catalog_name="analytics",
        egress_allowlist=("catalog.example",),
    )

    assert result == schema
    assert received["metadata_location"] == "https://catalog.example/metadata.json"


def test_legacy_iceberg_format_bridge_checks_returned_location_before_io() -> None:
    identifier = TableIdentifier(namespace=("default",), name="events")
    handle = TableHandle(
        catalog_plugin_id="iceberg.rest",
        catalog_instance_id="analytics",
        catalog_revision=2,
        identifier=identifier,
        format_plugin_id="iceberg",
        handle_version=1,
        metadata={"metadata_location": "https://blocked.example/metadata.json"},
    )

    with pytest.raises(ValidationFailure, match="egress allowlist"):
        _load_legacy_iceberg_schema(
            handle=handle,
            catalog_name="analytics",
            egress_allowlist=("catalog.example",),
        )


def test_schema_loading_enforces_catalog_egress_before_provider_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asset_id = uuid4()
    store = _FakeStore(asset_id)
    monkeypatch.setattr(
        store,
        "get_workspace_catalog",
        lambda context, name: {
            "name": name,
            "options": {"uri": "https://blocked.example/catalog"},
        },
    )
    called = False

    def load_catalog(name: str, **options: Any) -> _FakeCatalog:
        nonlocal called
        called = True
        raise AssertionError("provider must not be called for a denied host")

    with pytest.raises(ValidationFailure, match="egress allowlist"):
        get_asset_schema(
            store,  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            load_catalog_fn=load_catalog,
            egress_allowlist=("catalog.example",),
        )
    assert called is False


def test_schema_loading_resolves_secret_references_before_provider_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asset_id = uuid4()
    store = _FakeStore(asset_id)
    monkeypatch.setenv("CATALOG_TOKEN", "sentinel-secret")
    monkeypatch.setattr(
        store,
        "get_workspace_catalog",
        lambda context, name: {
            "name": name,
            "options": {
                "uri": "https://catalog.example/api",
                "token": {"secret": "CATALOG_TOKEN"},
            },
        },
    )
    received: dict[str, Any] = {}

    def load_catalog(name: str, **options: Any) -> _FakeCatalog:
        received.update(options)
        return _FakeCatalog(_FakeTable(_nested_schema()))

    get_asset_schema(
        store,  # type: ignore[arg-type]
        asset_id,
        ControlPlaneActor.for_platform_admin("admin"),
        load_catalog_fn=load_catalog,
        egress_allowlist=("catalog.example",),
    )

    assert received["token"] == "sentinel-secret"


def test_schema_fingerprint_includes_collection_ids_and_matches_arrow_normalization() -> None:
    original = Schema(
        NestedField(
            field_id=1,
            name="items",
            field_type=ListType(
                element_id=2,
                element_type=StructType(
                    NestedField(field_id=3, name="value", field_type=StringType())
                ),
            ),
        )
    )
    changed_element = Schema(
        NestedField(
            field_id=1,
            name="items",
            field_type=ListType(
                element_id=99,
                element_type=StructType(
                    NestedField(field_id=3, name="value", field_type=StringType())
                ),
            ),
        )
    )

    assert schema_fingerprint(original) == schema_fingerprint(original.as_arrow())
    assert schema_fingerprint(original) != schema_fingerprint(changed_element)


def test_schema_fingerprint_rejects_excessive_arrow_nesting() -> None:
    nested: pa.DataType = pa.string()
    for _ in range(MAX_SCHEMA_DEPTH + 1):
        nested = pa.struct([pa.field("child", nested)])

    with pytest.raises(ValidationFailure, match="Arrow schema exceeds"):
        schema_fingerprint(pa.schema([pa.field("root", nested)]))


def test_get_asset_schema_requires_read_capability() -> None:
    asset_id = uuid4()
    with pytest.raises(AuthorizationFailure):
        get_asset_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor("outsider", ()),
            load_catalog_fn=lambda **_: pytest.fail("catalog must not be loaded"),
        )


def test_get_asset_schema_redacts_catalog_provider_errors() -> None:
    asset_id = uuid4()

    def failing_catalog(**_: object):
        raise ValueError("failed https://catalog-user:catalog-password@catalog.example")

    with pytest.raises(ValidationFailure, match="Schema discovery failed") as failure:
        get_asset_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            load_catalog_fn=failing_catalog,
        )

    assert "catalog-password" not in str(failure.value)


def test_get_asset_schema_rejects_excessive_node_count() -> None:
    asset_id = uuid4()
    schema = Schema(
        *(
            NestedField(field_id=index, name=f"field_{index}", field_type=StringType())
            for index in range(1, MAX_SCHEMA_NODES + 2)
        )
    )

    with pytest.raises(ValidationFailure, match="field-node limit"):
        get_asset_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            load_catalog_fn=lambda *_args, **_: _FakeCatalog(_FakeTable(schema)),
        )


def test_get_asset_schema_rejects_excessive_nesting_depth() -> None:
    asset_id = uuid4()
    nested: object = StringType()
    for index in range(MAX_SCHEMA_DEPTH + 1, 0, -1):
        nested = StructType(
            NestedField(
                field_id=index,
                name=f"level_{index}",
                field_type=nested,
            )
        )
    schema = Schema(
        NestedField(
            field_id=MAX_SCHEMA_DEPTH + 2,
            name="root",
            field_type=nested,
        )
    )

    with pytest.raises(ValidationFailure, match="nesting-depth limit"):
        get_asset_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            load_catalog_fn=lambda *_args, **_: _FakeCatalog(_FakeTable(schema)),
        )
