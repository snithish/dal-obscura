from __future__ import annotations

from dataclasses import replace
from typing import Any, cast
from uuid import UUID, uuid4

import pyarrow as pa
import pytest
from dal_obscura_plugin_api import PluginDescriptor, SchemaDescriptor, TableHandle, TableIdentifier
from dal_obscura_plugin_api.execution import close_preserving_error
from dal_obscura_plugin_api.schema.identity import schema_scope_digest

from dal_obscura.application.governance.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.kernel.identity.actor import ControlPlaneActor
from dal_obscura.plugins import PluginRegistry
from dal_obscura.runtime.secrets import SecretProviderConfig, load_secret_provider
from tests.control.assets.schema_cases import (
    _FakeCatalog,
    _FakeStore,
    _FakeTable,
    _nested_schema,
    discover_schema,
)
from tests.support.sources.schema_catalog import schema_registry


def test_discovered_nested_types_match_data_plane_schema_admission() -> None:
    from dal_obscura_plugin_api.paths import FieldSegment

    from dal_obscura.application.governance.assets.schema import (
        _arrow_field_node,
        _SchemaResponseBudget,
    )

    schema = _nested_schema()
    for field, arrow_field in zip(schema.fields, schema.as_arrow(), strict=True):
        node = _arrow_field_node(
            arrow_field,
            (FieldSegment(field.name, field.field_id),),
            schema_scope_digest(schema.as_arrow()),
            _SchemaResponseBudget(),
        )
        assert node["type"] == str(arrow_field.type)


def test_get_asset_schema_returns_typed_nested_paths() -> None:
    asset_id = uuid4()
    store = _FakeStore(asset_id)
    catalog = _FakeCatalog(_FakeTable(_nested_schema()))
    calls: list[tuple[str, dict[str, Any]]] = []

    def load_catalog(name: str, **options: Any) -> _FakeCatalog:
        calls.append((name, options))
        return catalog

    result = discover_schema(
        store,  # type: ignore[arg-type]
        asset_id,
        ControlPlaneActor.for_platform_admin("admin"),
        plugin_registry=schema_registry(load_catalog),
    )

    assert calls == [("analytics", {"type": "sql", "uri": "sqlite:///catalog.db"})]
    assert catalog.loaded_identifier == "default.events"
    assert result["schema_version"] == 1
    assert result["supported_masks"] == ["null", "redact", "hash", "email", "keep_last", "default"]
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


def test_get_asset_schema_routes_admitted_catalog_and_format_plugins() -> None:  # noqa: C901
    asset_id = uuid4()
    schema = pa.schema(
        [
            pa.field(
                "profile",
                pa.struct([pa.field("email", pa.string()), pa.field("$value", pa.int64())]),
                metadata={b"iceberg.field.id": b"2"},
            )
        ]
    )
    closed: list[str] = []
    format_close_fails = False

    class PublicStore(_FakeStore):
        def get_workspace_asset(self, asset_id: UUID) -> dict[str, object]:
            assert asset_id == self.asset_id
            return {
                "id": str(self.asset_id),
                "revision": 0,
                "catalog": "analytics",
                "name": "default.events",
                "table_identifier": "default.events",
                "backend": "fixture.format",
            }

        def get_workspace_catalog(self, name: str) -> dict[str, object]:
            assert name == "analytics"
            return {
                "name": name,
                "id": "00000000-0000-4000-8000-000000000002",
                "plugin_id": "fixture.catalog",
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
            api_version="3",
            config_version=1,
            distribution="fixture",
            version="1.0.0",
            output_formats=frozenset({"fixture.format"}),
            handle_versions=frozenset({1}),
        )

        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return (("default",),)

        def resolve_table(self, value, context):
            del context
            assert value == identifier
            return handle

        def list_tables(self, context, **kwargs):
            raise AssertionError("Schema discovery must not list tables")

        def close(self):
            closed.append("catalog")

    class PublicFormat:
        descriptor = PluginDescriptor(
            kind="table_format",
            plugin_id="fixture.format",
            api_version="3",
            config_version=1,
            distribution="fixture",
            version="1.0.0",
            handle_versions=frozenset({1}),
        )

        def schema(self, context):
            del context
            return SchemaDescriptor(arrow_schema=schema)

        def plan(self, *args, **kwargs):
            raise AssertionError("Schema discovery must not plan rows")

        def execute(self, *args, **kwargs):
            raise AssertionError("Schema discovery must not read rows")

        def close(self):
            closed.append("format")
            if format_close_fails:
                raise RuntimeError("format close failed")

    class Registry(PluginRegistry):
        def admitted(self):
            return {
                ("catalog", "fixture.catalog"): PublicCatalog.descriptor,
                ("table_format", "fixture.format"): PublicFormat.descriptor,
            }

        def load(self, kind, plugin_id):
            if (kind, plugin_id) == ("catalog", "fixture.catalog"):
                return lambda config, context: PublicCatalog()
            if (kind, plugin_id) == ("table_format", "fixture.format"):
                return lambda value, context: PublicFormat()
            raise AssertionError((kind, plugin_id))

    result = discover_schema(
        PublicStore(asset_id),  # type: ignore[arg-type]
        asset_id,
        ControlPlaneActor.for_platform_admin("admin"),
        plugin_registry=Registry(),
    )

    assert result["catalog"] == "analytics"
    assert result["target"] == "default.events"
    assert result["stable_field_ids"] is False
    assert cast(list[dict[str, object]], result["fields"])[0]["name"] == "profile"
    fields = cast(list[dict[str, object]], result["fields"])
    children = cast(list[dict[str, object]], fields[0]["children"])
    assert children[1]["human_path"] == 'profile.["$value"]'
    path = cast(dict[str, object], children[1]["path"])
    assert cast(list[dict[str, object]], path["segments"])[1]["kind"] == "field"
    assert closed == ["format", "catalog"]

    format_close_fails = True
    with pytest.raises(ValidationFailure, match="Schema discovery failed"):
        discover_schema(
            PublicStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            plugin_registry=Registry(),
        )
    assert closed[-2:] == ["format", "catalog"]
    format_close_fails = False

    class ForgedHandleCatalog(PublicCatalog):
        def resolve_table(self, value, context):
            del context
            return replace(handle, catalog_revision=4)

    class ForgedHandleRegistry(Registry):
        def load(self, kind, plugin_id):
            if (kind, plugin_id) == ("catalog", "fixture.catalog"):
                return lambda config, context: ForgedHandleCatalog()
            return super().load(kind, plugin_id)

    with pytest.raises(ValidationFailure, match="Schema discovery failed"):
        discover_schema(
            PublicStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            plugin_registry=ForgedHandleRegistry(),
        )

    class ForgedFormat(PublicFormat):
        descriptor = PluginDescriptor(
            kind="table_format",
            plugin_id="other.format",
            api_version="3",
            config_version=1,
            distribution="fixture",
            version="1.0.0",
        )

    class ForgedRegistry(Registry):
        def load(self, kind, plugin_id):
            if (kind, plugin_id) == ("table_format", "fixture.format"):
                return lambda value, context: ForgedFormat()
            return super().load(kind, plugin_id)

    with pytest.raises(ValidationFailure, match="Schema discovery failed"):
        discover_schema(
            PublicStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            plugin_registry=ForgedRegistry(),
        )


def test_get_asset_schema_rejects_persisted_unknown_catalog_option_before_factory() -> None:
    asset_id = uuid4()
    store = _FakeStore(asset_id)
    cast(Any, store).get_workspace_catalog = lambda name: {
        "name": name,
        "id": "00000000-0000-4000-8000-000000000002",
        "plugin_id": "fixture.catalog",
        "revision": 1,
        "options": {"uri": "https://catalog.example", "debug": True},
    }
    descriptor = PluginDescriptor(
        kind="catalog",
        plugin_id="fixture.catalog",
        api_version="3",
        config_version=1,
        distribution="fixture",
        version="1.0.0",
        config_schema={"fields": [{"name": "uri", "required": True}]},
    )
    loaded = False

    class Registry(PluginRegistry):
        def admitted(self):
            return {("catalog", "fixture.catalog"): descriptor}

        def load(self, kind, plugin_id):
            nonlocal loaded
            loaded = True
            raise AssertionError("unknown options must fail before factory loading")

    with pytest.raises(ValidationFailure, match="unsupported fields"):
        discover_schema(
            store,  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            plugin_registry=Registry(),
        )
    assert loaded is False


def test_schema_loading_enforces_catalog_egress_before_provider_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asset_id = uuid4()
    store = _FakeStore(asset_id)
    monkeypatch.setattr(
        store,
        "get_workspace_catalog",
        lambda name: {
            "name": name,
            "id": "00000000-0000-4000-8000-000000000002",
            "plugin_id": "iceberg.catalog",
            "options": {"uri": "https://blocked.example/catalog"},
        },
    )
    called = False

    def load_catalog(name: str, **options: Any) -> _FakeCatalog:
        nonlocal called
        called = True
        raise AssertionError("provider must not be called for a denied host")

    with pytest.raises(ValidationFailure, match="egress allowlist"):
        discover_schema(
            store,  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            plugin_registry=schema_registry(load_catalog),
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
        lambda name: {
            "name": name,
            "id": "00000000-0000-4000-8000-000000000002",
            "plugin_id": "iceberg.catalog",
            "options": {
                "uri": "https://catalog.example/api",
                "token": {"secret": "CATALOG_TOKEN", "scope": "catalog:analytics"},
            },
        },
    )
    received: dict[str, Any] = {}

    def load_catalog(name: str, **options: Any) -> _FakeCatalog:
        received.update(options)
        return _FakeCatalog(_FakeTable(_nested_schema()))

    discover_schema(
        store,  # type: ignore[arg-type]
        asset_id,
        ControlPlaneActor.for_platform_admin("admin"),
        plugin_registry=schema_registry(load_catalog),
        egress_allowlist=("catalog.example",),
        secret_provider=load_secret_provider(
            SecretProviderConfig(config={"scope_grants": {"catalog:analytics": ["CATALOG_TOKEN"]}})
        ),
    )

    assert received["token"] == "sentinel-secret"


def test_get_asset_schema_requires_read_capability() -> None:
    asset_id = uuid4()
    with pytest.raises(AuthorizationFailure):
        discover_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor("outsider", ()),
            plugin_registry=schema_registry(
                lambda *args, **kwargs: pytest.fail("catalog must not be loaded")
            ),
        )


def test_get_asset_schema_redacts_catalog_provider_errors() -> None:
    asset_id = uuid4()

    def failing_catalog(**_: object):
        raise ValueError("failed https://catalog-user:catalog-password@catalog.example")

    with pytest.raises(ValidationFailure, match="Schema discovery failed") as failure:
        discover_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            plugin_registry=schema_registry(failing_catalog),
        )

    assert "catalog-password" not in str(failure.value)


def test_schema_plugin_cleanup_does_not_mask_primary_error() -> None:
    class FailingClose:
        def close(self) -> None:
            raise RuntimeError("cleanup failed")

    with pytest.raises(ValueError, match="primary failure"):
        try:
            raise ValueError("primary failure")
        except ValueError:
            close_preserving_error(FailingClose())
            raise
