from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from typing import Any, cast
from uuid import UUID, uuid4

import pyarrow as pa
import pytest
from dal_obscura_plugin_api import PluginDescriptor, SchemaDescriptor, TableHandle, TableIdentifier
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, ListType, MapType, NestedField, StringType, StructType
from sqlalchemy.orm import Session

from dal_obscura.control.access import ControlPlaneActor
from dal_obscura.control.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.control.schema_service import (
    capture_asset_schema,
    get_asset_schema,
    schema_fingerprint,
)
from dal_obscura.policy.schema_bounds import MAX_SCHEMA_DEPTH, MAX_SCHEMA_NODES
from dal_obscura.policy.schema_identity import numeric_field_id, schema_scope_digest
from dal_obscura.sources.plugin_runtime import _close_plugin_preserving_error
from dal_obscura.sources.plugins import PluginRegistry
from dal_obscura.sources.secrets import EnvSecretProvider
from tests.support.schema_catalog import schema_registry


def discover_schema(
    store: Session, asset_id: UUID, actor: ControlPlaneActor, **options: Any
) -> dict[str, object]:
    captured = capture_asset_schema(
        store,
        asset_id,
        actor,
        egress_allowlist=options.get("egress_allowlist", ()),
        plugin_registry=options.get("plugin_registry"),
    )
    return get_asset_schema(captured, **options)


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
            "id": str(asset_id),
            "revision": 0,
            "backend": "iceberg",
            "catalog": "analytics",
            "name": "default.events",
            "table_identifier": "default.events",
        }

    def get_workspace(self) -> object:
        return object()

    def get_workspace_catalog(self, name: str) -> dict[str, object]:
        assert name == "analytics"
        return {
            "name": "analytics",
            "id": "00000000-0000-4000-8000-000000000002",
            "plugin_id": "iceberg.catalog",
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
                        element_id=4, element_type=StringType(), element_required=False
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


def test_discovered_nested_types_match_data_plane_schema_admission() -> None:
    from dal_obscura.control.schema_service import _arrow_field_node, _SchemaResponseBudget
    from dal_obscura.policy.paths import FieldSegment

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


def test_arrow_synthetic_field_ids_are_schema_scoped() -> None:
    integer_schema = pa.schema([pa.field("value", pa.int64())])
    string_schema = pa.schema([pa.field("value", pa.string())])

    integer_id = numeric_field_id(
        integer_schema.field("value"), ("value",), schema_scope_digest(integer_schema)
    )
    string_id = numeric_field_id(
        string_schema.field("value"), ("value",), schema_scope_digest(string_schema)
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
            api_version="2",
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
            api_version="2",
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
            return {("catalog", "fixture.catalog"): PublicCatalog.descriptor}

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
            api_version="2",
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
        api_version="2",
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
        secret_provider=EnvSecretProvider(
            config={"scope_grants": {"catalog:analytics": ["CATALOG_TOKEN"]}}
        ),
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


@pytest.mark.parametrize("location", ["schema", "field", "list-element", "map-value"])
@pytest.mark.parametrize("part", ["key", "value"])
def test_schema_identity_distinguishes_binary_metadata(location: str, part: str) -> None:
    def schema(raw: bytes) -> pa.Schema:
        metadata = {raw: b"value"} if part == "key" else {b"key": raw}
        leaf = pa.field("leaf", pa.string(), metadata=metadata)
        field = (
            pa.field("items", pa.list_(leaf))
            if location == "list-element"
            else pa.field("labels", pa.map_(pa.string(), leaf))
            if location == "map-value"
            else leaf
        )
        return (
            pa.schema([pa.field("leaf", pa.string())], metadata=metadata)
            if location == "schema"
            else pa.schema([field])
        )

    binary = schema(b"\xff")
    literal_escape = schema(b"\\xff")

    assert schema_fingerprint(binary) != schema_fingerprint(literal_escape)
    assert schema_scope_digest(binary) != schema_scope_digest(literal_escape)


def test_schema_identity_ignores_metadata_insertion_order() -> None:
    def schema(metadata: dict[bytes, bytes]) -> pa.Schema:
        return pa.schema([pa.field("value", pa.string(), metadata=metadata)], metadata=metadata)

    forward = schema({b"\xff": b"\x00", b"text": b"\\xff"})
    reverse = schema({b"text": b"\\xff", b"\xff": b"\x00"})
    assert schema_fingerprint(forward) == schema_fingerprint(reverse)
    assert schema_scope_digest(forward) == schema_scope_digest(reverse)


@pytest.mark.parametrize("name", ["界", "x" * 4095 + '\\"界', "🙂" * 4097])
def test_schema_identity_matches_independent_version_two_encoding(name: str) -> None:
    schema = pa.schema(
        [pa.field(name, pa.list_(pa.field("item", pa.string(), metadata={b"k": b"\xff"})))],
        metadata={b"s": b"v"},
    )
    expected = {
        "encoding": 2,
        "fields": [
            {
                "name": name,
                "nullable": True,
                "metadata": [],
                "type": {
                    "id": "list<item: string>",
                    "value_field": {
                        "name": "item",
                        "nullable": True,
                        "metadata": [["aw==", "/w=="]],
                        "type": {"id": "string"},
                    },
                },
            }
        ],
        "metadata": [["cw==", "dg=="]],
    }
    for digest, ensure_ascii in [(schema_fingerprint, False), (schema_scope_digest, True)]:
        encoded = json.dumps(
            expected, sort_keys=True, separators=(",", ":"), ensure_ascii=ensure_ascii
        ).encode("utf-8")
        assert digest(schema) == hashlib.sha256(encoded).hexdigest()


def test_schema_scope_rejects_json_expansion_within_arrow_ipc_budget() -> None:
    schema = pa.schema([pa.field("界" * 400_000, pa.string())])
    # UTF-8 Arrow IPC fits; the existing ASCII scope encoding expands each name
    # character to six bytes. Authoring fingerprints use UTF-8 and still fit.
    assert len(schema_fingerprint(schema)) == 64
    with pytest.raises(ValueError, match="Schema encoding exceeds"):
        schema_scope_digest(schema)


@pytest.mark.parametrize("digest", [schema_fingerprint, schema_scope_digest])
def test_schema_identity_rejects_base64_expansion_within_arrow_ipc_budget(digest) -> None:
    schema = pa.schema([pa.field("value", pa.string())], metadata={b"key": b"x" * 1_600_000})
    with pytest.raises((ValidationFailure, ValueError), match="Schema encoding exceeds"):
        digest(schema)


def test_get_asset_schema_reports_scope_encoding_budget(monkeypatch) -> None:
    schema = pa.schema([pa.field("界" * 400_000, pa.string())])
    monkeypatch.setattr(
        "dal_obscura.control.schema_service.load_asset_schema", lambda *args, **kwargs: schema
    )
    asset_id = uuid4()
    with pytest.raises(ValidationFailure, match="Schema encoding exceeds"):
        discover_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
        )


def test_get_asset_schema_rejects_path_amplification_before_building_all_nodes(monkeypatch) -> None:
    from dal_obscura.control import schema_service

    schema = pa.schema(
        [
            pa.field(
                "ancestor_" + "x" * 100_000,
                pa.struct([pa.field(f"child_{index}", pa.string()) for index in range(100)]),
            )
        ]
    )
    monkeypatch.setattr(schema_service, "load_asset_schema", lambda *args, **kwargs: schema)
    visited = []
    original = schema_service.numeric_field_id

    def record_field(field, names, scope):
        visited.append(field.name)
        return original(field, names, scope)

    monkeypatch.setattr(schema_service, "numeric_field_id", record_field)
    asset_id = uuid4()
    with pytest.raises(ValidationFailure, match="Schema response exceeds the 8388608-byte budget"):
        discover_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
        )
    assert 1 < len(visited) < 101


def test_get_asset_schema_preserves_long_names_within_response_budget(monkeypatch) -> None:
    from dal_obscura.control import schema_service
    from dal_obscura.interfaces.http.routes.schemas import AssetSchemaResponse

    name = '界."' * 4096
    schema = pa.schema([pa.field(name, pa.struct([pa.field("leaf", pa.string())]))])
    monkeypatch.setattr(schema_service, "load_asset_schema", lambda *args, **kwargs: schema)
    asset_id = uuid4()
    result = discover_schema(
        _FakeStore(asset_id),  # type: ignore[arg-type]
        asset_id,
        ControlPlaneActor.for_platform_admin("admin"),
    )
    encoded = AssetSchemaResponse.model_validate(result).model_dump_json().encode("utf-8")
    assert len(encoded) <= 8 * 1024 * 1024
    node = json.loads(encoded)["fields"][0]
    assert node["name"] == name
    assert node["children"][0]["path"]["segments"][0]["name"] == name
    assert node["children"][0]["human_path"] == f"[{json.dumps(name, ensure_ascii=False)}].leaf"


def test_schema_response_budget_matches_serialized_http_contract(monkeypatch) -> None:
    from dal_obscura.control import schema_service
    from dal_obscura.interfaces.http.routes.schemas import AssetSchemaResponse

    schema = _nested_schema().as_arrow()
    monkeypatch.setattr(schema_service, "load_asset_schema", lambda *args, **kwargs: schema)
    asset_id = uuid4()

    def discover():
        return discover_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
        )

    expected_bytes = len(AssetSchemaResponse.model_validate(discover()).model_dump_json().encode())
    budget = schema_service._SchemaResponseBudget(remaining=expected_bytes)
    monkeypatch.setattr(schema_service, "_SchemaResponseBudget", lambda: budget)
    assert (
        len(AssetSchemaResponse.model_validate(discover()).model_dump_json().encode())
        == expected_bytes
    )
    assert budget.remaining == 0
    budget.remaining = expected_bytes - 1
    with pytest.raises(ValidationFailure, match="Schema response exceeds"):
        discover()


@pytest.mark.parametrize(
    ("original", "changed"),
    [
        pytest.param(pa.list_(pa.string()), pa.large_list(pa.string()), id="list-width"),
        pytest.param(pa.list_(pa.string(), 2), pa.list_(pa.string(), 3), id="fixed-list-size"),
        pytest.param(
            pa.map_(pa.string(), pa.int32()),
            pa.map_(pa.string(), pa.int32(), keys_sorted=True),
            id="map-key-order",
        ),
        pytest.param(
            pa.list_(pa.field("item", pa.string(), nullable=True)),
            pa.list_(pa.field("item", pa.string(), nullable=False)),
            id="child-nullability",
        ),
        pytest.param(pa.timestamp("us", "UTC"), pa.timestamp("us", "Europe/Paris"), id="timezone"),
        pytest.param(pa.decimal128(10, 2), pa.decimal128(10, 3), id="decimal-scale"),
        pytest.param(
            pa.struct([pa.field("first", pa.string()), pa.field("second", pa.string())]),
            pa.struct([pa.field("second", pa.string()), pa.field("first", pa.string())]),
            id="child-order",
        ),
    ],
)
def test_schema_identity_preserves_type_semantics(
    original: pa.DataType, changed: pa.DataType
) -> None:
    before = pa.schema([pa.field("value", original)])
    after = pa.schema([pa.field("value", changed)])
    assert schema_fingerprint(before) != schema_fingerprint(after)
    assert schema_scope_digest(before) != schema_scope_digest(after)


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
            _close_plugin_preserving_error(FailingClose())
            raise


def test_get_asset_schema_rejects_excessive_node_count() -> None:
    asset_id = uuid4()
    schema = Schema(
        *(
            NestedField(field_id=index, name=f"field_{index}", field_type=StringType())
            for index in range(1, MAX_SCHEMA_NODES + 2)
        )
    )

    with pytest.raises(ValidationFailure, match="Schema discovery failed") as caught:
        discover_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            plugin_registry=schema_registry(lambda *_args, **_: _FakeCatalog(_FakeTable(schema))),
        )
    assert "node" in str(caught.value.__cause__)


def test_get_asset_schema_rejects_excessive_nesting_depth() -> None:
    asset_id = uuid4()
    nested: object = StringType()
    for index in range(MAX_SCHEMA_DEPTH + 1, 0, -1):
        nested = StructType(NestedField(field_id=index, name=f"level_{index}", field_type=nested))
    schema = Schema(NestedField(field_id=MAX_SCHEMA_DEPTH + 2, name="root", field_type=nested))

    with pytest.raises(ValidationFailure, match="Schema discovery failed") as caught:
        discover_schema(
            _FakeStore(asset_id),  # type: ignore[arg-type]
            asset_id,
            ControlPlaneActor.for_platform_admin("admin"),
            plugin_registry=schema_registry(lambda *_args, **_: _FakeCatalog(_FakeTable(schema))),
        )
    assert "depth" in str(caught.value.__cause__)


@pytest.fixture(autouse=True)
def query_boundary(monkeypatch):
    from tests.support.storage_queries import install_query_doubles

    install_query_doubles(
        monkeypatch,
        [
            "get_workspace",
            "get_workspace_asset",
            "get_workspace_catalog",
            "list_asset_owners",
            "list_asset_grants",
        ],
    )
