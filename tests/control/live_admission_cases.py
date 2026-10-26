"""SDK providers for exercising published snapshots through public read admission."""

from contextlib import contextmanager
from dataclasses import replace

import pyarrow as pa
from dal_obscura_plugin_api import DiscoveryPage, PluginDescriptor, SchemaDescriptor, TableHandle
from dal_obscura_plugin_api.options import PluginOptions
from dal_obscura_plugin_api.paths import FieldSegment, ListElementSegment, MapKeySegment
from dal_obscura_plugin_api.schema.identity import schema_field_id, schema_scope_digest
from dal_obscura_plugin_api.schema.index import walk_schema_fields

from dal_obscura.adapters.providers.published import LiveConfigCatalogRegistry
from dal_obscura.kernel.plans.snapshot import PublishedSource


class AdmissionPlugins:
    options = PluginOptions({})

    def __init__(self, *keys):
        self._descriptors = {
            (kind, plugin_id): PluginDescriptor(
                kind=kind,
                plugin_id=plugin_id,
                api_version="3",
                config_version=1,
                distribution="fixture",
                version="1.0",
                output_formats=frozenset(
                    name for entry_kind, name in keys if entry_kind == "table_format"
                )
                if kind == "catalog"
                else frozenset(),
            )
            for kind, plugin_id in keys
        }
        self.schema = pa.schema([])
        self.configs = []
        self.location = None

    def admitted(self):
        return dict(self._descriptors)

    def format_options(self, plugin_id, *, captured=None):
        return self.options.for_plugin(plugin_id, captured)

    def load(self, kind, plugin_id):
        registry = self
        backend = next(name for kind, name in self._descriptors if kind == "table_format")
        descriptor = self._descriptors[(kind, plugin_id)]
        if kind == "catalog":

            class Catalog:
                def __init__(self, config, context):
                    self.config = config
                    self.descriptor = descriptor
                    registry.configs.append(config)

                def list_namespaces(self, context, *, namespace=()):
                    return ()

                def list_tables(self, context, *, continuation=None, limit=500):
                    return DiscoveryPage(())

                def resolve_table(self, identifier, context):
                    return TableHandle(
                        catalog_plugin_id=self.config.plugin_id,
                        catalog_instance_id=self.config.instance_id,
                        catalog_revision=self.config.revision,
                        identifier=identifier,
                        format_plugin_id=backend,
                        handle_version=1,
                        metadata={}
                        if registry.location is None
                        else {"location": registry.location},
                    )

                def close(self):
                    pass

            return Catalog

        class Format:
            def __init__(self, handle, context):
                self.descriptor = descriptor

            def schema(self, context):
                return SchemaDescriptor(arrow_schema=registry.schema)

            def plan(self, *args):
                raise AssertionError("Admission must not scan data")

            def execute(self, *args):
                raise AssertionError("Admission must not scan data")

            def close(self):
                pass

        return Format


@contextmanager
def snapshot_context(asset, schema, *, plugin_registry, path_enforcer=None):
    class SnapshotReader:
        def get_asset_and_catalog(self, catalog, target):
            return asset

    store = SnapshotReader()
    if plugin_registry is not None:
        plugin_registry.schema = schema
    registry = LiveConfigCatalogRegistry(
        store, plugin_registry=plugin_registry, path_enforcer=path_enforcer
    )
    try:
        with registry.open(asset.catalog.name, asset.target) as context:
            yield context
    finally:
        registry.close()


def capture_catalog(asset, *, plugin_registry=None, path_enforcer=None, location=None):
    if plugin_registry is not None:
        plugin_registry.location = location
    with snapshot_context(
        asset, pa.schema([]), plugin_registry=plugin_registry, path_enforcer=path_enforcer
    ):
        assert plugin_registry is not None
        return plugin_registry.configs[-1]


def admit_snapshot(asset, schema):
    # Schema cases use a fixed provider binding; their varied admission/policy values stay exact.
    asset = replace(
        asset,
        catalog=replace(asset.catalog, plugin_id="fixture.catalog"),
        source=PublishedSource(
            "fixture.format", asset.source.table or asset.target, asset.source.options
        ),
    )
    plugins = AdmissionPlugins(("catalog", "fixture.catalog"), ("table_format", "fixture.format"))
    with snapshot_context(asset, schema, plugin_registry=plugins):
        pass


def admission_identities(schema):
    """Build persisted approval fixtures using the public SDK field vocabulary."""
    scope = schema_scope_digest(schema)
    result = {}
    for field, typed_path in walk_schema_fields(schema):
        path = tuple(
            segment.name
            if isinstance(segment, FieldSegment)
            else "$element"
            if isinstance(segment, ListElementSegment)
            else "$key"
            if isinstance(segment, MapKeySegment)
            else "$value"
            for segment in typed_path.segments
        )
        identity, _ = schema_field_id(field, path, scope_digest=scope)
        result[(path, identity)] = str(field.type)
    return result
