"""Schema-only catalog double shared by HTTP evaluation scenarios."""

from dal_obscura_plugin_api import DiscoveryPage, SchemaDescriptor, TableHandle
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType


def schema_registry(load_catalog):
    """Replace catalog/format I/O while retaining the production SDK lifecycle."""
    from dal_obscura.sources.sql_catalog import DESCRIPTOR

    resolved = {}

    class Registry:
        def admitted(self):
            return {("catalog", DESCRIPTOR.plugin_id): DESCRIPTOR}

        def load(self, kind, plugin_id):
            return {
                ("catalog", "iceberg.sql"): lambda config, context: _SchemaCatalog(
                    config, context, load_catalog, resolved
                ),
                ("table_format", "iceberg"): lambda handle, context: _SchemaFormat(
                    handle, context, resolved
                ),
            }[(kind, plugin_id)]

    return Registry()


class _SchemaCatalog:
    from dal_obscura.sources.sql_catalog import DESCRIPTOR as descriptor

    def __init__(self, config, context, load_catalog, resolved):
        self.config = config
        self.resolved = resolved
        self.provider = load_catalog(config.instance_id, **dict(config.options))

    def list_namespaces(self, context, *, namespace=()):
        return (("default",),)

    def list_tables(self, context, *, continuation=None, limit=500):
        return DiscoveryPage(())

    def resolve_table(self, identifier, context):
        target = ".".join((*identifier.namespace, identifier.name))
        self.resolved[target] = self.provider.load_table(target)
        return TableHandle(
            catalog_plugin_id=self.descriptor.plugin_id,
            catalog_instance_id=self.config.instance_id,
            catalog_revision=self.config.revision,
            identifier=identifier,
            format_plugin_id="iceberg",
            handle_version=1,
        )

    def close(self):
        close = getattr(self.provider, "close", None)
        if callable(close):
            close()


class _SchemaFormat:
    from dal_obscura.sources.iceberg_plugin import IcebergFormatPlugin

    descriptor = IcebergFormatPlugin.descriptor

    def __init__(self, handle, context, resolved):
        self.handle = handle
        self.resolved = resolved

    def schema(self, context):
        schema = self.resolved[
            ".".join((*self.handle.identifier.namespace, self.handle.identifier.name))
        ].schema()
        return SchemaDescriptor(arrow_schema=schema.as_arrow())

    def plan(self, *args, **kwargs):
        raise AssertionError("Schema discovery must not plan a scan")

    def execute(self, *args, **kwargs):
        raise AssertionError("Schema discovery must not read rows")

    def close(self):
        pass


class EvaluationTable:
    def schema(self) -> Schema:
        return Schema(
            NestedField(field_id=1, name="id", field_type=LongType()),
            NestedField(field_id=2, name="email", field_type=StringType()),
            NestedField(field_id=3, name="region", field_type=StringType()),
        )


class EvaluationCatalog:
    def load_table(self, identifier: str) -> EvaluationTable:
        assert identifier == "prod.users"
        return EvaluationTable()
