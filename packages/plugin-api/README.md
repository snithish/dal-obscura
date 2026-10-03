# Dal Obscura plugin API

This directory is an independently buildable wheel for catalog and table-format
adapters. It contains only versioned contracts and depends on PyArrow, never on
the service package or its private modules.

Build a release artifact from this directory with:

```sh
uv build --wheel --out-dir dist
```

All public adapters target this package and declare an explicit entry point
in the admitted plugin lock. Service-side code under
`src/dal_obscura/sources/plugins` owns admission and lifecycle management; it
does not define a second set of SDK contracts.

`ExecutionContext` values are request scoped: deadlines must be timezone-aware,
correlation IDs are bounded, and cancellation hooks must be
callable. Core creates these contexts for each operation; plugins must not persist
them or place live callbacks/connections in serialized task payloads.

The catalog resolves structured `TableIdentifier` values without flattening or
splitting literal names. Continuation tokens are opaque. Core renders targets with
canonical quoted segments when necessary, so `a.b.c`, `["a.b"].c`, and
`a.["b.c"]` remain distinct identities.

`TableFormatPlugin.plan` receives exact top-level Arrow field names in projection
order, including fields needed by row filters. Dots and `*` are literal names here;
core expands client wildcards and nested paths before calling the plugin. Return
those fields with their complete nested types. Core prunes nested output and
applies masks after enforcing the full policy/caller row filter. `row_filter` is
an optional DuckDB SQL optimization hint sent only to plugins declaring
`filter_pushdown`; it never transfers enforcement responsibility to the backend.

Return zero tasks for an empty scan, or at most `request.max_tasks` immutable tasks covering
all work exactly once. Group row groups/files when the work exceeds that budget;
preserve parallel tasks when splittable work exists. Core does not invent a task
for an empty plan. `execute` returns the projected schema and a lazy iterable of
matching Arrow record batches. Resource ownership lasts until exhaustion, error,
or early close; close readers in iterator `finally` blocks. Core opens execution
plugins only when iteration begins, validates each batch, and closes them when
iteration ends. Call `context.check_active()` before and after costly reads and before requesting
more lazy work. It raises `TimeoutError` or `InterruptedError`.
A schema change between authorization and planning is rejected before tickets
are created.
When a catalog pins `TableHandle.snapshot_id`, schema descriptors must report that
same snapshot. Core rejects a missing or different snapshot before planning.

## One bound handle, one scan request

Catalog factories accept `(CatalogConfig, ExecutionContext)` and validate their
configuration during construction. Catalog instances expose `list_namespaces`,
`list_tables`, `resolve_table`, and `close`; there is no second validation hook.

Format factories accept `(TableHandle, ExecutionContext)`. The handle is bound
once, and every later operation uses that binding:

```python
import pyarrow as pa
from dal_obscura_plugin_api import ScanRequest, ScanTask

format_plugin = format_factory(handle, context)
try:
    descriptor = format_plugin.schema(context)
    full = descriptor.arrow_schema
    projected = pa.schema([full.field(name) for name in ("id", "profile")], metadata=full.metadata)
    request = ScanRequest(schema=projected, max_tasks=8)
    for task in format_plugin.plan(request, context):
        assert isinstance(task, ScanTask)
        output_schema, batches = format_plugin.execute(task, context)
        # Consume lazily, honoring cancellation and closing on early stop.
finally:
    format_plugin.close()
```

`ScanRequest.schema` is the exact projected Arrow schema, including order,
nested types, nullability and metadata; `request.columns` contains literal names.
`SchemaDescriptor` contains the Arrow schema, optional pinned snapshot and stable
field-ID claim. Core derives schema fingerprints itself. Descriptors and catalog
options are recursively immutable; `PluginDescriptor.to_json()` returns detached
metadata for admission and UI tooling.

Planning may return a list or a lazy iterator of `ScanTask`. Core and conformance
close both the iterator and iterable on completion, failure or budget rejection.
Plugins own the readers and connections they create and must close them on error,
exhaustion, or early close. No handle replay, untyped task sentinel, fingerprint
claim, schema-version counter, capability negotiation or compatibility wrapper is
part of this contract.

## Admission and contract compatibility

Release 0.2 uses plugin API 2. Entry-point groups are
`dal_obscura.catalogs.v2` and `dal_obscura.table_formats.v2`. Build plugins against
`dal-obscura-plugin-api==0.2.0`, run the conformance kit, and regenerate the exact
artifact lock. API 1 plugins and executable task encodings are not supported.
Initialize a fresh database; older configuration schemas and tickets are unsupported.
See the [deployment guide](../../docs/core-cutover.md).

The public SDK is the sole plugin contract. `PLUGIN_API_VERSION` defines the
current API version. Admission checks the current API and
configuration version, distribution, release, descriptor digest, and artifact
digest. Unknown versions and capabilities fail closed; missing descriptors are
errors. A self-consistent lock cannot override the supported-version checks.

Catalog descriptors declare their output format IDs. Both catalog and format
descriptors declare supported handle versions; resolved handles must match the
admitted catalog identity, format identity, and handle version. Schema and
execution use the registered format factory, including Iceberg resolved by an
external catalog. The native Iceberg engine remains an implementation detail.

SDK execution may return an iterable of Arrow batches so streaming never needs
to materialize the full result. Core validates schemas, bounded tasks, deadlines,
and each yielded batch. Tasks use `ScanTask`, whose root is a JSON object. Construction validates bounds
and recursively detaches/freezes input. Read its `payload` without mutation;
`to_json()` produces detached JSON and `from_json()` restores immutable work.
Values may contain null, booleans, signed/unsigned 64-bit integers, finite floats,
strings, arrays, and string-keyed objects. Live resources, callbacks, arbitrary
classes and executable serialization are rejected. Conformance executes tasks
after a real JSON round trip. No
production scan task uses pickle. Path allowlists, authentication, and key
rotation remain operational security features, not version-compatibility shims.

## Storage locations

`dal_obscura_plugin_api.storage.StorageRoot` provides contained local/file-URI or
AWS S3 roots using maintained Arrow filesystems. It owns path admission and bounded
metadata reads, not table-format semantics. The runtime filesystem stays inside
the opened plugin; handles/tasks carry locations only. Credentials come from the
worker AWS chain; endpoint and region are operator startup configuration.
See [cloud execution](../../docs/cloud-formats.md) for backend limits.
