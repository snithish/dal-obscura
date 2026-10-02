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
correlation IDs and capability names are bounded, and cancellation hooks must be
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

Return zero tasks for an empty scan, or at most `max_tasks` inert tasks covering
all work exactly once. Group row groups/files when the work exceeds that budget;
preserve parallel tasks when splittable work exists. Core does not invent a task
for an empty plan. `execute` returns the projected schema and a lazy iterable of
matching Arrow record batches. Resource ownership lasts until exhaustion, error,
or early close; close readers in iterator `finally` blocks. Core opens execution
plugins only when iteration begins, validates each batch, and closes them when
iteration ends. Check deadlines/cancellation before costly reads and after them.
A schema change between authorization and planning is rejected before tickets
are created.
When a catalog pins `TableHandle.snapshot_id`, schema descriptors must report that
same snapshot. Core rejects a missing or different snapshot before planning.

## Admission and contract compatibility

Release 0.2 uses plugin API 2. Entry-point groups are
`dal_obscura.catalogs.v2` and `dal_obscura.table_formats.v2`. Build plugins against
`dal-obscura-plugin-api==0.2.0`, run the conformance kit, and regenerate the exact
artifact lock. API 1 plugins and executable task encodings are not supported.
Configuration migration preserves catalogs, policies, ownership and audit while
invalidating old tickets; see the [cutover runbook](../../docs/core-cutover.md).

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
and each yielded batch. Tasks must round-trip through bounded passive JSON: null, booleans, finite numbers,
strings, lists and string-keyed mappings. Live resources, callbacks, arbitrary
classes and executable serialization are rejected. Immutable mappings are accepted
and encoded as JSON objects. Conformance executes JSON-round-tripped tasks. No
production scan task uses pickle. Path allowlists, authentication, and key
rotation remain operational security features, not version-compatibility shims.
