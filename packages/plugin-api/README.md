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
`src/dal_obscura/common/plugin_api` owns admission and lifecycle management; it
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
