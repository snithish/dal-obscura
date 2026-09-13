# Dal Obscura plugin API

This directory is an independently buildable wheel for catalog and table-format
adapters. It contains only versioned contracts and depends on PyArrow, never on
the service package or its private modules.

Build a release artifact from this directory with:

```sh
uv build --wheel --out-dir dist
```

The service-side compatibility contracts under
`src/dal_obscura/common/plugin_api` remain until the migration is complete. New
external adapters should target this package and declare an explicit entry point
in the admitted plugin lock.

`ExecutionContext` values are request scoped: deadlines must be timezone-aware,
correlation IDs and capability names are bounded, and cancellation hooks must be
callable. Core creates these contexts for each operation; plugins must not persist
them or place live callbacks/connections in serialized task payloads.
