# Dal Obscura manifest/Parquet plugin

This is an independently buildable proof distribution for the public
`dal-obscura-plugin-api`. It registers a `manifest` catalog and a
`parquet.dataset` table format without importing the Dal Obscura service.

The catalog accepts only an operator supplied JSON manifest and a configured
filesystem root. Each table entry pins a revision, Arrow schema IPC payload,
top-level field IDs, and an immutable list of files. Every member is resolved
under the root with symlink-aware checks. The Parquet format validates each file
against the pinned schema and produces one bounded task per row group; it rejects
row-filter pushdown, unsupported tasks, changed membership, and schema drift.

Run its local fixture lane from the repository checkout:

```sh
PYTHONPATH=packages/manifest-parquet-plugin/src:packages/plugin-api/src \
  uv run --no-sync pytest packages/manifest-parquet-plugin/tests -q
```

The package is a qualification fixture, not a hostile-code sandbox. Imported
Python plugins remain trusted operator code and must be admitted by the registry
lock before use.
