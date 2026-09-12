# Dal Obscura plugin conformance kit

This package contains public contract checks for catalog and table-format plugin
distributions. It imports only `dal-obscura-plugin-api` and PyArrow; external
adapters do not need the service or its private modules.

Run the local fixture lane from the repository checkout with:

```sh
PYTHONPATH=packages/plugin-conformance/src:packages/plugin-api/src \
  uv run --no-sync pytest packages/plugin-conformance/tests -q
```

`run_format_checks` returns a machine-readable `ConformanceResult` containing
package/plugin identity, Arrow version, pass/fail checks, failures, skips, and a
JSON representation suitable for CI artifacts. The fixture includes nested
struct, list, and map values. The kit validates declared capabilities, bounded
task counts, schema descriptors, and output batch schemas; it does not pretend
to sandbox in-process Python plugins.

