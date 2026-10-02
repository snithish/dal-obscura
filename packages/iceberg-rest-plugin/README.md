# Iceberg REST catalog plugin

This independently buildable wheel exposes the admitted `iceberg.rest` catalog
factory through the public plugin API. It uses PyIceberg's REST catalog client,
rejects credential-bearing or query-bearing endpoints, bounds table discovery,
and returns immutable metadata locations for the governed Iceberg executor.

The package does not implement a second scan serializer; the core Iceberg format
adapter remains the only trusted execution path. Run package tests with:

```bash
PYTHONPATH=packages/iceberg-rest-plugin/src:packages/plugin-api/src \
  uv run --no-sync pytest packages/iceberg-rest-plugin/tests -q
```


The rebuilt 0.2.0 adapter targets plugin API 2. Configuration is validated on
construction; deadlines and cancellation use `context.check_active()`. Formats
bind their handle in the factory and accept an exact `ScanRequest` schema,
returning immutable `ScanTask` JSON objects. See the
[SDK contract](../plugin-api/README.md) and [conformance kit](../plugin-conformance/README.md).
