# Manifest/Parquet plugin

This independent API 2 package registers `manifest` and `parquet.dataset` without
importing service internals. Arrow owns filesystem and Parquet reads.

Catalog options are `root` and `manifest_path`, using local paths/file URIs or
AWS S3 URIs. Storage uses the worker AWS credential chain. Operator startup
`AWS_REGION` and `AWS_ENDPOINT_URL_S3` configure private/test endpoints; handles
and tickets contain no storage credentials.

A manifest contains `revision` and a `tables` object. Each entry contains only
`namespace` (array), `name`, `files` (root-relative paths), and `schema_ipc`
(base64 Arrow schema). Object keys are labels; structured namespace/name define
identity. Core derives schema identities from Arrow metadata; no duplicate
field-ID list is supplied. The manifest and schema are pinned in the handle.

Members remain under the admitted root; local symlinks are rejected. Each file
must match the pinned schema. Row groups are balanced into at most the requested
number of independently executable tasks, streaming bounded Arrow batches.
Cancellation, changed membership, schema drift and unsupported tasks fail closed.
Plain Parquet has immutable-file semantics: replacement files require a new
manifest revision. It does not interpret table-format delete logs.

```bash
uv run --no-sync pytest packages/manifest-parquet-plugin/tests
uv run --no-sync pytest tests/plugin_platform/test_cloud_formats.py
```

See the [SDK](../plugin-api/README.md) and
[conformance kit](../plugin-conformance/README.md). Imported plugins are trusted
operator code; artifact locks are admission controls, not a hostile-code sandbox.
