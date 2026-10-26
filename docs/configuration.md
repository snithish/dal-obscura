# Configuration

Startup environment selects process settings, plugin admission and the secret
backend. Published database records select catalogs, policies, admitted schemas
and identity mapping. Keep deployment settings separate from governed records.
The current Flight settings and validation live in
[`runtime/configuration.py`](../src/dal_obscura/runtime/configuration.py). HTTP
startup settings live in [`bootstrap/control_config.py`](../src/dal_obscura/bootstrap/control_config.py).

## Plugin admission and arguments

Set `DAL_OBSCURA_PLUGIN_LOCK_FILE` to an operator-generated lock of installed
wheel artifacts. The lock identifies distribution, version, SDK API, descriptor
and installed artifact. Local composition can construct locks for bundled wheels;
external plugins always need explicit admission. See [qualification](testing.md).

Catalog writes, persisted configuration capture and asset bindings require an
admitted startup registry for bundled and external plugins alike. The Iceberg
catalog exposes common typed fields and allows additional PyIceberg properties.
Its catalog type can be inferred by PyIceberg when omitted. Providers validate
backend properties at construction. Connection edits preserve additional properties
for descriptors that allow them.

Set `DAL_OBSCURA_CATALOG_EGRESS_ALLOWLIST` to the same comma-separated hosts in
both processes. Production requires an explicit list. The host validates merged
catalog options before resolving references, then checks declared URI fields
against endpoint policy before provider construction or cache lookup. A granted
URI reference can contain credentials; credential fields remain opaque values.
Declare endpoint fields as URI fields so the host can distinguish them from
credentials. Additional provider properties remain the provider's responsibility.

`DAL_OBSCURA_PLUGIN_OPTIONS` is a JSON object of namespaced global arguments:

```json
{"dal.plugins.iceberg.catalog.warehouse":"s3://warehouse"}
```

The longest admitted plugin ID selects the namespace. Unknown namespaces fail
startup. Plugin defaults apply first, global values next, and instance options
last. Options are detached and immutable. Catalog arguments arrive in
`CatalogConfig.options`; format arguments arrive in `ExecutionContext.options`.
Each plugin validates supported arguments during construction.
Published asset format options override global format values during planning.
Tickets capture the effective options; fetching a ticket does not merge in newer
global settings.

## Secret backends

`DAL_OBSCURA_SECRET_PROVIDER_KIND` selects an admitted `secret_backend` plugin
once at startup. `DAL_OBSCURA_SECRET_PROVIDER_CONFIG` supplies its options and
operator-owned `scope_grants`. Environment and mounted-file backends ship as a
separate package. Custom backends implement the [SDK contract](../packages/plugin-api/README.md).
Changing the backend requires restarting the process.

```json
{"prefix":"DAL_OBSCURA_SECRET_","scope_grants":{"catalog:analytics":["TOKEN"]}}
```

A published secret reference is exactly `{"secret":"TOKEN","scope":"catalog:analytics"}`.
Core verifies the requesting scope and grant before calling the backend. Backends
resolve values and close resources; they cannot grant access. Credentials never
enter passive handles or tickets. Mounted files must be regular, non-linked,
bounded UTF-8 files under a fixed directory. Rotation replaces files atomically.

Production profiles require PostgreSQL, appropriate TLS settings and explicit
scope grants. Use worker identity for object storage credentials. Browser sessions,
OIDC/JWKS configuration and source claim mapping are governed published records;
see [identity and UI](governance-ui.md).
