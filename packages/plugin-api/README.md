# Plugin SDK

The SDK is independent of the service. Catalogs, formats and secret backends use
public immutable contracts and static descriptors. Providers must not import
`dal_obscura` internals. Python 3.14 is required; the API version is defined in
`dal_obscura_plugin_api.contracts` and entry-point groups are versioned with it.

## Catalogs and formats

A catalog factory takes `(CatalogConfig, ExecutionContext)`, validates configuration
at construction, and returns discovery, resolution and cleanup methods. A format
factory binds `(TableHandle, ExecutionContext)` once. Its methods are
`schema(context)`, `plan(ScanRequest, context)` and `execute(ScanTask, context)`.
The format must retain the captured snapshot and honor exact output metadata.

`ScanRequest` carries the projected Arrow schema, order, nested types, metadata,
task budget and optional declared pushdown hint. `ScanTask` recursively freezes
bounded JSON objects. Use `to_json()` and `from_json()` for detached wire data.
Plan independent disjoint work, group excess fragments within the budget, and
return zero tasks for empty scans. A backend hint never replaces the complete
service-side filter.

Descriptors declare plugin identity, capabilities, output formats, handle versions
and configuration schema. Ship `dal_obscura-plugin.json` as package data beside the
entry-point module. The host admits exact installed distribution, version, API,
descriptor and artifact locks before importing the factory.
Use `load_packaged_descriptor` from `dal_obscura_plugin_api.descriptors` in the
provider module to read the same packaged metadata at runtime. Pass the installed
distribution name, `__package__`, plugin kind and plugin ID. The helper obtains the
distribution version from installed metadata. Keep capabilities and configuration
fields in the JSON file so admission and runtime describe the same contract.
The SDK validates declared fields and exposes immutable `config_fields` on the
descriptor. Fields support strings, URI strings, booleans, integers, finite
numbers, string enums and scoped secret references. Names must fit the operator
form's identifier syntax. Catalog string and URI fields also accept an explicit
scoped secret reference. The host authorizes and resolves these references before
provider construction; other field types retain their declared value types.
Defaults must name declared fields and match their types. With declared fields,
unknown catalog options are rejected unless
`additionalProperties` is explicitly `true`. This lets a provider expose common
typed fields while owning its remaining backend properties. The flag must be a
boolean; declared fields retain their type and required checks. Omitted `fields`
leaves catalog option validation to the provider unless `additionalProperties`
is `false`. An empty list declares no options by default.
Formats admit only declared non-secret options into signed work and reject secret
references even in string fields. URI options must not contain credentials or
credential query parameters. Use worker identity for storage credentials.
The host still owns secret grants, endpoint
policy and bounds on stored configuration.

Declare endpoints and storage locations as URI fields. The host checks them after
secret resolution before provider construction, while credential fields remain
opaque. URI references can contain authorized credentials. Additional backend
properties stay under provider validation.

## Execution

`ExecutionContext` carries deadline, correlation ID, cancellation, immutable format
options and an optional host path-admission callback. Use `@active` on synchronous
methods with a named `context` argument. The decorator guards entry/exit and
pulls from generator methods. Use `checked_iterator(values, context)` for lazy
iterables returned by ordinary methods. Close resources on every exit path.
Use `close_preserving_error` from `execution` for one resource and
`close_all_preserving_error` for a collection. Cleanup retains any active failure,
including cancellation, and otherwise exposes the first cleanup failure after
attempting all resources.

Long provider operations still need checkpoints between blocking calls. Decorators
cannot interrupt opaque blocking IO. Honor `context.check_path` before storage IO
where provided. Share path/schema utilities from the SDK rather than copying core
implementations. Global catalog options merge into `CatalogConfig.options`; global
format options arrive through `ExecutionContext.options`.

## Secret backends

A secret factory takes `(options, ExecutionContext)` and returns a backend with
`descriptor`, `get_secret(key)` and `close()`. Register it in the versioned
`dal_obscura.secret_backends.v3` entry-point group. Core selects one admitted backend
at startup and owns scope grants and reference authorization. Backend options
exclude grants. Keep values out of descriptors, handles, tasks and tickets.

Qualify through the [conformance kit](../plugin-conformance/README.md) and the
[installed-wheel release lane](../../docs/testing.md).
