# Plugin contract policy

Development cutover, 2026-09-27: there are no supported older or future runtime
contracts. Breaking changes may update the service, SDK, fixtures, and bootstrap
database together. No historical ticket bytes, pickle import paths, schema
aliases, or migration chains must be preserved. Recreate disposable databases
and issue new tickets when the stored contract changes.

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
and each yielded batch. Only trusted internal tasks are serialized; client input
is never deserialized with pickle. Path allowlists, authentication, and key
rotation remain operational security features, not version-compatibility shims.
