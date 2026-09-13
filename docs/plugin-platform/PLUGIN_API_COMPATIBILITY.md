# Plugin API compatibility policy

**Planning update, 2026-09-13:** this file records the baseline v1 policy.
[N02/N03](IMPLEMENTATION_PLAN.md) replace duplicated internal contracts with the
canonical public SDK and one explicitly supported version set. Breaking changes
require version bumps and offline cutover, not runtime compatibility shims.
The future multiple-major-version option below is outside the approved scope.
Protected pickle definitions remain unchanged. See [cleanup](CLEANUP_PLAN.md).

The plugin SDK is a separately built distribution. Its public contract is
identified by `PLUGIN_API_VERSION`, currently `"1"`, and by the integer
`config_version` carried in each descriptor. The service admits only the
versions exported by `dal_obscura.common.plugin_api`; an installed plugin that
claims another API or configuration version is incompatible and is rejected
before its factory is imported.

Version 1 follows these rules:

- Patch releases may fix validation, documentation, and adapter bugs without
  changing the meaning of existing fields or error codes.
- Additive optional fields and capabilities require a compatible config/schema
  interpretation and must keep unknown capabilities fail-closed.
- Removing a field, changing its meaning or type, changing serialization or
  lifetime guarantees, or adding a required operation requires a new API major.
- A configuration interpretation that cannot safely read an existing persisted
  value requires a new `config_version`; the control plane must provide an
  explicit migration or reject the configuration for review again.
- Core and public SDK version sets are tested for alignment. The registry lock
  records the exact API/config versions, distribution, release, descriptor
  digest, and artifact digest. A self-consistent lock does not override the
  core's supported-version set.

Plugins must target the lowest API/config version they need and declare only
capabilities implemented by that version. The service may support multiple
major versions in a future release, but it will never guess a compatibility
fallback or import an unadmitted factory. Historical publications retain their
recorded plugin identity and revision; changing the admitted plugin generation
requires a new review and publication.

Pair metadata is part of the v1 descriptor contract. A catalog descriptor must
list every table-format plugin ID in `output_formats`; both catalog and format
descriptors must list the handle versions they support in `handle_versions`.
The control plane requires the explicit format declaration, a non-empty handle
version intersection, and compatible capabilities. Capability overlap by itself
never admits a pair. Resolved handles are checked against the selected format
ID and declared handle version before schema or data execution.
