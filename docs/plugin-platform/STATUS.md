# Plugin platform progress ledger

Baseline reviewed: `5208eee9af35e38b5ab8294524a0485611c2b0a7`.
Implementation follow-up through `57163da` (exact scanned image digest promotion;
virtualized nested schema navigation;
persisted catalog option revalidation;
nested manifest provider IDs;
bounded synthetic Arrow IDs;
serialized browser/OIDC auth state;
manifest standalone boundary;
legacy broad-grant admission guard;
typed path budgets;
quoted JSON path boundaries;
reserved identity namespaces;
quoted collection path parsing;
manifest field-ID bounds;
canonical provider ID API regression;
provider identity vocabulary;
bounded provider field identities;
external-catalog Iceberg schema bridge;
catalog mutation lock ordering;
workspace generation lock ordering;
direct policy mutation locking;
bounded discovery page typing;
table-handle identity binding;
public page-entry bounds;
public continuation validation;
public discovery concurrency bound;
plugin lock identity validation;
publication rollback coverage;
idempotency-key validation;
publication idempotency coverage;
unsafe schema-identity rejection;
production plugin-lock configuration;
descriptor option type validation;
asset binding plugin validation;
schema-scoped synthetic field IDs;
direct asset mutation row locks;
operator plugin-lock startup loading;
forged descriptor regression;
control-plane descriptor identity checks;
control-plane built-in registry startup;
control-plane schema/evaluation plugin
routing; conformance lifecycle deadlines;
public discovery identity boundary;
public discovery lifecycle;
data-plane catalog lifecycle validation; public adapter cleanup;
authenticated UI navigation;
catalog lifecycle conformance;
descriptor field enforcement;
discovery page boundary;
core table-handle boundary;
core option boundary;
aggregate catalog checks;
catalog revision API contract test;
catalog configuration CAS;
REST namespace traversal;
conformance cleanup gate;
catalog binding replacement;
public-response compatibility fix;
strict provider identity validation;
admitted control-plane catalog
discovery; nested conformance bounds and
structured identities; retired catalog-generation draining;
deterministic catalog-generation
cleanup; manifest identity/pagination fix;
catalog revision binding and stale
review protection; explicit plugin API compatibility
policy and aligned version vocabulary; catalog revision propagation regression
at `55f598c`; public adapter resource cleanup at `5afa3ae`; earlier slices remain
listed in the packet entries below).

Review date: 2026-09-13. **Paid-production release: HOLD.**

This task began with review/planning documents and now includes incremental runtime,
UI, and plugin-contract slices. Database changes remain additive, external
plugin wheels are still unverified, and pickle serialization remains unchanged.
All private UI pages now share the authenticated workspace gate: signed-out users
see only the sign-in/reconnect surface, including when a management hash is opened
directly. Production UI bundles now ignore the explicit `?demo` query and always use the authenticated workspace path; Vite development builds retain the local synthetic demo. The REST plugin descriptor is packaged under the exact static-admission
filename expected by the registry; online wheel construction is still an external
CI gate in this offline workspace.
The browser authorization suite records the concealed-resource contract after
valid CSRF validation (`404` for an asset outside the actor's visibility), and the
registry accepts both root and setuptools package-local static descriptor layouts
without importing plugin factories.
Public catalog configuration objects now reject non-JSON-like values and bounded
shape violations before factory invocation. The manifest/Parquet plugin rejects
symlink components in governed paths, and malformed unallowlisted entry points are
ignored so arbitrary installed metadata cannot block admission reloads.
The UI API client now broadcasts authenticated `401` responses; the app fences
loaded policy, schema, plugin, and management state before showing the sign-in
surface again, covering server-side session expiry and revocation while a page is
open.
Manifest and Parquet plugin operations now enforce deadline/cancellation checks
during initialization and per-file/per-row-group planning, so expired work stops
before additional member metadata is opened.
REST table discovery now rejects non-string provider identifier segments instead of
coercing malformed values into new logical identities.
REST credentials now require HTTPS, and optional warehouse/OAuth URI options reject
unsupported schemes, embedded credentials, query strings, and fragments before the
PyIceberg catalog is constructed.
The REST catalog descriptor now advertises its supported `scope` and
`oauth2-server-uri` options in both the runtime and packaged static descriptor, so
normal authenticated UI configuration can express the full admitted REST auth shape.
REST catalog lazy provider construction is now serialized with a lock and double-check, preventing concurrent discovery requests from creating duplicate PyIceberg sessions. A thread-pool regression verifies one provider instance is reused.
REST catalogs now expose idempotent terminal `close()` behavior, releasing provider/catalog sessions and rejecting reuse after cleanup; focused tests verify cleanup exactly once.
Public `TableHandle` values now validate plugin identities, revisions, versions,
snapshot IDs, and bounded JSON-like metadata before any handle can enter trusted
ticket serialization.
Public scan tasks now accept only bounded inert primitives or frozen dataclass
fields before entering the same serializer; opaque task objects are rejected at
planning time.
Lazy public plugin output is now checked batch by batch for Arrow type, exact
declared schema, and byte limits before it reaches DuckDB or Flight.
Admitted catalog/format factories that expose runtime descriptors are checked for
kind and plugin identity before use.
Browser session and idle lifetimes are bounded at control-plane construction (24
hours absolute, 2 hours idle) so cached UI privilege cannot be indefinite.
The manifest/Parquet adapter now validates its ticket-bound schema identity metadata
against the pinned Arrow schema before opening a format. Collection paths use the
same `$element`, `$key`, and `$value` markers as core policy resolution, including
large-list and fixed-size-list encodings.
Connections UI cards now show each catalog's admitted adapter identity and use
backend-neutral onboarding copy, so plugin catalogs are not presented as an
Iceberg-only workflow.
The public format bridge also rejects a descriptor that claims stable provider
IDs when its Arrow schema has no provider ID metadata, keeping plugin claims
aligned with the schema admission contract.
  Public discovery now accepts only SDK table identifiers and bounded printable
  continuation tokens, preventing forged provider-shaped objects from becoming
  governed catalog identities.
Conformance catalog checks now sample deadline and cancellation before and after
configuration validation and namespace discovery, preventing an expired plugin
from entering table traversal.
Public plugin format calls now carry one request context through factory, schema, planning, and execution. Deadline/cancellation checks run after provider calls and before every lazy Arrow batch, preventing cancelled or expired plugins from continuing into DuckDB or Flight.
Public plugin schemas now pass through the shared Arrow node, depth, and encoded
size budgets before they can be planned or executed.
Synthetic evaluation now supports fixed-size Arrow lists with correctly sized
typed values and canonical `$element` paths.
Built-in Iceberg startup no longer imports the optional public plugin SDK unless
an external catalog plugin is explicitly selected; missing external wheels fail
at that boundary instead of preventing the core service from starting.
The data-plane CLI now prints startup requirements before loading environment or
database state when invoked with `--help`/`-h`.
CI now builds the server and all independently packaged plugin wheels once,
installs those exact artifacts into a fresh environment, and runs core routing
tests before image publication.
Local probes are recorded in [the review](IMPLEMENTATION_REVIEW.md).
Earlier implementation evidence remains in [the UI ledger](../ui-v2/EXECUTION_STATUS.md).

The conformance runner now enforces a per-batch Arrow byte budget in addition to batch/row counts, matching the core plugin bridge and rejecting oversized output before qualification. The focused plugin/control-plane/authentication suite, TypeScript build, UI
lifecycle tests, Ruff, and Ty all pass after this follow-up. The full acceptance
matrix remains open where it requires live PostgreSQL concurrency, TLS/OIDC Flight,
browser accessibility, provider endpoints, clean wheel/image builds, or recovery
artifacts.

## State meanings

- `not-started`: no implementation under the new packet has begun.
- `implementing`: code/tests exist but at least one packet criterion is incomplete.
- `implemented-unverified`: intended implementation exists; required execution
  evidence is missing. This state is not acceptance.
- `accepted`: every criterion owned by that packet passed, with evidence tied to
  the relevant commit/artifact and the covered A subcases listed. Broader A scenarios
  spanning later packets remain open until their complete evidence exists. Later
  regressions reopen the packet; packet acceptance is not product release approval.
- `blocked`: name the external dependency and remaining work; never treat it as done.

## Ordered queue

- X00 baseline and constraints: **implemented-unverified**; acceptance registry,
  baseline diagnostics, and immutable synthetic ticket/task-boundary inventory are
  recorded. Runtime preservation tests do not replace the independent compatibility
  lane.
- X01 selected-only publication: **implemented-unverified**; initial activation now
  scopes to selected asset/catalog. Focused SQLite/API evidence passed; Flight and
  PostgreSQL evidence remain open.
- X02 immutable draft/review: **implemented-unverified**; strict review requires an
  explicit saved draft and legacy rule hashes are bound. PostgreSQL race evidence and
  full snapshot binding remain open.
- X03 publication/grant/binding transactions: **implementing**; asset-row locks now
  serialize shared-rule, draft, restore, owner, grant, and admitted-schema
  mutations with publication, and binding/access writes expose an optional
  monotonic asset revision precondition that returns 409 on stale writers.
  PostgreSQL barrier evidence, full transaction rollback/idempotency, and
  multi-process grant/binding evidence remain open. Delegated grant mutations now
  lock the asset before checking grant-manager authority, eliminating a stale
  authorization snapshot between revocation and replacement. Publication
  capability checks now also run after acquiring that asset lock, so revocations
  ordered before activation cannot be bypassed by a pre-lock authorization read.
  Review issuance follows the same lock ordering, while publish performs live schema
  I/O before the lock and rechecks persisted evidence after it, avoiding provider
  calls under a long database row lock.
  Shared policy edits and personal draft save/restore now authorize after the same
  asset lock, closing revocation windows across all review-affecting mutations.
  Grant capability is also enforced in the application service after locking, so
  direct service callers cannot bypass route-level authorization.
  Catalog configuration now has its own monotonic revision, locked upserts, and
  additive migration `20260913_0015`; review evidence records and verifies that
  revision so provider changes force fresh evaluation.
- X04 canonical evaluation: **implemented-unverified**; resolved mask values now
  flow from canonical preview and an unmatched-principal regression passes.
- X05 canonical bounded schemas: **implemented-unverified**; canonical Arrow schema
  encoding includes nested metadata/IDs and a shared node/depth/serialized-byte
  guard. Iceberg schema return, planning, and projected execution paths now
  enforce the same limits before recursive work. Direct Arrow fingerprint calls
  use that guard too. Migration and full entry-route/byte-budget evidence remain
  open.
- X06 safe schema evolution: **implementing**; typed evaluation paths now preserve
  literal dotted names, schema-field records carry optional stable IDs and typed
  paths through migration `20260912_0011`, and admitted identities are carried into
  immutable manifests and checked before data-plane planning. Provider-derived IDs,
  collection-path identity and the full evolution policy remain open. Migration
  `20260912_0013` backfills deterministic legacy IDs and paths for existing rows
  and fails closed on duplicate logical paths. Review tokens bind the persisted admitted-schema
  digest in addition to the live Iceberg digest. Renaming a field, even with the
  same provider ID, is an explicit reapproval event because its canonical path
  changes. Schemas without provider IDs now use schema-scoped synthetic identities
  across nested struct/list/map paths; any semantic shape change therefore
  requires reapproval instead of inheriting a display-name grant.
  Compiled schema metadata now records `stable_ids`; planning rejects a stable
  publication when the live schema no longer exposes provider IDs.
  Control-plane external Arrow schema responses now scope synthetic IDs to the
  complete schema shape and path, preventing type or nested-shape changes from
  reusing an approved field identity.
- X07 configuration/secrets/IO: **implementing**; nested dynamic class-loader options
  are rejected, and schema/evaluation/review provider calls now use the configured
  catalog egress validator. Explicit environment secret references now resolve in
  discovery and schema paths. Typed provider configs, provider-returned IO
  enforcement, and production secret-provider lifecycle evidence remain open.
  Storage paths now canonicalize local roots and URI authorities/decoded segments
  before every root check; credential-bearing and query/fragment roots are rejected.
  Catalog option payloads are also bounded by depth, node count, collection width,
  key length, string length, finite numeric values, and JSON-compatible value types
  before provider calls.
- X08 budgets and atomic reload: **implementing**; discovery now bounds provider
  iterators before materialization and checks cancellation/deadline per item while
  retaining deque traversal. Synthetic evaluation now bounds encoded fixture bytes
  before provider work. Plugin admission exposes build-then-swap reload snapshots;
  catalog registries build complete candidates before a locked metadata/adapter swap,
  so failed reloads cannot expose mixed generations. Provider-specific transport
  timeouts and multi-worker capacity remain open. A process-wide bounded discovery
  semaphore now rejects excess concurrent operations promptly and releases slots on
  every provider failure/cancellation path. Workspace discovery also caps each
  authenticated issuer/subject at two concurrent operations and removes idle keyed
  slots after release; cross-worker aggregate limits and provider termination remain
  open.
  Catalog registries now expose an explicit terminal close, reject reuse after
  shutdown, close retired generations after metadata operations drain, and clean
  up partially built adapters when replacement reload fails; cross-worker provider
  termination remains open.
- X09 UI lifecycle: **implemented-unverified**; initial-load epoch and synchronous
  logout fencing plus stale history/preview/review/publish response checks are fixed.
  Authenticated `401` responses now also fence loaded private state and return the
  browser to the sign-in surface after expiry or revocation. Deferred browser tests
  and full operation-state coverage remain open. Hash routing and post-login
  location restoration now preserve management context across refresh and OIDC
  redirects.
- X10 complete authoring/management: **implemented-unverified** for the deny-all UI
  path; controls now expose save/test/review/publish actions when rules are empty.
  Full editor, activation, accessibility, and browser evidence remain open.
- X11 public SDK: **implementing**; an independently buildable
  `packages/plugin-api` wheel now contains the versioned contracts and has an
  offline compile/metadata regression. Core and SDK contracts now reject malformed
  plugin IDs, unbounded capabilities, invalid catalog revisions, and non-canonical
  schema fingerprints, and unsafe/unbounded declarative config schemas.
  Request-scoped execution contexts now require timezone-aware deadlines, bounded
  correlation/capability values, and callable cancellation hooks in both core and
  public contracts. Explicit `CatalogFactory` and `TableFormatFactory` protocols
  now describe the core-injected construction boundary. Catalog table identifiers in both contracts now enforce a
  bounded segment count/length and printable string segments, preventing malformed
  provider identities from crossing the plugin boundary.
Core and public SDK descriptors now enforce one version-1 capability vocabulary and export it for adapters; unknown semantic claims fail before admission.
Registry admission now rejects self-consistent locks that claim unsupported API or config versions, and status diagnostics identify unsupported API versions without importing factories.
  Core and public SDK now export one explicit API/config version vocabulary, and
  `docs/plugin-platform/PLUGIN_API_COMPATIBILITY.md` defines additive, breaking,
  and migration rules. The SDK regression statically verifies that its import
  surface excludes service, FastAPI, SQLAlchemy, and PyIceberg modules.
  Catalog and table-format protocols now expose config validation, structured
  namespace listing, and deterministic close operations. REST and manifest
  adapters implement bounded lifecycle methods; clean-wheel and external
  lifecycle evidence remain open.
  Core and public catalog configuration boundaries now reject arbitrary option
  objects, oversized maps, deep nesting, control characters, and non-finite
  values before factory invocation.
  The conformance runner now fails a plugin that omits `close()` instead of
  treating missing cleanup as a pass.
  Catalog conformance also executes `validate_config()` and structured namespace
  listing before table traversal, failing plugins that omit either operation.
  Governance UI pages now use hash-backed navigation, guard direct hash changes
  when drafts are unsaved, and restore the selected page after OIDC redirects.
  REST catalog namespace lifecycle now honors provider-scoped traversal and
  safely filters bounded root listings for legacy providers.
  Catalog revisions are now returned in workspace metadata and accepted as an
  expected write precondition through the API and Connections UI.
  Core `CatalogConfig` now applies the same bounded JSON-like option validation
  as the public SDK before any adapter receives provider options.
  Core `TableHandle` now validates plugin identities, revisions, snapshots, and
  inert bounded metadata before trusted task serialization.
  Core and public `DiscoveryPage` values now reject oversized pages, malformed
  entries, and unsafe continuation tokens at construction time.
  Admitted catalog descriptor fields are now enforced server-side before catalog
  persistence, so UI bypasses cannot submit unknown or missing required options.
  Service-side compatibility contracts and online wheel artifact evidence remain
  open.
- X12 admitted loading and Iceberg adapter: **implementing**; entry-point loading now
  fails closed when a request names an unallowlisted installation, with registry
  regression coverage. Built-in Iceberg routing and artifact-lock verification
  now has an explicit immutable built-in registration and data-plane startup
  admission path. Static descriptor identity/API checks and optional descriptor /
  distribution digest locks are enforced before factory import. Build tooling now
  exposes `build_plugin_lock` to generate the exact immutable lock tuple from a
  validated entry point and descriptor. Full SDK adapter routing and clean-wheel
  artifact evidence remain open. Extended five-part locks now load the bounded
  static descriptor by default and normalize descriptor read/parse failures before
  factory import; legacy three-part locks keep their compatibility fallback.
  Multi-kind static descriptor documents can now describe both catalog and
  table-format entry points in one wheel; the loader selects exactly the matching
  kind/ID before any factory import. The data-plane catalog registry now bridges
  public SDK catalog/format factories into the existing governed `TableFormat` and
  trusted `ScanTask` serializer, recreating request contexts at execution time and
  rejecting plugin output schema changes. Public format instances now close after
  schema/planning and lazy execution; catalog adapters expose idempotent terminal
  cleanup and reject reuse after close. Registry shutdown now closes cached public
  catalog generations too. Public task payloads are constrained to
  bounded inert data before entering the trusted ScanTask serializer. Authenticated
  plugin diagnostics also expose enabled, not-installed, and
  incompatible states for operator-expected IDs without importing factories or
  enumerating unapproved installations.
- X13 plugin routing and migration: **implementing**; immutable compiled asset
  manifests now record explicit catalog and table-format adapter identities, and
  the data plane rejects explicit bindings it cannot honor before provider setup.
  Published catalog plugin revisions now reach public SDK adapters and ticket-bound handles; direct routing regression covers a non-zero revision.
  Published-config resolution now optionally requires both identities to exist in
  the active admitted registry generation; the production data plane wires the
  trusted Iceberg generation at startup. Catalog construction now receives that
  generation and invokes the admitted `iceberg.sql` factory rather than selecting
  an implementation from mutable request/config strings.
  Public SDK catalog/format factories can now be resolved through the same registry
  bridge and execute through the existing ticket path; external Iceberg metadata
  locations are checked against the configured storage allowlist before executor
  construction. Publication authoring now accepts external IDs only from the admitted
  registry, requires overlapping declared capabilities for the selected pair, and
  persists those IDs into runtime config. PostgreSQL/mixed-version migration evidence
  remains open.
  Control-plane discovery and diagnostics now use the same admitted public catalog
  factory for external IDs, with bounded continuation pages, catalog revisions,
  and deterministic plugin cleanup; live provider and browser evidence remains open.
  Control-plane schema loading, synthetic evaluation, review-token verification, and
  policy-version publication now resolve the same admitted catalog/table-format pair
  for external IDs. Public Arrow schemas receive the shared bounds and nested path/
  identity treatment before UI evidence is issued; legacy Iceberg module loading and
  pickle-backed task paths remain unchanged.
  The control-plane CLI now admits the trusted built-in Iceberg catalog/format
  generation at startup, so authenticated plugin diagnostics and connection
  authoring use the same registry boundary as the data plane.
  Control-plane schema discovery also verifies each returned catalog and
  table-format descriptor against the admitted kind/ID before provider work.
  External catalog handles bound to the admitted `iceberg` format now use an
  explicit compatibility bridge into the built-in Iceberg schema adapter. The
  bridge validates provider-returned metadata locations against the configured
  storage egress allowlist before any storage access, preserving the public
  catalog identity boundary while keeping legacy Iceberg and pickle paths
  unchanged.
  Arrow provider field IDs are now bounded, UTF-8, and printable before they
  become stable identities; malformed metadata falls back to a schema-scoped
  synthetic identity and cannot widen a reviewed field grant.
  Explicit schema-field saves and manifest identities now normalize provider
  IDs to the shared `iceberg:<id>` vocabulary before persistence, keeping
  control-plane review evidence and data-plane drift checks comparable.
  Manifest field-ID entries are also bounded and printable before identity
  derivation, so an operator-controlled manifest cannot inject oversized or
  control-bearing grant keys.
  The independent manifest package keeps this validation local and imports only
  the public plugin SDK; core service modules are not required by its wheel.
  Canonical field-path parsing now preserves quoted literal `$element`, `$key`,
  and `$value` names instead of interpreting them as collection markers, so
  reserved names cannot collide with list/map descendants.
  Provider metadata cannot claim the core-reserved `synthetic:` or `legacy:`
  namespaces as stable IDs; those values fall back to bounded schema-scoped
  identities.
  Quoted path parsing now uses JSON decoder boundaries, so field names containing
  brackets and escaped characters remain addressable without delimiter bugs.
  Typed field paths now enforce shared segment-count, field-name length, and
  printable-text limits before schema resolution or transport execution.
  Field IDs in typed paths are also constrained to nonnegative 32-bit integers,
  matching the protobuf contract and preventing malformed identity values from
  reaching schema resolution.
  Legacy published policies without persisted schema admission now fail closed
  when they contain wildcard, mask, or nested-parent selectors, preventing
  schema additions from expanding access.
  Both service composition roots now accept an optional operator-mounted
  `DAL_OBSCURA_PLUGIN_LOCK_FILE`; its bounded parser rejects symlinks,
  group/world-writable files, malformed identities, duplicates, and incomplete
  five-part locks before any factory import.
  Repository owner, schema-field, and grant replacement methods now acquire the
  asset row lock themselves, so direct callers cannot bypass the X03 revision
  serialization boundary.
  Control-plane Arrow synthetic field IDs now include the complete schema-shape
  digest and logical path, matching the data-plane identity rule so semantic
  changes require reapproval.
  Asset binding saves now validate admitted catalog/format identity, capability
  overlap, and table-format declarative options before persisting configuration.
  Descriptor-backed catalog and table-format options now enforce declared string,
  URI, and explicit secret-reference types before provider construction.
  The production Compose reference now passes the optional plugin-lock path to
  both application services and documents its in-container mount contract.
  Legacy Iceberg discovery now rejects malformed, non-printable, or oversized
  namespace/table segments instead of coercing them into new logical identities.
  Additive migration `20260913_0014` persists qualified catalog/format identities
  and optional plugin revisions beside immutable published rows; reads merge those
  identities back into legacy-compatible manifests.
  Published catalog plugin revisions now survive data-plane config resolution and reach public SDK adapters, so resolved handles retain immutable binding-generation identity instead of defaulting to revision zero.
  Catalog configuration revisions are now carried separately in compiled
  publication config and take precedence when constructing runtime catalog
  handles, preventing provider-option edits from reusing old review evidence.
  Republishing an existing catalog now replaces its compiled module, options, and
  revision in the next immutable generation; stale catalog settings cannot survive
  a reviewed configuration change.
  A deterministic `dal-obscura-migrate plugin-bindings` dry-run reports bound,
  exactly migratable, and unsupported legacy rows; `--apply` fills only exact
  built-in Iceberg identities in one transaction. Runtime registry routing,
  migration of legacy manifests, and mixed-version rollout evidence remain open.
- X14 plugin UI: **implementing**; standalone plugin descriptors now enforce
  bounded JSON-like form metadata and reject remote/executable content. An
  authenticated `/v1/plugins` endpoint and Connections view render admitted
  adapter capabilities and descriptor-driven configuration fields, including
  secret-reference inputs, without installation controls. Connections now submit
  the selected admitted catalog and table-format IDs while retaining the legacy
  Iceberg module compatibility mapping. Pair responses now mark capability-
  incompatible combinations and the form filters them; the management view also renders the
  authenticated lifecycle status inventory. Browser evidence remains open.
  Signed-out navigation is fenced at the top-level render boundary, so a direct
  management URL cannot display private workspace views before login.
- Public catalog discovery now requires the full lifecycle (`validate_config`,
  bounded namespace listing, table listing, and `close`) before provider results
  can enter the control-plane inventory. Namespace iteration is deadline and
  cancellation checked and bounded before table pages are materialized.
- The data-plane public catalog bridge applies the same lifecycle contract and
  validates configuration before resolving tables. Partially constructed or
  mismatched plugins are closed before the request fails.
- X15 conformance kit: **implementing**; a standalone public-API-only package
  now provides nested Arrow goldens, catalog discovery/page checks,
  capability-negative checks, bounded plan and output-schema validation, explicit
  cancellation checks, cleanup proof, optional expected-task coverage validation,
  skip semantics, and machine-readable results. Output validation now consumes
  batches incrementally with fixed batch and row budgets, and cancellation/deadlines
  are checked before requesting each planned task or output batch. Catalog checks
  reject malformed identifiers, duplicate identities, repeated continuation tokens,
  and page/table budget overruns; missing expected catalog entries are covered by
  an explicit negative fixture. Oversized schema descriptors are rejected by
  bounded nested-node/depth, field, and serialized-byte checks before format
  execution. Deliberately
  bad fixture plugins, provider/Flight/consumer lanes, and external distribution
  evidence remain open. CI now builds the API, conformance, manifest/Parquet, and
  REST wheels, installs those exact artifacts in an isolated environment, and runs
  their tests before image publication.
- X16 REST Iceberg qualification: **implementing**; an independently buildable
  `dal-obscura-iceberg-rest` wheel now exposes an admitted PyIceberg REST catalog,
  validates endpoint/config boundaries, paginates bounded table discovery, and
  returns immutable metadata locations for the trusted Iceberg executor without
  copying provider IO credentials into ticket-bound handles. The static descriptor
  now uses the registry's exact `dal_obscura-plugin.json` package-data name. Live
  REST server, auth, TLS, and clean-wheel evidence remain open; local wheel
  construction is blocked by unavailable build dependencies. Provider table
  discovery preserves identifier types and rejects malformed segments at the
  public boundary; credentialed HTTP endpoints and unsafe auxiliary URIs are
  rejected before provider construction.
- X17 independent manifest/Parquet plugin: **implementing**; the independent
  `dal-obscura-manifest-parquet` package registers `manifest` and `parquet.dataset`
  entry points against the public SDK. Its catalog pins an operator-owned manifest,
  revision, nested Arrow schema, top-level field IDs, and root-scoped immutable file
  membership; its format validates every member schema and emits one task per
  Parquet row group with nested projection support. Nested descendants now receive
  deterministic schema-scoped synthetic IDs anchored to manifest top-level IDs.
  Escape, symlink, unreadable-member, schema-drift, and corrupt-member execution
  tests pass; deadline/cancellation checks run during manifest and row-group
  planning. Discovery now emits and consumes string continuation tokens derived
  from structured identifiers, and optional namespace/name fields preserve
  literal dotted table names while retaining legacy dotted manifest keys.
  Core admission wiring, retry evidence, and live wheel/UI/
  consumer qualification remain open.
- X18 consumer qualification: **implementing**; Python/DuckDB connector tests and
  the Java/Spark Maven reactor pass in the local lanes (the JVM lane required
  socket permission). Real TLS/OIDC Flight runs, exact nested goldens across both
  admitted pairs, retry/expiry/revocation behavior, and clean version-matrix
  artifacts remain open.
- X19 secure deployment and identity lifecycle: **implementing**; production
  Compose now separates migration/control/data credentials, provisions isolated
  PostgreSQL roles, orders readiness through migration and post-migration ticket
  grants, and uses `/readyz` for application healthchecks. Real PostgreSQL SQL
  denial/allowance, TLS, IdP, browser, restart, and clean-artifact evidence remain
  open.
- X20 recovery and upgrades: **implementing**; data-plane ticket signing now
  supports one active HMAC key plus explicitly configured retired verification
  keys, with bounded rotation/rollback instructions and duplicate-key rejection.
  Encrypted PostgreSQL backup/restore, restored-session invalidation evidence,
  plugin drain/remove transitions, and mixed-version upgrade tests remain open.
- X21 performance/observability/test efficiency: **implementing**; Flight
  schema/planning/streaming paths now emit thread-safe process-local success/error
  duration aggregates with fixed low-cardinality operation names, and the Flight
  health action can expose a bounded copy of those aggregates. Dynamic
  principals, tables, URIs, and correlation IDs cannot become metric labels.
  Standalone package test modules now have unique import names and root pytest
  explicitly includes each package source path, removing cross-package collection
  collisions. Capacity profiling, benchmark thresholds, test-lane timing, and
  dead-code inventory remain open.
- X22 exact-artifact CI: **implementing**; the container publication job now
  depends on a pinned governance-UI install/type-check/build lane, and the Python
  type-check lane now resolves both independently buildable plugin package source
  roots explicitly, and standalone plugin wheel tests now gate image publication.
  Artifact
  digest, browser, provider, consumer, recovery, and mandatory-security gates
  remain open.
- X23 independent review/release decision: **not-started**.

Next implementation action: continue X03 with PostgreSQL barrier/CAS evidence and
then complete X06 provider-derived and collection field identity rules. Do not add new
providers before Phase A's security/correctness prerequisites are accepted.

### X05 bounded synthetic Arrow field IDs — `0dbfc40`

- State: implemented-unverified.
- Behavior: deterministic synthetic IDs emitted for nested Arrow schema paths now
  fit the shared nonnegative 32-bit `FieldPath` contract. This prevents valid
  schemas without provider IDs from failing during schema-path serialization.
- Green evidence: schema service and schema API suites passed (16), Ruff and
  `git diff --check` passed.
- Remaining gaps: provider-backed schema evolution, live PostgreSQL barriers,
  external wheels, browser, consumer, recovery, and release gates remain open.

### X06 nested manifest provider identities — `56c5e43`

- State: implementing.
- Behavior: the standalone manifest/Parquet plugin now preserves bounded nested
  Parquet/Iceberg field IDs, including collection children, and rejects duplicate
  identities. Deterministic synthetic IDs remain the fallback only when providers
  omit an identity.
- Green evidence: manifest plugin suite passed (15), Ruff and `git diff --check`
  passed.
- Remaining gaps: live provider evolution, PostgreSQL barriers, external wheels,
  browser/consumer qualification, recovery, and release gates remain open.

### X07 persisted catalog option revalidation — `54855b1`

- State: implementing.
- Behavior: discovery and schema loading now revalidate restored or legacy external
  catalog rows against the admitted descriptor before resolving secrets or invoking
  a provider factory. Unknown and missing fields fail closed; built-in Iceberg keeps
  its explicit compatibility contract.
- Green evidence: catalog-option, discovery, and schema suites passed (31), Ruff
  and `git diff --check` passed.
- Remaining gaps: provider-specific secret lifecycle, returned-location/DNS
  enforcement, PostgreSQL barriers, live wheels, browser, consumer, recovery, and
  release gates remain open.

### X10 virtualized nested schema navigation — `3f46816`

- State: implemented-unverified.
- Behavior: schema fields are flattened into a bounded scroll window with fixed
  row height and overscan, keeping mounted rows bounded for very large schemas.
  Expand/collapse, selection, search expansion, and Arrow-key navigation remain
  keyboard accessible; the package test command now includes every UI test module.
- Green evidence: UI type-check, Vite build, and five lifecycle/tree tests passed.
- Remaining gaps: real browser accessibility audit, 10,000-node measured p95,
  backend activation journeys, and production artifact evidence remain open.

### X22 exact scanned image promotion — `57163da`

- State: implementing.
- Behavior: protected-branch CI now builds one immutable GHCR candidate manifest,
  scans the exact digest returned by Buildx, and promotes release tags with
  `docker buildx imagetools create`. Pull requests retain a local amd64 scan;
  non-PR builds no longer rebuild a separate publish artifact.
- Green evidence: CI workflow contract tests (4), YAML parse, and `git diff --check`
  passed.
- Remaining gaps: hosted runner execution, signed artifact/image provenance,
  provider/consumer/browser/recovery gates, and independent release review remain
  open.

### X19 serialized browser and OIDC state — `ac38f89`

- State: implementing.
- Behavior: browser-session resolve/revoke and one-time OIDC transaction consume
  now lock their rows for the transaction, preventing concurrent callback replay
  and a request from refreshing a session after logout across control-plane
  workers.
- Green evidence: browser-session and OIDC route suites passed (8), Ruff and
  `git diff --check` passed.
- Remaining gaps: real PostgreSQL interleavings, live IdP/TLS, browser evidence,
  recovery, and production deployment gates remain open.

### X13 external-catalog Iceberg schema bridge — `5b152aa`

- State: implementing.
- Behavior: admitted external catalog handles whose format identity is the
  built-in `iceberg` adapter now route through an explicit compatibility path
  that constructs the legacy Iceberg schema reader from the provider's bounded
  metadata location. The location is checked against the configured egress
  allowlist before storage access; malformed metadata and invalid Arrow schemas
  fail closed. Existing legacy discovery and pickle-backed execution paths are
  unchanged.
- Green evidence: `tests/control_plane/test_schema_service.py` (13 passed),
  aggregate plugin/control-plane acceptance suite (all passed), Ruff, Ty, and
  `git diff --check`.
- Remaining gaps: live REST provider, TLS/OIDC, clean wheel, and mixed-version
  migration evidence remain open.

### X06 bounded provider field identities — `c52fc89`

- State: implementing.
- Behavior: provider field IDs read from Arrow metadata must be valid UTF-8,
  printable, non-empty, and at most 128 characters. Invalid metadata is treated
  as absent and receives a schema-scoped synthetic identity, so malformed IDs
  cannot become trusted stable grant keys.
- Green evidence: `tests/infrastructure/adapters/test_published_config.py`
  (26 passed), Ruff, Ty, and `git diff --check`.
- Remaining gaps: live provider-derived IDs and the complete schema-evolution
  acceptance matrix remain open.

### X06 provider identity vocabulary — `56e9f91`

- State: implementing.
- Behavior: explicit schema-field writes and manifest-derived identities now
  canonicalize unqualified provider IDs to `iceberg:<id>`, matching the shared
  data-plane identity contract while retaining synthetic identities for nested
  descendants and legacy IDs.
- Green evidence: publication-store, published-config, and manifest plugin
  suites (40 passed), Ruff, Ty, and `git diff --check`.
- Remaining gaps: full provider-derived identity propagation through live
  external schemas and schema-evolution/reapproval evidence remain open.

### X06 canonical provider ID API regression — `bd710ea`

- State: implementing.
- Behavior: the control-plane asset schema API regression now asserts that
  unqualified submitted provider IDs are returned in the canonical
  `iceberg:<id>` vocabulary while literal dotted and nested paths remain distinct.
- Green evidence: targeted asset API test, Ruff, and `git diff --check`.
- Remaining gaps: live provider-derived identity propagation and the complete
  schema-evolution/reapproval matrix remain open.

### X06 manifest field-ID bounds — `52cdd04`

- State: implementing.
- Behavior: the manifest/Parquet catalog rejects control-bearing and oversized
  top-level provider IDs before deriving nested identities or returning a table
  handle. This keeps independent plugin metadata within the shared bounded
  schema identity contract.
- Green evidence: manifest plugin suite (14 passed), Ruff, Ty, and
  `git diff --check`.
- Remaining gaps: live manifest wheel/consumer qualification and complete
  provider-derived schema-evolution evidence remain open.

### X04/X06 quoted collection path parsing — `34b21a2`

- State: implementing.
- Behavior: human field paths retain whether a segment was quoted. Literal
  reserved collection names now round-trip as field segments while unquoted
  markers continue to resolve list elements and map keys/values.
- Green evidence: field-path suite (20 passed), Ruff, Ty, and
  `git diff --check`.
- Remaining gaps: browser/consumer nested-path evidence and complete schema
  evolution acceptance remain open.

### X06 reserved identity namespaces — `b7ca710`

- State: implementing.
- Behavior: provider field metadata using the core-generated `synthetic:` or
  `legacy:` prefixes is rejected as a stable identity and replaced with a
  schema-scoped synthetic identity.
- Green evidence: published-config identity suite (25 passed), Ruff, Ty, and
  `git diff --check`.
- Remaining gaps: live provider-derived IDs and complete schema-evolution
  acceptance remain open.

### X04 quoted JSON path boundaries — `4abb24f`

- State: implementing.
- Behavior: canonical human path parsing now decodes quoted JSON names with
  proper string boundaries, preserving names that contain `]`, `[`, or escape
  sequences while keeping collection markers typed.
- Green evidence: field-path suite (21 passed), Ruff, Ty, and
  `git diff --check`.
- Remaining gaps: browser/consumer nested-path evidence and complete schema
  evolution acceptance remain open.

### X05 typed field-path budgets — `86e9f01`

- State: implementing.
- Behavior: `FieldPath` enforces a 64-segment maximum and 256-character
  printable field-name limit. The same contract covers wire values and Flight
  descriptor paths before nested schema resolution.
- Green evidence: field-path suite (22 passed), Ruff, Ty, and
  `git diff --check`.
- Remaining gaps: Flight/browser nested-path evidence and full schema-boundary
  acceptance remain open; sandboxed Flight socket tests remain environment
  restricted.

### X11/X17 manifest standalone boundary — `0df956a`

- State: implementing.
- Behavior: manifest field-ID validation and canonicalization are implemented
  locally inside the independent package, preserving its public-SDK-only import
  surface while retaining the bounded identity contract.
- Green evidence: manifest plugin suite (14 passed), standalone import scan,
  Ruff, Ty, and `git diff --check`.
- Remaining gaps: clean wheel installation and live manifest/consumer
  qualification remain open.

### X05 typed field-ID range — `7791809`

- State: implementing.
- Behavior: typed path construction rejects boolean, negative, and out-of-range
  field IDs before wire serialization or schema resolution.
- Green evidence: field-path suite (23 passed), Ruff, Ty, and
  `git diff --check`.
- Remaining gaps: Flight/browser nested-path evidence and full schema-boundary
  acceptance remain open.

### X06 legacy broad-grant admission guard — `c43b15e`

- State: implementing.
- Behavior: published generations lacking schema admission reject wildcard and
  nested-parent policy selectors before serving reads; scalar explicit selectors
  retain legacy readability. This prevents implicit access expansion while old
  rows await migration or republish.
- Green evidence: published-config suite (27 passed), Ruff, Ty, and
  `git diff --check`.
- Remaining gaps: PostgreSQL migration evidence, live schema evolution, and
  consumer/browser qualification remain open.

### X03 direct mutation row locks — `165015f`

- State: implementing.
- Behavior: repository owner, schema-field, and grant replacements now issue
  `SELECT ... FOR UPDATE` on the asset row before checking or advancing its
  revision. Direct repository callers therefore retain the same compare-and-set
  serialization as the application service path.
- Green evidence: publication-store, grant-authorization, and asset API suites
  (19 passed), Ruff, Ty, and `git diff --check`.
- Remaining gaps: live PostgreSQL barrier, rollback/idempotency, and multi-process
  evidence remain open. Pickle compatibility is unchanged.

### X06 schema-scoped Arrow identities — `cede42f`

- State: implementing.
- Behavior: external Arrow schema fields without provider IDs now receive
  deterministic synthetic IDs derived from the full schema-shape digest and
  logical path. Equivalent schemas retain identities; type or nested-shape
  changes require reapproval instead of inheriting grants.
- Green evidence: schema service and published-config suites (31 passed), Ruff,
  Ty, and `git diff --check`.
- Remaining gaps: provider-derived IDs from live format metadata and complete
  schema-evolution acceptance remain open. Pickle compatibility is unchanged.

### X06 direct schema identity boundary — `2386a02`

- State: implementing.
- Behavior: direct repository callers can no longer coerce arbitrary schema
  names, paths, or field IDs into reviewed metadata. Names and path segments
  are bounded printable text; explicit field IDs must be bounded printable
  strings; malformed values become safe validation failures at the service
  boundary while legacy generated IDs remain compatible.
- Green evidence: publication-store and asset-mutation suites (9 passed), Ruff,
  and `git diff --check`.
- Remaining gaps: PostgreSQL barrier evidence, provider-derived IDs from live
  format metadata, and complete schema-evolution acceptance remain open.
  Pickle compatibility is unchanged.

### X03 publication idempotency coverage — `40d9cf7`

- State: implementing.
- Behavior: the control-plane API now proves a committed publication replay
  returns the original result, operation lookup is available to the authorized
  actor, and reusing the same key with a different request body returns a 409
  conflict.
- Green evidence: `tests/interfaces/control_plane/test_api_publish_flow.py`
  (1 passed), Ruff, and `git diff --check`.
- Remaining gaps: PostgreSQL barrier interleavings, injected rollback points,
  multi-process races, and full audit/operation transaction evidence remain
  open. Pickle compatibility is unchanged.

### X03 idempotency-key validation — `a212df7`

- State: implementing.
- Behavior: publication and operation lookup now require a trimmed, bounded,
  printable idempotency key. Blank, control-character, non-text, and oversized
  values fail validation instead of disabling replay protection or entering a
  database lookup.
- Green evidence: policy-version and API publication suites (3 passed), Ruff,
  and `git diff --check`.
- Remaining gaps: PostgreSQL barrier interleavings, injected rollback points,
  multi-process races, and full audit/operation transaction evidence remain
  open. Pickle compatibility is unchanged.

### X03 publication rollback coverage — `5b34f9b`

- State: implementing.
- Behavior: an injected failure after publication generation/pointer work and
  before the audit completes rolls back the active pointer, immutable
  generation, audit event, and idempotency operation in one session transaction.
- Green evidence: `tests/control_plane/test_policy_version_service.py` (3
  passed), Ruff, and `git diff --check`.
- Remaining gaps: PostgreSQL barrier interleavings, independent-process races,
  failure injection at every transaction boundary, and production recovery
  evidence remain open. Pickle compatibility is unchanged.

### X03 direct policy mutation locking — `80fedc2`

- State: implementing.
- Behavior: repository-level policy-rule replacement now locks the asset row
  internally before deleting or inserting rules. Direct repository callers
  therefore cannot bypass the publication serialization boundary enforced by
  application services.
- Green evidence: publication-store and policy-authorization suites (12
  passed), Ruff, and `git diff --check`.
- Remaining gaps: PostgreSQL barrier interleavings, full revision/CAS coverage,
  and multi-process grant/binding evidence remain open. Pickle compatibility is
  unchanged.

### X03 workspace generation lock ordering — `3bd526c`

- State: implementing.
- Behavior: publication acquires the workspace cell lock after its asset lock
  and before reading/compiling the candidate. Runtime settings and auth-provider
  repository mutations acquire that same cell lock internally, preventing
  workspace-generation inputs from committing between candidate construction
  and activation.
- Green evidence: policy-version, publication-store, workspace API, and publish
  flow suites (18 passed), Ruff, Ty, and `git diff --check`.
- Remaining gaps: PostgreSQL barrier/process evidence and failure injection at
  every transaction boundary remain open. Pickle compatibility is unchanged.

### X12 plugin lock identity validation — `ca88316`

- State: implementing.
- Behavior: operator-mounted five-part plugin locks now reject malformed plugin
  IDs, control-character or oversized identity values, and non-canonical
  descriptor/artifact digests before registry admission or factory import.
- Green evidence: plugin lock suite (8 passed), Ruff, and `git diff --check`.
- Remaining gaps: clean wheel provenance, duplicate installed distributions,
  external package admission, and production artifact evidence remain open.
  Pickle compatibility is unchanged.

### X03 catalog mutation lock ordering — `36633af`

- State: implementing.
- Behavior: catalog upserts now acquire the workspace cell lock before reading
  or advancing catalog configuration revisions, joining asset publication,
  runtime, and identity mutations under one deterministic generation boundary.
- Green evidence: publication-store, catalog-option, catalog API, and publish
  flow suites (21 passed), Ruff, and `git diff --check`.
- Remaining gaps: PostgreSQL barrier/process evidence and complete rollback
  injection matrix remain open. Pickle compatibility is unchanged.

### X08 public discovery concurrency bound — `b5dd0d4`

- State: implementing.
- Behavior: admitted public catalog discovery now consumes the same bounded
  process-wide semaphore as the legacy Iceberg path. Slots are released when
  factory lookup, provider lifecycle, iteration, or cleanup fails, so external
  plugins cannot bypass the aggregate admission limit or strand capacity.
- Green evidence: catalog-discovery suite (14 passed), Ruff, and
  `git diff --check`.
- Remaining gaps: cross-worker aggregate limits, provider interruption, and
  live timeout/termination evidence remain open. Pickle compatibility is
  unchanged.

### X13 public continuation validation — `7b12b68`

- State: implementing.
- Behavior: the data-plane public catalog bridge now validates continuation
  tokens as bounded printable strings before hashing or retaining traversal
  state. Malformed, oversized, control-character, and unhashable provider
  values fail closed with deterministic validation errors.
- Green evidence: public plugin adapter suite (15 passed), Ruff, and
  `git diff --check`.
- Remaining gaps: live provider pagination, cross-worker capacity, and external
  wheel/consumer evidence remain open. Pickle compatibility is unchanged.

### X08/X13 public page-entry bounds — `71af4ce`

- State: implementing.
- Behavior: control-plane and data-plane public catalog bridges now require
  bounded tuple/list page containers and enforce the 500-entry page limit before
  iterating provider results. Forged endless or oversized iterables cannot
  bypass discovery memory limits.
- Green evidence: catalog discovery and public adapter suites (31 passed), Ruff,
  and `git diff --check`.
- Remaining gaps: cross-worker aggregate limits, provider interruption, live
  pagination, and external wheel/consumer evidence remain open. Pickle
  compatibility is unchanged.

### X13 table-handle identity binding — `7bff5d8`

- State: implementing.
- Behavior: control-plane schema routing and the data-plane public catalog
  bridge now require a resolved handle to match the admitted catalog plugin,
  catalog instance, catalog revision, and requested table identifier before a
  format adapter is constructed.
- Green evidence: schema-service and public-adapter suites (28 passed), Ruff,
  and `git diff --check`.
- Remaining gaps: live provider identity evidence, mixed-version PostgreSQL
  migration, and external wheel/consumer qualification remain open. Pickle
  compatibility is unchanged.

### X07/X14 asset binding validation — `3c3709b`

- State: implementing.
- Behavior: asset saves now reject non-admitted catalog/format identities,
  capability-incompatible pairs, and table-format options outside the admitted
  descriptor form. Validation occurs before persistence and is independent of
  browser controls.
- Green evidence: asset plugin-binding and policy authorization suites (9 passed),
  Ruff, Ty, and `git diff --check`.
- Remaining gaps: provider-specific typed options, live secret/IO enforcement,
  and browser/provider acceptance remain open. Pickle compatibility is unchanged.

### X07 descriptor option types — `9cae43f`

- State: implementing.
- Behavior: admitted descriptor fields now validate declared string/URI types and
  require secret fields to use an explicit `{secret: name}` reference before
  values reach providers or are persisted. Existing bounded shape and egress
  checks remain in force.
- Green evidence: catalog-option and asset-plugin suites (4 passed), Ruff, Ty,
  and `git diff --check`.
- Remaining gaps: provider-specific semantic validation, secret-provider
  lifecycle, and live IO/browser evidence remain open. Pickle compatibility is
  unchanged.

### X03 catalog binding replacement — `1771f26`

- State: implementing.
- Behavior: republishing a catalog with changed module, options, or revision now
  replaces the matching compiled catalog entry in the next immutable generation.
- Green evidence: policy-version, compiler, and API publication tests (51 passed),
  Ruff, and `git diff --check`.
- Remaining gaps: PostgreSQL barrier/rollback/idempotency and mixed-version evidence
  remain open. Pickle compatibility is unchanged.

### X11 catalog lifecycle contract — `364f4cc`

- State: implementing.
- Behavior: public and core SDK catalog protocols expose config validation,
  structured namespace listing, and terminal close; table-format protocols expose
  close. REST and manifest adapters implement the lifecycle methods.
- Green evidence: REST/manifest package suites (27 passed), Ruff, Ty, UI type check,
  and `git diff --check`.
- Remaining gaps: clean isolated wheels, external provider lifecycle, and complete
  X11/X12 acceptance evidence remain open. Pickle compatibility is unchanged.

### X15 explicit cleanup conformance — `b178cc3`

- State: implementing.
- Behavior: catalog and format conformance now require an explicit callable
  `close()` and record missing cleanup as a failure. Fixture plugins declare the
  lifecycle contract; the redundant cancellation branch was removed.
- Green evidence: `packages/plugin-conformance/tests/test_conformance_runner.py`
  (23 passed), Ruff, Ty, and `git diff --check`.
- Remaining gaps: provider interruption, real consumer cleanup, and external wheel
  evidence remain open. Pickle compatibility is unchanged.

### X15 catalog lifecycle conformance — `af14daa`

- State: implementing.
- Behavior: catalog conformance now requires and executes config validation and
  namespace listing before bounded table discovery; missing lifecycle operations
  fail the run.
- Green evidence: conformance runner suite (24 passed), Ruff, Ty, and
  `git diff --check`.
- Remaining gaps: live provider timeout/termination, consumer cleanup, and external
  wheel evidence remain open. Pickle compatibility is unchanged.

### X09 authenticated UI navigation — `b9b2531`

- State: implemented-unverified.
- Behavior: management pages are addressable through bounded hashes, direct hash
  changes honor unsaved-draft confirmation, and SSO login restores the requested
  page through browser session storage when available.
- Green evidence: governance UI lifecycle tests (3 passed), local TypeScript compile,
  Vite production build, and `git diff --check`.
- Remaining gaps: real-browser accessibility, IdP callback journeys, and complete
  deferred operation ordering remain open. Pickle compatibility is unchanged.

### X13 public discovery lifecycle — `87900c8`

- State: implementing.
- Behavior: admitted public catalog discovery executes configuration validation and
  structured namespace listing before bounded table traversal, rejects malformed
  or oversized namespace streams, and always closes the provider.
- Green evidence: `tests/control_plane/test_catalog_discovery.py` (11 passed),
  Ruff, Ty, and `git diff --check`.
- Remaining gaps: live provider termination, cross-worker capacity, external wheel,
  and browser evidence remain open. Pickle compatibility is unchanged.

### X12 data-plane catalog lifecycle — `4942dfc`

- State: implementing.
- Behavior: the public catalog bridge requires every lifecycle operation and runs
  `validate_config()` before serving. Validation failures close the newly created
  provider before returning an error; missing lifecycle methods are rejected.
- Green evidence: public adapter and discovery suites (23 passed), Ruff, Ty, and
  `git diff --check`.
- Remaining gaps: clean isolated wheels, external provider lifecycle, and live
  serving evidence remain open. Pickle compatibility is unchanged.

### X13 public discovery identity boundary — `a539c3f`

- State: implementing.
- Behavior: control-plane discovery rejects forged table identifier objects and
  oversized or control-character continuation tokens before inventory persistence.
- Green evidence: `tests/control_plane/test_catalog_discovery.py` (12 passed),
  Ruff, and `git diff --check`.
- Remaining gaps: live provider termination, cross-worker capacity, external wheel,
  and browser evidence remain open. Pickle compatibility is unchanged.

### X15 conformance lifecycle deadlines — `a57a1f6`

- State: implementing.
- Behavior: conformance checks reject expired or cancelled catalog contexts around
  lifecycle validation and namespace enumeration before requesting table pages.
- Green evidence: `packages/plugin-conformance/tests/test_conformance_runner.py`
  (25 passed), Ruff, Ty, and `git diff --check`.
- Remaining gaps: provider interruption, real consumer cleanup, and external wheel
  evidence remain open. Pickle compatibility is unchanged.

### X13 control-plane schema routing — `5d61ea5`

- State: implementing.
- Behavior: schema loading, synthetic policy evaluation, review-token live-schema
  checks, and policy-version publication now accept the active admitted plugin
  registry. External catalog/table-format pairs are resolved through the public SDK
  lifecycle with bounded execution contexts, strict handle/descriptor identity
  checks, nested Arrow path identities, and deterministic cleanup. The exact legacy
  Iceberg loader remains the compatibility path when no external binding is selected.
- Green evidence: control-plane schema/evaluation/review/API suites (19 passed),
  Ruff, Ty, and `git diff --check`.
- Remaining gaps: control-plane CLI registry/lock loading, live provider and
  PostgreSQL evidence, external wheel evidence, and browser end-to-end evidence
  remain open. Pickle compatibility is unchanged.

### X12/X13 control-plane startup admission — `5fce4c9`

- State: implementing.
- Behavior: the control-plane CLI now builds and injects the trusted built-in
  Iceberg catalog/table-format registry before constructing the FastAPI app. The
  UI plugin inventory, connection authoring, discovery, schema review, and
  publication therefore share the same admitted built-in generation as the data
  plane.
- Green evidence: control-plane CLI suite (19 passed), Ruff, Ty, and
  `git diff --check`.
- Remaining gaps: operator-mounted external plugin-lock parsing, clean external
  wheel evidence, and live PostgreSQL/browser/provider acceptance remain open.
  Pickle compatibility is unchanged.

### X13 control-plane descriptor identity — `0bf6b1b`

- State: implementing.
- Behavior: control-plane schema discovery now requires returned catalog and
  table-format instances to expose public descriptors whose kind and plugin ID
  exactly match the admitted binding before validation, resolution, or schema
  access proceeds.
- Green evidence: `tests/control_plane/test_schema_service.py` (11 passed), Ruff,
  Ty, and `git diff --check`.
- Remaining gaps: external wheel/provider evidence and live PostgreSQL/browser
  acceptance remain open. Pickle compatibility is unchanged.

### X12/X13 operator plugin-lock loading — `ebf71b6`

- State: implementing.
- Behavior: both control-plane and data-plane startup accept an optional
  `DAL_OBSCURA_PLUGIN_LOCK_FILE` and pass its validated five-part locks into the
  immutable registry alongside trusted built-ins. The parser is bounded, refuses
  symlink or group/world-writable files, rejects malformed/duplicate entries, and
  performs no factory imports while reading the lock.
- Green evidence: lock-file, built-in registry, and control-plane CLI suites
  (25 passed), Ruff, Ty, and `git diff --check`.
- Remaining gaps: clean wheel/artifact provenance, production policy requiring a
  lock, live PostgreSQL/provider/browser acceptance, and full data-plane external
  plugin execution remain open. Pickle compatibility is unchanged.

### X19 production plugin-lock configuration — `15a2f9b`

- State: implementing.
- Behavior: the production Compose reference forwards `DAL_OBSCURA_PLUGIN_LOCK_FILE`
  to both application services, and the operator runbook documents mounting one
  immutable lock at the same in-container path before enabling external wheels.
- Green evidence: `tests/architecture/test_production_deployment_contract.py`
  (1 passed), `git diff --check`.
- Remaining gaps: actual clean image/wheel assembly, mounted-lock deployment, and
  live production security evidence remain open.

### X16 REST namespace lifecycle — `8c73c41`

- State: implementing.
- Behavior: REST catalog namespace listing accepts structured scoped requests,
  invokes provider-scoped APIs when available, and filters bounded root listings
  for legacy providers without that overload.
- Green evidence: REST package suite (15 passed), Ruff, and `git diff --check`.
- Remaining gaps: live REST provider, TLS/auth, clean-wheel, and consumer evidence
  remain open. Pickle compatibility is unchanged.

### X03 catalog configuration CAS — `1e2308a`

- State: implementing.
- Behavior: catalog upserts lock the row and reject stale expected revisions;
  workspace metadata exposes the current revision and Connections submits it when
  editing an existing catalog, surfacing a refresh conflict to the operator.
- Green evidence: catalog API and repository suites (17 passed), local TypeScript
  compile and Vite production build, Ruff, and `git diff --check`.
- Remaining gaps: PostgreSQL multi-process barriers, transaction rollback and live
  production/browser evidence remain open. Pickle compatibility is unchanged.

### X11 core option boundary — `7d1119c`

- State: implementing.
- Behavior: core `CatalogConfig` now validates bounded JSON-like provider options
  before adapters receive them, matching public SDK enforcement.
- Green evidence: plugin contract and public adapter suites (16 passed), Ruff, Ty,
  and `git diff --check`.
- Remaining gaps: clean-wheel, external provider, and production admission evidence
  remain open. Pickle compatibility is unchanged.

### X11 core table-handle boundary — `c636b25`

- State: implementing.
- Behavior: core `TableHandle` now rejects malformed plugin IDs, negative revisions,
  invalid snapshots, and arbitrary or oversized metadata before trusted serialization.
- Green evidence: plugin contract and public-adapter suites (16 passed), Ruff, Ty,
  and `git diff --check`.
- Remaining gaps: clean-wheel, external provider, and production admission evidence
  remain open. Pickle compatibility is unchanged.

### X11 discovery page boundary — `6408704`

- State: implementing.
- Behavior: core and public `DiscoveryPage` contracts enforce a 500-entry page
  bound, valid structured identifiers, and bounded printable continuations before
  discovery traversal consumes them.
- Green evidence: plugin contract, conformance, and public-adapter suites (39
  passed), Ruff, Ty, and `git diff --check`.
- Remaining gaps: live provider pagination and cross-worker capacity evidence
  remain open. Pickle compatibility is unchanged.

### X14 descriptor field enforcement — `fb394d6`

- State: implementing.
- Behavior: external catalog writes validate submitted options against the admitted
  descriptor's declarative fields, rejecting unknown fields and missing required
  values before persistence or provider construction.
- Green evidence: catalog validation and API suites (15 passed), Ruff, Ty, and
  `git diff --check`.
- Remaining gaps: browser accessibility, live external plugin forms, and clean-wheel
  evidence remain open. Pickle compatibility is unchanged.

The aggregate local control-plane/plugin lane now passes after the revision field
contract update: 262 tests across control-plane routes, plugin conformance, REST,
and manifest packages.

### X05 shared Arrow schema bounds — `fd20e72`

- State: implemented-unverified.
- Behavior: `common.schema_bounds.validate_arrow_schema_bounds` is the shared
  guard for Arrow node count, nesting depth, and serialized schema size. The
  control-plane fingerprint path and Iceberg format adapter apply it to schema
  returns, planning inputs, and projected execution schemas. Traversal is
  iterative and bounded; oversized or deeply nested schemas fail before policy
  admission can recurse over them.
- Green evidence: schema-bound, schema-service, and Iceberg regression tests (20
  passed), Ruff, Ty, and `git diff --check` pass.
- Remaining gaps: PostgreSQL migration/compatibility evidence and every live
  API/Flight entry-route budget scenario remain unverified. Pickle compatibility
  is unchanged.
- Next permitted packet: continue X03 PostgreSQL barriers/CAS and X06 provider
  identity evidence.

### X21 bounded Flight metrics — `ad62fea`

- State: implementing.
- Behavior: `ServiceMetrics` provides bounded, thread-safe operation/outcome
  counters and elapsed-duration totals. Flight `get_schema`, `get_flight_info`,
  and `do_get` paths record success or error without exposing request-specific
  labels. Invalid/high-cardinality operation names and unsupported outcomes are
  rejected. The authenticated Flight health action exposes a copy-on-read
  snapshot only when aggregates exist.
- Green evidence: `tests/test_observability.py` (2 passed) plus Flight health
  action coverage (4 passed), Ruff, Ty, and
  `git diff --check` pass. Existing Flight integration tests were attempted but
  cannot bind their ephemeral `0.0.0.0:0` listener in this restricted sandbox;
  the failure is recorded as an environment gate and requires a network-enabled
  runner for transport evidence.
- Remaining gaps: exporter/endpoint integration, workload capacity thresholds,
  memory/stream profiling, timing-separated test lanes, and dead-code removal
  remain open. Pickle compatibility is unchanged.
- Next permitted packet: continue X03 PostgreSQL barriers/CAS and X06 evolution
  evidence; do not advertise external providers before Phase A acceptance.

### X20 bounded ticket-key rotation — `38f1449`

- State: implementing.
- Behavior: `HmacTicketCodecAdapter` signs with the active key and verifies
  against a finite, explicitly configured list of retired keys. Runtime config
  parses comma-separated previous keys, rejects duplicates, and requires the
  same production minimum length as the active key. Production Compose and its
  runbook document rotation, expiry-window removal, and rollback. The opaque
  ticket payload and trusted pickle task boundary are unchanged.
- Green evidence: ticket codec and runtime-config tests (23 passed), Ruff, Ty,
  and `git diff --check` pass.
- Remaining gaps: encrypted PostgreSQL backup/restore, restore invalidation
  drills, plugin drain/remove transitions, mixed-version migration tests, and
  measured recovery objectives remain open. Pickle compatibility is unchanged.
- Next permitted packet: add backup/restore verification and plugin lifecycle
  state checks once PostgreSQL integration infrastructure is available.

### X19 production database privilege ordering — `18f8e64`

- State: implementing.
- Behavior: production Compose now runs a one-shot `postgres-grants` service after
  migrations. It grants the data-plane role only the DML needed for
  `public.data_plane_tickets`; control-plane and data-plane startup wait for that
  service to complete. The first-boot role initializer remains schema/migration
  scoped and does not assume the ticket table exists yet.
- Green evidence: `bash -n deployment/production/postgres-init/01-roles.sh`,
  `tests/architecture/test_production_deployment_contract.py` (1 passed),
  `docker compose --env-file deployment/production/.env.example -f
  deployment/production/compose.yaml config --quiet`, and `git diff --check`.
- Remaining gaps: real PostgreSQL role/rollback/process evidence, backup/restore,
  TLS/IdP/browser, plugin, consumer, and production acceptance gates remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X03 PostgreSQL barriers/CAS evidence.

The initial grant slice named the ticket table incorrectly; `18f8e64` corrects the
target to the ORM-backed `data_plane_tickets` table. The corrected deployment
contract and Compose rendering pass.

Latest implementation slices after the schema migration: X08 provider-page budget
and cancellation checks are validated by
`tests/control_plane/test_catalog_discovery.py` (5 passed) with Ruff clean. The
slice is intentionally not marked accepted because live provider timeout,
multi-worker capacity, and atomic reload evidence are still required.

X12 admission loading now rejects unallowlisted entry points before invoking a
factory, and allowlisted entries without distribution provenance fail closed
before import; `tests/plugin_platform/test_registry.py` passes all 7 cases with
Ruff and Ty clean. Built-in Iceberg routing and artifact-lock verification remain
open.

### X12 distribution provenance gate — `524bb6a`

- State: implementing.
- Behavior: an allowlisted entry point must expose distribution name and version
  metadata matching the immutable plugin lock. Missing provenance is rejected
  before any factory import, so an unverified installation cannot inherit the
  admission allowlist.
- Green evidence: `tests/plugin_platform/test_registry.py` (7 passed), Ruff,
  Ty, and `git diff --check` all pass.
- Remaining gaps: built-wheel artifact provenance, duplicate distribution
  conflicts, runtime routing, and external plugin conformance remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X12 artifact-lock and built-in adapter wiring.

### X06 rename policy regression — `4c0ada8`

- State: implementing.
- Behavior: schema admission requires the reviewed canonical path and provider
  identity to match together. A field rename with an unchanged Iceberg ID is
  rejected with a review-again outcome, preventing a renamed sensitive field from
  inheriting a policy whose path was never reviewed.
- Green evidence: `tests/infrastructure/adapters/test_published_config.py` (14
  passed), Ruff, Ty, and `git diff --check` all pass.
- Remaining gaps: provider-derived IDs for every backend, explicit additive/drop
  evolution rules, PostgreSQL/browser/consumer evidence, and external plugin
  qualification remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X06 evolution rules alongside X03 PostgreSQL
  transaction evidence.

### X13 manifest binding enforcement — `51eb258`

- State: implementing.
- Behavior: when an immutable publication carries plugin identities, the data
  plane validates the catalog and table-format pair before constructing a runtime
  adapter. Unsupported or tampered bindings fail closed; legacy manifests without
  the new metadata retain their documented compatibility path.
- Green evidence: `tests/infrastructure/adapters/test_published_config.py` (15
  passed), Ruff, Ty, and `git diff --check` all pass.
- Remaining gaps: admitted registry routing, artifact/descriptor locks, legacy
  migration tooling, external catalogs/formats, and mixed-version rollout remain
  open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X13 runtime registry routing and X12 artifact
  lock verification.

### X11/X14 descriptor form boundary — `7e0e6f3`

- State: implementing.
- Behavior: both the service-side plugin contract and standalone SDK recursively
  bound descriptor form schemas by depth, nodes, object/array size, key/string
  length, and JSON-like value types. Remote references, script/HTML fields,
  executable URL content, and opaque values are rejected before UI rendering or
  plugin import.
- Green evidence: plugin contract and package tests pass (3), Ruff, Ty, and
  `git diff --check` all pass.
- Remaining gaps: authenticated descriptor/capability API, static descriptor
  loading, artifact locks, plugin routing, and browser accessibility evidence
  remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X14 descriptor API design after X13 runtime
  registry routing is wired.

### X14 descriptor-driven connection authoring — `daf84cf`

- State: implementing.
- Behavior: platform admins can read a bounded `/v1/plugins` descriptor payload
  containing admitted catalog/table-format metadata and intersected pair
  capabilities. The governance Connections view renders these capabilities and
  versions and now generates bounded configuration fields from the selected
  catalog descriptor, masking secret-reference inputs and validating required
  values before submission. The writable route remains explicitly limited to
  the admitted built-in Iceberg adapter; descriptors do not grant installation
  or authorization power. The built-in descriptor now names the actual `user`
  option consumed by the catalog adapter.
- Green evidence: plugin API/route tests (3), built-in plugin tests, Ruff, Ty,
  TypeScript, Vite build, UI lifecycle tests (2), and `git diff --check` pass.
- Remaining gaps: runtime registry wiring for external adapters, static
  descriptor/artifact locks, external REST/Parquet descriptors, backend field
  revalidation for plugin forms, and browser accessibility/IdP evidence remain
  open.
- Pickle compatibility: unchanged.
- Next permitted packet: wire admitted registry generations into data-plane
  startup before advertising external plugin pairs.

The descriptor's password field now matches the server's validated secret-reference
option name (`839c16f`); no raw credential field is exposed.

### X15 public conformance kit — `2d35c0d`

- State: implementing.
- Behavior: `packages/plugin-conformance` is independently importable with only
  the public plugin API and PyArrow. It supplies deterministic nested
  struct/list/map goldens, capability-negative checks, bounded task planning and
  output-schema checks, explicit cancellation checks, cleanup proof, optional
  expected-task coverage validation, skip semantics, and machine-readable
  pass/fail results. Deliberate negative tests prove missing capabilities,
  schema mutation, and duplicate task coverage are rejected. Plan iterators are
  bounded before materialization, declared output schemas are enforced for every
  batch, expired contexts fail early, and optional plugin cleanup runs on success
  or failure. Results include package/core/Arrow versions, capability matrix,
  failures, skips, and optional artifact identity for exact CI traceability.
- Green evidence: package tests (8), compileall, Ruff, and Ty pass using the
  package-only `PYTHONPATH` lane.
- Remaining gaps: conforming Iceberg integration, intentionally incorrect
  provider implementations, governed Flight/consumer lanes, and external
  distribution evidence remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: add deliberately incorrect task-coverage fixtures and
  provider lifecycle lanes before qualifying REST Iceberg.

### X12/X13 immutable runtime admission — `d62392e`

- State: implementing.
- Behavior: the plugin registry captures entry points and trusted built-ins in one
  reload generation. Factory loads use that captured generation instead of
  rescanning installed packages on request. Data-plane startup admits the
  qualified in-tree Iceberg catalog (`iceberg.sql`) and format (`iceberg`) pair;
  published explicit bindings are rejected when either identity is absent from
  the active snapshot. Legacy manifests without plugin metadata retain the
  documented compatibility path.
- Green evidence: `tests/plugin_platform/test_registry.py` (9 passed),
  `tests/plugin_platform/test_builtin_plugins.py` (1 passed),
  `tests/infrastructure/adapters/test_published_config.py` (16 passed), runtime
  identity tests, Ruff, Ty, and `git diff --check` pass.
- Remaining gaps: independent wheel descriptor/artifact locks, external plugin
  routing through SDK factories, legacy-manifest migration tooling, and live
  PostgreSQL/provider/browser/consumer gates remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: complete artifact/descriptor lock validation before
  adding external REST or manifest/Parquet providers.

### X12 descriptor and artifact lock validation — `df8c98b`

- State: implementing.
- Behavior: entry-point admission supports an optional static descriptor loader
  and validates kind, ID, API version, distribution, and version before import.
  Extended five-part locks can additionally pin the canonical descriptor digest
  and a deterministic digest of installed distribution files; malformed lock
  records fail closed. Existing three-part distribution/version/API locks remain
  compatible.
- Green evidence: `tests/plugin_platform/test_registry.py` (12 passed), Ruff, Ty,
  and `git diff --check` pass.
- Remaining gaps: production-generated lock files, clean wheel artifact
  provenance, editable-install detection, external SDK factories, and live
  provider/browser/consumer gates remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: generate and verify immutable lock artifacts in the build
  lane before qualifying an external provider.

### X12 build-time immutable lock generation — `e3160f7`

- State: implementing.
- Behavior: `build_plugin_lock` creates the exact five-part immutable lock from a
  validated entry point and static descriptor: distribution, version, API version,
  descriptor digest, and installed-artifact digest. It rejects descriptor identity
  mismatches and missing or conflicting entry-point provenance before a lock can be
  written, so generated locks cannot silently describe a different wheel.
- Green evidence: `tests/plugin_platform/test_registry.py` (13 passed), Ruff, Ty,
  and `git diff --check` pass.
- Remaining gaps: checked-in/generated lock-file workflow, clean-wheel provenance,
  editable-install rejection, external SDK factories, and live provider,
  browser, and consumer gates remain open. Pickle compatibility is unchanged.
- Next permitted packet: wire lock generation into the release/build lane and add
  a clean-wheel verification job before external provider qualification.

### X13 admitted catalog routing — `47c7c47`

- State: implementing.
- Behavior: published-config resolution passes the active plugin registry into
  catalog construction. Configured catalogs carry the qualified `iceberg.sql`
  identity, and the registry loads its admitted factory to construct the existing
  Iceberg adapter. Callers without a registry retain the exact compatibility
  path; no pickle task class or payload changed.
- Green evidence: catalog-registry, published-config, and built-in-plugin tests
  pass (23 combined), Ruff, Ty, and `git diff --check` pass.
- Remaining gaps: SDK-native external factory adapters, plugin/config revision
  persistence, legacy-manifest migration, real artifact locks, and live
  PostgreSQL/provider/browser/consumer gates remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: bind plugin instance/config revisions into publication
  records before adding external catalog or format packages.

### X13 persisted plugin bindings — `9d2789c`

- State: implementing.
- Behavior: published catalog and asset rows now carry nullable qualified plugin
  IDs and optional revisions through additive migration `20260913_0014`. The
  repository derives only the exact built-in mapping (`IcebergCatalog` →
  `iceberg.sql`, Iceberg backend → `iceberg`); unknown legacy rows remain
  nullable and use their existing compatibility interpretation. Published reads
  merge persisted IDs into the immutable manifest before data-plane admission.
- Green evidence: publication-store and published-config tests pass (19), Ruff,
  Ty, and `git diff --check` pass; SQLite migration head upgrades cleanly through
  the existing fixture setup.
- Remaining gaps: plugin instance/config revision semantics and CAS binding
  updates, dry-run migration tooling, external SDK factories, artifact locks, and
  live PostgreSQL/provider/browser/consumer gates remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: add revision/CAS validation for mutable asset bindings
  before external plugin onboarding.

### X06 schema-scoped synthetic identities — `336d2c3`

- State: implementing.
- Behavior: one shared identity routine preserves provider field IDs and derives
  bounded `synthetic:<schema-scope>:<path>` IDs for fields and collection
  children without provider IDs. The scope includes the complete semantic schema,
  so additions, removals, renames, type/nullability changes, and nested
  collection changes invalidate the admitted set and return the existing
  review-again path. Schema responses expose whether every field has a stable
  provider ID.
- Green evidence: published-config and schema-service tests pass (26), Ruff, Ty,
  and `git diff --check` pass.
- Remaining gaps: provider adapters must emit/consume these IDs, persisted
  stable-ID mode and schema-scope metadata, explicit additive/drop evolution
  policy, and PostgreSQL/browser/consumer evidence remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: bind stable-ID mode and schema-scope metadata into
  compiled publication records and review evidence.

### X06 persisted identity stability mode — `449d586`

- State: implementing.
- Behavior: compiled schema admissions now persist a `stable_ids` flag derived
  from their reviewed identities. The data plane rejects a publication marked
  stable when a live schema has missing provider IDs, while synthetic/legacy
  admissions remain on the explicit reapproval path.
- Green evidence: publication-compiler and published-config suites pass (53),
  Ruff, Ty, and `git diff --check` pass.
- Remaining gaps: plugin-native schema adapters, persisted scope digest and
  migration metadata, explicit additive/drop policy, PostgreSQL/provider/browser/
  consumer evidence remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: bind schema scope digest and plugin/config revisions to
  review evidence and publication identity.

### X06 UI schema reapproval state — `f552660`

- State: implementing.
- Behavior: the asset editor consumes the schema response's `stable_field_ids`
  signal and clearly warns operators when schema-scoped identities require
  reapproval. The warning is informational; authorization remains server-side.
- Green evidence: governance UI TypeScript compilation and Vite production build
  pass; UI lifecycle tests remain green.
- Remaining gaps: browser accessibility/IdP evidence and backend schema-scope
  migration remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X03 transaction evidence and X07 provider IO
  enforcement before onboarding external adapters.

### X07 canonical storage-path enforcement — `b19e1c8`

- State: implementing.
- Behavior: the shared path enforcer resolves local roots, normalizes URI scheme
  and authority, decodes and collapses traversal segments, and compares complete
  roots rather than string prefixes. Credential-bearing roots and query/fragment
  locations fail during configuration; metadata paths using encoded traversal are
  denied before scan execution.
- Green evidence: path-rule, Iceberg regression, and published-config suites pass
  (32), Ruff, Ty, and `git diff --check` pass.
- Remaining gaps: provider-returned IO/redirect/DNS enforcement, supervised
  cancellation, secret lifecycle, PostgreSQL and live deployment evidence remain
  open.
- Pickle compatibility: unchanged.
- Next permitted packet: add provider-returned location checks at catalog
  resolution and Iceberg task planning boundaries.

### X07 provider-returned location enforcement — `1e43e8e`

- State: implementing.
- Behavior: Iceberg catalog resolution now validates returned metadata locations
  and URI storage properties against the published path enforcer before exposing
  a table descriptor. Empty metadata locations and locations outside governed
  roots fail before schema/planning work.
- Green evidence: catalog-registry and path-rule suites pass (12), Ruff, Ty, and
  `git diff --check` pass.
- Remaining gaps: redirect/DNS enforcement, supervised provider cancellation,
  secret lifecycle, PostgreSQL and live deployment evidence remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: add bounded provider timeout/cancellation and test
  resource cleanup at discovery and schema boundaries.

The returned-location check also handles authority-wide URI roots (for example,
`s3://bucket/`) without weakening bucket isolation; the focused path suite now
covers that case.

X11 package check: `tests/plugin_platform/test_plugin_api_package.py` passes and
the package source compiles without importing the service distribution. Public
catalog options are bounded to printable JSON-like values before factory
invocation, and `TableHandle` metadata is bounded/inert before ticket
serialization. Building
the wheel with `uv build --wheel --no-build-isolation` is currently blocked because
the isolated environment has no `setuptools`; the normal online wheel/artifact
gate remains unverified and is not claimed by the test.

Integration boundary check after the migration and lock slices:
`UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane/test_assets_api.py tests/interfaces/control_plane/test_schema_api.py tests/plugin_platform -q`
passed after the X13 manifest metadata change. Ruff passed on every changed Python path and `git diff --check`
is clean. The expanded migration/schema/publication boundary suite also passes;
browser, PostgreSQL, Flight, consumer, and production lanes remain open.

After the cell lock-order slice, the same expanded boundary suite passed again,
including publication-store, policy-version, schema migration, plugin, and
published-config adapter tests.

The schema/review egress slice also passed schema-service, schema API, and policy
version tests with Ruff and Ty clean. Its provider-call regression proves a denied
catalog host is rejected before the loader is invoked.

The subsequent boundary run included catalog API coverage and passed at 100%.

The secret-resolution slice added schema and catalog discovery regressions and
passed their focused suites with Ruff clean. Secret values are supplied only to
provider calls and are not returned in API responses.

The evaluation byte-budget slice passed its focused helper/service checks with
Ruff and Ty clean; process-wide admission and live timeout/cancellation evidence
remain open.

### X09 UI lifecycle test lane — `499571f`

- State: implemented-unverified.
- Behavior: management and paginated history requests now share a small,
  dependency-free epoch guard so stale responses cannot overwrite a newer view.
  The governance UI exposes a Node built-in test script covering stale-response
  rejection and monotonic epoch advancement.
- Green evidence: `node --experimental-strip-types --test
  apps/governance-ui/tests/lifecycle.test.mjs` passed (2); direct TypeScript
  project check and Vite production build passed.
- Package-manager note: the pinned pnpm/Corepack wrapper could not run offline
  because Corepack attempted to fetch pnpm metadata; this is an environment gate,
  not a source failure.
- Remaining gaps: browser-level lifecycle, real IdP/session, accessibility, and
  full operation-state coverage remain open for X19/X22.
- Pickle compatibility: unchanged.
- Next permitted packet: complete X03 PostgreSQL barriers and X06 evolution rules;
  keep browser acceptance open until the real authenticated lane runs.

### X03 asset binding creation CAS — `c815aa7`

- State: implementing.
- Behavior: asset upsert now treats a missing binding as implicit revision zero.
  A nonzero `expected_revision` is rejected with a conflict before any row is
  inserted, closing a compare-and-set hole on first creation.
- Red/green evidence: the new API regression was red with a 200 response before
  the fix and passes with the stale-update and stale-grant regressions in
  `tests/interfaces/control_plane/test_assets_api.py` (3 passed).
- Exact checks: focused pytest and Ruff checks passed; `git diff --check` passed.
- Remaining gaps: independent PostgreSQL barriers/processes, publication
  rollback/idempotency under lost responses, and binding/grant race evidence
  remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X03 PostgreSQL/CAS transaction evidence, then
  X06 provider-derived and collection field identities.

### X06 collection schema identities — `1795312`

- State: implementing.
- Behavior: published schema admission now traverses list, large-list,
  fixed-size-list, and map children with canonical `$element`, `$key`, and
  `$value` path segments. Stable IDs and types for nested collection leaves are
  checked before an executable table format is returned.
- Red/green evidence: collection element/key/value acceptance and element-ID
  drift regressions in `tests/infrastructure/adapters/test_published_config.py`
  pass; Ruff and `git diff --check` pass.
- Remaining gaps: provider-derived identity rules for every backend, rename and
  addition policy, migration uniqueness proof, and PostgreSQL/browser/consumer
  acceptance remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X06 explicit schema-evolution policy tests,
  while X03 PostgreSQL transaction barriers remain an external gate.

### X06 manifest handle identity boundary — `a318b97`

- State: implementing.
- Behavior: manifest catalog handles are checked again by the Parquet format
  adapter before file access. Missing, malformed, reordered, or forged identity
  metadata is rejected; list, large-list, fixed-size-list, and map paths use the
  core collection markers.
- Green evidence: manifest plugin suite (9 tests), Ruff, Ty, and `git diff --check`
  pass.
- Remaining gaps: provider-native nested IDs for other adapters, persisted schema
  scope/evolution policy, PostgreSQL/browser/consumer acceptance, and clean wheel
  evidence remain open.
- Pickle compatibility: unchanged; only the pre-existing trusted task boundary is
  used.
- Next permitted packet: continue X06 explicit schema-evolution policy and X03
  PostgreSQL transaction-barrier evidence.

### X14 generic catalog onboarding copy — `079324e`

- State: implementing.
- Behavior: configured catalog cards render the persisted adapter identity, and
  the empty onboarding state no longer assumes Iceberg. The existing descriptor
  driven form and plugin-pair routing remain the source of selectable adapters.
- Green evidence: governance UI TypeScript build and lifecycle tests pass; diff
  checks pass.
- Remaining gaps: browser accessibility journeys, live provider discovery,
  activation semantics, and clean artifact evidence remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X03/X06 correctness work and X14 browser
  acceptance evidence.

### X12 stable schema descriptor claims — `102c768`

- State: implementing.
- Behavior: public format descriptors claiming stable field IDs are checked
  against the returned Arrow schema before planning or execution. A false claim
  fails closed at the plugin boundary.
- Green evidence: public adapter suite (6 tests), Ruff, Ty, and `git diff --check`
  pass.
- Remaining gaps: installed wheel compatibility, provider-native nested identity
  evidence, browser/consumer runs, and live release artifacts remain open.
- Pickle compatibility: unchanged; no serializer or task payload format changed.
- Next permitted packet: continue X12/X15 installed-artifact and provider
  conformance evidence after Phase A PostgreSQL correctness gates.

### X05 public schema budget boundary — `c8d2450`

- State: implementing.
- Behavior: the public plugin bridge applies the shared Arrow schema traversal
  and encoding budgets before stable-ID checks, projection, or task planning.
- Green evidence: public adapter suite (7 tests), Ruff, Ty, and `git diff --check`
  pass, including a schema deeper than the configured limit.
- Remaining gaps: live route budget coverage, clean wheel compatibility, provider
  conformance, browser/consumer evidence, and release artifact gates remain open.
- Pickle compatibility: unchanged; schema validation occurs before the existing
  trusted task serializer is involved.
- Next permitted packet: continue X03/X06 correctness and X12/X15 artifact
  conformance work.

### X04 fixed-size collection evaluation — `808241a`

- State: implementing.
- Behavior: synthetic fixtures generate valid fixed-size-list values and expose
  their top-level and nested element paths consistently with variable/large lists.
- Green evidence: evaluation service suite (5 tests), Ruff, Ty, and
  `git diff --check` pass.
- Remaining gaps: full scalar/mask golden matrix, live schema/provider evidence,
  browser and Flight consumer runs remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X04/A05–A06 golden evaluation coverage and
  X03/X06 transaction/evolution evidence.

### X12 optional SDK startup boundary — `0de9d98`

- State: implementing.
- Behavior: the catalog registry lazily imports the public-plugin bridge only for
  non-built-in catalog IDs. A minimal installation can start and serve built-in
  Iceberg without the external SDK; selecting an external plugin still fails
  closed until its admitted wheel is installed.
- Green evidence: catalog registry and public adapter suites (15 tests), Ruff,
  Ty, and direct minimal-environment import probe pass.
- Remaining gaps: clean wheel dependency declarations, external plugin artifact
  compatibility, provider conformance, and release image evidence remain open.
- Pickle compatibility: unchanged; import routing occurs before the existing
  trusted task serializer.
- Next permitted packet: continue X12 clean artifact verification and X15
  provider/consumer conformance.

### X21 operator CLI discoverability — `7dd89d3`

- State: implementing.
- Behavior: `dal-obscura --help` is available without runtime credentials,
  database connectivity, or a published cell and documents the required and
  common `DAL_OBSCURA_*` variables.
- Green evidence: data-plane CLI tests (2), Ruff, Ty, and `git diff --check` pass.
- Remaining gaps: installed console-script artifact verification, live startup,
  and production deployment evidence remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X21 test-efficiency/capacity evidence and X22
  exact artifact gates.

### X22 installed plugin routing lane — `54b6dc1`

- State: implementing.
- Behavior: the release workflow builds exact server, SDK, conformance,
  manifest/Parquet, and REST wheels and exercises the installed plugin bridge in
  an isolated environment before the container job can run.
- Green evidence: workflow YAML parses and `git diff --check` passes locally;
  hosted execution remains required for wheel/build proof.
- Remaining gaps: hosted artifact digests, browser/provider/consumer/recovery
  gates, vulnerability/SBOM evidence, and architecture matrix execution remain
  open.
- Pickle compatibility: unchanged; the lane exercises the existing ticket path
  without changing serialization.
- Next permitted packet: run the hosted clean-artifact and security lanes, then
  continue X19/X20 recovery and lifecycle evidence.

### X06 duplicate identity rejection — `5c17d04`

- State: implementing.
- Behavior: published schema admission rejects a live provider schema that
  reuses a stable field ID anywhere in its nested tree, preventing ambiguous
  path resolution and dictionary overwrite from becoming implicit access.
- Red/green evidence: the duplicate-ID regression in
  `tests/infrastructure/adapters/test_published_config.py` was red before the
  guard and passes with the collection identity suite (3 focused tests passed);
  Ruff passed.
- Remaining gaps: provider-specific identity guarantees, explicit rename/add
  policy, and PostgreSQL/browser/consumer acceptance remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X06 evolution policy and X03 PostgreSQL
  transaction barriers.

### X06 reviewed selection expansion — `5f2e196`

- State: implementing.
- Behavior: when admitted schema identities exist, publication compilation
  expands wildcard and parent column selections to the reviewed canonical leaf
  paths. Mask entries receive the same expansion, so later schema additions
  cannot enter through an unresolved wildcard or parent grant.
- Red/green evidence: compiler regressions for wildcard plus mask expansion and
  nested-parent expansion pass; the complete `tests/control_plane/test_publication_compiler.py`
  module passed (34 tests), with Ruff and Ty clean.
- Remaining gaps: review-time population of the admitted set from every backend,
  explicit rename/addition policy, and PostgreSQL/browser/consumer acceptance
  remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X06 schema evolution policy and X03
  PostgreSQL transaction barriers.

### X22 governance UI test gate — `7db7a28`

- State: implementing.
- Behavior: the exact CI governance-UI job runs the dependency-free lifecycle
  test script before the TypeScript/Vite production build, so stale-response
  fencing cannot regress silently behind a green bundle build.
- Red/green evidence: the CI contract regression was red before the workflow
  step was added and passes with `tests/architecture/test_ci_workflow.py` (3
  passed); direct Node lifecycle tests, TypeScript, and Vite build remain green.
- Remaining gaps: clean pnpm install, artifact digest/provenance, browser/IdP,
  accessibility, plugin wheel, provider, consumer, recovery, and production
  acceptance gates remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X03/X06 correctness work; X22 remains open
  until the CI artifact lanes run in a clean environment.

### X06 admission digest verification — `4c7c76d`

- State: implementing.
- Behavior: the data-plane registry recomputes and verifies the immutable
  admitted-schema field digest before matching any live identities. Tampered or
  malformed digest metadata now fails closed with a review-again error.
- Red/green evidence: the admission-digest tamper regression was red before the
  guard and passes with numeric-ID, collection, duplicate-ID, and drift tests;
  Ruff and Ty passed on the changed adapter.
- Remaining gaps: end-to-end review-time admission population, provider identity
  guarantees, schema rename/addition policy, and PostgreSQL/browser/consumer
  evidence remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X06 evolution policy and X03 PostgreSQL
  transaction barriers.

### X10 authoring controls — `9fac614`

- State: implemented-unverified.
- Behavior: the governance UI now exposes explicit rule precedence controls and
  a JSON principal-condition editor. Invalid condition text remains local,
  announces an error, and blocks saving; valid conditions use the existing
  draft/review/publish workflow.
- Green evidence: direct TypeScript check, Vite production build, and Node
  lifecycle tests passed; the expanded Python boundary suite remains green.
- Remaining gaps: browser journey and accessibility evidence, typed mask UX
  coverage, active-generation/config activation proof, and real IdP/session
  acceptance remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X10 management/accessibility coverage while
  X03 PostgreSQL and X06 provider-evolution gates remain open.

### X10 typed mask values — `0e049c0`

- State: implemented-unverified.
- Behavior: the UI keeps `keep_last` values as bounded integers and preserves
  default-mask JSON scalar types instead of coercing booleans and numbers into
  strings. Invalid values remain subject to the control-plane validator before
  review/publication.
- Green evidence: direct TypeScript check, Vite production build, Node lifecycle
  tests, and `git diff --check` passed.
- Remaining gaps: browser/a11y authoring journeys and server-side golden mask
  value/type coverage remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X10 browser/accessibility coverage and X04
  evaluation goldens; PostgreSQL and external consumer gates remain open.

### X10 workspace generation management — `b1ee51e`

- State: implemented-unverified.
- Behavior: authenticated platform administrators can list staged generations,
  create a snapshot from the current workspace draft, and explicitly activate a
  selected generation through `/v1/workspace/publications`. The Connections UI
  displays serving versus staged state and exposes activation only to admins.
- Green evidence: workspace API publication lifecycle and admin-scope tests
  pass; direct TypeScript check, Vite production build, Node lifecycle tests,
  Ruff, and `git diff --check` pass.
- Remaining gaps: activation rollback/impact UX, browser/a11y journeys, real
  IdP/session evidence, PostgreSQL transaction barriers, and production artifact
  validation remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X10 activation conflict/rollback coverage and
  X03 PostgreSQL transaction barriers.

### X10 generation activation CAS — `515cf37`

- State: implementing.
- Behavior: workspace generation activation accepts an optional expected active
  publication ID and uses the existing atomic compare-and-set database path. The
  UI sends the serving generation ID when promoting a staged snapshot, so stale
  operator views receive a conflict instead of overwriting newer state.
- Red/green evidence: workspace activation lifecycle and stale-generation tests
  pass; Ruff, TypeScript, and `git diff --check` pass.
- Remaining gaps: activation rollback/impact display, PostgreSQL barrier/process
  evidence, browser/a11y journeys, and production artifact validation remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X10 rollback/impact semantics and X03
  PostgreSQL transaction barriers.

### X10 workspace publication audit — `29bb743`

- State: implementing.
- Behavior: workspace snapshot creation and generation activation now append
  tenant-scoped audit events in the same transaction, preserving the
  authenticated actor identity, request correlation, publication counts, and
  manifest hash. Stale compare-and-set activation failures emit no success
  event because the transaction aborts before the audit write.
- Green evidence: workspace publication lifecycle, stale-generation, and audit
  visibility tests pass; Ruff, Ty, and `git diff --check` pass on changed paths.
- Remaining gaps: PostgreSQL rollback and barrier/process evidence, browser and
  accessibility journeys, real IdP/session validation, and production artifact
  validation remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X03 PostgreSQL transaction barriers and X10
  activation impact/error UX.

### X10 publication impact display — `af40e51`

- State: implementing.
- Behavior: staged and serving workspace generations now expose immutable asset
  and catalog counts plus creation time. The Connections UI shows this impact
  beside each generation before an administrator activates it.
- Green evidence: workspace publication API tests pass with scope-count and
  timestamp assertions; direct TypeScript check, Vite production build, Node
  lifecycle tests, Ruff, and `git diff --check` pass.
- Remaining gaps: safe rollback and failed-activation impact UX, browser and
  accessibility journeys, real IdP/session validation, PostgreSQL barriers, and
  production artifact validation remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X10 activation rollback/error UX and X03
  PostgreSQL transaction barriers.

### X10 activation conflict UX — `395122d`

- State: implementing.
- Behavior: the Connections UI distinguishes a stale generation compare-and-set
  conflict (HTTP 409) from other activation failures and tells the operator to
  refresh before retrying, while preserving the serving generation.
- Green evidence: direct TypeScript check, Vite production build, and
  `git diff --check` pass.
- Remaining gaps: browser/a11y proof of the interaction, rollback impact
  confirmation, real IdP/session validation, PostgreSQL barriers, and release
  artifact validation remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X03 PostgreSQL transaction barriers and X10
  staged runtime/auth activation semantics.

### X10 settings generation state — `90934cf`

- State: implementing.
- Behavior: the Settings UI now reports the serving configuration generation,
  its asset/catalog scope, and the number of staged generations waiting for
  explicit activation. Runtime and identity edits therefore have visible
  staged-versus-serving state instead of implying an immediate worker change.
- Green evidence: direct TypeScript check, Vite production build, and
  `git diff --check` pass.
- Remaining gaps: backend revisioned runtime/auth activation and bootstrap
  recovery, browser/a11y proof, PostgreSQL barriers, real IdP/session evidence,
  and release artifact validation remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: implement staged runtime/auth activation semantics or
  continue X03 PostgreSQL transaction barriers.

### X03/X10 workspace audit boundary — `ea01654`

- State: implementing.
- Behavior: workspace publication audit emission is now explicit to the
  workspace activation delegate. Asset-level policy publication continues to
  use its existing asset audit path and cannot be mislabeled as a workspace
  generation transition.
- Green evidence: asset publication regression and workspace lifecycle tests
  pass; Ruff, Ty, and `git diff --check` pass on changed paths.
- Remaining gaps: PostgreSQL barrier/process evidence, complete operation and
  audit rollback matrix, browser/a11y proof, real IdP/session validation, and
  production artifact validation remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X03 PostgreSQL transaction barriers and X10
  staged runtime/auth activation semantics.

### X10 staged settings audit — `96cc34e`

- State: implementing.
- Behavior: runtime ticket-limit and authentication-provider draft updates now
  record the authenticated actor, workspace tenant, request correlation, and
  bounded non-secret change details. Settings remain draft state until a
  publication is explicitly created and activated.
- Green evidence: settings API tests assert audit actions, actor identity, and
  redacted details; Ruff and Ty pass on changed paths.
- Remaining gaps: revisioned settings activation/rollback and bootstrap
  recovery, browser/a11y proof, PostgreSQL barriers, real IdP/session evidence,
  and production artifact validation remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X03 PostgreSQL transaction barriers and X10
  staged runtime/auth activation semantics.

### X10 catalog draft audit — `9cbb9e7`

- State: implementing.
- Behavior: catalog connection updates now record actor-scoped workspace audit
  events with catalog identity, adapter module, and sorted option keys. URI,
  password, and secret-reference values never enter audit details.
- Green evidence: catalog API suite passes with audit redaction assertions;
  Ruff and Ty pass on changed paths.
- Remaining gaps: revisioned catalog activation/rollback and provider health
  evidence, browser/a11y proof, PostgreSQL barriers, real IdP/session evidence,
  and production artifact validation remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X03 PostgreSQL transaction barriers and X10
  staged runtime/auth activation semantics.

The combined control-plane, catalog, schema, publication, migration, plugin, and
published-config boundary suite passed at 100% after the budget change.

The post-audit boundary run passed at 100% across control-plane, asset, catalog,
schema, policy-version, workspace, settings, plugin, migration, and
published-config tests. Direct TypeScript and Vite production builds remain
green; the local sandbox cannot bind an additional listener because localhost
port 5173 is already occupied by the existing UI process.

The publication-impact aggregation was then explicitly typed and passed its
repository Ty/Ruff checks; full-project Ty still reports unrelated existing
fixture/discovery diagnostics and remains an open release gate.

Test-harness hygiene removed a duplicate `_client()` definition from the shared
control-plane helper. The complete control-plane interface suite passed after
the cleanup; no runtime or pickle paths changed.

Project-wide Ty diagnostics were then cleared by annotating bounded namespace
traversal and demo/plugin fixture mappings and removing stale test suppressions.
`uv run --no-sync ty check`, Ruff, and the affected focused suites all pass.

Full pytest exposed and fixed a route-inventory drift for the workspace
publication endpoints. The remaining full-suite failures are environment-gated
Flight socket binds, heavyweight benchmark subprocesses, and connector fixture
assumptions; they remain release evidence gaps rather than being suppressed.

The Flight health helper no longer logs readiness exception text. A sentinel
provider-error regression confirms the secret is absent from logs while the
stable unavailable error remains intact; focused Ruff/Ty checks pass. The
existing bind-failure test remains unexecutable in this sandbox because socket
bind is denied by the environment.

Ticket cleanup now emits an opaque warning instead of an exception traceback,
keeping asynchronous database/provider errors out of logs. Ruff and Ty pass on
the changed path; pickle and ticket payload formats are unchanged.

The production Compose reference now injects distinct migration,
control-plane, and data-plane PostgreSQL URLs, with least-privilege role
provisioning documented and enforced by the deployment contract test. This is
configuration hardening only; live role grants and PostgreSQL race evidence
remain unexecuted release gates.

The bundled PostgreSQL reference now creates those three roles on first volume
initialization, sets unique passwords from dedicated secrets, and applies
least-privilege default table/sequence grants. Shell syntax and deployment
contract tests pass; live role permission probes and migration/runtime startup
remain unexecuted.

`docker compose --env-file deployment/production/.env.example -f
deployment/production/compose.yaml config --quiet` also passes, confirming the
role-initializer mount and required variables render as valid Compose.

The initializer now grants migrator `USAGE, CREATE` on `public` and application
roles only `USAGE`, allowing clean-volume migrations while keeping application
schema changes outside their privileges. Shell, contract, and Compose checks
remain green.

Production Compose healthchecks now probe `/readyz` for both control-plane and
data-plane services, so database/runtime readiness gates service health instead
of liveness alone. Deployment contract and Compose-render checks pass; live
container startup remains unexecuted.

The direct-Arrow bounds regression and Ty/Ruff checks passed in
`3263645`; the full schema byte-budget and every-entry-route acceptance matrix
remains open.

### X04 synthetic fixture semantics — `4ab9031`

- State: implemented-unverified.
- Behavior: policy evaluation now distinguishes omitted rows from an explicit
  zero-row input, preserves the authorized output schema for empty results,
  validates supplied rows at the Arrow boundary, and records a digest of the
  exact schema-bound fixture. Generated fixtures cover typed dates, timestamps,
  times, decimals, binary values, nested structs, and non-string map keys;
  unsupported types fail with a safe validation error.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/control_plane/test_evaluation_service.py
  tests/interfaces/control_plane/test_schema_api.py -q` passed (8); Ruff and
  Ty passed on changed paths; TypeScript remains unchanged from the prior UI
  slice.
- Environment/dependency: Darwin arm64, repository `.venv`, no external
  catalog, IdP, Flight server, or customer data.
- Pickle compatibility: no pickle modules, serializers, or payloads changed.
- Remaining gaps: full mask/type golden matrix, provider-call assertions,
  process-wide evaluation admission, real Iceberg/Flight parity, and the
  PostgreSQL race gates remain open.
- Next permitted packet: X03 PostgreSQL CAS/barrier slice, then X06 schema
  evolution rules.

Follow-up regression coverage in `1c6c97e` asserts invalid user-supplied rows
return the stable redacted validation response at the HTTP boundary.

### X03 asset revision preconditions — `6aac008`

- State: implementing.
- Behavior: asset bindings, owners, grants, and admitted schema metadata now
  advance a monotonic revision. API callers may send `expected_revision`; after
  the publication row lock, stale values fail with HTTP 409 and cannot overwrite
  the committed value. Asset detail exposes the current revision and the UI sends
  it for owner and grant edits.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/interfaces/control_plane/test_assets_api.py
  tests/control_plane/test_asset_mutation_locks.py -q` passed (16); migration
  tests passed after updating the head assertion; Ruff, Ty, TypeScript, and
  `git diff --check` passed.
- Migration: additive `20260912_0012` adds `assets.revision` with a zero default.
- Pickle compatibility: no pickle modules, serializers, or payloads changed.
- Remaining gaps: PostgreSQL barrier-controlled races, operation/audit atomicity,
  and independent multi-process evidence remain required before acceptance.
- Next permitted packet: X03 PostgreSQL CAS/barrier slice, then X06 admitted
  schema identity and evolution rules.

### X03 review-token generation binding — `9fbeb51`

- State: implementing.
- Behavior: signed review evidence now captures the governed asset revision;
  publication rejects a token after binding, owner, grant, or admitted-schema
  metadata changes, even when the policy draft and live schema are unchanged.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/interfaces/control_plane/test_schema_api.py -q` passed (6), with
  the stale metadata token regression included; Ruff and Ty passed on changed
  paths.
- Pickle compatibility: unchanged.
- Remaining gaps: PostgreSQL barrier-controlled races and transaction recovery
  evidence remain open.
- Next permitted packet: X03 PostgreSQL CAS/barrier slice, then X06 evolution
  rules.

### X03 publication rollback evidence — `101fd00`

- State: implementing.
- Behavior: an injected failure after the initial publication pointer update
  rolls back the immutable publication and audit event together; no partial
  generation remains visible to a retry.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/interfaces/control_plane/test_policy_versions_api.py::test_publication_failure_after_activation_rolls_back_audit_and_generation -q`
  passed; Ruff passed on the changed test.
- Scope: SQLite transaction evidence only. PostgreSQL barrier interleavings,
  idempotency replay after a lost response, and multi-process grant/binding
  races remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: PostgreSQL X03 barriers, then X06 evolution rules.

### X22 governance UI CI gate — `7fcf6b9`

- State: implementing.
- Behavior: CI installs `apps/governance-ui` from its pinned pnpm lockfile,
  runs the TypeScript build, and blocks image scanning/publication until that
  lane passes.
- Green evidence: local `pnpm --dir apps/governance-ui check` and `build` pass;
  workflow syntax was updated without changing runtime behavior.
- Remaining gaps: exact tested image/server/UI digest promotion, browser and
  accessibility journeys, provider/consumer/recovery lanes, and mandatory
  security-failure blocking remain open under X22.
- Pickle compatibility: unchanged.
- Next permitted packet: X03 PostgreSQL barrier evidence and X06 evolution rules;
  X22 remains a later release gate.

### X11 contract identity validation — `7e4e34d`

- State: implementing.
- Behavior: both the service-side and independently packaged plugin contracts
  validate bounded IDs, capability metadata, catalog generations, and canonical
  schema digest shape before admission or execution.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/plugin_platform/test_plugin_contract_validation.py
  tests/plugin_platform/test_registry.py tests/plugin_platform/test_plugin_api_package.py -q`
  passed (9); Ruff and Ty passed on changed Python paths.
- Remaining gaps: clean-environment wheel build, static descriptor/artifact lock
  verification, external package admission, and service compatibility matrix.
- Pickle compatibility: unchanged.
- Next permitted packet: complete X03 PostgreSQL barriers and X06 evolution rules;
  do not advertise external plugins from this slice alone.

### X06 runtime schema admission — `32a7fa0`

- State: implementing.
- Behavior: persisted admitted schema fields are included in immutable compiled
  asset manifests with a canonical digest. The published data-plane catalog
  registry checks each admitted path and stable field ID against the live Arrow
  schema before returning an executable table format, normalizing numeric
  PyIceberg IDs and common Iceberg type aliases, and rejecting rebound or removed
  fields so schema drift cannot expand access.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/control_plane/test_publication_compiler.py
  tests/infrastructure/adapters/test_published_config.py
  tests/control_plane/test_policy_authorization.py
  tests/control_plane/test_schema_service.py -q` passed (53); Ruff and Ty passed
  on changed paths.
- Compatibility: publications without an admitted schema remain supported;
  pickle modules, serializers, and task payloads are unchanged.
- Remaining gaps: provider-derived IDs for all collection nodes, typed collection
  path encoding, legacy-row migration/uniqueness proof, and PostgreSQL/browser/
  consumer acceptance evidence remain open.
- Next permitted packet: X03 PostgreSQL barrier evidence, then X06 collection
  identity and explicit schema-evolution policy tests.

Follow-up compatibility regression in `ccb00bc` proves a namespaced admitted
`iceberg:1` identity matches PyIceberg's numeric `PARQUET:field_id=1` metadata,
while a rebound numeric ID remains rejected.

Post-normalization boundary verification: the expanded control-plane, migration,
plugin, publication, schema, and published-config suite passed at 100% with
`UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane
tests/interfaces/control_plane/test_assets_api.py tests/interfaces/control_plane/
test_schema_api.py tests/interfaces/control_plane/test_policy_versions_api.py
tests/plugin_platform tests/common/config_store/test_schema_migrations.py
tests/infrastructure/adapters/test_published_config.py -q`.

### X03 replacement rollback regression — `5420249`

- State: implementing.
- Behavior: an injected audit failure while replacing an already-active policy
  generation leaves the prior active generation, history, and audit count intact.
  This complements the initial-publication rollback test and exercises the same
  request transaction through the replacement path.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/interfaces/control_plane/test_policy_versions_api.py::test_replacement_publication_failure_preserves_previous_active_generation -q`
  passed; Ruff passed on the changed test.
- Scope: SQLite transaction regression only. PostgreSQL barriers, lost-response
  idempotency replay, and multi-process grant/binding races remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: PostgreSQL X03 barrier evidence, then X06 collection
  identity and explicit schema-evolution policy tests.

### X03 publication lock ordering — `75f753b`

- State: implementing.
- Behavior: active-generation activation now locks the owning cell row after the
  selected asset row and before pointer replacement. This gives PostgreSQL
  publication, grant, and binding operations a consistent asset-then-cell lock
  order and serializes concurrent first activations in one cell.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/control_plane/test_publication_store.py
  tests/interfaces/control_plane/test_policy_versions_api.py -q` passed (all);
  Ruff and `git diff --check` passed.
- Remaining gaps: real PostgreSQL barrier interleavings, rollback under process
  failure, idempotency replay after lost responses, and multi-process grant/
  binding evidence remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: PostgreSQL X03 barrier evidence, then X06 collection
  identity and explicit schema-evolution policy tests.

### X06 legacy identity migration — `5c0f89d`

- State: implementing.
- Behavior: migration `20260912_0013` backfills deterministic `legacy:` field IDs
  and canonical single-name paths for rows created before schema identity columns
  existed. The preceding asset revision migration retains its non-null server
  default so SQLite and PostgreSQL upgrades preserve existing asset rows.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/common/config_store/test_schema_migrations.py -q` passed (7);
  Ruff passed on migration and test paths.
- Remaining gaps: provider-derived IDs for all collection nodes, typed collection
  path encoding, and explicit rename/addition evolution policy remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: PostgreSQL X03 barrier evidence, then X06 collection
  identity and schema-evolution policy tests.

Local parity check: the running development database at
`/private/tmp/dal-obscura-ui-dev-8821.db` was upgraded with
`DAL_OBSCURA_DATABASE_URL=sqlite+pysqlite:////private/tmp/dal-obscura-ui-dev-8821.db
uv run --no-sync dal-obscura-migrate upgrade`, which reported
`config-store schema upgraded to head`. Direct sandbox HTTP probing is blocked
by the local network boundary; no production readiness claim is made from the
running-process probe.

### X03 delegated grant authorization lock — `88704f6`

- State: implementing.
- Behavior: delegated grant-manager authorization now takes the publication asset
  row lock before reading current authority or replacing the full grant list. The
  authorization snapshot and revisioned write therefore share one transaction
  generation, preventing a concurrent revocation from being bypassed.
- Green evidence: `tests/control_plane/test_asset_grant_authorization.py` and
  `tests/interfaces/control_plane/test_assets_api.py` passed (16); Ruff, Ty, and
  `git diff --check` passed.
- Remaining gaps: PostgreSQL barrier/process evidence and full idempotency/recovery
  drills remain open. Pickle compatibility is unchanged.

### X07 bounded catalog option payloads — `b969dba`

- State: implementing.
- Behavior: catalog options are validated as bounded JSON before secret resolution
  or provider calls, with fixed limits for nesting, node count, object keys,
  collection width, key length, and string length. Unsupported runtime values fail
  closed.
- Green evidence: catalog API tests passed (12), Ruff, Ty, and `git diff --check`
  passed.
- Remaining gaps: typed per-provider option schemas, DNS/redirect enforcement, and
  production secret-provider lifecycle evidence remain open. Pickle compatibility
  is unchanged.

### X08 atomic catalog registry reload — `5013f99`

- State: implementing.
- Behavior: catalog adapters are fully constructed before a locked two-field swap of
  executable catalogs and configuration metadata. A failed factory leaves the prior
  generation intact; readers take the same lock when observing or resolving it.
- Green evidence: catalog registry tests passed (8), Ruff, Ty, and
  `git diff --check` passed.
- Remaining gaps: provider transport cancellation/timeouts, multi-worker capacity,
  and external registry-generation evidence remain open.

### X12 static wheel descriptor boundary — `5e640bc`

- State: implementing.
- Behavior: qualified distributions may ship a bounded `dal_obscura-plugin.json`
  descriptor. The loader reads metadata without importing factory code, binds kind
  and plugin ID to the entry point, derives distribution provenance from the wheel,
  rejects unknown fields/unreadable or oversized files, and preserves fail-closed
  behavior for malformed descriptors.
- Green evidence: `tests/plugin_platform/test_registry.py` (16 passed), Ruff, Ty,
  and `git diff --check` passed.
- Remaining gaps: generated lock-file workflow, clean-wheel artifact provenance,
  external SDK factories, and live provider/browser/consumer gates remain open.

### X15 bounded conformance output — `ab32d53`

- State: implementing.
- Behavior: public conformance checks consume record batches incrementally and stop
  at fixed batch and row budgets instead of materializing an unbounded iterable.
  Format execution checks cancellation before every task, with regression coverage
  for cancellation between tasks.
- Green evidence: package conformance tests passed (10); package-only Ruff, Ty, and
  `git diff --check` passed.
- Remaining gaps: deliberately incorrect provider fixtures, conforming Iceberg and
  consumer lanes, and external wheel evidence remain open.

### X12 static descriptor error normalization — `8ecefcb`

- State: implementing.
- Behavior: descriptor reads now normalize missing, unreadable, undecodable, and
  malformed metadata into the registry's fail-closed plugin admission error before
  any factory import. This keeps filesystem/provider-specific exceptions out of the
  transport boundary.
- Green evidence: registry tests passed (16), Ruff, Ty, and `git diff --check` passed.
- Remaining gaps: clean-wheel provenance and external distribution evidence remain
  open. Pickle compatibility is unchanged.

### X12 extended lock admission — `0d727e9`

- State: implementing.
- Behavior: five-part immutable plugin locks admit static descriptors by default,
  while legacy three-part locks retain their explicit compatibility fallback. The
  lock therefore covers descriptor identity without requiring a code import during
  static validation.
- Green evidence: registry tests passed (16), Ruff, Ty, and `git diff --check` passed.
- Remaining gaps: generated lock artifacts and clean-wheel installation evidence
  remain open.

### X07 finite catalog options — `9a940a8`

- State: implementing.
- Behavior: bounded catalog option validation rejects non-finite numeric values in
  addition to depth, node, collection, key, string, and JSON-type limits, before
  secret resolution or provider calls.
- Green evidence: catalog option tests passed, Ruff, Ty, and `git diff --check` passed.
- Remaining gaps: provider-specific schemas, network egress enforcement, and
  production secret-provider lifecycle evidence remain open.

### X13 legacy plugin binding migration — `9ed158e`

- State: implementing.
- Behavior: `dal-obscura-migrate plugin-bindings` deterministically classifies
  published legacy catalog/asset rows as bound, exactly migratable, or unsupported.
  `--apply` fills only exact built-in Iceberg identities inside one transaction and
  leaves unsupported rows untouched for operator review.
- Green evidence: migration CLI and config-store tests passed (5), Ruff, Ty, and
  `git diff --check` passed.
- Remaining gaps: external plugin mappings, runtime registry routing, mixed-version
  rollout, PostgreSQL migration evidence, and end-to-end manifest migration remain
  open. Pickle compatibility is unchanged.

### X12 safe lifecycle status — `6fe4596`

- State: implementing.
- Behavior: the authenticated plugin inventory reports enabled, not-installed, and
  incompatible states for allowlisted or built-in IDs. Status collection reads only
  entry-point metadata and never imports a factory or enumerates unapproved IDs.
- Green evidence: registry and plugin API tests passed (19), Ruff, Ty, and
  `git diff --check` passed.
- Remaining gaps: clean-wheel artifact provenance, external lifecycle transitions,
  and live browser/provider gates remain open.

### X19 OIDC fixture boundary correction — `69055e1`

- State: implementing.
- Behavior: the heavyweight connector fixture now persists only a remote JWKS URL,
  matching the production OIDC contract. Static JWKS material remains rejected at
  the control-plane boundary; local fixture signing is retained solely for test
  token generation.
- Green evidence: connector fixture tests passed (3), Ruff and `git diff --check`
  passed.
- Remaining gaps: live IdP, TLS, browser, and production PostgreSQL evidence remain
  open. Pickle compatibility is unchanged.

### X22 plugin package type-check wiring — `463c759`

- State: implementing.
- Behavior: CI's root Ty invocation includes the public plugin API and conformance
  package source roots, so independently buildable package imports are checked in
  the same quality lane without requiring unpublished wheels in the environment.
- Green evidence: CI workflow contract tests, full Ruff, Ty with both extra search
  paths, and `git diff --check` passed.
- Remaining gaps: exact wheel/image digests, external artifact, provider, consumer,
  browser, and mandatory-security gates remain open.

### X15 standalone wheel CI qualification — `d4f81f0`

- State: implementing.
- Behavior: CI builds the public plugin API and conformance wheels independently,
  installs the exact artifacts into a clean virtual environment, executes the
  conformance tests, and makes the container job depend on that result.
- Green evidence: workflow contract tests, Ruff, and `git diff --check` passed.
- Remaining gaps: signed artifact/image digests, deliberately bad external fixtures,
  provider, consumer, and browser gates remain open.

### X14 lifecycle status in Connections — `50d0181`

- State: implementing.
- Behavior: the UI fetches the authenticated plugin state inventory and displays
  enabled, not-installed, or incompatible adapter status next to the admitted
  descriptor and configuration surfaces. No installation or authority controls are
  exposed.
- Green evidence: Governance UI TypeScript build, Vite production build, lifecycle
  tests (2), and `git diff --check` passed.
- Remaining gaps: browser-driven authentication/authorization, external plugin
  routing, and production provider/consumer gates remain open.

### X03 publication authorization lock ordering — `229cd6d`

- State: implementing.
- Behavior: publication capability authorization is evaluated after the asset row
  lock, using the same transaction generation as grant revocation and candidate
  activation. This closes the stale pre-lock authorization window.
- Green evidence: ordering regression, idempotency publication regression, Ruff,
  Ty, and `git diff --check` passed.
- Remaining gaps: real PostgreSQL barrier/process interleavings, rollback after
  process failure, and full recovery evidence remain open.

### X03 review/provider lock boundary — `4528943`

- State: implementing.
- Behavior: review issuance authorizes on the locked asset generation. Publish
  validates live provider schema before locking, then performs a no-I/O evidence and
  authorization recheck after locking before activation.
- Green evidence: policy-version API tests (12), Ruff, Ty, and `git diff --check`
  passed.
- Remaining gaps: PostgreSQL barriers/processes, provider change races, rollback,
  idempotency recovery, and live deployment evidence remain open.

### X03 post-lock policy mutation authorization — `20b579c`

- State: implementing.
- Behavior: shared policy replacement and personal draft save/restore perform edit
  capability checks after locking the publication asset row, so revocations that
  commit before the mutation are observed before any write.
- Green evidence: policy authorization, draft, grant, and publication tests passed;
  Ruff, Ty, and `git diff --check` passed.
- Remaining gaps: PostgreSQL multi-process barriers, rollback/idempotency recovery,
  and live deployment evidence remain open.

### X03 service-boundary grant authorization — `f320ed9`

- State: implementing.
- Behavior: grant replacement enforces the caller's grant capability inside the
  application service after acquiring the asset lock, preserving the same invariant
  for non-HTTP callers and background workflows.
- Green evidence: asset grant API/authorization tests, Ruff, Ty, and
  `git diff --check` passed.
- Remaining gaps: PostgreSQL interleavings, transaction recovery, and production
  identity evidence remain open.

### X08 bounded discovery admission — `ac30712`

- State: implementing.
- Behavior: catalog discovery admits at most eight concurrent process-local
  operations, fails promptly when saturated, and releases its slot on success,
  cancellation, deadline, or provider failure. Existing page, namespace, table,
  and deadline bounds remain enforced inside the admitted operation.
- Green evidence: catalog discovery tests (7), Ruff, Ty, and `git diff --check`
  passed.
- Remaining gaps: per-session limits, cross-worker aggregate capacity, provider
  transport termination, and live workload evidence remain open.

## Latest evidence entry

Packet/slice: X05/X06/X07/X10/X19/X22 bounded schema, nested provider identity, persisted option validation, virtualized UI navigation, serialized browser/OIDC state, and exact image promotion
State: implementing
Baseline and resulting commits: `3ba59bb` -> `0dbfc40`, `56c5e43`, `54855b1`, `3f46816`, `57163da`, `ac38f89`
Files/contracts changed: `src/dal_obscura/control_plane/application/schema_service.py`,
`src/dal_obscura/control_plane/infrastructure/session_store.py`,
`packages/manifest-parquet-plugin/src/dal_obscura_manifest_parquet/catalog.py`,
`src/dal_obscura/control_plane/application/catalog_service.py`, focused schema,
manifest, catalog-option, browser-session, and OIDC route tests, and this ledger
`apps/governance-ui/src/schema_tree.ts`, governance UI tree tests, and this ledger
`.github/workflows/ci.yml`, CI workflow contract tests, and this ledger
Findings addressed (R IDs): R04/R05 nested identity bounds/provider stability; R06/R07 persisted option boundary; R12/R13 large-schema UI and concurrent auth replay; R14/R15 artifact integrity
Acceptance cases/test node IDs (A IDs): A06/A08 schema subcases; A14 navigation subcases; A12 login/replay subcases
Failing behavior before the change: synthetic nested Arrow IDs could exceed the 32-bit
path contract; concurrent callbacks and logout/session refresh were not row-serialized
Implementation behavior after the change: synthetic IDs are bounded deterministically;
nested provider IDs survive manifest routing; restored external catalog options are
descriptor-validated before provider use; schema navigation mounts a bounded window;
session and one-time login rows are locked during read-modify-write operations;
release tags are promoted from the exact scanned image digest
Exact commands and exit results: schema/API suite (16), manifest suite (15), catalog
option/discovery suite (31), UI tree/lifecycle suite (5), CI workflow suite (4), browser/OIDC suite (8), governed
aggregate suite, Ruff, UI build/lifecycle checks, and `git diff --check` passed
Environment/dependency and wheel/image/plugin-lock identities: Darwin 25.6.0 arm64;
no external wheel or production image was available in this workspace
Evidence files or CI artifact links: focused test nodes and this ledger
Pickle compatibility/unchanged-boundary check: no pickle source, payload, or referenced
import path changed
Migration/rollback impact: schema-field persistence normalization is additive for new
writes; legacy rows remain readable and live external migration evidence is open
Remaining acceptance gaps or blockers: PostgreSQL barriers, live REST/TLS/OIDC,
browser/consumer qualification, clean wheel/image provenance, recovery, and independent
review remain open
Next permitted packet: X03 PostgreSQL CAS/barrier slice where PostgreSQL is available;
otherwise continue locally executable X06/X07/X08 acceptance work

## Evidence entry template

Copy this section for each atomic slice. Replace every placeholder; do not delete
fields to hide missing evidence.

```text
Packet/slice:
State:
Baseline and resulting commit:
Files/contracts changed:
Findings addressed (R IDs):
Acceptance cases/test node IDs (A IDs):
Failing behavior before the change:
Implementation behavior after the change:
Exact commands and exit results:
Environment/dependency and wheel/image/plugin-lock identities:
Evidence files or CI artifact links:
Pickle compatibility/unchanged-boundary check:
Migration/rollback impact:
Remaining acceptance gaps or blockers:
Next permitted packet:
```

For live evidence record hardware, timestamps, provider/IdP versions, synthetic
dataset identity, and which processes/artifacts participated. Redact secrets at
capture time. Do not store live credentials, customer rows, or executable untrusted
pickle payloads in this documentation.

## Review completion evidence

- Repository source and existing plan/ledger reviewed at the baseline above.
- Focused schema/auth/demo tests exited successfully; exact command in the review.
- Local probes reproduced unrelated initial activation, stale shared-rule review,
  nested dot-path collision, collection-ID digest collision, and admission of
  provider class-loader options. These defects remain unfixed.
- Plugin architecture, 24 implementation packets, 23 acceptance scenarios, and
  explicit release gates written. This is planning completion only.
- Documentation structure checks verified sequential R/X/A IDs, required packet
  fields, and local link targets; whitespace checks also passed before commit.
- Full suites, browser/real-IdP, PostgreSQL races, production TLS/Compose, consumers,
  recovery, performance, and independent security/UX review were not run here.

Do not move a packet to `accepted` on the strength of this review's focused tests.
