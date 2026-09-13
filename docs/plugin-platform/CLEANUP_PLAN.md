# Deletion and consolidation plan

Owners N02/N14. This replaces the retention-only planning rule in
[the old inventory](DEAD_CODE_INVENTORY.md); that inventory remains historical
evidence until its executable prose guard is removed during implementation.
Breaking changes are authorized outside the protected pickle boundary.

## Disposition and proof

Do not equate “imported” with “must retain.” Migrate the current caller and remove
the old production path in the same delivered slice. Check source imports, CLI
entry points, package metadata, generated artifacts, persisted records and trusted
serialized classes before deletion. The objective is one implementation of each
responsibility, not a lower line count achieved by minification or hidden complexity.

1. **Consolidate SDK contracts (N02).** Inspect
   src/dal_obscura/common/plugin_api/contracts.py and
   packages/plugin-api/src/dal_obscura_plugin_api/contracts.py. Move ownership of
   non-serialized descriptors/config/handle/context validators to the public SDK;
   migrate both planes and third-party packages to direct canonical imports.
   Delete duplicate internal declarations and unused exports. Do not leave a
   re-export facade. Declare the SDK dependency in the server distribution.
   Proof: installed wheels without checkout imports, invalid output rejection and
   zero remaining old contract callers outside protected serialization.
2. **Remove short locks and aliases (N02).** Inspect common/plugin_api/registry.py,
   builtin_plugins.py, lock parsing, catalog_service, published_config and
   control_plane/interfaces/routes/plugins.py. Delete three-part lock acceptance,
   legacy module-name selection, fabricated default descriptors and registry-none
   fallback. Keep one authoritative built-in descriptor set. Enforce startup
   admission. Known persisted records may use a separate offline conversion; the
   serving parser never accepts the old shape. Unknown input must fail before
   dynamic imports/provider IO.
3. **Remove unscoped secrets/permissive options (N02/N04).** Inspect
   data_plane/infrastructure/adapters/secret_providers.py and provider/config callers. Require
   explicit authorized scope; remove scope-less success paths. Examine ignored
   kwargs/context arguments and resolved-but-discarded values. Delete only after
   determining whether each is dead or reveals missing enforcement. Do not
   “clean up” missing enforcement by dropping the check or accepting arbitrary
   environment names. Probe actual resolution and leak boundaries.
4. **Remove duplicate browser state and hardcoding (N06/N07/N10).** Replace the
   monolithic apps/governance-ui/src/main.tsx shell and global styles with the
   feature layout in UX_REQUIREMENTS.md. Delete replaced lifecycle/navigation
   helper code, hardcoded Iceberg fields/first-format selection and manual cache
   state. Keep a small composition entry point and one transport. Fixtures belong
   only in test tooling; remove browser demo bypasses from the normal production
   bundle. Do not ship old/new UI behind a migration flag.
5. **Remove obsolete token-login paths (N05).** Trace bootstrap/session routes,
   CLI profile defaults and UI token/password examples. Keep audited operator
   initialization/recovery and legitimate machine OIDC credentials. Remove
   bootstrap-token browser sessions and unsupported demo shortcuts once normal
   OIDC parity works. This is not authorization to remove machine-client auth.
6. **Replace tests that lock prose or internals (owning slice, then N14).** Review
   tests/architecture/test_dead_code_inventory.py, test_operator_plugin_lock_docs.py,
   test_capacity_runbook.py, UI lifecycle arithmetic/route-parser tests and
   duplicate stub adapter matrices. Map each asserted invariant first. Remove
   prose/specific-source-string checks once command behavior/config validation is
   owned elsewhere. Replace arithmetic checks with one parameterized rendered
   interleaving test. Keep a simple docs link check where valuable, not assertions
   requiring sentences or “KEEP” dispositions. Never drop a unique security case.
   Also replace test_local_demo_ui.py's literal obsolete image-tag assertion in
   N01 and update test_control_plane_route_inventory.py when N02/N05 remove routes.
   Preserve live build/proxy/CSP/auth coverage and the canonical route inventory.
   Do not keep obsolete code or failing assertions until a later cleanup packet.
7. **Archive completed planning (this review).** X00–X23 and earlier UI queues
   remain historical evidence; active work is N01–N16 only. Keep one compact
   ledger and an invariant registry, not repeated completion narratives. Do not
   create an executable test merely to assert these packet counts.
8. **Consolidate public policy APIs (N02).** Remove GET/PUT policy-rules and POST
   policy-preview, migrating api.ts saveRules/preview and other actual callers
   to /draft and /policy-evaluate. Keep policy_service.preview_asset_policy as
   an internal helper while the canonical evaluator uses it. Retain published
   policy records/history and their internal readers. The absence of a public
   route does not prove that its underlying data or helpers are dead.

## Explicitly retain

- Existing pickle serializers, serialized scan/task classes, class import paths
  and payload semantics, including common/table_format/ports.py and serialized
  Iceberg implementation classes. Leave original definitions where they are;
  do not move them and add a facade. Existing trusted fixtures must still roundtrip.
- public_plugin_adapter's necessary translation into the frozen internal task
  boundary and runtime validation of third-party output. Remove duplicated
  non-serialized models if safe; keep the actual trust/serialization enforcement.
- Active protobuf/generated transport contracts from their source schema,
  independent SDK/conformance packages, governed Python/JVM connectors, bounded
  streaming and canonical backend policy evaluator.
- Existing useful PostgreSQL/publication/schema/session/secret tests that cover
  a distinct fault, even if they make test counts larger.
- Existing control-plane database records and immutable history. Code deletion
  does not authorize record destruction or automatic republishing.

The old root application/domain/infrastructure/interfaces trees and unsupported
Delta/Avro/Unity adapters have already been removed from the active runtime.
Verify package boundaries once; do not invent new removal work for absent code.
Investigate newly discovered candidates using the same proof, not a predetermined
list of module names that must disappear.

## Breaking cutover procedure (future implementation)

1. Define one canonical record/API/plugin version and update all current callers.
   Bump SDK/config major versions when semantics require; reject unknown versions.
2. For valid durable old records only, implement a bounded offline dry-run/apply
   conversion in existing migration tooling. Never import strings from records.
   Report unsupported records with stable IDs and reasons. Preserve immutable
   history; convert mutable metadata or add explicit current binding records.
3. Test on populated disposable PostgreSQL, with backup and transaction rollback.
   Cut over in maintenance mode; stop admissions and drain/expire/invalidate
   tickets before serving the single supported artifact set.
4. Startup verifies current schema/version/lock. It never converts, reseeds or
   silently falls back. Unknown old input fails with an actionable error.
5. Prove rollback through the previous complete artifact set and its backup, not
   simultaneous mixed-version readers. Do not mutate or reinterpret pickle blobs.

## Completion accounting

Per slice record deleted/replaced paths, migrated callers, package/entry-point
check, preserved unique invariant tests, production/test logical SLOC delta,
removed/added dependencies, focused execution evidence and release proof still due.
Cleanup-only slices must reduce handwritten production logical SLOC; new required
features have separate accounting. A small increase necessary for security must
be identified as such, never hidden by deleting tests or compressing formatting.
No generic repository/service abstraction or shared utility layer without at least
two real callers and a demonstrable reduction in duplicated behavior.
