# Remaining-work ledger

Review baseline: c46415282a2a796cf737a9c3b7f7ba941f19bf7b (2026-09-13).
**Release HOLD. Implementation continues; unresolved live gates remain VERIFY.**
Execution authority: [implementation plan](IMPLEMENTATION_PLAN.md).
Previous completion notes: [archived ledger](STATUS_ARCHIVE_20260913.md).

Second review at c6230b1 found no intervening implementation changes. Queue status
is unchanged. F11–F14 refine N01/N02/N03/N09/N11/N12: current operational guidance,
removal of obsolete test/API contracts, bounded audit queries and editor-to-publisher
draft handoff. B05/B13/B15/B16/B19 specify their exact expected outcomes.

## Queue

Only N packets are active. Completed parts of X00–X23 were removed through the
[reconciliation](IMPLEMENTATION_REVIEW.md); do not repeat that implementation.
All N packets below contain remaining changes or qualification, not claims that
their underlying functionality is wholly absent.

- N01 — Baseline/toolchain: VERIFY; B01/B02 (CI Node 24 install/image/advisory evidence remains).
- N02 — Contracts/deletion: OPEN; requires N01; B03/G01–G04.
- N03 — Pairing/mutation contracts: OPEN; requires N02; B04/B05.
- N04 — Secret/IO/resource enforcement: OPEN; requires N02/N03; B06.
- N05 — Identity/normal authentication: OPEN; requires N03; B07/B08.
- N06 — Design foundation/shell: OPEN; requires N01; B09.
- N07 — Async UI/recovery: OPEN; requires N03/N05/N06; B10.
- N08 — Asset/policy authoring: OPEN; requires N03/N06/N07; B11/B12.
- N09 — Review/publication/history: OPEN; requires N07/N08; B13.
- N10 — Connection/plugin lifecycle: OPEN; requires N03/N04/N06/N07; B14.
- N11 — Access/settings/audit/consumers: OPEN; requires N05/N07/N09/N10; B15.
- N12 — Two-process transactional qualification: OPEN; requires N03/N04/N05; B16.
- N13 — Independent packages/live consumers: OPEN; requires N02/N03/N04/N05/N12; B17.
- N14 — Lean tests/capacity: OPEN; requires N01/N07/N08/N10/N12/N13; B18/B19.
- N15 — Deployment/recovery/artifact integrity: OPEN; requires N05/N12/N13/N14; B20/B21.
- N16 — Independent security/UX acceptance: OPEN; requires N01–N15; B22/all G/B.

## Implementation update — 5ad6f38 (2026-09-14)

- Packet / status / candidate commit / owner: N06/B09 foundation partial / VERIFY / \`5ad6f38\` / governance UI.
- Observable behavior delivered: changes, activity, connections, and settings routing now live in a typed management feature module with one shared management DTO contract. The root remains responsible for session identity, transport orchestration, and route selection.
- Changed paths: \`apps/governance-ui/src/components/ManagementViews.tsx\`, \`apps/governance-ui/src/main.tsx\`. No backend, session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: direct UI TypeScript build, Vite production build (272.19 kB JavaScript / 82.35 kB gzip), all 5 UI lifecycle/schema tests, and \`git diff --check\` passed.
- Remaining N06/B09 work: adopt the selected accessible primitives/icon system, typed deep links, responsive visual review at required sizes, CSP/browser/axe evidence, and keyboard/screen-reader journeys. Remaining N07–N16 packets and live release gates stay open; release remains HOLD.

## Implementation update — b2e559d (2026-09-14)

- Packet / status / candidate commit / owner: N06/B09 and N09/B13 deep-link partial / VERIFY / \`b2e559d\` / governance UI.
- Observable behavior delivered: URL locations now have a typed parser for page, asset, draft, tab, and positive history-version parameters. Asset workspace tabs restore on refresh, tab changes update same-origin history, and copied review links retain the active tab alongside the exact asset and draft IDs.
- Changed paths: \`apps/governance-ui/src/navigation.ts\`, \`apps/governance-ui/src/main.tsx\`, \`apps/governance-ui/src/components/AssetWorkspace.tsx\`, and navigation tests. No backend, session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: 6 UI tests, direct TypeScript build, Vite production build (272.94 kB JavaScript / 82.59 kB gzip), and \`git diff --check\` passed.
- Remaining N06/N09 work: browser route/back/refresh qualification, semantic version detail links, accessible primitive/icon system, responsive visual review, CSP/axe and keyboard/screen-reader evidence. Release remains HOLD.

## Implementation update — f99f441 (2026-09-14)

- Packet / status / candidate commit / owner: N06/B09 foundation partial / VERIFY / \`f99f441\` / governance UI.
- Observable behavior delivered: catalog and plugin lifecycle management now lives in a dedicated typed \`ConnectionsView\` component. Catalog discovery cancellation, plugin-pair admission checks, secret-reference-only configuration, catalog CAS saves, table-format selection, diagnostics, and publication create/activate flows are preserved while the root shell only coordinates route data and callbacks.
- Changed paths: \`apps/governance-ui/src/components/ConnectionsView.tsx\`, \`apps/governance-ui/src/main.tsx\`. No backend, session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: direct UI TypeScript build, Vite production build (271.97 kB JavaScript / 82.27 kB gzip), and all 5 UI lifecycle/schema tests passed; \`git diff --check\` passed.
- Remaining N06/B09 work: componentize the remaining asset/editor and management pages, adopt the selected accessible primitives/icon system, typed deep links, responsive visual review at required sizes, CSP/browser/axe evidence, and keyboard/screen-reader journeys. Release remains HOLD.

## Implementation update — bdff349 (2026-09-14)

- Packet / status / candidate commit / owner: N06/B09 foundation partial / VERIFY / \`bdff349\` / governance UI.
- Observable behavior delivered: the full asset workspace is now a dedicated typed feature component, including nested schema virtualization, policy rule authoring, deny-all drafts, policy tests, immutable history restore, access delegation, and Python/DuckDB/Spark/Arrow consumer snippets. Existing authorization gates, review tokens, revision checks, unsaved-change protection, and callback-driven async behavior remain unchanged.
- Changed paths: \`apps/governance-ui/src/components/AssetWorkspace.tsx\`, \`apps/governance-ui/src/main.tsx\`. No backend, session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: direct UI TypeScript build, Vite production build (271.97 kB JavaScript / 82.29 kB gzip), all 5 UI lifecycle/schema tests, and \`git diff --check\` passed.
- Remaining N06/B09 work: adopt the selected accessible primitives/icon system, typed deep links, responsive visual review at required sizes, CSP/browser/axe evidence, and keyboard/screen-reader journeys. Remaining N07–N16 packets and live release gates stay open; release remains HOLD.

## Implementation update — 502d012 (2026-09-14)

- Packet / status / candidate commit / owner: N05/B07 identity boundary partial / VERIFY / \`502d012\` / OIDC JWKS adapter.
- Observable behavior delivered: configured OIDC issuers are now preserved exactly for JWT \`iss\` validation, including meaningful trailing slashes. Discovery still constructs the standards endpoint with one separator and rejects an empty issuer before any provider setup.
- Changed paths: \`src/dal_obscura/data_plane/infrastructure/adapters/identity_oidc_jwks.py\` and its focused tests. No session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: 17 JWKS adapter tests, 40 actor/OIDC session tests, Ruff, and \`ty\` checks passed.
- Remaining N05/B07 work: live two-process OIDC freshness, principal-kind persistence across live IdP paths, revocation/disabled-account timing, browser login/logout evidence, and supported-profile bootstrap retirement. Release remains HOLD.

## Implementation update — ac7dda3 (2026-09-14)

- Packet / status / candidate commit / owner: N04/B06 and N05/B08 endpoint validation partial / VERIFY / \`ac7dda3\` / control-plane identity validation.
- Observable behavior delivered: issuer and JWKS endpoint configuration now rejects query data in addition to credentials and fragments, preventing ambiguous OIDC discovery destinations. Validation remains fail-closed before persistence or provider construction.
- Changed paths: \`src/dal_obscura/control_plane/application/auth_provider_validation.py\` and its focused tests. No session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: settings and auth-provider tests passed (7 tests); Ruff and \`ty\` checks passed.
- Remaining N04/B06 and N05/B08 work: real hostile transport counters, DNS/private-address policy, cancellation cleanup, live IdP/browser journeys, session freshness/revocation, and supported-profile bootstrap retirement. Release remains HOLD.

## Implementation update — 8ee68fc (2026-09-14)

- Packet / status / candidate commit / owner: N07/B10 async recovery partial / VERIFY / \`8ee68fc\` / governance UI.
- Observable behavior delivered: asset search and pagination now carry a dedicated abort signal. Superseded inventory requests are cancelled during a new search/page, logout, authentication expiry, and component teardown; epoch checks still fence any response that races cancellation.
- Changed paths: \`apps/governance-ui/src/main.tsx\`. No backend, session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: direct UI TypeScript build, Vite production build (272.19 kB JavaScript / 82.36 kB gzip), and all 5 lifecycle/schema tests passed.
- Remaining N07/B10 work: replace manual epoch state with a session-scoped query/mutation cache, add rendered deferred-response race tests across every workflow, and qualify 403/404/409/422/429/503 recovery states. Release remains HOLD.

## Implementation update — b7bc1e8 (2026-09-14)

- Packet / status / candidate commit / owner: N04/B06 partial / VERIFY / `b7bc1e8` / Iceberg REST plugin.
- Observable behavior delivered: the REST catalog adapter now applies bounded connect/read timeouts to every PyIceberg HTTP request, including the provider's initial configuration fetch. An active execution deadline narrows both values per request, and cancellation is checked immediately before transport. Timeout options are admitted as typed integer milliseconds with strict lower and upper bounds; invalid values fail before provider construction.
- Changed paths: `packages/iceberg-rest-plugin/src/dal_obscura_iceberg_rest/catalog.py`, its static plugin descriptor, and focused REST plugin tests. No pickle serializer, serialized class, payload, or import path changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest packages/iceberg-rest-plugin/tests/test_rest_plugin.py tests/plugin_platform tests/interfaces/control_plane/test_plugins_api.py -q` — all collected tests passed; plugin Ruff check and format check passed; `ty check packages/iceberg-rest-plugin/src` passed. The timeout propagation test records a two-second execution budget being applied as a smaller `(connect, read)` pair.
- Remaining N04/B06 work: real hostile local transport counters, redirect/DNS/private-address enforcement across metadata/data/delete destinations, cancellation that closes provider tasks, credential redaction scans, deployment network policy and live storage qualification. Release remains HOLD.

## Implementation update — 0fd82d9 (2026-09-14)

- Packet / status / candidate commit / owner: N05/B08 partial / VERIFY / `0fd82d9` / OIDC session routes.
- Observable behavior delivered: configured post-login redirects are now constrained to the callback gateway origin and reject credentials, query strings, fragments, and missing hosts. A malformed or external destination fails with a safe 503 instead of redirecting a newly authenticated browser away from the deployment.
- Changed paths: `control_plane/interfaces/routes/session.py` and its OIDC regression tests. No cookie, session, identity, bootstrap, pickle serializer, payload, or import path changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_oidc_login.py -q` — 8 passed; changed-path Ruff and `ty` checks passed.
- Remaining N05/B08 work: live OIDC code/PKCE browser journey, exact typed principal persistence, freshness/revocation and two-process evidence, secure cookie/CSP inspection, and supported-profile bootstrap retirement. Release remains HOLD.

Follow-up `9e15217` makes the REST timeout ceiling unconditional: provider-supplied
request timeout arguments are overwritten with the active bounded `(connect, read)`
pair, with focused coverage for an oversized caller timeout.

## Implementation update — 0bcf2b1 (2026-09-14)

- Packet / status / candidate commit / owner: N05/B07 partial plus N02 demo maintenance / VERIFY / `0bcf2b1` / identity boundary and Keycloak fixture.
- Observable behavior delivered: federated subject and group keys now carry distinct type tags, eliminating the `subject="group:x"` versus `group="x"` collision. The offline migration converts exact-issuer legacy keys (including escaped delimiters) to typed keys, remains idempotent for current keys, and still refuses ambiguous slash-stripped history. The demo provisioner now emits typed owner/grant keys, supplies asset revision preconditions, and uses the revisioned draft/publication API after the retired policy route was removed.
- Changed paths: identity encoder, offline migration, control-plane identity tests, demo provisioner/tests. No local identity representation or pickle serializer, serialized class, payload, or import path changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane tests/application/access_flow tests/infrastructure/adapters/test_published_config.py tests/common/config_store -q` — all collected tests passed; focused demo and identity suites passed; changed-path Ruff and `ty` checks passed.
- Remaining N05/B07 work: live two-process OIDC freshness, principal-kind persistence in all live IdP paths, revocation/disabled-account timing, and browser login/logout evidence. N02 still needs offline operator execution against real populated records. Release remains HOLD.

Authoritative post-identity qualification (`/tmp/dal-obscura-identity.xml`) collected
830 tests, passed 816, skipped the same 14 explicit opt-in lanes, and reported zero
failures/errors in 125.768 seconds with loopback/subprocess permissions enabled.

Follow-up `54bea07` makes typed-key migration rerunnable after an issuer is removed
from configuration; current `issuer|u|subject` and `issuer|g|group` shapes are
recognized without treating them as unresolved legacy data. Five migration tests pass.

## Implementation update — 17e6855 (2026-09-14)

- Packet / status / candidate commit / owner: N04/B06 plus N05/B08 partial / VERIFY / `17e6855` / OIDC token transport.
- Observable behavior delivered: authorization-code and local demo token exchanges now use a no-redirect opener. A configured token endpoint cannot redirect a code, client credentials, or password exchange to another origin; the existing bounded ten-second timeout and generic failure response remain in force.
- Changed paths: `control_plane/interfaces/session_api.py` only. No cookie, identity, pickle serializer, serialized class, payload, or import path changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_oidc_login.py tests/interfaces/control_plane/test_actor_auth.py -q` — 40 passed; changed-path Ruff and `ty` checks passed.
- Remaining N04/B06 work: live hostile transport counters, DNS/private-address policy, credential redaction, cancellation cleanup and deployment network controls. Remaining N05/B08 work is live IdP/browser evidence. Release remains HOLD.

## Implementation update — 21ffed2 (2026-09-14)

- Packet / status / candidate commit / owner: N06/B09 foundation partial / VERIFY / `21ffed2` / governance UI.
- Observable behavior delivered: the workspace access surface is now a dedicated typed `LoginPanel` component. OIDC sign-in, local development sign-in, retry, and fail-closed auth messaging remain behaviorally unchanged; authentication errors announce assertively to assistive technology. The shell no longer owns the login markup.
- Changed paths: `apps/governance-ui/src/components/LoginPanel.tsx` and its shell import. No backend, session, pickle serializer, payload, or import path changed.
- Evidence: direct UI `tsc -b`, Vite production build (272.04 kB JavaScript / 82.06 kB gzip), and all 5 lifecycle/component-adjacent tests passed.
- Remaining N06/B09 work: componentize the full shell/pages, adopt the selected accessible primitives/icon system, typed deep links, responsive visual review at required sizes, CSP/browser/axe evidence, and keyboard/screen-reader journeys. Release remains HOLD.

Follow-up `f640759` extracts runtime and identity-provider management into a typed
`SettingsView` component. Revisioned saves, staged-generation status, redaction
messaging, and the production-empty-provider state remain intact; direct UI
TypeScript/Vite and lifecycle checks pass.

## Implementation update — 8a615c3 (2026-09-14)

- Packet / status / candidate commit / owner: N01/B01-B02 / VERIFY / `8a615c3` / toolchain and documentation.
- Observable behavior delivered: CI and the UI image now select Node 24; the UI manifest and lock importer use exact tested dependency versions and declare the supported Node/pnpm engines. The repository-wide Ruff formatting drift and line-length failure are corrected. Agent, policy-authoring and baseline documents now describe the actual control-plane/data-plane layout and UI-first workflow while preserving the pickle boundary.
- Changed paths: CI runtime matrix, `ui/Dockerfile`, governance UI manifest/lock, 40 formatted Python sources/tests, `AGENTS.md`, `docs/policy-authoring.md`, `docs/plugin-platform/TECHNOLOGY.md`, and `BASELINE_20260914.md`. No serializer, serialized class, payload or import path changed. Production/test logical SLOC remains measured in the baseline; formatting accounts for the source churn (363 insertions, 264 deletions).
- Evidence: authoritative `uv run --no-sync pytest --durations=20 --junitxml=/tmp/dal-obscura-baseline.xml -q -rs` — 820 collected, 806 passed, 14 explicit skips, 0 failures in 125.531s; `uv run --no-sync ruff check .` and `ruff format --check .` — passed; governance UI `tsc -b`, Vite build and 5 lifecycle tests — passed (82.09 kB gzip JS). The Node 24 clean-install/image job remains a CI qualification gate. A post-fix rerun is recorded below.
- Remaining N01/B02 work: execute the Node 24 clean install and immutable image/advisory checks in CI, then advance N02 contract consolidation. Release remains HOLD.

Follow-up `docs/plugin-platform/BASELINE_20260914.md` was corrected after
source inspection: the standalone SDK is the sole current contract validator;
`common/plugin_api` now owns only registry, lockfile and lifecycle infrastructure.

## Implementation update — 772a6cb (2026-09-14)

- Packet / status / candidate commit / owner: N01/B02 / VERIFY / `772a6cb` / quality fixtures.
- Observable behavior delivered: all repository type-check diagnostics are resolved with explicit nullable ORM assertions, typed HTTP test responses, a deliberately untyped mock boundary, and a cast for the intentionally malformed lock fixture. No production validator or security check was weakened.
- Changed paths: four test modules only; no production logical SLOC or dependency change and no pickle path change.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync ty check --extra-search-path packages/plugin-api/src --extra-search-path packages/plugin-conformance/src --extra-search-path packages/manifest-parquet-plugin/src --extra-search-path packages/iceberg-rest-plugin/src` — all checks passed; repository Ruff check/format — passed; owning tests — 63 passed.
- Remaining N01/B02 work: run the Node 24 frozen install, image smoke and advisory/license checks in CI. Release remains HOLD.

## Implementation update — b451c79 (2026-09-14)

- Packet / status / candidate commit / owner: N05/F04 partial / VERIFY / `b451c79` / OIDC session routes.
- Observable behavior delivered: an OIDC callback without an explicit post-login destination now returns to the gateway origin derived from its callback URI. A configured post-logout URI can no longer redirect a successful login into the logout flow.
- Changed paths: `control_plane/interfaces/routes/session.py` and its OIDC regression test. No session cookie, identity encoding, bootstrap, pickle serializer or payload behavior changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_oidc_login.py -q` — 5 passed; changed-path Ruff check/format — passed.
- Remaining N05 work: live local/production OIDC browser journey, exact typed principal persistence, bootstrap retirement, freshness/revocation and two-process evidence. Release remains HOLD.

Post-fix full qualification (`/tmp/dal-obscura-final.xml`) collected 821 tests,
passed 807, skipped the same 14 explicit opt-in nodes, and reported zero
failures/errors in 125.394 seconds.

## Implementation update — a0c0940 (2026-09-14)

- Packet / status / candidate commit / owner: N02/F06 partial / VERIFY / `a0c0940` / governance UI.
- Observable behavior delivered: new catalog connections created from the UI now persist the admitted plugin ID (`iceberg.sql` or an external ID) directly. The UI no longer emits the retired built-in Python class path for new records; legacy rows remain subject to the explicit offline binding migration and serving parser checks.
- Changed paths: `apps/governance-ui/src/main.tsx` only; no pickle serializer, payload or persisted record was rewritten.
- Evidence: governance UI TypeScript build, Vite production build (272.01 kB JS / 82.09 kB gzip) and 5 lifecycle tests — passed.
- Remaining N02 work: remove remaining legacy module aliases from write/compile paths after the offline migration proves populated fixtures; retain strict unknown-record failure and the canonical SDK contract. Release remains HOLD.

## Implementation update — 2388668 (2026-09-14)

- Packet / status / candidate commit / owner: N03/B05 partial / VERIFY / `2388668` / API acceptance tests.
- Observable behavior covered: existing asset binding and grant mutations that omit `expected_revision` return the structured 428 precondition error and leave the resource unchanged. This closes two previously untested B05 cases without weakening creation semantics.
- Changed paths: `tests/interfaces/control_plane/test_assets_api.py` only; no production or pickle changes.
- Evidence: owning asset API suite — 20 passed; changed-path Ruff check/format — passed.
- Remaining N03 work: complete parameterized cross-resource race/process evidence, generated DTO coverage, and the referenced-draft review contract. Release remains HOLD.

## Implementation update — 96573bb (2026-09-14)

- Packet / status / candidate commit / owner: N02/F13 partial / VERIFY / `96573bb` / control-plane API.
- Observable behavior delivered: the retired `/v1/assets/{asset_id}/policy-rules` and `/v1/assets/{asset_id}/policy-preview` handlers are deleted. The revisioned `/draft`, `/policy-evaluate`, `/policy-review`, and `/policy-versions` routes remain the sole policy workflow; the absence test confirms the retired path cannot dispatch or reveal resource metadata.
- Changed paths: `control_plane/interfaces/routes/policies.py` and its owning API test. No persisted policy records, internal evaluator helper, pickle serializer, serialized class, or payload changed. Production logical SLOC decreased by 9; test logical SLOC increased by 5 for the explicit 405/no-leak assertion.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_assets_api.py tests/architecture/test_control_plane_route_inventory.py -q` — 17 passed; changed-route Ruff check — passed.
- Remaining N02/F13 work: verify all direct callers and remove any obsolete lock/config aliases in their owning slices; preserve internal evaluation and published history. Release remains HOLD.

READY means all prerequisites pass. OPEN means remaining work with prerequisites
not yet complete. ACTIVE means an agent owns a bounded slice. VERIFY means code
exists but required execution or human evidence is missing. DONE requires every
FR/NFR and acceptance subcase to pass on the candidate. Missing access/environment
is recorded explicitly; it is never PASS or a waived skip.

## Implementation update — 2b66520 (2026-09-14)

- Packet / status / candidate commit / owner: N06/B09 and N09/B13 deep-link partial / VERIFY / `2b66520` / governance UI.
- Observable behavior delivered: typed `version` deep links now open the asset History tab, validate the requested revision against the loaded asset history, fetch the immutable policy snapshot with an abort signal, fence stale responses, and render authorized rule details with a restore action. Selecting a revision updates the same-origin URL so the view is reproducible and shareable.
- Changed paths: `apps/governance-ui/src/api.ts`, `apps/governance-ui/src/components/AssetWorkspace.tsx`, `apps/governance-ui/src/main.tsx`, and `apps/governance-ui/src/styles.css`. No backend, session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: direct UI TypeScript build, Vite production build (275.83 kB JavaScript / 83.27 kB gzip), all 6 UI lifecycle/schema tests, and `git diff --check` passed.
- Remaining N06/N09 work: browser back/refresh qualification, accessible primitive/icon system, responsive visual review, CSP/axe and keyboard/screen-reader evidence, plus live review/publication gates. Release remains HOLD.

## Implementation update — 31ab15d (2026-09-14)

- Packet / status / candidate commit / owner: N04/B06 and N05/B08 transport hardening partial / VERIFY / `31ab15d` / OIDC JWKS adapter.
- Observable behavior delivered: dynamic OIDC discovery and key refresh now use a bounded five-second request through an opener with redirects disabled. A provider cannot move JWKS retrieval to an unvalidated origin during authentication; local HTTP fixtures and injected fetchers remain supported.
- Changed paths: `src/dal_obscura/data_plane/infrastructure/adapters/identity_oidc_jwks.py` and `tests/infrastructure/adapters/test_identity_oidc_jwks.py`. No session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: 18 JWKS adapter tests, changed-path Ruff, and `ty` checks passed.
- Remaining N04/N05 work: hostile local transport counters, DNS/private-address policy, credential redaction, cancellation/resource cleanup, live IdP/browser freshness and revocation journeys, and deployment network controls. Release remains HOLD.

## Implementation update — bb41307 (2026-09-14)

- Packet / status / candidate commit / owner: N06/B09 and N09/B13 navigation partial / VERIFY / `bb41307` / governance UI.
- Observable behavior delivered: asset tab and policy-version selections now create same-origin browser history entries. Back/forward events restore the typed tab/version state, and asset deep-link changes reload the selected asset and draft through the existing epoch and cancellation fences. Hash navigation remains guarded by the unsaved-change prompt.
- Changed paths: `apps/governance-ui/src/main.tsx` and `apps/governance-ui/src/components/AssetWorkspace.tsx`. No backend, session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: direct UI TypeScript build, Vite production build (276.45 kB JavaScript / 83.43 kB gzip), all 6 UI lifecycle/schema tests, and `git diff --check` passed.
- Remaining N06/N09 work: live browser back/refresh evidence, accessible primitive/icon system, responsive visual review, CSP/axe and keyboard/screen-reader evidence, and live review/publication gates. Release remains HOLD.

## Implementation update — 610c597 (2026-09-14)

- Packet / status / candidate commit / owner: N07/B10 recovery partial / VERIFY / `610c597` / governance UI.
- Observable behavior delivered: a shared recovery mapper now preserves distinct operator guidance for 403, 404, 409, 422, 429, and 503 responses, including request IDs. Management loads, paginated history/activity, inventory, asset loading, draft save, policy test, review, publish reconciliation, and history restore all use the mapper while retaining private state on recoverable failures.
- Changed paths: `apps/governance-ui/src/recovery.ts`, `apps/governance-ui/src/main.tsx`, and `apps/governance-ui/tests/lifecycle.test.mjs`. No backend, session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: direct UI TypeScript build, Vite production build (276.99 kB JavaScript / 83.64 kB gzip), all 7 UI lifecycle/schema/recovery tests, and `git diff --check` passed.
- Remaining N07 work: session-scoped query/mutation cache, rendered deferred-response race coverage for every workflow, and live recovery qualification for all listed statuses. Release remains HOLD.

Follow-up `1e240c3` expands the recovery mapper evidence to all governed HTTP
statuses (403, 404, 409, 422, 429, and 503); the UI lifecycle/schema/recovery
test count is now 8.

## Implementation update — 8f4957d (2026-09-14)

- Packet / status / candidate commit / owner: N04/B06 and N05/B08 browser boundary partial / VERIFY / `8f4957d` / control-plane security middleware.
- Observable behavior delivered: all responses now include `X-Frame-Options: DENY`, same-origin opener isolation, and a restrictive Permissions-Policy. HTTPS requests additionally receive one-year HSTS with subdomains; HTTP local development remains usable without an HSTS side effect.
- Changed paths: `src/dal_obscura/control_plane/interfaces/api.py` and `tests/interfaces/control_plane/test_actor_auth.py`. No session, identity, pickle serializer, serialized class, payload, or import path changed.
- Evidence: 34 actor/auth tests, changed-path Ruff, and `ty` checks passed, including an HTTPS TestClient assertion for HSTS.
- Remaining N04/N05 work: hostile transport/DNS/private-address enforcement, credential redaction, cancellation/resource cleanup, live IdP/browser freshness and revocation journeys, and deployment network controls. Release remains HOLD.

## Implementation update — 16dea42 (2026-09-14)

- Packet / status / candidate commit / owner: N07/B10 and N10/N11 recovery partial / VERIFY / `16dea42` / governance UI.
- Observable behavior delivered: connection discovery, diagnostics, catalog and asset registration, snapshot creation/activation, runtime settings, identity-provider settings, owner changes, and delegated grants now use the shared status-aware recovery mapper. Operators see permission, missing-resource, stale-revision, validation, throttling, and outage guidance consistently while prior serving state remains intact.
- Changed paths: `apps/governance-ui/src/components/ConnectionsView.tsx`, `apps/governance-ui/src/components/SettingsView.tsx`, and `apps/governance-ui/src/components/AssetWorkspace.tsx`. No backend, session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: direct UI TypeScript build, Vite production build (276.86 kB JavaScript / 83.62 kB gzip), all 8 UI lifecycle/schema/recovery tests, and `git diff --check` passed.
- Remaining N07/N10/N11 work: session-scoped query/mutation cache, rendered deferred-response race coverage, live status recovery journeys, permission matrix/browser evidence, and production qualification. Release remains HOLD.

## Implementation update — da2237e (2026-09-14)

- Packet / status / candidate commit / owner: N06/B09 visual foundation partial / VERIFY / `da2237e` / governance UI.
- Observable behavior delivered: the shared stylesheet now defines semantic light/dark canvas, surface, text, border, selection, focus, and status tokens; dark preference and explicit dark mode use the same token contract. The document prevents page-level horizontal overflow while preserving inner table/code scrolling and honors reduced-motion preferences.
- Changed paths: `apps/governance-ui/src/styles.css`. No backend, session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: direct UI TypeScript build, Vite production build (276.86 kB JavaScript / 83.62 kB gzip; 20.10 kB CSS / 4.82 kB gzip), all 8 UI lifecycle/schema/recovery tests, and `git diff --check` passed.
- Remaining N06 work: apply tokens consistently across component styles, adopt Lucide/accessibility primitives, and complete required 390/768/1440/200% browser, axe, keyboard, and screen-reader evidence. Release remains HOLD.

## Implementation update — 0b6af3a (2026-09-14)

- Packet / status / candidate commit / owner: N06/B09 visual foundation partial / VERIFY / `0b6af3a` / governance UI.
- Observable behavior delivered: primary navigation now uses one typed, dependency-free SVG icon primitive with semantic names for assets, history, activity, connections, and settings. Icons are decorative beside visible labels and inherit the same focus/color behavior as their controls; no remote assets or package install is required.
- Changed paths: `apps/governance-ui/src/components/Icon.tsx`, `apps/governance-ui/src/main.tsx`, and `apps/governance-ui/src/styles.css`. No backend, session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: direct UI TypeScript build, Vite production build (278.51 kB JavaScript / 84.36 kB gzip; 19.27 kB CSS / 4.63 kB gzip), all 8 UI lifecycle/schema/recovery tests, and `git diff --check` passed.
- Remaining N06 work: complete the selected Lucide/accessibility primitive contract, apply semantic tokens consistently across all components, and run required visual, axe, keyboard, and screen-reader evidence. Release remains HOLD.

## Implementation update — 51ee26a (2026-09-14)

- Packet / status / candidate commit / owner: N08/B11 authoring partial / VERIFY / `51ee26a` / governance UI.
- Observable behavior delivered: policy rule edits now have a bounded in-memory undo/redo history (100 snapshots), visible Undo/Redo controls, and Cmd/Ctrl+Z / Cmd/Ctrl+Shift+Z shortcuts. History is cleared whenever the asset or authenticated workspace changes, never enters localStorage or server state, and every undo/redo invalidates saved/reviewed status until explicitly saved and reviewed again.
- Changed paths: `apps/governance-ui/src/main.tsx` and `apps/governance-ui/src/components/AssetWorkspace.tsx`. No backend, session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: direct UI TypeScript build, Vite production build (279.80 kB JavaScript / 84.66 kB gzip; 19.27 kB CSS / 4.63 kB gzip), all 8 UI lifecycle/schema/recovery tests, and `git diff --check` passed.
- Remaining N08 work: rendered lossless editor roundtrip coverage, duplicate-rule workflow, typed condition builder, all mask-value cases, responsive/large-tree browser stress, and complete B11/B12 qualification. Release remains HOLD.

## Implementation update — d8f08af (2026-09-14)

- Packet / status / candidate commit / owner: N08/B11 authoring partial / VERIFY / `d8f08af` / governance UI.
- Observable behavior delivered: each editable policy rule can be duplicated from the rule list. The copy preserves principals, exact nested field paths, masks and typed values, row predicates, and conditions, receives a new precedence ordinal, becomes selected, and invalidates any saved/reviewed status until explicitly saved and reviewed.
- Changed paths: `apps/governance-ui/src/main.tsx` and `apps/governance-ui/src/components/AssetWorkspace.tsx`. No backend, session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: direct UI TypeScript build, Vite production build (280.44 kB JavaScript / 84.83 kB gzip; 19.27 kB CSS / 4.63 kB gzip), all 8 UI lifecycle/schema/recovery tests, and `git diff --check` passed.
- Remaining N08 work: rendered lossless editor roundtrip coverage, typed condition builder, all mask-value cases, responsive/large-tree browser stress, and complete B11/B12 qualification. Release remains HOLD.

## Implementation update — 87c9b24 (2026-09-14)

- Packet / status / candidate commit / owner: N08/B11 authoring partial / VERIFY / `87c9b24` / governance UI.
- Observable behavior delivered: policy conditions now have a structured editor for claim names, equality or membership operators, and text operands with explicit AND semantics. The advanced JSON editor remains available as a lossless fallback; invalid rows are surfaced before mutation and never silently discarded.
- Changed paths: `apps/governance-ui/src/components/AssetWorkspace.tsx` and `apps/governance-ui/src/styles.css`. No backend, session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: direct UI TypeScript build, Vite production build (282.46 kB JavaScript / 85.37 kB gzip; 19.73 kB CSS / 4.71 kB gzip), all 8 UI lifecycle/schema/recovery tests, and `git diff --check` passed.
- Remaining N08 work: rendered lossless roundtrip coverage, all mask-value cases, local undo/redo browser coverage, responsive/large-tree stress, and complete B11/B12 qualification. Release remains HOLD.

## Implementation update — 4ee5257 (2026-09-14)

- Packet / status / candidate commit / owner: N06/B09 and N08/B11 command palette partial / VERIFY / `4ee5257` / governance UI.
- Observable behavior delivered: Cmd/Ctrl+K now filters destinations to the current session's available management scope and searches the authorized asset inventory already loaded in the workspace. Selecting an asset reuses the existing discard guard and epoch-fenced loader; inaccessible management actions are not presented as commands.
- Changed paths: `apps/governance-ui/src/main.tsx` and `apps/governance-ui/src/styles.css`. No backend, session, pickle serializer, serialized class, payload, or import path changed.
- Evidence: direct UI TypeScript build, Vite production build (283.04 kB JavaScript / 85.56 kB gzip; 19.83 kB CSS / 4.72 kB gzip), all 8 UI lifecycle/schema/recovery tests, and `git diff --check` passed.
- Remaining N06/N08 work: keyboard cycling and browser accessibility evidence, server-backed asset search beyond loaded pages, complete mobile/desktop verification, and full B09/B11/B12 qualification. Release remains HOLD.

Qualification follow-up (2026-09-14): the elevated full Python suite completed with
837 collected, 823 passed, 14 explicit environment/benchmark skips, and zero
failures or errors. The unprivileged attempt was discarded because the sandbox
blocks local socket binds required by Flight and HTTP fixture tests.

## Evidence inherited, with limits

The historical ledger reports Python/socket-enabled suite and PostgreSQL checks
and a local Python/DuckDB probe. This review did not rerun them. Repository-level
race tests do not establish two-process API races; stub consumer reads and pair
descriptors do not qualify live catalogs; backup helper tests do not establish
timed restore. See F08/F09 and N12/N13/N15. Reuse the existing harnesses.

## Implementation update — c1b4866 (2026-09-13)

- Packet / status / candidate commit / owner: N11 partial / VERIFY / `c1b4866` / control-plane + UI.
- Observable behavior delivered: `/v1/audit/events/page` provides bounded keyset pagination ordered by `(created_at, id)`; non-admin visibility is enforced with database `EXISTS` predicates and asset-specific reads still require the read capability. The existing list endpoint now uses the same scoped query. Activity loads 50 events and can request later pages without an unbounded fetch.
- Changed paths: audit application/repository/service, policy routes, UI API/activity view, ORM and migration `20260913_0016_audit_keyset_index.py`. No pickle serializer, payload class, or import path changed.
- Primary tests: `tests/interfaces/control_plane/test_audit_api.py`, `tests/architecture/test_control_plane_route_inventory.py`, `tests/common/config_store/test_schema_migrations.py`.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_audit_api.py tests/architecture/test_control_plane_route_inventory.py tests/common/config_store -q` — 20 passed. `pnpm --dir apps/governance-ui check` — passed. `ruff check` on changed Python paths — passed. UI production build is VERIFY because local Corepack could not fetch pinned `pnpm@12.3.4` from the npm registry.
- Remaining N11/B15 work: complete permission matrix, settings/access/consumer qualification, production browser evidence, and independent security/UX evidence.

## Implementation update — 018929a (2026-09-13)

- Packet / status / candidate commit / owner: N11 partial / VERIFY / `018929a` / control-plane.
- Observable behavior delivered: the audit page accepts bounded actor, action, resource type, outcome, correlation/request ID, and inclusive time-window filters. Filters are applied in SQL before the stable keyset limit, with text lengths enforced at the HTTP boundary.
- Primary test: `tests/interfaces/control_plane/test_audit_api.py::test_audit_page_filters_before_pagination` covers compound filtering, actor filtering, and overlong input rejection.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_audit_api.py tests/architecture/test_control_plane_route_inventory.py -q` — 5 passed; changed Python `ruff check` — passed.
- Remaining N11/B15 work: complete permission matrix, settings/access/consumer qualification, production browser evidence, and independent UX/security evidence.

## Implementation update — e4764f5 (2026-09-13)

- Packet / status / candidate commit / owner: N11 partial / VERIFY / `e4764f5` / governance UI.
- Observable behavior delivered: Activity now exposes actor, action, request ID, resource type, outcome, and time-window filters. Applying or clearing filters reloads the server-scoped keyset query; loading more retains the same filter set.
- Evidence: `apps/governance-ui/node_modules/.bin/tsc -p apps/governance-ui/tsconfig.json` — passed. The pnpm wrapper remains environment-blocked while resolving the pinned package manager from the npm registry.
- Remaining N11/B15 work: full permission matrix, settings and consumer handoff qualification, production browser evidence, and independent UX/security review.

The follow-up `35e3192` fences filter changes by invalidating the prior management epoch and clearing old pages before the replacement request, so a stale audit cursor or deferred response cannot cross filter scopes. Direct TypeScript/Vite production verification passed again.

Regression evidence after the N11 slices: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane tests/common/config_store -q` — all collected tests passed (100%); no skips were introduced.

Direct UI production verification also passed: `apps/governance-ui/node_modules/.bin/tsc -p apps/governance-ui/tsconfig.json && apps/governance-ui/node_modules/.bin/vite build` produced the Vite bundle (260.29 kB JavaScript, 14.22 kB CSS). The package-manager wrapper remains blocked only when it attempts to fetch its pinned Corepack metadata.

Plugin-platform and architecture regression evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/plugin_platform tests/architecture -q` — all collected tests passed (100%).

## Implementation update — session capability metadata (working slice, 2026-09-13)

- Packet / status / candidate commit / owner: N05/F01 plus N11/B15 partial / VERIFY / `11298ef` / control-plane + governance UI.
- Observable behavior delivered: authenticated `/v1/session` responses now include a safe workspace capability list. Platform administrators receive `workspace:admin`; ordinary actors receive an empty list. The UI session contract carries the metadata for future workspace-level navigation gates while resource-specific permissions remain derived from `GET /v1/assets/{asset_id}/access`.
- Changed paths: session API actor response, UI session DTO, and actor-auth exact-contract tests. Pickle serializer and payload paths are unchanged.
- Evidence: actor-auth and OIDC login tests passed; changed-path Ruff passed; UI TypeScript, Vite production build, and lifecycle tests passed.
- Remaining N05/N11/B15 work: structured principal-kind persistence, two-process OIDC freshness, browser login/logout/expiry, complete actor capability matrix, and independent security/UX review.

Follow-up `1387657` extends abort propagation through plugin, catalog,
publication, runtime, and authentication-provider reads. Connections discovery
and diagnostics abort superseded requests on component teardown or replacement;
stale errors do not overwrite the current view. Direct UI TypeScript/Vite build
and lifecycle tests pass.

Follow-up `9707ffa` adds OpenAPI response models for session actor metadata and
effective asset capabilities, with route-inventory assertions that prevent
future untyped contract drift. Local issuer omission remains compatible with
the established response shape.

Follow-up `4f65f4b` validates helper/service payloads into those response models
inside the routes, removing the production type-check diagnostics caused by the
generic service wrapper while retaining the same JSON contract.

Follow-up `fcc6a13` uses the session capability contract in the UI shell: signed-
out users and non-admin actors see management destinations disabled with an
explicit browser tooltip, while scoped asset/history/activity navigation stays
available. Server authorization remains authoritative for direct callers.

Follow-up `d0c490d` clears read-only draft-handoff mode when the user selects a
different asset, preventing an author’s review state from leaking into another
asset’s editor. The existing load and edit epochs continue to fence the switch.

Follow-up `0ee3ee7` stops asset history, grants, and effective-access failures
from becoming empty success states. Any protected read failure now leaves the
previous editor intact and reports that access metadata could not be loaded.

Follow-up `5408068` removes hard-coded runtime values from the unconfigured
settings state. Blank fields with examples now require positive operator input,
so a successful `null` runtime read cannot be mistaken for serving configuration.

Follow-up `c77162d` normalizes an omitted additive session-capability field to an
empty list during rolling upgrades, preserving a fail-closed management shell
when an older control-plane instance is briefly serving the UI.

Follow-up `86c0b7a` advances workspace, inventory, and management epochs during
component teardown in addition to aborting requests, closing the final fallback
race where session-option work could pass a stale-state check after unmount.

## Implementation update — e2e CAS fixture alignment (2026-09-14)

- Packet / status / candidate commit / owner: N03/B05 maintenance / VERIFY / `c6b78e4` / e2e fixtures.
- Observable behavior delivered: the Iceberg e2e setup now supplies the required asset revision precondition for schema and owner writes, using the revision produced by the preceding schema update. The test no longer relies on a removed unguarded mutation path.
- Changed paths: `tests/test_e2e_smoke.py`; production CAS and pickle boundaries are unchanged.
- Evidence: elevated `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/test_e2e_smoke.py -q` — 1 passed.
- Remaining N03/B05 work: browser mutation evidence, real cross-process races, and live consumer qualification.

## Implementation update — identity migration (working slice, 2026-09-13)

- Packet / status / candidate commit / owner: N05/F01 partial / VERIFY / pending atomic commit / control-plane.
- Observable behavior delivered: `dal-obscura-migrate identity-keys` previews and, with `--apply`, transactionally converts legacy federated owner, grant, draft, audit, publication, ticket, and policy-rule values when exactly one configured issuer (including its historical trailing-slash variant) identifies the value. Delimiter and percent characters are escaped with the same canonical encoder used by runtime actor admission. Unknown or ambiguous history fails closed before mutation.
- Changed paths: `common/identity.py`, `common/config_store/identity_migration.py`, migration CLI, access encoder integration, and focused migration tests. Pickle serializer and payload paths are unchanged.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/common/config_store/test_identity_migration.py -q` — 2 passed; changed-path `ruff check` — passed; operator procedure is documented in `docs/operators.md`.
- Remaining N05/F01 work: structured principal-kind persistence or an independently approved encoding contract, two-process OIDC freshness, browser login/logout/expiry, and ambiguous-history operator evidence.

Follow-up `bf62ca0` closes a migration edge case: local `local|...` and
`group:local|...` keys are intentionally left untouched, with a focused mixed
workspace regression test (3 migration tests passed).

Follow-up `79314a7` also distinguishes exact-issuer canonical escaped keys from
slash-stripped history and rejects the latter when escapes make the original
subject ambiguous (4 migration tests passed).

Post-slice regression: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane tests/common/config_store tests/plugin_platform tests/architecture -q` — all collected tests passed; UI lifecycle tests and direct TypeScript/Vite build also passed. The already-running local endpoints remain healthy (`/healthz` 200, UI root 200). Release remains HOLD pending live OIDC, process-boundary, real catalog/consumer, capacity, recovery, and independent review gates.

## Implementation update — 7c20db5 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B04 partial / VERIFY / `7c20db5` / plugin SDK + control-plane + governance UI.
- Observable behavior delivered: v1 plugin descriptors now carry explicit `output_formats` and `handle_versions`. Pair listing, asset binding, publication compilation, schema discovery, and public adapter execution require the catalog's declared format ID, a shared handle version, and capability compatibility; capability overlap alone cannot admit a pair. Resolved public handles are checked before format factory execution. Discovery with multiple admitted formats requires an explicit UI selection.
- Changed paths: duplicated public/core SDK descriptors and static descriptor loader, built-in and external plugin descriptors, pair route, compiler, asset/schema services, public adapter, UI DTO/Connections view, and regression tests. No pickle serializer, payload class, or import path changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_plugins_api.py tests/plugin_platform tests/control_plane/test_asset_service_plugins.py tests/control_plane/test_publication_compiler.py tests/control_plane/test_schema_service.py -q` — all collected tests passed; changed-path Ruff passed; direct UI `tsc` and Vite production build passed (261.51 kB JavaScript, 14.22 kB CSS).
- Remaining N03/B04 work: real three-pair built-wheel compatibility, explicit mutation preconditions, provider/consumer live qualification, and browser evidence. Release remains HOLD.

## Implementation update — UI stale-response fencing (working slice, 2026-09-13)

- Packet / status / candidate commit / owner: N07/F03 partial / VERIFY / pending atomic commit / governance UI.
- Observable behavior delivered: policy restore captures the active asset/load and edit epochs before awaiting the server and cannot overwrite a newer editor; successful restore advances the edit fence. Publish pending state is cleared only for the active load, while logout clears it with private state. Initial-load authentication options and catalog discovery responses are fenced after awaits; selecting a different catalog cannot be overwritten by an older discovery response. Multiple admitted format choices remain explicit.
- Changed path: `apps/governance-ui/src/main.tsx`; protected API and pickle boundaries unchanged.
- Evidence: `node --test tests/lifecycle.test.mjs tests/schema_tree.test.mjs` — 5 passed; direct TypeScript/Vite production build passed (261.81 kB JavaScript, 14.22 kB CSS).
- Remaining N07/F03 work: rendered browser interleaving tests for restore/publish/discovery, settings/error/permission workflow completeness, and independent UX/security review.

## Implementation update — settings/provider editor (working slice, 2026-09-13)

- Packet / status / candidate commit / owner: N11/F04 partial / VERIFY / `a48c75b` / governance UI + control-plane settings.
- Observable behavior delivered: authenticated Settings exposes editable OIDC issuer, audience, JWKS URL, group claims, and enabled state through the existing validated provider-chain route. Save messaging distinguishes staged draft state from serving configuration. Legacy `[redacted]` sensitive values are accepted only as round-trip placeholders and preserved from the server-side row. A transient management read failure keeps the last scoped data visible with an unavailable notice rather than rendering an empty success state.
- Changed paths: UI API and Settings view, auth-provider validation, repository redaction preservation. No pickle serializer or payload path changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_settings_api.py tests/control_plane/test_auth_provider_validation.py -q` — 5 passed; changed-path Ruff passed; direct UI TypeScript/Vite build passed (263.56 kB JavaScript, 14.22 kB CSS).
- Remaining N11/F04 work: revision preconditions for provider/runtime writes, last-admin-safe recovery, production OIDC/browser evidence, and independent permission/UX review.

## Implementation update — e7ecdf5 (2026-09-13)

- Packet / status / candidate commit / owner: N05/F01 partial / VERIFY / `e7ecdf5` / control-plane + UI.
- Observable behavior delivered: federated actor keys preserve the exact issuer string, and percent/delimiter characters in subjects and groups are escaped before composing owner, grant, draft, audit, and session lookup keys. Local actors retain their existing unscoped representation. The Access view no longer strips a trailing issuer slash in its guidance.
- Primary test: `tests/control_plane/test_access_identity.py` covers exact trailing-slash issuers, delimiter/percent escaping, and local compatibility; existing actor-auth tests remain green.
- Evidence: actor-auth and identity tests — 34 passed; direct UI TypeScript/Vite build — passed; changed Python `ruff check` — passed. Pickle boundary unchanged.
- Remaining N05/F01 work: structured principal-kind storage, explicit offline conversion for existing federated records, ambiguous-history rejection, and live two-process OIDC freshness/browser evidence.

## Implementation update — a589f3f (2026-09-13)

- Packet / status / candidate commit / owner: N09/F14 partial / VERIFY / `a589f3f` / control-plane + governance UI.
- Observable behavior delivered: evaluation, server review, and policy-version publication accept an explicit saved `draft_id` plus revision. Review tokens bind the selected asset draft ID, author, revision, and content hash, so a publisher can activate an editor-owned draft without falling back to the publisher's personal draft. Foreign or discarded draft references are concealed as not found; stale revisions and token/draft mismatches fail closed. The UI carries the draft ID/revision from load through test, review, and publish.
- Changed paths: policy/evaluation/review/publication services, repository draft lookup, route schemas, governance UI API/editor state, and cross-identity API regression fixtures. No pickle serializer, payload class, or import path changed.
- Primary tests: `tests/interfaces/control_plane/test_schema_api.py::test_publisher_can_review_and_publish_editor_draft_by_explicit_id` plus existing schema/policy-version/control-plane suites.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_schema_api.py tests/interfaces/control_plane/test_policy_versions_api.py tests/control_plane -q` — all collected tests passed. `ruff check` on changed Python paths — passed. `cd apps/governance-ui && npm run build` — TypeScript and Vite production build passed (263.71 kB JavaScript, 14.22 kB CSS). UI lifecycle/schema tests — 5 passed.
- Remaining N09/F14 work: rendered browser handoff flow, two-process publisher/editor race evidence, token replay/expiry matrix, and independent security/UX review. Release remains HOLD.

Follow-up `1e83d78` adds an authorized `GET /v1/assets/{asset_id}/draft/{draft_id}`
handoff endpoint and a same-origin UI link format (`/?asset=...&draft=...#assets`).
Opening that link loads the immutable saved draft into a read-only editor while
retaining server-side evaluation, review, and publish actions. Route inventory,
API, and UI regression/build checks pass; browser rendering and multi-process
handoff races remain VERIFY.

Follow-up `af81794` aligns the selected-draft error contract with B05: stale
evaluation revisions return 409, missing selected drafts remain concealed as
404, and the cross-identity regression proves an editor change invalidates a
publisher's expected revision.

Follow-up `65adae6` returns and renders separate draft-author and reviewer
identities in completed review evidence, closing the authorship distinction in
the handoff response. Full before/after diff rendering and independent browser
acceptance remain VERIFY.

Follow-up `67e6475` keeps the selected draft ID on read-only handoff refreshes,
so reload/access actions cannot silently switch back to the publisher's draft.

## Implementation update — 2d005bb (2026-09-13)

- Packet / status / candidate commit / owner: N02/B03 partial / VERIFY / `2d005bb` / plugin registry.
- Observable behavior delivered: runtime admission and lock-file loading now require the exact five-part distribution/version/API/descriptor-digest/artifact-digest lock. The registry always reads a factory-free static descriptor (or an explicitly injected descriptor loader in tests); the fabricated three-part descriptor fallback is deleted.
- Changed paths: `common/plugin_api/registry.py` and plugin registry tests. No pickle serializer, payload class, or import path changed.
- Primary tests: `tests/plugin_platform/test_registry.py`, `tests/plugin_platform/test_lockfile.py` — all passed; changed-path Ruff passed.
- Remaining N02/B03 work: remove legacy module-name aliases and permissive serving readers, retain bounded offline conversion for known persisted records, and prove clean installed-wheel migration.

## Implementation update — cebe127 (2026-09-13)

- Packet / status / candidate commit / owner: N02/B03 plus N09 draft integrity / VERIFY / `cebe127` / control-plane + data-plane.
- Observable behavior delivered: the control-plane composition root now admits one authoritative built-in registry when no external lock is supplied; duplicate route descriptors and an unused validator alias are deleted. Serving manifests require canonical plugin IDs, reject retired module identities, and merge migrated binding columns before runtime validation. Restoring history updates the existing draft's base policy generation together with its revision.
- Changed paths: control-plane app/plugin route, catalog validator, repository draft persistence, data-plane published-config adapter, and focused tests. No pickle serializer, payload class, or import path changed.
- Primary tests: audit, policy-version, catalog-option, published-config, and plugin-route suites passed; changed-path Ruff passed.
- Remaining N02/B03 work: remove remaining legacy module readers from migration-only boundaries where safe, enforce explicit secret scopes, and prove clean installed-wheel migration. Remaining N09 work includes race and browser evidence.

## Implementation update — 56decd6 (2026-09-13)

- Packet / status / candidate commit / owner: N02/N04/B03/B06 partial / VERIFY / `56decd6` / secret provider + catalog validation.
- Observable behavior delivered: secret references are now always shaped as `{secret, scope}` with a non-empty scope equal to the requesting boundary. Unscoped references fail before provider lookup; catalog descriptors and inline-secret validation enforce the same rule; data-plane identity resolution uses the explicit `identity` scope.
- Changed paths: secret provider, catalog option validation, data-plane identity loader, and migrated focused fixtures. No pickle serializer, payload class, or import path changed.
- Primary tests: secret-provider, catalog-option, catalog API, schema, and data-plane interface suites passed; changed-path Ruff passed.
- Remaining N04/B06 work: prove scoped secret behavior across all provider/config callers, leak-boundary checks, and production credential rotation evidence.

## Implementation update — 2467b29 (2026-09-13)

- Packet / status / candidate commit / owner: N02/B03 partial / VERIFY / `2467b29` / publication compiler.
- Observable behavior delivered: compiled Iceberg manifests now persist the canonical `iceberg.sql` catalog plugin ID instead of the retired Python class-path identity. Internal catalog construction metadata remains unchanged, and the strict data-plane binding validator accepts newly published rows.
- Primary tests: publication compiler suite and elevated end-to-end Iceberg Flight smoke passed; pickle boundary unchanged.
- Remaining N02/B03 work: clean installed-wheel migration proof and removal of remaining legacy readers from migration-only code.

Regression evidence after the canonical manifest cutover: elevated local-socket
`UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest -q` completed with
all collected tests passing and only the repository's existing marked skips. The
socket-backed connector, Flight, and E2E Iceberg lanes also passed separately.
Direct UI TypeScript/Vite production build remains green (264.44 kB JavaScript,
14.22 kB CSS). Release remains HOLD for the unresolved N03–N16 live qualification,
capacity, recovery, and independent review gates.

Regression after the handoff slices: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv
run --no-sync pytest tests/control_plane tests/interfaces/control_plane
tests/common/config_store tests/plugin_platform tests/architecture -q` passed
all collected tests. Plugin conformance passed (2 tests); direct UI TypeScript/
Vite production build passed (264.88 kB JavaScript, 14.22 kB CSS); UI lifecycle
and schema tests passed (5). No pickle fixture or serializer paths changed.

## Implementation update — 4e5f314 (2026-09-13)

- Packet / status / candidate commit / owner: N02/B03 partial / VERIFY / `4e5f314` / plugin SDK + packaging.
- Observable behavior delivered: `dal-obscura-plugin-api` is an explicit runtime dependency resolved from the repository SDK package and included in the Docker builder context. The runtime, control plane, lock builder, and tests import shared descriptor/configuration contracts directly from the public SDK. The duplicate `src/dal_obscura/common/plugin_api/contracts.py` module and its re-export surface were deleted; `PluginKind` is exported by the SDK. Pickle serializers, serialized classes/import paths, and task payload semantics were not changed.
- Changed and deleted paths: `pyproject.toml`, `uv.lock`, `Dockerfile`, public SDK `__init__.py`, plugin registry/lock adapters, built-in plugin wiring, lock-builder script, and contract/registry tests; deleted internal duplicate contracts module.
- Primary tests: `tests/plugin_platform`, `tests/control_plane`, `tests/interfaces/control_plane`, `tests/infrastructure/adapters`, and `tests/architecture` all passed; changed-path Ruff passed. `uv build` passed and wheel metadata contains `Requires-Dist: dal-obscura-plugin-api`.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/plugin_platform tests/control_plane tests/interfaces/control_plane tests/infrastructure/adapters -q`; `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/plugin_platform tests/architecture -q`; targeted `ruff check`; `uv build`.
- Remaining N02/B03 work: strict old-input rejection and maintenance-mode conversion for known persisted records, removal of any remaining migration-only legacy readers where safe, and clean installed-wheel migration proof. Release remains HOLD.

## Implementation update — 38f9dc2 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B05 partial / VERIFY / `38f9dc2` / control-plane repository + API.
- Observable behavior delivered: existing catalog updates and asset metadata, owner, grant, and schema replacements now require the current revision. Missing preconditions fail with HTTP 428 and stale values retain HTTP 409 compare-and-set behavior; create-at-zero remains valid. Rejected writes leave the resource revision and data unchanged.
- Changed paths: control-plane revision error, repository guards, shared route error mapping, and catalog/asset/repository regression tests. No pickle serializer, payload class, or import path changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_catalogs_api.py tests/interfaces/control_plane/test_assets_api.py tests/control_plane/test_publication_store.py tests/control_plane/test_asset_grant_authorization.py -q` — 34 passed; changed-path Ruff and `git diff --check` passed.
- Remaining N03/B05 work: runtime/auth-provider write preconditions, explicit activation generation precondition, safe structured error envelopes, process race evidence, and generated DTO/browser proof. Release remains HOLD.

Follow-up `d9ddea4` migrates the existing control-plane, integration, connector,
and E2E fixture callers to send the revision required by the new CAS contract,
including sequential owner/grant updates. `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache
uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane -q`
passed; changed test paths pass Ruff. Socket-backed Flight and benchmark lanes
remain environment-blocked in this sandbox and are still VERIFY.

The clean wheel gate for N02 also passed on 2026-09-13: the server and SDK
wheels were installed into `/tmp/dal-obscura-wheel-smoke`, with the process
started from `/tmp` and no checkout path, and imports resolved from that target
for both `dal_obscura` and `dal_obscura_plugin_api`. The SDK wheel was built
from `packages/plugin-api`; the server wheel metadata declares the SDK
dependency. This is artifact evidence only; live plugin admission and
maintenance-mode conversion remain open.

Follow-ups `d9ddea4` and `051bfe0` complete the existing owner/grant/schema
fixture migration for the CAS contract, including multi-step schema drift and
cross-identity draft handoff cases. The combined regression lane
`UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest
tests/plugin_platform tests/control_plane tests/interfaces/control_plane
tests/infrastructure/adapters -q` passed with no failures. Socket-bound Flight,
benchmark, and live process-boundary lanes remain VERIFY in this environment.

## Implementation update — 9fc6197 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B05 partial / VERIFY / `9fc6197` / workspace activation route + UI.
- Observable behavior delivered: publication activation now requires an explicit serving-generation precondition. A caller sends the current publication ID for replacement or JSON `null` for first activation; an omitted body/field returns 428 before mutation. The UI always supplies the precondition and preserves the existing 409 stale-generation CAS behavior.
- Changed paths: workspace route, UI transport, activation API tests, and missing-precondition regression coverage. No pickle serializer or payload path changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_config_activation.py tests/interfaces/control_plane/test_workspace_api.py -q` — 7 passed; UI `tsc` and changed-path Ruff passed.
- Remaining N03/B05 work: runtime/auth-provider write preconditions, safe structured error envelopes, process race evidence, and generated DTO/browser proof. Release remains HOLD.

## Implementation update — 21290df (2026-09-13)

- Packet / status / candidate commit / owner: N03/B05 partial / VERIFY / `21290df` / runtime settings + control-plane UI.
- Observable behavior delivered: runtime ticket settings now carry a persisted revision through migration `20260913_0017`. First creation remains revision zero; existing updates require `expected_revision`, stale values return 409, and omitted values return 428 before mutation. Authenticated GET/PUT responses expose the revision, while the UI sends it as a request precondition without submitting it as an unknown field.
- Changed paths: runtime ORM/repository/workspace service, settings route schemas, Alembic migration, UI transport, schema migration/settings/inventory tests. Pickle serializers and task payloads are unchanged.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane -q` — all passed; runtime/settings migration tests and changed-path Ruff passed; UI TypeScript passed.
- Remaining N03/B05 work: auth-provider chain revision preconditions, safe structured error envelopes, process race evidence, and generated DTO/browser proof. Release remains HOLD.

## Implementation update — 14edd5b (2026-09-13)

- Packet / status / candidate commit / owner: N02/B03 partial / VERIFY / `14edd5b` / compiler + data-plane manifest reader + offline migration.
- Observable behavior delivered: newly compiled catalog and asset manifests carry canonical `catalog.type` metadata without a Python module identity. The serving reader rejects legacy module-shaped catalog config before registry/provider resolution. The explicit `dal-obscura-migrate plugin-bindings --apply` path rewrites only the known Iceberg shape, reports changes once, and is idempotent on rerun; unknown records remain unsupported.
- Changed paths: publication compiler, published-config adapter, plugin-binding migration, and focused migration/serving tests. Pickle serializer and serialized task paths remain untouched.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/common/config_store/test_plugin_bindings.py tests/infrastructure/adapters/test_published_config.py tests/control_plane/test_publication_compiler.py -q` — all passed; changed-path Ruff and `git diff --check` passed.
- Remaining N02/B03 work: strict old lock/secret/API input matrix, migration qualification on populated PostgreSQL with maintenance cutover, and release artifact admission. Release remains HOLD.

## Implementation update — 2e23b25 (2026-09-13)

- Packet / status / candidate commit / owner: N02/N05 UI authentication cleanup / VERIFY / `2e23b25` / governance UI.
- Observable behavior delivered: the shipped UI no longer contains the development `?demo` workspace, local fixture asset, demo persona buttons, or demo-login client method. Workspace reads, policy evaluation, draft saves, reviews, publishes, and restores now always use the authenticated control plane. SSO and explicitly enabled local bootstrap-token login remain available for production and local parity respectively.
- Changed and deleted paths: `apps/governance-ui/src/main.tsx`, `apps/governance-ui/src/api.ts`, deleted `apps/governance-ui/src/fixtures.ts`. No backend demo route was changed, preserving existing server-side compatibility tests and local operator fixtures. No pickle serializer, payload class, or import path changed.
- Evidence: `node --experimental-strip-types --test tests/*.test.mjs` — 5 passed; `node_modules/.bin/tsc -p tsconfig.json && node_modules/.bin/vite build` — passed (263.59 kB JavaScript, 14.22 kB CSS); generated bundle contains no `demo-login`, `Use demo persona`, `Explicit demo`, or `?demo` strings. `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/architecture/test_local_demo_ui.py tests/examples/test_ui_smoke.py -q` — 3 passed.
- Remaining N02/N05 work: backend demo endpoint retirement and fixture migration require a separate compatibility decision; live OIDC/browser expiry, two-process freshness, and independent security/UX review remain VERIFY. Release remains HOLD.

## Implementation update — 96bfdf6 (2026-09-13)

- Packet / status / candidate commit / owner: N02/B03 partial / VERIFY / `96bfdf6` / control-plane + governance UI + test fixtures.
- Observable behavior delivered: public `/policy-rules` and `/policy-preview` routes are absent from OpenAPI and return side-effect-free 404 tombstones. The UI and test fixtures use revisioned `/draft`; evaluation uses `/policy-evaluate`. Workspace publication consumes the latest saved draft per asset, and inventory policy status recognizes non-empty saved drafts.
- Changed paths: policy route adapter, repository publication/status assembly, UI API and README, route inventory, and migrated API fixtures. Internal policy helpers and protected pickle paths remain intact.
- Evidence: migrated control-plane/API suites and route inventory passed; direct UI TypeScript/Vite build passed. `rg` finds no production callers for retired route methods. Release remains HOLD.
- Remaining N02/B03 work: remove remaining duplicate SDK/contract definitions, three-part lock and alias residue, enforce strict old-input rejection and maintenance-mode offline conversion, and prove clean installed-wheel migration.

## Implementation update — d139566 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B05 partial / VERIFY / `d139566` / auth-provider settings + governance UI.
- Observable behavior delivered: authentication provider chains now persist a shared revision through migration `20260913_0018`. Initial creation uses revision zero; existing replacements require `expected_revision`, stale writes return 409, and omitted preconditions return 428 before mutation. Responses expose the revision and the UI sends it with provider saves. Redacted secret arguments remain preserved.
- Changed paths: auth-provider ORM/repository/service/route, migration, validation redaction, UI API/editor, and CAS regression fixtures. Pickle serializers, serialized task classes, and import paths were not changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane -q` — all passed; `tests/plugin_platform` and `tests/infrastructure/adapters` passed; UI `tsc` and changed-path Ruff passed.
- Remaining N03/B05 work: safe structured error envelopes, two-process freshness/race evidence, and generated DTO/browser proof. Release remains HOLD.

## Implementation update — a3bef98 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B05 partial / VERIFY / `a3bef98` / control-plane API.
- Observable behavior delivered: HTTP 409 revision conflicts and 428 missing-precondition failures now include a safe `error` object with stable code, message, and request ID while retaining the existing `detail` field for current clients. Successful responses and unrelated error contracts are unchanged.
- Changed paths: FastAPI HTTP exception handler and CAS regression assertions. Pickle serializers and task payloads are unchanged.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane -q` — all passed; changed-path Ruff, format, and diff checks passed.
- Remaining N03/B05 work: field-level errors where useful, two-process freshness/race evidence, and generated DTO/browser proof. Release remains HOLD.

## Implementation update — 87d4817 (2026-09-13)

- Packet / status / candidate commit / owner: N12/B16 partial / VERIFY / `87d4817` / PostgreSQL race harness.
- Observable behavior delivered: the integration race suite now provisions an auth chain and exercises barrier-coordinated concurrent runtime-settings and auth-provider replacements. Each scenario requires exactly one revision-zero commit and one `PublicationConflictError`, complementing existing draft, grant, and binding races.
- Changed paths: `tests/integration/control_plane/test_publication_races.py`; no production or pickle paths changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/integration/control_plane/test_publication_races.py -q` — 5 scenarios skipped because `DAL_OBSCURA_POSTGRES_TEST_URL` is not configured in this sandbox; Ruff passed. CI PostgreSQL execution is still required for acceptance.
- Remaining N12/B16 work: run the full two-process PostgreSQL/API/Flight interleaving matrix with termination and lost-response evidence. Release remains HOLD.

## Implementation update — 7fae923 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B04 partial / VERIFY / `7fae923` / control-plane API contracts.
- Observable behavior delivered: runtime and authentication-provider settings routes now publish explicit Pydantic response models. OpenAPI describes nullable runtime settings and redacted provider records, including revisions, so generated clients can consume a stable schema.
- Changed paths: settings response DTOs, route annotations, and OpenAPI regression assertions. Pickle serializers and task payloads are unchanged.
- Evidence: settings and UI-shell tests passed; changed-path Ruff/format checks passed. Full generated TypeScript client regeneration remains open because the UI currently uses handwritten transport types.
- Remaining N03/B04 work: generate/check browser DTOs from the OpenAPI contract, complete real pair compatibility, and run live consumer/browser evidence. Release remains HOLD.

## Implementation update — 923ad50 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B05 partial / VERIFY / `923ad50` / configuration migration tests.
- Observable behavior delivered: the idempotent schema migration regression now tracks Alembic head `20260913_0018`, covering the auth-provider revision column introduced in the preceding atomic slice.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/common/config_store/test_schema_migrations.py -q` — 7 passed.
- Remaining N03/B05 work: complete generated browser DTO wiring and live process-boundary evidence. Release remains HOLD.

## Implementation update — 21dacc0 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B05 partial / VERIFY / `21dacc0` / auth-provider revision persistence.
- Observable behavior delivered: the auth-chain revision is persisted on the cell as well as provider rows, so removing every provider does not reset compare-and-swap state. An authenticated revision endpoint lets the UI continue guarded writes after an empty chain; delete and recreate regressions advance revisions 1→2→3.
- Changed paths: cell ORM/migration, repository/service/route revision endpoint, UI settings loading, and API tests. Pickle serializers and task payloads are unchanged.
- Evidence: settings, OpenAPI, and migration tests passed; UI TypeScript passed; changed-path Ruff/format checks passed.
- Remaining N03/B05 work: generated DTO/browser proof and live process-boundary execution. Release remains HOLD.

## Implementation update — local parity verification (2026-09-13)

- Packet / status / candidate commit / owner: N03/B05 / VERIFY / working tree verification / control-plane + UI.
- Observable behavior delivered: the disposable UI database was upgraded from `20260913_0016` to packaged head `20260913_0018`; the updated control plane restarted successfully and serves the authenticated provider list and revision endpoint. `GET /v1/settings/auth-providers/revision` with the local admin bearer returned `{"revision":0}`, the provider list returned `[]`, and the Vite UI root returned HTTP 200.
- Evidence: `DAL_OBSCURA_DATABASE_URL=sqlite+pysqlite:////private/tmp/dal-obscura-ui-dev-8821.db uv run dal-obscura-migrate upgrade`; live curl checks against `127.0.0.1:8821` and `127.0.0.1:5173`.
- Remaining release gates: production OIDC/browser lifecycle, generated DTO/browser proof, real PostgreSQL race matrix, live catalog/consumer qualification, capacity/recovery, and independent security/UX review. Release remains HOLD.

## Verification update — broad regression (2026-09-13)

- `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane tests/plugin_platform tests/infrastructure/adapters -q` passed with all collected tests green after the persistent auth revision changes. The UI TypeScript/Vite build and changed-path Ruff checks also pass. No pickle serializer or task payload files were modified.
- Release remains HOLD for the required PostgreSQL two-process barriers, live OIDC/browser lifecycle, generated DTO/browser proof, real catalog/consumer matrix, capacity/recovery, and independent security/UX acceptance.

## Implementation update — 1a33f92 (2026-09-13)

- Packet / status / candidate commit / owner: N05/B08 partial / VERIFY / `1a33f92` / control-plane security headers.
- Observable behavior delivered: API and auth responses now emit a strict same-origin Content-Security-Policy with no `unsafe-eval`, unrestricted scripts, framing, or form targets, alongside existing anti-sniff/referrer protections.
- Evidence: actor-auth security-header regression passed; changed-path Ruff passed. A built-browser CSP inspection remains part of the live OIDC/browser gate.
- Remaining N05/B08 work: real OIDC code/PKCE browser journey, cookie/expiry/role freshness, and production profile bootstrap/recovery evidence. Release remains HOLD.

## Implementation update — 64642e1 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B04 partial / VERIFY / `64642e1` / plugin configuration forms.
- Observable behavior delivered: admitted plugin descriptors now validate boolean, integer, numeric, enum, string, URI, and secret-reference values with declared choices. The Connections UI renders typed controls and serializes values without coercing booleans or numbers to strings; unsupported descriptor types still fail closed.
- Changed paths: catalog descriptor validation, Connections form rendering/serialization, and focused tests. Pickle serializers and task payloads are unchanged.
- Evidence: plugin option and route tests passed; UI TypeScript passed; changed-path Ruff/format checks passed.
- Remaining N03/B04 work: generated DTO/browser proof, real three-pair compatibility and consumer qualification. Release remains HOLD.

## Implementation update — 7884cca (2026-09-13)

- Packet / status / candidate commit / owner: N03/B04 partial / VERIFY / `7884cca` / plugin descriptor validation.
- Observable behavior delivered: numeric descriptor fields now require finite numbers; booleans, integers, enums, strings, URIs, and secret references retain their declared types and reject coercion or non-finite values before plugin factory use.
- Evidence: catalog option validation tests passed; changed-path Ruff/format checks passed. Browser and built-wheel typed-form qualification remain open.
- Remaining N03/B04 work: generated DTO/browser proof, real three-pair compatibility and consumer qualification. Release remains HOLD.

## Implementation update — 9854b45 (2026-09-13)

- Packet / status / candidate commit / owner: N06/B09 partial / VERIFY / `9854b45` / governance UI shell.
- Observable behavior delivered: account identity and sign-out remain visible below the mobile breakpoint, and users can select System, Light, or Dark theme. Only the non-sensitive theme preference is persisted; dark-mode surfaces and controls use the documented contrast-oriented palette.
- Evidence: UI TypeScript and Vite production build passed; no private session or policy state is persisted by the theme control. Manual responsive/accessibility review remains required.
- Remaining N06/B09 work: command palette, icon system, menu drawer, rendered 390/768/1440 layouts, keyboard/screen-reader and axe review. Release remains HOLD.

## Implementation update — 1bea336 (2026-09-13)

- Packet / status / candidate commit / owner: N06/B09 partial / VERIFY / `1bea336` / governance UI navigation.
- Observable behavior delivered: Cmd/Ctrl+K opens a keyboard-accessible command palette for authorized destination navigation and workflow help. It closes on Escape/backdrop, supports filtering, and contains no publish/delete/revoke action. Palette state is ephemeral and does not persist policy or identity data.
- Evidence: UI TypeScript and Vite production build passed. Manual keyboard focus restoration, responsive rendering, and screen-reader review remain required.
- Remaining N06/B09 work: menu drawer, Lucide icon system, focus restoration, rendered layout/a11y checks, and full page state coverage. Release remains HOLD.

## Implementation update — 81b4fee (2026-09-13)

- Packet / status / candidate commit / owner: N06/B09 partial / VERIFY / `81b4fee` / governance UI accessibility.
- Observable behavior delivered: closing the command palette by Escape, backdrop, or command selection restores focus to the element that invoked it, preserving keyboard navigation continuity.
- Evidence: UI TypeScript and Vite production build passed. Manual keyboard, screen-reader, and responsive layout acceptance remains required.
- Remaining N06/B09 work: menu drawer, Lucide icon system, full responsive rendering and accessibility review. Release remains HOLD.

## 2026-09-13 — N11 access capability contract

- Scope: effective asset capability visibility for the authenticated UI.
- Observable behavior delivered: `GET /v1/assets/{asset_id}/access` is read-authorized and derives all four asset capabilities on the server. It reports an allow flag and deduplicated explanation for platform-admin, owner-derived, delegated, and denied capabilities. The Access view renders this response before owner/grant mutation controls, so role labels and client-side guesses cannot grant authority.
- Verification: asset API tests pass; UI TypeScript compilation and Vite production build pass; targeted Ruff checks pass after formatting.
- Remaining gate: browser and multi-actor acceptance still require the N11/B15 live journey and independent UX/security review.

## 2026-09-13 — N11 management failure states

- Scope: authenticated management navigation and forbidden/error handling.
- Observable behavior delivered: management loads now retain a distinct error state and render an explicit unavailable/permission message with a retry action instead of falling through to empty tables or default settings. HTTP 403 is explained as missing management permission; other failures identify server/session availability.
- Verification: UI TypeScript compilation and Vite production build pass.
- Remaining gate: browser permission matrix and independent UX/security evidence remain VERIFY under B15/B22.

## 2026-09-13 — N03 route inventory update

- Scope: public contract inventory for effective asset access.
- Observable behavior delivered: the canonical OpenAPI route inventory now includes `GET /v1/assets/{asset_id}/access` and asserts its read-only method set, preventing future route drift from the reviewed contract.
- Verification: route inventory and asset API tests pass.

## 2026-09-13 — N08/N09 capability-aware editor controls

- Scope: align policy authoring and publication controls with server-derived asset access.
- Observable behavior delivered: the editor becomes read-only when the authenticated actor lacks `edit`; history restore follows the same boundary; review remains available for an authorized publisher reviewing an explicitly selected draft; publication actions require the server-reported `publish` capability. Backend authorization remains authoritative for every mutation.
- Verification: UI TypeScript compilation, Vite production build, and control-plane/API suites pass.
- Remaining gate: rendered multi-actor browser evidence and independent UX/security acceptance remain VERIFY.

## 2026-09-13 — typing cleanup for review and plugin admission

- Scope: static correctness in production code paths.
- Observable behavior delivered: plugin descriptor parsing now narrows validated output-format and handle-version collections before constructing immutable descriptors; review token validation narrows selected drafts before author checks; settings route adapters preserve typed response contracts; redacted configuration round-tripping uses explicit mapping casts at recursive boundaries. Runtime behavior and pickle serialization are unchanged.
- Verification: changed-path Ruff passes; policy schema tests pass. Full `ty check` still reports only unresolved optional plugin-package imports and existing test-only typing diagnostics in the uninstalled workspace.

## 2026-09-13 — B15 owner capability regression

- Scope: actor matrix coverage for the access contract.
- Observable behavior delivered: the API regression suite now verifies an owner receives exactly `read` and `edit`, while `publish` and `grant` remain denied until explicitly delegated.
- Verification: actor-auth and asset API suites pass.

## 2026-09-13 — N07 abortable read transport

- Scope: cancellation of superseded authenticated UI reads.
- Observable behavior delivered: the single API transport accepts `AbortSignal`; initial workspace, asset detail/schema/access/draft/history/grant reads, and management history/audit/summary/observation reads now receive request signals. Replacing a load, logging out, or unmounting aborts the prior request before the next state can render; stale epoch checks remain as a second identity fence.
- Verification: UI TypeScript compilation, Vite production build, and all UI tests pass.
- Remaining gate: the plan still calls for a TanStack Query cache migration and rendered deferred-response coverage; those are not claimed complete by this slice.

## 2026-09-13 — N07 structured client failures

- Scope: safe error observability at the browser transport boundary.
- Observable behavior delivered: the typed UI client now parses the server's structured `code`, `message`, `request_id`, optional `current_revision`, and `field_errors` without exposing response credentials or raw bodies. Management failures include the correlated request ID in the recovery message.
- Verification: UI TypeScript compilation, Vite production build, UI tests, and settings error-envelope tests pass.
- Remaining gate: mutation-specific conflict reconciliation and rendered deferred-response coverage remain VERIFY.

## 2026-09-14 — N10 governed catalog editing

- Scope: authenticated catalog management UX and secret-preserving updates.
- Observable behavior delivered: authorized operators can open an existing catalog, edit safe scalar options, see the admitted adapter selected, and cancel without mutation. Catalog names are locked during edit to preserve optimistic revision and secret scope. Secret fields are represented as deployment-managed references; clearing an untouched field sends the server redaction marker so the repository preserves the existing secret instead of erasing it. Unsupported or no-longer-admitted adapters fail closed with a refresh instruction.
- Verification: UI TypeScript compilation, Vite production build, eight UI tests, and `git diff --check` pass. No pickle serializer or task payload paths changed.
- Remaining gate: browser proof must verify non-secret prefill, redacted secret behavior, keyboard focus, and multi-actor permissions under N10/B09/B15. Release remains HOLD.

## 2026-09-14 — N08/B11 null default mask

- Scope: complete mask value semantics for policy authoring and execution.
- Observable behavior delivered: a `default` mask now accepts JSON `null` through publication validation, emits a typed DuckDB `cast_to_type(NULL, ...)` expression, and preserves the source Arrow field type while returning null values. Numeric, boolean, and string defaults retain their existing scalar behavior; container values remain rejected.
- Verification: publication compiler and DuckDB transform suites passed (all nodes); changed-path Ruff and format checks passed; `git diff --check` passed. Pickle serializers and task payloads are unchanged.
- Remaining gate: rendered browser round-trip coverage for every mask type and nested large-tree stress remain VERIFY under B11/B12. Release remains HOLD.

## 2026-09-14 — N08/B11 mask editor defaults

- Scope: prevent invalid intermediate mask drafts in the policy editor.
- Observable behavior delivered: selecting redact initializes `[REDACTED]`, keep-last initializes `4`, and default initializes an editable empty string. Null/default values remain editable through the JSON scalar control, while null and no-mask selections preserve their explicit semantics.
- Verification: UI TypeScript compilation, Vite production build, eight UI tests, and `git diff --check` pass. No backend or pickle paths changed.
- Remaining gate: rendered component round-trip must exercise every mask type and verify numeric/boolean/string/null defaults under B11. Release remains HOLD.

## 2026-09-14 — broad regression after UI and mask slices

- `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest -q` passed with 837 collected, 823 passed, 14 explicit opt-in skips, and zero failures/errors using loopback/subprocess permissions. UI TypeScript/Vite and eight UI tests remain green.
- The skips are the documented benchmark, consumer qualification, PostgreSQL race/recovery, and other opt-in release lanes; they are not treated as completion evidence. N01–N16 remain open/VERIFY where live browser, OIDC, hostile transport, consumer, capacity, recovery, and independent acceptance evidence is still required. Release remains HOLD.

## 2026-09-14 — N08/B11 nullable mask schema

- Scope: align masked Arrow schemas with null-producing `null` and `default: null` expressions.
- Observable behavior delivered: direct null-producing masks now mark output fields nullable while preserving the original Arrow type and metadata. This keeps schema declarations truthful for non-nullable source fields and nested projections.
- Verification: all DuckDB transform tests passed (38 nodes), changed-path Ruff passed, and `git diff --check` passed. Pickle serializers and task payloads are unchanged.
- Remaining gate: rendered nested component round-trip and consumer matrix still need to verify nullability across Python/Arrow, DuckDB, and Spark. Release remains HOLD.

## 2026-09-14 — N08/B12 nested tree semantics

- Scope: accessible virtual schema-tree navigation for nested fields.
- Observable behavior delivered: flattened tree rows now carry sibling `aria-posinset` and `aria-setsize` values at each depth, while the rendered row identity uses the server-stable field ID. Global virtualization indices remain separate for keyboard scrolling and bounded mounting.
- Verification: UI TypeScript compilation, Vite production build, all eight UI tests, and `git diff --check` pass.
- Remaining gate: the required 10,000-node Playwright stress journey, 200-row mount cap, 200% zoom, and screen-reader review remain VERIFY. Release remains HOLD.

- Follow-up test coverage now asserts root, sibling, and nested descendant position metadata explicitly; the full UI test set remains green.

- The virtual tree now handles Home/End in addition to Arrow and Enter/Space navigation, with focus scrolled into view before transfer. Browser stress and screen-reader evidence remain VERIFY.

- Local smoke: the existing control plane on `127.0.0.1:8821` returned HTTP 200 for an authenticated catalog listing, and the Vite UI on `127.0.0.1:5173` returned HTTP 200. These checks confirm serving processes only; they do not substitute for the required browser/IdP acceptance.

## 2026-09-14 — N10/B09 governed asset inventory state

- Scope: expose serving and policy state at the asset onboarding surface.
- Observable behavior delivered: workspace asset rows now resolve active immutable publication metadata in bounded batch queries and return `active_policy_version` plus `last_published_at`. The authenticated UI renders policy status, draft status, active version, and publication recency beside the asset selector, with explicit missing-policy and never-published states. Mutable drafts are not used to claim serving state.
- Verification: asset API suite passed (20 tests), UI TypeScript compilation and eight UI tests passed, and `git diff --check` passed. Pickle serializers and task payloads are unchanged.
- Remaining gate: browser visual/keyboard evidence, strict response models, and live multi-actor publication checks remain VERIFY under N03/N10/B09/B15. Release remains HOLD.

- Follow-up regression: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest -q` passed with 837 collected, 823 passed, 14 documented skips, and zero failures/errors. UI TypeScript compilation, Vite build, and eight UI tests remain green.

## 2026-09-14 — N03 typed asset inventory responses

- Scope: remove untyped response boundaries from asset inventory routes.
- Observable behavior delivered: full and cursor-paginated asset inventory endpoints now validate responses through Pydantic models, including nullable active policy and publication timestamp fields. Route authorization and concealed-resource behavior remain unchanged.
- Verification: asset API and inventory-read suites passed (28 tests); changed-path Ruff and format checks passed. Remaining strict DTO coverage for other routes and generated TypeScript checks stays VERIFY under N03.

## 2026-09-14 — N03 typed catalog management responses

- Scope: strengthen management API contracts used by authenticated Connections UI.
- Observable behavior delivered: catalog summaries and connectivity diagnostics now use shared Pydantic response models with bounded, explicit fields. Discovery remains dynamic until plugin-specific table contracts are formalized.
- Verification: catalog and workspace API suites passed (24 tests); changed-path Ruff and format checks passed. Broader response-model and generated DTO coverage remains VERIFY.

## 2026-09-14 — N03 typed plugin registry responses

- Scope: make plugin admission data consumable through a stable management contract.
- Observable behavior delivered: authenticated plugin listing now validates admitted descriptors, catalog/format pair results, and allowlisted lifecycle states with shared response models. Registry allowlist filtering and dynamic plugin config schemas remain unchanged.
- Verification: plugin API tests passed; changed-path Ruff and format checks passed. Generated TypeScript DTO coverage and live plugin lifecycle acceptance remain VERIFY.

## 2026-09-14 — N03 typed workspace lifecycle responses

- Scope: type authenticated workspace status and publication management responses.
- Observable behavior delivered: summary counts, control-plane/data-plane observations, publication listings, creation results, and activation results now validate through shared response models. Existing generation preconditions and authorization behavior remain unchanged.
- Verification: workspace API suite passed (18 tests); changed-path Ruff and format checks passed. Generated DTO coverage and live browser publication acceptance remain VERIFY.

## 2026-09-14 — N11 typed audit responses

- Scope: stabilize redacted activity data consumed by the authenticated Activity view.
- Observable behavior delivered: list and keyset-paginated audit routes now validate event fields and cursor envelopes through shared Pydantic response models, while preserving database scope, filter bounds, redacted details, and request correlation.
- Verification: audit API suite passed (4 tests); changed-path Ruff and format checks passed. Broader strict DTO generation and independent permission review remain VERIFY.

## 2026-09-14 — N03/N09 typed policy history responses

- Scope: stabilize immutable policy history and publish results used by Changes and History views.
- Observable behavior delivered: asset/global policy history, keyset pages, published version details, and policy-version creation results now validate through shared response models. Actor scope, review checks, and publication semantics remain unchanged.
- Verification: policy-version and publish-flow suites passed (13 tests); changed-path Ruff and format checks passed. Restore response typing, generated DTOs, and live publish/reconciliation evidence remain VERIFY.

## 2026-09-14 — N10 catalog inventory impact

- Scope: make catalog lifecycle impact visible before operator activation.
- Observable behavior delivered: Connections now displays server-reported catalog status and governed-asset count beside each admitted catalog, while preserving escaped untrusted labels and plugin admission controls.
- Verification: UI TypeScript compilation, Vite production build, eight UI tests, and `git diff --check` passed. Browser visual/keyboard lifecycle evidence remains VERIFY.

## 2026-09-14 — N05 typed browser-auth responses

- Scope: harden the authenticated UI bootstrap and OIDC discovery boundary.
- Observable behavior delivered: session login options and public OIDC configuration now validate through shared response models. Null OIDC availability, safe login shortcuts, and secret omission remain explicit; bootstrap/OIDC authentication behavior is unchanged.
- Verification: OIDC and actor-auth suites passed (41 tests); changed-path Ruff and format checks passed; UI TypeScript compilation passed. Live IdP/browser freshness and logout evidence remain VERIFY.

- Follow-up regression with loopback/subprocess permissions: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest -q` passed with 837 collected, 823 passed, 14 documented skips, and zero failures/errors after all typed response slices. Release remains HOLD for live-only gates.

## 2026-09-14 — N09/N11 post-publication inventory refresh

- Scope: keep governed asset status aligned after a successful publish response.
- Observable behavior delivered: direct and reconciled committed publication outcomes now trigger an authoritative asset inventory refresh, so active policy metadata updates in-place without a full page reload. Existing load/edit identity fences remain in force.
- Verification: UI TypeScript compilation, eight UI tests, and `git diff --check` passed. Release remains HOLD pending rendered publish/reconciliation and multi-actor evidence.

## 2026-09-14 — N03 policy authoring response contracts

- Scope: remove untyped response boundaries from policy draft, evaluation, review, restore, and idempotency-operation routes.
- Observable behavior delivered: draft revisions, bounded DuckDB evaluation evidence, optional server review authority, restored drafts, and caller-scoped publication operations now validate through shared Pydantic response models. The public `schema` JSON field remains compatible through an explicit model alias; pickle serializers and ticket payloads are unchanged.
- Verification: policy draft/schema/publish-flow suites and OpenAPI route inventory passed (24 tests); changed-path Ruff/format checks and `git diff --check` passed. Generated TypeScript DTO coverage and rendered review/publish evidence remain VERIFY under N03/N09.

## 2026-09-14 — N03 typed settings mutations

- Scope: complete response validation for runtime and authentication-provider settings writes.
- Observable behavior delivered: authenticated settings mutations now publish the same explicit response models already used by settings reads, so generated clients receive stable runtime revisions and redacted provider arrays after writes.
- Verification: settings/actor-auth suites and OpenAPI route inventory passed (38 tests); changed-path Ruff/format checks passed. Generated DTO wiring and live multi-actor settings qualification remain VERIFY.

## 2026-09-14 — N03 typed asset mutations

- Scope: stabilize owner, grant, schema-metadata, and asset-binding mutation responses.
- Observable behavior delivered: asset mutation routes now publish explicit owner, delegated-capability, schema-field, and asset identity contracts; list grants also expose a typed capability item shape. Existing authorization, self-escalation, plugin-pair, and revision checks remain unchanged.
- Verification: actor-auth, draft/version, and OpenAPI route suites passed (49 tests); changed-path Ruff/format checks and `git diff --check` passed. Generated DTO coverage and rendered permission-matrix evidence remain VERIFY under N03/N11.

- Follow-up regression: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest -q` exited 0 across the full repository after the response-model slices. Live OIDC, process-boundary, real-consumer, capacity, deployment, and independent UX/security gates remain VERIFY; release remains HOLD.

## 2026-09-14 — N03 typed catalog mutation response

- Scope: close the catalog create/update response contract used by Connections.
- Observable behavior delivered: catalog upsert now validates and documents its stable catalog identity response while preserving plugin admission, option validation, and revision preconditions.
- Verification: catalog API and OpenAPI route suites passed (15 tests); Ruff and format checks passed. Generated DTO coverage and live catalog lifecycle qualification remain VERIFY.

## 2026-09-14 — N03 referenced-draft revision preconditions

- Scope: close the compare-and-swap gap for editor-to-publisher draft handoff.
- Observable behavior delivered: evaluation, review, and publication now require an expected revision whenever a saved draft is selected by immutable ID. Existing foreign or missing draft IDs retain concealed 404 behavior; stale revisions remain 409; missing revisions return 428 before the mutation can commit.
- Verification: complete schema API suite passed (7 tests), referenced-draft/version regression passed, and changed-path Ruff/format checks passed. Two-process race evidence and generated DTO/browser proof remain VERIFY under B05/B13/B16.

- Follow-up regression: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest -q` exited 0 across the full repository after the precondition change. Release remains HOLD for live-only gates.

## 2026-09-14 — N07 session-scoped inventory query cache

- Scope: begin the required replacement of manual UI inventory cancellation/cache state.
- Observable behavior delivered: the UI now pins `@tanstack/react-query` 5.102.8, scopes inventory keys by exact issuer/principal/search/cursor, passes the Query cancellation signal to the shared API transport, deduplicates identical page reads, and clears/cancels private queries on session changes, expiry, logout, and teardown. Local policy drafts remain component state.
- Verification: UI TypeScript compilation, Vite production build (93.69 kB gzip JavaScript), and 11 UI tests passed; `git diff --check` passed. Remaining N07 work is migration of management/mutation workflows and rendered deferred-response coverage; release remains HOLD.

## 2026-09-14 — N07 session-scoped management query reads

- Scope: extend the session-bound query cache from asset inventory into management read workflows.
- Observable behavior delivered: Changes, Activity, Connections, and Settings reads now use stable TanStack Query keys containing the exact authenticated session scope, pass cancellation signals through the shared transport, deduplicate concurrent reads, and clear on session transitions. Existing epoch fences remain while cursor and mutation workflows are migrated.
- Verification: UI TypeScript compilation, Vite production build (93.88 kB gzip JavaScript), 11 UI tests, and `git diff --check` passed. Remaining N07 work is cursor/mutation migration and rendered deferred-response coverage; release remains HOLD.

## 2026-09-14 — N07 management cursor query reads

- Scope: remove manual AbortController ownership from Changes and Activity pagination.
- Observable behavior delivered: history and audit cursor pages now use session-scoped Query keys containing cursor and filter state, receive Query cancellation signals, and ignore cancellation as a recoverable UI outcome. Filter changes cancel the management query family before clearing visible results; epoch fences still protect component state during the remaining migration.
- Verification: UI TypeScript compilation, Vite production build (93.82 kB gzip JavaScript), 11 UI tests, and `git diff --check` passed. Remaining N07 work is mutation migration and rendered deferred-response coverage; release remains HOLD.

## 2026-09-14 — N07 connection discovery query reads

- Scope: route catalog discovery and diagnostics through the shared session-scoped query client.
- Observable behavior delivered: Connections discovery and connection checks now use stable catalog query keys, pass cancellation signals to the transport, deduplicate repeated requests, and treat Query cancellation as non-error UI state. The old per-component discovery and diagnostic AbortControllers were removed; catalog save, govern, and publication mutations remain on the next migration step.
- Verification: UI TypeScript compilation, Vite production build (93.90 kB gzip JavaScript), 11 UI tests, and `git diff --check` passed. Rendered discovery cancellation and remaining mutation coverage stay VERIFY; release remains HOLD.

## 2026-09-14 — N07 management mutation cache invalidation

- Scope: keep cached management and inventory reads coherent after authenticated settings, catalog, asset-registration, and publication mutations.
- Observable behavior delivered: successful writes invalidate only the current session's affected Query families before the existing reload flow runs. Refetch failures cannot rewrite a successful mutation into an error state because invalidation is deliberately background work; server revision and authorization responses remain authoritative.
- Verification: UI TypeScript compilation, Vite production build (93.99 kB gzip JavaScript), 11 UI tests, and `git diff --check` passed. Main policy and asset-access mutation wiring plus rendered deferred-response coverage remain VERIFY; release remains HOLD.

## 2026-09-14 — N07 session-scoped asset detail reads

- Scope: route asset detail, nested schema, immutable history, grants, access, and selected draft lookup through the shared query client.
- Observable behavior delivered: asset reads now use keys containing session scope, asset identity, and draft identity; every query receives the transport AbortSignal and prior asset families are cancelled before navigation. Cached API objects are treated as immutable and hydrated into a separate editor value, preventing schema decoration from mutating shared query state.
- Verification: UI TypeScript compilation, Vite production build (94.05 kB gzip JavaScript), 11 UI tests, and `git diff --check` passed. Policy/asset mutations and rendered deferred-response coverage remain VERIFY; release remains HOLD.

## 2026-09-14 — N07 asset history and access query wiring

- Scope: finish query ownership for immutable version inspection and access-management cache coherence.
- Observable behavior delivered: policy-version detail lookups now use session/asset/version query keys with Query cancellation, while owner and delegated-capability writes invalidate the asset and inventory families after successful server responses. The existing version epoch and concealed-error behavior remain intact.
- Verification: UI TypeScript compilation, Vite production build (94.09 kB gzip JavaScript), 11 UI tests, and `git diff --check` passed. Draft/evaluate/review/publish/restore mutation wiring and rendered deferred-response coverage remain VERIFY; release remains HOLD.

## 2026-09-14 — N07 policy mutation identity fences

- Scope: make policy draft, evaluation, review, publication, and restore continuations explicit about selected draft identity.
- Observable behavior delivered: every awaited policy workflow captures draft ID and revision alongside existing session/resource/edit fences, refuses to apply stale success or failure state, and invalidates the current asset, inventory, and management query families after successful draft/publication/restore outcomes. Publication reconciliation keeps its original idempotency key.
- Verification: UI TypeScript compilation, Vite production build (94.18 kB gzip JavaScript), 11 UI tests, and `git diff --check` passed. Rendered deferred mutation scenarios and live uncertain-outcome evidence remain VERIFY; release remains HOLD.

## 2026-09-14 — N06 Lucide icon foundation

- Scope: align the shared UI icon primitive with the specified Lucide icon system.
- Observable behavior delivered: the existing semantic `IconName` contract now maps to `lucide-react` components for navigation, status, search, and account actions. Callers and accessible labels remain unchanged, while icon geometry and stroke behavior come from one maintained dependency instead of handwritten path data.
- Verification: UI TypeScript compilation, Vite production build (96.01 kB gzip JavaScript), 11 UI tests, and `git diff --check` passed. Full shell visual, responsive, axe, and screen-reader evidence remains VERIFY under N06; release remains HOLD.

## 2026-09-14 — N07 initial session cache handoff

- Scope: preserve the first authenticated inventory result across the asynchronous session state commit.
- Observable behavior delivered: the bootstrap flow records the loaded session's exact query scope before setting React session state, so the session-transition cleanup effect does not discard the private page it just fetched. Logout, expiry, and explicit session changes still cancel and clear the client before the signed-out view renders.
- Verification: UI TypeScript compilation, Vite production build (96.03 kB gzip JavaScript), 11 UI tests, and `git diff --check` passed. Rendered deferred-response and live authentication evidence remain VERIFY; release remains HOLD.

## 2026-09-14 — N03 generated control-plane DTO contract

- Scope: make the browser client consume a checked-in OpenAPI snapshot and generated TypeScript DTOs for authenticated control-plane responses.
- Observable behavior delivered: `openapi/control-plane.json` is the reviewed contract snapshot; `scripts/generate-api-types.mjs` regenerates `src/generated/control_plane.d.ts` and supports a deterministic `--check` freshness gate. Transport response boundaries now use generated schemas with explicit adapters for nullable and legacy UI shapes. Asset detail and nested schema routes now publish typed response models, eliminating `unknown` success payloads in OpenAPI.
- Verification: `node scripts/generate-api-types.mjs --check`, UI TypeScript compilation, Vite production build (96.26 kB gzip JavaScript), 11 UI tests, focused control-plane OpenAPI tests, and `git diff --check` passed. Node 24 frozen install/image/advisory proof, generated DTO CI wiring, and full browser acceptance remain VERIFY; release remains HOLD.

## 2026-09-14 — N03 discovery and browser-auth response typing

- Scope: close remaining `unknown` JSON success responses on catalog discovery and browser login/logout mutations.
- Observable behavior delivered: catalog table discovery, local bootstrap login, demo login, and logout now have explicit response models in the public OpenAPI contract. The UI transport consumes generated DTOs for those calls; cookie/session behavior is unchanged.
- Verification: UI TypeScript compilation, 11 UI tests, focused route-inventory OpenAPI tests, Ruff, non-heavy pytest hooks, and `git diff --check` passed. Live OIDC/browser freshness, Node 24 frozen install/image/advisory proof, and full acceptance remain VERIFY; release remains HOLD.

## 2026-09-14 — Regression environment boundary

- Scope: record the full-suite verification attempt after the response-contract changes.
- Observable result: the repository-wide pytest run reached the suite but sandbox policy denied ephemeral loopback binds used by Arrow Flight and OIDC fixtures (`Operation not permitted`); the resulting failures are environment-bound startup failures, not assertion regressions. The non-heavy hook suite and all changed-path tests remain green.
- Verification: full run exit 1 with bind-denial diagnostics; focused route/API tests, UI TypeScript, UI tests, generated DTO freshness, Ruff, and service health checks passed. A full Flight/e2e run requires a host with ephemeral loopback bind permission and remains VERIFY; release remains HOLD.

## 2026-09-14 — N03 structured API error envelope

- Scope: make control-plane HTTP failures uniformly correlated and machine-readable.
- Observable behavior delivered: every `HTTPException` now retains its existing status and `detail` while adding a stable non-sensitive error code, human message, and request ID. Revision, validation, authentication, authorization, not-found, rate-limit, oversized-request, and readiness classes map to explicit codes; affected tests assert the new contract without depending on generated IDs.
- Verification: focused catalogs, schema/evaluation, OIDC, actor-auth, and publication suites passed; changed-path Ruff and `git diff --check` passed. Request-validation handler coverage, live hostile transport, and full production acceptance remain VERIFY; release remains HOLD.

## 2026-09-14 — N02 retired policy request models

- Scope: remove residual request-model definitions for the retired public policy routes.
- Observable behavior delivered: unused `PolicyRulesRequest` and `PolicyPreviewRequest` classes are deleted; `/draft` and `/policy-evaluate` now own the only public request contracts, with the evaluation request carrying its bounded synthetic input fields directly. No protected evaluator or pickle path changed.
- Verification: schema API and route-inventory tests passed, obsolete names have zero production callers, Ruff and formatting checks passed. Full N02 migration/deletion proof and installed-wheel qualification remain VERIFY; release remains HOLD.

## 2026-09-14 — N10 generic catalog plugin identity

- Scope: remove Iceberg-specific branching from the generic catalog connection UI.
- Observable behavior delivered: inventory responses expose the server-resolved `plugin_id`, legacy built-in module rows are normalized at the control-plane boundary, and the UI derives fields, defaults, discovery pairings, and labels only from admitted plugin descriptors. Descriptor defaults preserve compatibility options without embedding backend IDs in the UI.
- Verification: focused catalog/inventory/asset API tests (38 passed), UI TypeScript compilation, Vite production build, 11 UI tests, generated DTO freshness, Ruff, and non-heavy pre-commit hooks passed. Full browser visual/accessibility, lifecycle, and production acceptance evidence remain VERIFY; release remains HOLD.
- Atomic implementation commit: `7e69148`.

## 2026-09-14 — N10 backend-neutral schema copy

- Scope: remove the remaining backend-specific label from the nested schema workspace.
- Observable behavior delivered: empty schema search states now describe the control-plane schema generically, keeping the consumer UI aligned with plugin-selected formats.
- Verification: UI TypeScript compilation, 11 UI tests, Vite production build, generated DTO freshness, and pre-commit non-heavy hooks passed. Full visual/accessibility and independent acceptance evidence remain VERIFY; release remains HOLD.
- Atomic implementation commit: `52c06c6`.

## 2026-09-14 — N10 generic catalog form wording

- Scope: finish removing backend-specific wording from catalog authoring controls.
- Observable behavior delivered: URI labels and examples are now catalog-neutral, so external plugin descriptors can present their own configuration without misleading SQL or storage assumptions.
- Verification: UI TypeScript compilation, 11 UI tests, Vite production build, and pre-commit non-heavy hooks passed. Full visual/accessibility and independent acceptance evidence remain VERIFY; release remains HOLD.
- Atomic implementation commit: `715047b`.

## 2026-09-14 — N10 descriptor-default regression

- Scope: pin the compatibility path where a plugin descriptor supplies defaults instead of a UI-specific backend field.
- Observable behavior delivered: an admitted catalog can be created with only its required URI while descriptor defaults remain server-validated and preserved by the authoring flow.
- Verification: catalog API suite (11 passed), Ruff, and pre-commit non-heavy hooks passed. Full production and independent acceptance evidence remain VERIFY; release remains HOLD.
- Atomic implementation commit: `41fdfed`.

## 2026-09-14 — N03/B05 middleware rejection envelope

- Scope: make request-size and malformed `Content-Length` rejections obey the same correlated API error contract as route-level failures.
- Observable behavior delivered: 400/413 middleware responses include stable error codes, safe messages, request IDs, and matching `x-request-id` headers before authentication or body parsing.
- Verification: control-plane actor and request-limit suites (43 passed), Ruff, and non-heavy pre-commit hooks passed. Live hostile transport and full release evidence remain VERIFY; release remains HOLD.
- Atomic implementation commit: `f5ea5bc`.

## 2026-09-14 — N03/B05 framework validation envelope

- Scope: close the remaining unstructured FastAPI query/path validation boundary.
- Observable behavior delivered: framework-generated 422 responses now expose the stable validation code, correlation header, safe message, and normalized field errors without echoing submitted values.
- Verification: audit, request-limit, and actor-auth suites (44 passed), Ruff, and non-heavy pre-commit hooks passed. Live hostile transport and release qualification remain VERIFY; release remains HOLD.
- Atomic implementation commit: `9c8cd9d`.

## 2026-09-14 — N03/B05 readiness error correlation

- Scope: align the public readiness failure response with the control-plane error contract.
- Observable behavior delivered: database readiness failures retain their health details and now include `not_ready`, a safe message, and a request ID mirrored in the response header.
- Verification: health and request-boundary suites (7 passed), Ruff, and non-heavy pre-commit hooks passed. Production outage and recovery evidence remain VERIFY; release remains HOLD.
- Atomic implementation commit: `b027153`.

## 2026-09-14 — N07 validation recovery details

- Scope: preserve structured field errors from the control plane through the browser transport and recovery UI.
- Observable behavior delivered: redacted validation fields are parsed as typed client data and named in recovery messages without exposing submitted values; malformed envelopes are ignored safely.
- Verification: UI TypeScript compilation, 12 UI tests, Vite production build, and non-heavy pre-commit hooks passed. Full rendered mutation and accessibility evidence remain VERIFY; release remains HOLD.
- Atomic implementation commit: `abaa6cd`.

## 2026-09-14 — N03/B05 conflict revision detail

- Scope: make stale revision responses directly actionable for API and UI clients.
- Observable behavior delivered: numeric current revisions are safely included in conflict envelopes when present in trusted repository errors, while arbitrary conflict text remains unchanged and non-sensitive.
- Verification: catalog and request-boundary suites (21 passed), Ruff, and non-heavy pre-commit hooks passed. Full multi-process race and browser evidence remain VERIFY; release remains HOLD.
- Atomic implementation commit: `573d80a`.

## 2026-09-14 — N03/B05 body validation fields

- Scope: retain safe field-level diagnostics when strict request models reject a body.
- Observable behavior delivered: 422 envelopes now normalize Pydantic locations into `field_errors` while retaining the original detail and never echoing submitted values; the UI can render the same typed shape for query and body failures.
- Verification: catalog, audit, and request-boundary suites (25 passed), Ruff, and non-heavy pre-commit hooks passed. Full rendered field-summary and browser acceptance remain VERIFY; release remains HOLD.
- Atomic implementation commit: `1ad8fbe`.

## Per-packet record template

## 2026-09-14 — N02/N13 external catalog publication identity

- Scope: close the manifest round-trip gap for admitted non-Iceberg catalog and table-format plugins.
- Observable behavior delivered; FR/NFR and B/G subcases: publication compilation now records an explicit catalog `plugin_id` and generic `plugin` runtime discriminator for external catalogs; the data-plane catalog config parser accepts that discriminator while retaining fail-closed rejection of removed legacy catalog types. Asset plugin bindings remain explicit and unchanged.
- Changed and deleted paths; old callers removed; protected pickle check: compiler, catalog registry, published-config adapter, repository identity extraction, and compiler/published-config regression tests changed. No paths deleted; no pickle serializer, serialized class, payload, or import path changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +29 production / +6 test logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: `tests/control_plane/test_publication_compiler.py` and `tests/infrastructure/adapters/test_published_config.py`; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane/test_publication_compiler.py tests/infrastructure/adapters/test_published_config.py tests/infrastructure/adapters/test_catalog_registry.py tests/common/config_store/test_plugin_bindings.py -q` (exit 0); changed-path Ruff and format checks (exit 0); 2026-09-14, local Python 3.12/uv environment.
- Artifact and fixture hashes; evidence locations: atomic commit `7054afd`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: real external catalog publication/activation and three consumer qualification pairs remain VERIFY under N12/N13; next action is complete lifecycle and live plugin evidence without loosening admission.
- Atomic implementation commits: `7054afd`.
- Human acceptance, if required: independent UX/security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N10 plugin lifecycle controls

- Scope: make admitted plugin lifecycle state operator-actionable in the authenticated management UI.
- Observable behavior delivered; FR/NFR and B/G subcases: platform admins can transition admitted catalog or table-format plugins through the strict lifecycle state machine (`enabled`, `draining`, `disabled`, `revoked`, `removed`) via an authenticated PATCH route; each transition is audited, the OpenAPI contract is generated, and Connections renders state selectors with confirmation for removal.
- Changed and deleted paths; old callers removed; protected pickle check: plugin schemas/routes, `ProvisioningService`, generated OpenAPI/TypeScript client, `ConnectionsView`, route inventory, and plugin API tests changed. No old route deleted; no pickle serializer, serialized class, payload, or import path changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +179 production/UI / +55 test/contract logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: `tests/interfaces/control_plane/test_plugins_api.py` and `tests/architecture/test_control_plane_route_inventory.py`; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: plugin API and route inventory suites (exit 0); governance UI TypeScript build, Vite build, UI tests, generated DTO freshness, changed-path Ruff/format and diff checks (exit 0); 2026-09-14, local Python 3.12/Node 24 toolchain.
- Artifact and fixture hashes; evidence locations: atomic commit `4d0e29d`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: lifecycle state is process-local by design; live multi-process revocation timing, browser/axe/screen-reader evidence, and independent release qualification remain VERIFY under N05/N10/N12/N16. Next action is qualify the running local UI/API and then continue the remaining acceptance gates.
- Atomic implementation commits: `4d0e29d`.
- Human acceptance, if required: independent UX/security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N08 canonical mask vocabulary

- Scope: remove the duplicate UI mask-option contract from nested policy authoring.
- Observable behavior delivered; FR/NFR and B/G subcases: one access-control vocabulary now owns the six supported mask types; authoritative schema responses expose that bounded set; the policy editor derives choices from the server response while preserving value validation and nested field behavior.
- Changed and deleted paths; old callers removed; protected pickle check: new `common/access_control/mask_types.py`, compiler/schema service/response model, generated OpenAPI client, `AssetWorkspace`, and schema tests changed. The UI-local mask option list was removed; no pickle serializer, serialized class, payload, or import path changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +27 production/UI / +1 test logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: `tests/control_plane/test_schema_service.py`; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: schema service/API suites (exit 0); governance UI TypeScript, Vite, UI tests, generated DTO freshness, Ruff/format and diff checks (exit 0); 2026-09-14, local Python 3.12/Node 24 toolchain.
- Artifact and fixture hashes; evidence locations: atomic commit `f495b6b`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: live all-mask rendered browser coverage, 10k-field capacity, and independent UX/accessibility review remain VERIFY under N08/N14/N16; next action is execute those qualification scenarios against the running UI.
- Atomic implementation commits: `f495b6b`.
- Human acceptance, if required: independent UX/security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N10 lifecycle transition UX guard

- Scope: align lifecycle controls with the server state machine.
- Observable behavior delivered: Connections only offers valid transitions for the current plugin lifecycle, preventing impossible requests while retaining confirmation for irreversible removal.
- Verification: governance UI TypeScript build and 12 UI tests passed; no backend or pickle paths changed.
- Atomic implementation commit: `86b0772`.
- Remaining subcases: browser accessibility and live multi-process lifecycle qualification remain VERIFY; release remains HOLD.

## 2026-09-14 — N06 responsive navigation drawer

- Scope: implement the required small-screen navigation behavior for the authenticated governance shell.
- Observable behavior delivered; FR/NFR and B/G subcases: below 768px the desktop rail becomes a fixed, keyboard-dismissible drawer with an accessible menu button, labelled backdrop, valid navigation focus restoration, and the same authorization-gated destinations. Selecting a destination or pressing Escape closes the drawer; desktop layouts retain the persistent rail.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/main.tsx`, `apps/governance-ui/src/components/Icon.tsx`, and `apps/governance-ui/src/styles.css`; no backend, plugin, serializer, or pickle paths changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +27 UI production lines; no dependencies added or removed (`Menu` is from the existing Lucide dependency).
- Primary invariant test owners; tests consolidated/deleted: existing governance UI tests plus TypeScript/build checks; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node node_modules/typescript/bin/tsc -b tsconfig.json` (exit 0), `node node_modules/vite/bin/vite.js build` (exit 0; 321.70 kB JS / 96.99 kB gzip), `node --experimental-strip-types --test tests/*.test.mjs` (12 passed), 2026-09-14, Node 24/local workspace. The commit hook's repository-wide `ty` job remains a known baseline failure in unrelated routes/examples and was skipped for the atomic commit.
- Artifact and fixture hashes; evidence locations: atomic implementation commit `63a4f41`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: rendered keyboard, screen-reader, contrast, 200% zoom, and responsive browser evidence remain VERIFY under N06/N16; release remains HOLD. Next action is continue the next ready acceptance slice and retain the live visual evidence requirement.
- Atomic implementation commits: `63a4f41`.
- Human acceptance, if required: independent UX/accessibility review remains VERIFY; release remains HOLD.

## 2026-09-14 — N04 REST URI boundary correction

- Scope: close a local-target correctness gap in the REST catalog adapter.
- Observable behavior delivered; FR/NFR and B/G subcases: explicitly local `file:///...` warehouse URIs are accepted for the supported local/private deployment path; remote auxiliary URIs still require an authority, non-local file authorities are rejected, and malformed catalog or auxiliary ports fail before provider construction. Credential-bearing and query-bearing URI rejection is unchanged.
- Changed and deleted paths; old callers removed; protected pickle check: `packages/iceberg-rest-plugin/src/dal_obscura_iceberg_rest/catalog.py` and `tests/integration/test_io_boundary.py`; no core, data-plane, serializer, or pickle paths changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +31 production/test logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: integration IO-boundary tests now cover local file acceptance and malformed-port rejection; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/integration/test_io_boundary.py packages/iceberg-rest-plugin/tests/test_rest_plugin.py -q` (26 passed); changed-path Ruff and format checks (exit 0); 2026-09-14, Python 3.12/uv local workspace.
- Artifact and fixture hashes; evidence locations: atomic implementation commit `a9f9362`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: live DNS/redirect/private-destination counters, credential redaction, cancellation cleanup, and deployment network enforcement remain VERIFY under B06/N04; next action is continue those transport-boundary checks without weakening explicit local/private allowlists.
- Atomic implementation commits: `a9f9362`.
- Human acceptance, if required: independent security and release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N04 published storage path enforcement

- Scope: close the existing database-to-runtime gap for storage path rules.
- Observable behavior delivered; FR/NFR and B/G subcases: runtime drafts retain configured `path_rules`, publication compilation includes them in the immutable runtime manifest, the published-config store reads them, and the data-plane composition root installs a `PathRuleEnforcer` for every catalog registry generation. Metadata and file tasks therefore receive the same published root boundary instead of silently running with no enforcer.
- Changed and deleted paths; old callers removed; protected pickle check: runtime domain/compiler, publication repository, published runtime/catalog registry adapter, data-plane composition root, and focused regression tests changed. No migration, backend, serializer, serialized class, payload, or pickle import path changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +52 production/test logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: compiler runtime-manifest, published-config, path-rule, and data-plane identity suites; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: focused publication/published-config/schema-migration suite (75 passed), data-plane/path-rule/catalog suite (21 passed), changed-path Ruff/format and diff checks (exit 0), 2026-09-14, Python 3.12/uv local workspace.
- Artifact and fixture hashes; evidence locations: atomic implementation commit `24e8890`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: runtime authoring still exposes no path-rule editor and live hostile destination/DNS/redirect/cancellation counters remain VERIFY under B06/N04; next action is continue transport qualification and document deployment network controls.
- Atomic implementation commits: `24e8890`.
- Human acceptance, if required: independent security/release review and real destination evidence remain VERIFY; release remains HOLD.

## 2026-09-14 — N04 authenticated path-rule management

- Scope: make the existing published storage boundary configurable through the normal authenticated settings workflow.
- Observable behavior delivered; FR/NFR and B/G subcases: runtime settings now accept and return a bounded path-root list, validate roots through the same `PathRuleEnforcer` used by the data plane before persistence, include only redacted roots in the audit event, and render a JSON editor in Settings with client-side structural feedback. Existing expected-revision behavior remains in force; empty roots remain an explicit local-development choice.
- Changed and deleted paths; old callers removed; protected pickle check: runtime schemas/routes/services/repository, path-rule adapter, generated OpenAPI/TypeScript DTOs, Settings UI, and API inventory/settings tests changed. No migration, serializer, serialized class, payload, or pickle import path changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +80 production/UI/test logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: control-plane settings, inventory-read, compiler, published-config, and path-rule suites; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: settings/inventory/compiler/published-config/path-rule suite (all passed), UI TypeScript compilation, Vite build (322.55 kB JS / 97.39 kB gzip), 12 UI tests, generated DTO freshness, changed-path Ruff/format and diff checks (exit 0), 2026-09-14, Python 3.12/Node 24 local workspace.
- Artifact and fixture hashes; evidence locations: atomic implementation commit `3a86a7a`; regenerated `apps/governance-ui/openapi/control-plane.json` and `src/generated/control_plane.d.ts`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: live DNS/redirect/private-destination controls, cancellation cleanup, production network enforcement, and independent browser/accessibility evidence remain VERIFY under B06/N04/N06/N16; next action is continue the live transport and consumer qualification gates.
- Atomic implementation commits: `3a86a7a`.
- Human acceptance, if required: independent security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N07 centralized UI cancellation detection

- Scope: make all authenticated UI surfaces classify cancellation consistently during deferred reads and writes.
- Observable behavior delivered; FR/NFR and B/G subcases: a shared `isAbortError` helper recognizes both browser `AbortError` and query/provider `CancelledError`; main workspace, connections, asset access, and settings flows now use the same recovery rule and suppress expected cancellation noise.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/async.ts`, `main.tsx`, `ConnectionsView.tsx`, `AssetWorkspace.tsx`, and `SettingsView.tsx`; no backend, serializer, payload, migration, dependency, or API changes.
- Production/test logical SLOC delta; dependencies added/removed and reason: -6 duplicated UI lines, +10 shared helper/import lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing lifecycle/query suite, TypeScript compilation, and production build; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `tsc -p apps/governance-ui/tsconfig.json --noEmit` (exit 0), `node --experimental-strip-types --test tests/*.test.mjs` (13 passed), Vite production build (exit 0; 325.43 kB JS / 98.06 kB gzip), 2026-09-14, Node 24 local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic implementation commit `e8210e5`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: rendered deferred-response matrix, live logout/401/revocation, browser accessibility, and independent security/release evidence remain VERIFY under N05–N07/N16; next action is continue live transport and consumer qualification while retaining release HOLD.
- Atomic implementation commits: `e8210e5`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N13 JVM/Spark authenticated consumer qualification

- Scope: make the JVM consumer fixture exercise the real OIDC/JWKS path deterministically and qualify the Spark connector against the current candidate.
- Observable behavior delivered; FR/NFR and B/G subcases: each JVM fixture now reserves a loopback JWKS port, publishes a bounded local JWKS URL, and starts a disposable loopback HTTP server serving only `GET /jwks.json`. The fixture keeps backward-compatible construction for callers without a JWKS port, and its policy selectors use explicit nested leaves so publication admission remains fail-closed. Pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `tests/support/build_connector_fixture.py`, `connectors/jvm/connector-testkit-jvm/src/main/java/io/dalobscura/connectors/testkit/FixtureBuilderRunner.java`, `FixtureBundle.java`, `LocalDalObscuraServer.java`, and `FixtureBuilderRunnerTest.java`; no production serializers, serialized classes/import paths, payloads, migrations, dependencies, or tests deleted.
- Production/test logical SLOC delta; dependencies added/removed and reason: +174 JVM/test/fixture lines; no dependencies changed. The JWKS server binds only to `127.0.0.1`, returns bounded status responses, and is stopped with the fixture process.
- Primary invariant test owners; tests consolidated/deleted: JVM client, Spark datasource, fixture parser, and Spark integration tests remain owners; no tests deleted. External clean-wheel, TLS/OIDC, PostgreSQL race/recovery, hostile transport, capacity, and three external consumer-pair evidence remain open.
- Exact commands, exit codes, UTC date, runtime versions, environment: `mvn -f connectors/jvm/pom.xml -pl integration-tests-jvm -am -Dtest=SparkReadIT -Dsurefire.failIfNoSpecifiedTests=false test` (exit 0; 6 passed), `mvn -f connectors/jvm/pom.xml verify` (exit 0; client 7 passed, Spark unit 29 passed, fixture 2 passed, integration 6 passed), `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/test_support_build_connector_fixture.py -q` (exit 0; 3 passed), and pre-commit non-heavy pytest (850 passed, 7 skipped, 15 deselected), 2026-09-14, Java 17/Spark 3.5.6/Python 3.12/uv.
- Artifact and fixture hashes; evidence locations: implementation commit `249e11c`; Maven reports under `connectors/jvm/*/target/surefire-reports`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: clean installed wheels and real DuckDB/Spark/JVM cells across SQL-Iceberg, REST-Iceberg, and manifest/Parquet, live OIDC PKCE/freshness/revocation, hostile transport counters, PostgreSQL races/recovery, mixed-load capacity, pinned deployment integrity/SBOM, browser accessibility, and independent review remain VERIFY under N04–N16. Next action is continue candidate-bound consumer and deployment qualification while retaining release HOLD.
- Atomic implementation commits: `249e11c`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N04 local Iceberg storage-property enforcement

- Scope: extend returned-location checks to local filesystem paths carried in Iceberg IO properties.
- Observable behavior delivered; FR/NFR and B/G subcases: catalog resolution now checks URI values and absolute/file local paths in provider storage options against the published path roots. An out-of-root local warehouse is rejected before the table descriptor is admitted; existing S3/URI checks and explicit local roots remain unchanged.
- Changed and deleted paths; old callers removed; protected pickle check: `src/dal_obscura/data_plane/infrastructure/adapters/catalog_registry.py` and `tests/infrastructure/adapters/test_catalog_registry.py`; no serializers, serialized classes, payloads, import paths, migrations, or dependencies changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +28 production/test logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: catalog registry returned-location tests cover unsafe local storage properties; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: catalog registry suite (12 passed), changed-path Ruff checks (exit 0), 2026-09-14, Python 3.12/uv local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic implementation commit `81d7b10`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: live hostile DNS/redirect/private destination counters, provider-level manifest/data/delete interception, cancellation cleanup, deployment network controls, browser/consumer qualification, and independent security/release evidence remain VERIFY under B06/N04/N16; next action is continue the live transport-counter harness and test actual provider requests under cancellation.
- Atomic implementation commits: `81d7b10`.
- Human acceptance, if required: independent security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N04 Iceberg metadata and delete-file path enforcement

- Scope: close the remaining in-process Iceberg path-policy gap for locations returned by table metadata and scan tasks.
- Observable behavior delivered; FR/NFR and B/G subcases: table loading checks the table location, manifest-list locations, and historical metadata-log locations against published storage roots. Planned and executed scan tasks check both the data file and every delete file before ArrowScan can open them. Out-of-root locations fail closed with `PermissionError`; the existing trusted pickle task transport is unchanged.
- Changed and deleted paths; old callers removed; protected pickle check: `src/dal_obscura/data_plane/infrastructure/table_formats/iceberg.py` and `tests/infrastructure/adapters/test_iceberg_phase0_regressions.py`; no serializers, serialized classes, payloads, import paths, migrations, or dependencies changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +58 production/test logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: Iceberg phase-regression tests cover unsafe delete-file and manifest/historical metadata locations; existing catalog/path-rule tests remain; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: focused Iceberg/catalog suite (22 passed), changed-path Ruff checks (exit 0), 2026-09-14, Python 3.12/uv local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic implementation commit `1f151d0e`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: live hostile redirect/DNS/private destination counters, provider-level manifest/data/delete interception, cancellation cleanup, deployment network controls, browser/consumer qualification, and independent security/release evidence remain VERIFY under B06/N04/N16; next action is build the live transport-counter harness and qualify DNS pinning/redirect behavior without weakening explicit local/private allowlists.
- Atomic implementation commits: `1f151d0e`.
- Human acceptance, if required: independent security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N07 policy validation summary

- Scope: make server-side policy field errors directly actionable in the editor.
- Observable behavior delivered: failed draft saves preserve the local rules, retain typed field errors from the correlated API envelope, and render them in a persistent focusable alert summary above the workspace. A successful save clears the summary; recovery status remains available for non-validation failures.
- Verification: governance UI TypeScript compilation, Vite build (323.00 kB JS / 97.50 kB gzip), 12 UI tests, and diff checks passed. No backend, plugin, serializer, or pickle paths changed.
- Atomic implementation commit: `32fadc0`.
- Remaining subcases: rendered field focus routing, all mutation/deferred-response browser scenarios, and independent accessibility review remain VERIFY under N07/N16; release remains HOLD.

## 2026-09-14 — N04 strict path-rule request validation

- Scope: close the direct API type boundary for runtime storage roots.
- Observable behavior delivered: runtime path-rule writes now reject non-list payloads, non-object entries, extra keys, non-string roots, and empty roots before persistence or enforcer construction; accepted entries remain exactly one non-empty `root` string.
- Verification: control-plane settings suite (6 passed), changed-path Ruff and diff checks passed. No serializer, pickle, migration, or external dependency changes.
- Atomic implementation commit: `a6652a0`.
- Remaining subcases: live redirect/DNS/private-destination counters, cancellation cleanup, and independent release evidence remain VERIFY under N04/N16; release remains HOLD.

## 2026-09-14 — N01/N02 plugin architecture documentation alignment

- Scope: remove stale fixed-backend claims from the operator, developer, and quickstart entry points.
- Observable behavior delivered: `docs/concepts.md`, `docs/development.md`, and `docs/quickstart.md` now describe allowlisted catalog/table-format plugins, descriptor-based selection, and the retained explicit security boundary.
- Verification: documentation diff/link scope is limited to three existing files; `git diff --check` passed. No runtime, API, serializer, or pickle behavior changed.
- Atomic implementation commit: `0b40ed1`.
- Remaining subcases: N01 installed-toolchain baseline and N13 real plugin/consumer qualification remain VERIFY; release remains HOLD.

Replace the corresponding queue entry and keep one current record per packet.
Link detailed logs/artifacts instead of appending repeated full narratives.

- Packet / status / candidate commit / owner:
- Observable behavior delivered; FR/NFR and B/G subcases:
- Changed and deleted paths; old callers removed; protected pickle check:
- Production/test logical SLOC delta; dependencies added/removed and reason:
- Primary invariant test owners; tests consolidated/deleted:
- Exact commands, exit codes, UTC date, runtime versions, environment:
- Artifact and fixture hashes; evidence locations:
- Remaining subcases; blocker and next concrete action:
- Atomic implementation commits:
- Human acceptance, if required: reviewer/date/artifact or VERIFY:

Do not mark N16 complete on build/lint success or substitute agent self-review
for independent review. Until candidate-bound gates all pass, release is HOLD.

## 2026-09-14 — N04 REST redirect boundary

- Scope: prevent provider-managed redirects from bypassing governed destination checks.
- Observable behavior delivered: the shared bounded REST session forces `allow_redirects=False` on every request, so a 3xx response cannot trigger an unvalidated follow-up request; callers must re-enter the connection boundary before using a new destination.
- Verification: REST plugin and IO-boundary suites (26 passed), changed-path Ruff/format and diff checks passed. No serializers, pickle paths, or dependencies changed.
- Atomic implementation commit: `b286610`.
- Remaining subcases: DNS destination pinning, private-address policy, returned metadata/delete locations, cancellation cleanup, and live request-counter evidence remain VERIFY under B06/N04; release remains HOLD.

## 2026-09-14 — N04 local file URI path enforcement

- Scope: align published storage path enforcement with the explicitly supported local REST warehouse URI form.
- Observable behavior delivered; FR/NFR and B/G subcases: `PathRuleEnforcer` now treats `file:///...` roots and descendants as local filesystem paths, accepts equivalent plain paths, and rejects remote file authorities and query-bearing roots. URI traversal and root-boundary checks remain enforced.
- Changed and deleted paths; old callers removed; protected pickle check: `src/dal_obscura/data_plane/infrastructure/adapters/path_rules.py` and its focused tests changed. No serializers, serialized classes, payloads, import paths, migrations, or dependencies changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +12 production / +14 test logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: `tests/infrastructure/adapters/test_path_rules.py` covers local URI acceptance, equivalent paths, descendant boundaries, and remote-authority rejection; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/infrastructure/adapters/test_path_rules.py tests/integration/test_io_boundary.py -q` (20 passed); changed-path Ruff and format checks (exit 0); 2026-09-14, Python 3.12/uv local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic commit `e48f497`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: live hostile DNS/private/redirect counters, cancellation cleanup, provider network policy, and independent security/release evidence remain VERIFY under B06/N04/N16; next action is continue the next bounded acceptance slice without loosening explicit local/private allowlists.
- Atomic implementation commits: `e48f497`.
- Human acceptance, if required: independent security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N07 cancellable UI mutations

- Scope: make authenticated browser mutations obey the same cancellation and session fencing as reads.
- Observable behavior delivered; FR/NFR and B/G subcases: the shared UI transport accepts `AbortSignal` for bootstrap, logout, draft save, policy evaluation/review, publication, reconciliation, and restore. App-owned controllers are aborted on logout, session expiry, unmount, and cleanup; aborted work is silent and cannot apply stale private state.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/api.ts` and `apps/governance-ui/src/main.tsx`; no backend, plugin, serializer, serialized class, payload, import path, migration, or dependency changes.
- Production/test logical SLOC delta; dependencies added/removed and reason: +73 UI logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing UI lifecycle/query tests and build checks; no tests deleted. Mutation cancellation remains a browser-rendered scenario for N07/N16.
- Exact commands, exit codes, UTC date, runtime versions, environment: TypeScript build (exit 0), UI test runner (12 passed), Vite production build (exit 0; 323.68 kB JS / 97.63 kB gzip), 2026-09-14, Node 24 local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic commit `0da0b6e`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: rendered deferred-response race matrix, full logout/401 browser journey, and independent accessibility/UX evidence remain VERIFY under N07/N16; next action is continue the next bounded acceptance slice without claiming browser evidence from build success.
- Atomic implementation commits: `0da0b6e`.
- Human acceptance, if required: independent UX/security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N04 strict path value typing

- Scope: close the direct adapter boundary for malformed storage-root values.
- Observable behavior delivered; FR/NFR and B/G subcases: `PathRuleEnforcer` rejects non-string roots and paths instead of coercing arbitrary objects into filesystem text; the published root allowlist remains fail-closed while local `file:///` support and traversal checks remain intact.
- Changed and deleted paths; old callers removed; protected pickle check: path-rule adapter and focused tests changed. No serializers, serialized classes, payloads, import paths, migrations, or dependencies changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +5 production / +3 test logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: `tests/infrastructure/adapters/test_path_rules.py`; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: path-rule and IO-boundary suite (21 passed), changed-path Ruff and format checks (exit 0), 2026-09-14, Python 3.12/uv local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic commit `2de4b01`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: live hostile destination counters, DNS/private-address enforcement, cancellation cleanup, and independent security/release evidence remain VERIFY under B06/N04/N16; next action is continue bounded backend/UI acceptance work.
- Atomic implementation commits: `2de4b01`.
- Human acceptance, if required: independent security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N10 catalog secret-reference round trip

- Scope: prevent the authenticated connection editor from corrupting an existing catalog credential reference during ordinary edits.
- Observable behavior delivered; FR/NFR and B/G subcases: leaving a secret field blank now preserves only the existing `{secret, scope}` reference; the literal `[redacted]` marker, raw strings, extra fields, and empty values are rejected from the round-trip. Secret values remain server-side.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/connection_options.ts`, `ConnectionsView.tsx`, and its unit test changed. No backend, plugin, serializer, serialized class, payload, import path, migration, or dependency changes.
- Production/test logical SLOC delta; dependencies added/removed and reason: +25 UI / +8 test logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: `apps/governance-ui/tests/connection_options.test.mjs`; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: TypeScript build (exit 0), UI test runner (13 passed), Vite production build (exit 0; 323.92 kB JS / 97.71 kB gzip), 2026-09-14, Node 24 local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic commit `6503f2f`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: live edit/discovery against a real secret provider, lifecycle propagation across workers, and independent security/release evidence remain VERIFY under N10/N13/N16; next action is continue generic lifecycle and consumer qualification without exposing credentials.
- Atomic implementation commits: `6503f2f`.
- Human acceptance, if required: independent security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N05 conflicting browser-cookie rejection

- Scope: make browser authentication fail closed when legacy and `__Host-` cookie names are sent together with different values.
- Observable behavior delivered; FR/NFR and B/G subcases: actor, admin, mutation, and logout dependencies now accept duplicate cookie values only when they match; conflicting session or CSRF cookies return a safe 400 before identity or revocation decisions.
- Changed and deleted paths; old callers removed; protected pickle check: control-plane route dependencies/session route and actor-auth tests changed. No serializers, serialized classes, payloads, import paths, migrations, or dependencies changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +31 production / +16 test logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: `tests/interfaces/control_plane/test_actor_auth.py` adds conflicting host/legacy session coverage; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: actor-auth suite (34 passed), changed-path Ruff and format checks (exit 0), 2026-09-14, Python 3.12/uv local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic commit `0da0c6b`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: live OIDC code/PKCE, account-role freshness/revocation, two-process session evidence, and independent security/release review remain VERIFY under B07/B08/N05/N16; next action is continue the normal-auth and permission qualification work without reintroducing browser-token shortcuts.
- Atomic implementation commits: `0da0c6b`.
- Human acceptance, if required: independent security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N04 local file authority alignment

- Scope: make REST URI admission and published path enforcement share one explicit local-file contract.
- Observable behavior delivered; FR/NFR and B/G subcases: REST catalog auxiliary URIs accept authority-free `file:///...` paths and reject all file URI authorities, including `localhost`, before provider construction. Remote storage URIs retain their existing authority, credential, query, port, and egress checks.
- Changed and deleted paths; old callers removed; protected pickle check: REST plugin validator and IO-boundary tests changed. No serializers, serialized classes, payloads, import paths, migrations, or dependencies changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +1 production / +5 test logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: `tests/integration/test_io_boundary.py` and REST plugin tests cover local acceptance and authority rejection; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: REST/IO-boundary suite (27 passed), changed-path Ruff and format checks (exit 0), 2026-09-14, Python 3.12/uv local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic commit `93dbe0a`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: live DNS/private/redirect counters, cancellation cleanup, provider network policy, and independent security/release evidence remain VERIFY under B06/N04/N16; next action is continue bounded security fixes and retain release HOLD.
- Atomic implementation commits: `93dbe0a`.
- Human acceptance, if required: independent security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N01 authoritative elevated full-suite refresh

- Scope: refresh the repository baseline after the completed security and UI lifecycle slices using a host that permits the loopback and subprocess checks required by the suite.
- Observable behavior delivered; FR/NFR and B/G subcases: no product behavior changed; this entry records qualification evidence only. Pickle serializers, serialized classes/import paths, and payload semantics remain untouched.
- Changed and deleted paths; old callers removed; protected pickle check: documentation only (`docs/plugin-platform/BASELINE_20260914.md` and this ledger entry); no source, migration, dependency, or fixture changes.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: the existing full suite and its explicit opt-in skip markers; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest --durations=20 --junitxml=/tmp/dal-obscura-current-elevated.xml -q -rs` (exit 0; 853 collected, 839 passed, 14 skipped, 0 failed, 0 errors, 131.115s), 2026-09-14, Python 3.12/uv local workspace with loopback permissions. UI checks remain TypeScript build (exit 0), 13 Node tests (exit 0), and Vite build (exit 0; 324.75 kB JS / 97.94 kB gzip).
- Artifact and fixture hashes; evidence locations: JUnit report `/tmp/dal-obscura-current-elevated.xml`; baseline delta in `docs/plugin-platform/BASELINE_20260914.md`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: the 14 skips are explicit benchmark, loopback-consumer, PostgreSQL race/recovery opt-ins; Node 24 clean-install/image/advisory, live OIDC, hostile transport counters, rendered browser/accessibility, real consumer pairs, recovery, and independent review remain VERIFY under N01/N04–N16. Next action is continue the next bounded acceptance slice while retaining release HOLD.
- Atomic implementation commits: `acb1487e` (UI code); this documentation evidence is recorded separately.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N07 cancellable connection mutations

- Scope: apply lifecycle cancellation to authenticated catalog, asset-registration, configuration-publication, and plugin-lifecycle writes in the governance UI.
- Observable behavior delivered; FR/NFR and B/G subcases: each connection mutation carries an `AbortSignal`; active writes abort when the view unmounts or session scope changes; aborted or stale responses cannot overwrite messages, publication state, or lifecycle controls. Server-held secret references and authorization boundaries remain unchanged.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/api.ts` and `apps/governance-ui/src/components/ConnectionsView.tsx`; no backend, plugin, serializer, serialized class, payload, import path, migration, or dependency changes.
- Production/test logical SLOC delta; dependencies added/removed and reason: +64 UI logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing lifecycle/query tests plus TypeScript compile and production build; no tests deleted. Rendered deferred-response and unmount evidence remains a browser acceptance owner for N07/N16.
- Exact commands, exit codes, UTC date, runtime versions, environment: UI TypeScript build (exit 0), `node --experimental-strip-types --test tests/*.test.mjs` (13 passed), Vite production build (exit 0; 324.75 kB JS / 97.94 kB gzip), 2026-09-14, Node 24 local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic commit `acb1487e`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: browser-rendered deferred mutation races, live 401/logout journey, cross-process lifecycle propagation, and independent accessibility/security/release review remain VERIFY under N07/N10/N16; next action is continue live acceptance and consumer qualification without claiming browser evidence from static checks.
- Atomic implementation commits: `acb1487e`.
- Human acceptance, if required: independent UX/security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N07 cancellable settings mutations

- Scope: extend lifecycle cancellation to authenticated runtime-limit and identity-provider settings writes.
- Observable behavior delivered; FR/NFR and B/G subcases: settings PUT requests carry an `AbortSignal`; active writes abort when the settings view unmounts or changes session scope; aborted or stale responses cannot overwrite draft status or error messaging. Server-side revision and publication controls remain authoritative.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/api.ts` and `apps/governance-ui/src/components/SettingsView.tsx`; no backend, plugin, serializer, serialized class, payload, import path, migration, or dependency changes.
- Production/test logical SLOC delta; dependencies added/removed and reason: +35 UI logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing lifecycle/query tests plus TypeScript compile and production build; no tests deleted. Rendered deferred settings responses and live session-expiry evidence remain browser acceptance owners for N07/N16.
- Exact commands, exit codes, UTC date, runtime versions, environment: UI TypeScript build (exit 0), `node --experimental-strip-types --test tests/*.test.mjs` (13 passed), Vite production build (exit 0; 325.25 kB JS / 98.02 kB gzip), 2026-09-14, Node 24 local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic commit `2240dd1`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: browser-rendered deferred settings races, live logout/401 and cross-process revocation, and independent accessibility/security/release review remain VERIFY under N05/N07/N16; next action is continue live acceptance and consumer qualification while retaining release HOLD.
- Atomic implementation commits: `2240dd1`.
- Human acceptance, if required: independent UX/security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N07 lifecycle CORS preflight

- Scope: make the cross-origin browser contract admit the plugin lifecycle `PATCH` mutation used by the authenticated Connections UI.
- Observable behavior delivered; FR/NFR and B/G subcases: configured UI origins now receive a successful preflight for `PATCH` with CSRF/content-type headers; unconfigured origins remain denied. No authentication or authorization decision was widened.
- Changed and deleted paths; old callers removed; protected pickle check: `src/dal_obscura/control_plane/interfaces/api.py` and `tests/interfaces/control_plane/test_ui_shell.py`; no serializers, payloads, migrations, dependencies, or unrelated routes changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +1 production / +16 test logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: `tests/interfaces/control_plane/test_ui_shell.py::test_cors_allows_plugin_lifecycle_patch_for_configured_ui_origin`; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: control-plane UI-shell suite (8 passed), changed-path Ruff and format checks (exit 0), 2026-09-14, Python 3.12/uv local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic commit `ac8aab2`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: live separate-origin browser authentication/CSRF, rendered accessibility, and independent security/release evidence remain VERIFY under N05–N07/N16; next action is continue live browser and consumer qualification while retaining release HOLD.
- Atomic implementation commits: `ac8aab2`.
- Human acceptance, if required: independent UX/security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N07 cancellable asset access mutations

- Scope: apply lifecycle cancellation to owner and delegated-capability writes in the asset Access view.
- Observable behavior delivered; FR/NFR and B/G subcases: access PUT requests carry an `AbortSignal`; active writes abort when the asset view unmounts or session scope changes; aborted or stale responses cannot overwrite capability state, messages, or cached private data.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/api.ts` and `apps/governance-ui/src/components/AssetWorkspace.tsx`; no backend, plugin, serializer, serialized class, payload, import path, migration, or dependency changes.
- Production/test logical SLOC delta; dependencies added/removed and reason: +37 UI logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing lifecycle/query tests plus TypeScript compile and production build; no tests deleted. Rendered deferred access races and live session-expiry evidence remain browser acceptance owners for N07/N16.
- Exact commands, exit codes, UTC date, runtime versions, environment: UI TypeScript build (exit 0), `node --experimental-strip-types --test tests/*.test.mjs` (13 passed), Vite production build (exit 0; 325.73 kB JS / 98.10 kB gzip), 2026-09-14, Node 24 local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic commit `4198e0f`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: browser-rendered deferred access mutations, live logout/401 and two-process revocation, and independent accessibility/security/release review remain VERIFY under N05/N07/N16; next action is continue live acceptance and plugin/consumer qualification while retaining release HOLD.
- Atomic implementation commits: `4198e0f`.
- Human acceptance, if required: independent UX/security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N04 startup-selected secret provider and scope grants

- Scope: close the control-plane secret-provider bypass and require operator-owned scope grants in production startup profiles.
- Observable behavior delivered; FR/NFR and B/G subcases: the control-plane CLI loads the same admitted environment provider configuration as the data plane and threads it through catalog discovery, schema loading, and synthetic policy evaluation. Configured `scope_grants` are checked before environment lookup; forged names fail closed. Production control-plane and data-plane startup reject missing or empty grant maps. Secret values remain out of persisted records, tickets, UI, and audit output.
- Changed and deleted paths; old callers removed; protected pickle check: secret provider/runtime configuration, control-plane dependency/composition/service plumbing, production Compose/env reference, operator/security docs, and focused tests changed. No pickle serializers, serialized classes/import paths, payloads, migrations, or external dependencies changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +54 production/test/deployment lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: secret-provider, runtime-config, control-plane CLI, schema/evaluation, and production-deployment contract suites; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: focused secret/runtime/deployment/CLI suite (45 passed), focused schema/evaluation/CLI/provider suite (46 passed), changed-path Ruff and format checks (exit 0), 2026-09-14, Python 3.12/uv local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic commit `96e79f3`; production examples in `deployment/production/.env.example` and `deployment/production/compose.yaml`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: live hostile DNS/private/redirect destination counters, secret-provider grant coverage for every deployed catalog, cancellation cleanup, real OIDC, browser/accessibility, consumer, PostgreSQL race, recovery, and independent review evidence remain VERIFY under N04–N16. Next action is continue transport/resource qualification without weakening explicit grants or private/local path support.
- Atomic implementation commits: `96e79f3`.
- Human acceptance, if required: independent security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N01 full-suite verification after secret gates

- Scope: execute the authoritative backend suite after cross-plane secret-provider and production startup changes.
- Observable behavior delivered; FR/NFR and B/G subcases: no additional product behavior changed; this entry records qualification evidence only. Pickle serializers, serialized classes/import paths, and payload semantics remain untouched.
- Changed and deleted paths; old callers removed; protected pickle check: documentation only (`docs/plugin-platform/BASELINE_20260914.md` and this ledger entry); no source, migration, dependency, or fixture changes.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: existing full suite and explicit opt-in skip markers; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest --durations=20 --junitxml=/tmp/dal-obscura-after-secret-gates.xml -q -rs` (exit 0; 860 collected, 846 passed, 14 skipped, 0 failed, 0 errors, 131.144s), 2026-09-14, Python 3.12/uv local workspace with loopback/subprocess permissions.
- Artifact and fixture hashes; evidence locations: JUnit report `/tmp/dal-obscura-after-secret-gates.xml`; baseline delta in `docs/plugin-platform/BASELINE_20260914.md`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: the 14 skips are explicit benchmark, loopback-consumer, PostgreSQL race/recovery opt-ins; Node 24 clean-install/image/advisory, live OIDC, hostile transport counters, rendered browser/accessibility, real consumer pairs, recovery, and independent review remain VERIFY under N01/N04–N16. Next action is continue live transport and production qualification while retaining release HOLD.
- Atomic implementation commits: `96e79f3` (implementation); this verification is recorded separately.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N04 literal destination safety

- Scope: reject unsafe literal catalog destinations before a provider can open a socket.
- Observable behavior delivered; FR/NFR and B/G subcases: link-local metadata-service, unspecified, and multicast literals are always rejected; private and loopback literals are rejected unless their exact host is explicitly allowlisted. Existing hostname egress checks and local/private allowlisted deployments remain available.
- Changed and deleted paths; old callers removed; protected pickle check: `src/dal_obscura/control_plane/application/catalog_service.py` and `tests/control_plane/test_catalog_option_validation.py`; no serializers, serialized classes, payloads, import paths, migrations, or dependencies changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +49 production/test logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: catalog-option validation, catalog API, and schema service suites; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: catalog-option/catalog-API/schema suite (41 passed), changed-path Ruff and format checks (exit 0), 2026-09-14, Python 3.12/uv local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic commit `bcb1bfd`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: DNS rebinding/pinning, redirect and returned metadata/data/delete destination counters, cancellation cleanup, and deployment network enforcement remain VERIFY under B06/N04; explicitly permitted local/private paths must continue to work. Next action is continue live transport qualification without weakening special-address denial.
- Atomic implementation commits: `bcb1bfd`.
- Human acceptance, if required: independent security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N01 suite verification after Iceberg path hardening

- Scope: qualify the Iceberg metadata/delete-file and UI cancellation slices against the authoritative backend and UI checks.
- Observable behavior delivered; FR/NFR and B/G subcases: no additional behavior changed by this entry; the elevated run confirms no regression across catalog resolution, Flight execution, policy enforcement, session flows, and plugin tests. The prior unprivileged run was invalidated by sandbox socket-bind denials; it is not used as release evidence.
- Changed and deleted paths; old callers removed; protected pickle check: documentation only (`docs/plugin-platform/BASELINE_20260914.md` and this ledger entry); no source, serializer, migration, dependency, or fixture changes.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: existing full suite, focused Iceberg/catalog tests, and Node UI suite; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: elevated `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest --durations=20 --junitxml=/tmp/dal-obscura-after-iceberg-paths-elevated.xml -q -rs` (exit 0; 870 collected, 856 passed, 14 skipped, 0 failed, 0 errors, 131.504s); UI `tsc` (exit 0), Node tests (13 passed), Vite build (exit 0; 325.43 kB JS / 98.06 kB gzip); 2026-09-14, Python 3.12 and Node 24 local workspace.
- Artifact and fixture hashes; evidence locations: JUnit report `/tmp/dal-obscura-after-iceberg-paths-elevated.xml`; baseline continuation in `docs/plugin-platform/BASELINE_20260914.md`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: benchmark and opt-in consumer/PostgreSQL/recovery skips remain explicit; live hostile DNS/redirect/private counters, provider-level network interception, rendered browser/accessibility, real consumer wheels, recovery, and independent review remain VERIFY under N01/N04–N16. Next action is continue the live transport-counter and clean-artifact qualification lanes while retaining release HOLD.
- Atomic implementation commits: `1f151d0e`, `e8210e5a`, `81d7b107`; verification is documentation only.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N04 Iceberg partition-statistics path guard

- Scope: cover one additional metadata-referenced storage destination in the Iceberg path boundary.
- Observable behavior delivered; FR/NFR and B/G subcases: partition-statistics file locations are now checked alongside metadata, manifest lists, and historical metadata logs before a table is admitted. Unsafe statistics paths fail closed with the same published-root policy.
- Changed and deleted paths; old callers removed; protected pickle check: `src/dal_obscura/data_plane/infrastructure/table_formats/iceberg.py`; no serializers, serialized classes, payloads, import paths, migrations, dependencies, or tests deleted.
- Production/test logical SLOC delta; dependencies added/removed and reason: +4 production lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing Iceberg metadata-location regression suite; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: Iceberg phase-regression suite (11 passed), changed-path Ruff check (exit 0), 2026-09-14, Python 3.12/uv local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic implementation commit `b87727a`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: live provider manifest/data/delete interception, DNS/redirect/private destination counters, cancellation cleanup, deployment network controls, browser/consumer qualification, and independent review remain VERIFY under B06/N04/N16; next action is continue live transport-counter and cancellation-resource qualification.
- Atomic implementation commits: `b87727a`.
- Human acceptance, if required: independent security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N04 pre-load Iceberg IO-option enforcement

- Scope: close the ticket execution path that could open provider IO options before applying storage-root policy.
- Observable behavior delivered; FR/NFR and B/G subcases: `IcebergTableFormat` checks URI, file-URI, and absolute local paths nested in its stored `io_options` before calling `StaticTable.from_metadata`; unsafe options fail closed without provider access. Existing trusted pickle task transport and metadata checks remain unchanged.
- Changed and deleted paths; old callers removed; protected pickle check: `src/dal_obscura/data_plane/infrastructure/table_formats/iceberg.py` and `tests/infrastructure/adapters/test_iceberg_phase0_regressions.py`; no serializers, serialized classes, payloads, import paths, migrations, dependencies, or tests deleted.
- Production/test logical SLOC delta; dependencies added/removed and reason: +29 production/test logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: Iceberg path-policy regressions cover unsafe IO options, delete files, manifests, historical metadata, and partition statistics; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: Iceberg phase-regression suite (12 passed), changed-path Ruff check (exit 0), 2026-09-14, Python 3.12/uv local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic implementation commit `8c11ebe`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: live provider network interception and hostile DNS/redirect/private counters, cancellation cleanup, deployment network controls, browser/consumer qualification, recovery, and independent review remain VERIFY under B06/N04/N13–N16; next action is continue live transport-counter and clean-wheel consumer qualification.
- Atomic implementation commits: `8c11ebe`.
- Human acceptance, if required: independent security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N04 double-encoded URI traversal hardening

- Scope: prevent provider-side second decoding from bypassing storage-root checks.
- Observable behavior delivered; FR/NFR and B/G subcases: URI path normalization now collapses a bounded second encoding layer for slash, backslash, and dot-segment markers before ancestry checks, and canonicalizes decoded backslashes. Double-encoded traversal fails closed while ordinary roots and descendants remain accepted.
- Changed and deleted paths; old callers removed; protected pickle check: `src/dal_obscura/data_plane/infrastructure/adapters/path_rules.py` and `tests/infrastructure/adapters/test_path_rules.py`; no serializers, serialized classes, payloads, import paths, migrations, dependencies, or tests deleted.
- Production/test logical SLOC delta; dependencies added/removed and reason: +11 production/test logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: path-rule traversal and URI-authority suite (9 passed); no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: path-rule suite (9 passed), changed-path Ruff check (exit 0), 2026-09-14, Python 3.12/uv local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic implementation commit `b423d96`; no external artifact generated.
- Remaining subcases; blocker and next concrete action: live DNS/redirect/private counters, provider request interception, cancellation cleanup, deployment network controls, browser/consumer qualification, recovery, and independent review remain VERIFY under B06/N04/N13–N16; next action is continue live hostile transport and clean-artifact consumer lanes.
- Atomic implementation commits: `b423d96`.
- Human acceptance, if required: independent security/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N01 final suite verification after URI hardening

- Scope: qualify the complete current tree after the Iceberg pre-load IO and double-encoded URI traversal guards.
- Observable behavior delivered; FR/NFR and B/G subcases: no additional runtime behavior changed by this entry; the elevated authoritative suite is green across the backend, Flight, plugin, session, and policy matrices. The protected pickle path remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: documentation only (`docs/plugin-platform/BASELINE_20260914.md` and this ledger entry); no source, serializer, migration, dependency, or fixture changes.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: full Python suite, focused URI/catalog/Iceberg tests, and Node UI tests; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: elevated `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest --durations=20 --junitxml=/tmp/dal-obscura-final-elevated.xml -q -rs` (exit 0; 871 collected, 857 passed, 14 skipped, 0 failed, 0 errors, 133.263s), 2026-09-14, Python 3.12/uv local workspace with loopback/subprocess permissions.
- Artifact and fixture hashes; evidence locations: JUnit report `/tmp/dal-obscura-final-elevated.xml`; baseline continuation in `docs/plugin-platform/BASELINE_20260914.md`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: the 14 skips are explicit benchmark, loopback-consumer, PostgreSQL race/recovery opt-ins; clean Node install/image/advisory, hostile live transport counters, real OIDC/browser/accessibility, wheel/consumer, recovery, and independent review evidence remain VERIFY under N01/N04–N16. Next action is run those live qualification lanes; release remains HOLD.
- Atomic implementation commits: `8c11ebe`, `b423d96`; verification is documentation only.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N02/N14 prose-only architecture test cleanup

- Scope: remove architecture tests that only asserted inventory prose or source-string literals after their invariants were mapped to owned behavioral coverage.
- Observable behavior delivered; FR/NFR and B/G subcases: no runtime behavior changed. The retained suites continue to own route inventory, UI/build/CSP/authentication behavior, plugin lock behavior, and executable capacity checks; deleted guards did not exercise those behaviors.
- Changed and deleted paths; old callers removed; protected pickle check: deleted `tests/architecture/test_dead_code_inventory.py`, `tests/architecture/test_operator_plugin_lock_docs.py`, and `tests/architecture/test_capacity_runbook.py`; no pickle serializers, serialized classes/import paths, ticket payloads, migrations, dependencies, or production modules changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: 79 test lines deleted; no dependencies changed. This reduces maintenance of source/prose assertions without changing product coverage.
- Primary invariant test owners; tests consolidated/deleted: `tests/architecture/test_control_plane_route_inventory.py`, `tests/architecture/test_local_demo_ui.py`, `tests/plugin_platform/test_lockfile.py`, `tests/plugin_platform/test_lock_builder.py`, and executable benchmark runners remain owners. The architecture suite now has 20 passing tests.
- Exact commands, exit codes, UTC date, runtime versions, environment: `uv run --no-sync pytest tests/architecture -q` (exit 0; 20 passed), `uv run --no-sync ruff check tests/architecture` (exit 0), and elevated `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest --durations=20 --junitxml=/tmp/dal-obscura-after-prose-cleanup.xml -q -rs` (exit 0; 868 collected, 854 passed, 14 skipped, 0 failed, 0 errors, 132.9s), 2026-09-14, Python 3.12/uv local workspace.
- Artifact and fixture hashes; evidence locations: atomic implementation commit `8c23724`; JUnit report `/tmp/dal-obscura-after-prose-cleanup.xml`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: N02 canonical SDK/lock cleanup and N14 coverage/capacity mapping remain open; clean Node install/image/advisory, hostile live transport counters, real OIDC/browser/accessibility, real consumer wheels, PostgreSQL race/recovery, and independent security/UX/release review remain VERIFY under N01/N04–N16. Next action is continue the remaining live qualification lanes while retaining release HOLD.
- Atomic implementation commits: `8c23724`; documentation commit records this ledger and baseline delta separately.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N07 diagnostic response fencing

- Scope: prevent concurrent catalog diagnostics from corrupting the visible busy state or replacing a newer result with an older response.
- Observable behavior delivered; FR/NFR and B/G subcases: each diagnostic request owns an epoch; only the current request may update diagnostics or clear the busy indicator. Aborted requests use the shared cancellation classifier and remain silent. Discovery keeps its existing epoch fence.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/components/ConnectionsView.tsx`; no backend, serializer, serialized class, payload, import path, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: +4 UI logical lines and one shared helper call; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing UI lifecycle/query tests and TypeScript/Vite checks remain owners; no tests deleted. A rendered browser interleaving remains required for N07/N16.
- Exact commands, exit codes, UTC date, runtime versions, environment: `apps/governance-ui/node_modules/.bin/tsc -p apps/governance-ui/tsconfig.json --noEmit` (exit 0), `node --experimental-strip-types --test tests/*.test.mjs` (13 passed), and `node_modules/.bin/vite build` (exit 0; 325.34 kB JS / 98.00 kB gzip), 2026-09-14, Node 24 local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic implementation commit `2b8f0d3`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: rendered deferred-response races, live logout/401 and cross-process revocation, real OIDC/browser/accessibility, hostile transport counters, consumer/recovery/capacity evidence, and independent security/UX/release review remain VERIFY under N04–N16. Next action is continue live qualification while retaining release HOLD.
- Atomic implementation commits: `2b8f0d3`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N05 strict OIDC provider option validation

- Scope: make the full supported OIDC provider configuration safe for UI and direct API callers.
- Observable behavior delivered; FR/NFR and B/G subcases: audience, subject/group/attribute claim paths, algorithms, clock leeway, JWKS refresh interval, and key-count limits now reject malformed types, empty values, control characters, invalid ranges, and oversized mappings before persistence or provider construction. Valid multi-audience and nested claim configurations remain accepted.
- Changed and deleted paths; old callers removed; protected pickle check: `src/dal_obscura/control_plane/application/auth_provider_validation.py` and `tests/control_plane/test_auth_provider_validation.py`; no serializers, serialized classes/import paths, ticket payloads, migrations, dependencies, or tests deleted.
- Production/test logical SLOC delta; dependencies added/removed and reason: +118 production/test logical lines; no dependencies changed. The added validation closes a provider-construction failure path and makes settings editing deterministic.
- Primary invariant test owners; tests consolidated/deleted: auth-provider validation and existing OIDC adapter suites; no tests deleted. Live OIDC code/PKCE and role-freshness journeys remain release owners.
- Exact commands, exit codes, UTC date, runtime versions, environment: focused validation suite (10 passed) and changed-path Ruff (exit 0), 2026-09-14, Python 3.12/uv local workspace. Commit hooks passed with Ruff/format/pytest hooks and repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic implementation commit `ad87f37`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: UI controls for every optional provider field, live OIDC/browser login/logout, account-role freshness and cross-process revocation, hostile transport counters, consumer/recovery/capacity evidence, and independent security/UX/release review remain VERIFY under N04–N16. Next action is continue live qualification while retaining release HOLD.
- Atomic implementation commits: `ad87f37`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N01 full-suite verification after OIDC validation

- Scope: qualify the complete tree after strict OIDC provider option validation and the prior UI diagnostic race fix.
- Observable behavior delivered; FR/NFR and B/G subcases: no additional runtime behavior changed by this verification; all backend, Flight, policy, session, plugin, and cleanup tests remain green. Pickle serializers, serialized classes/import paths, and payload semantics remain untouched.
- Changed and deleted paths; old callers removed; protected pickle check: documentation only (`docs/plugin-platform/BASELINE_20260914.md` and this ledger entry); no source, serializer, migration, dependency, or fixture changes.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: authoritative full suite and explicit opt-in skip markers; no tests deleted in this verification.
- Exact commands, exit codes, UTC date, runtime versions, environment: elevated `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest --durations=20 --junitxml=/tmp/dal-obscura-after-oidc-validation.xml -q -rs` (exit 0; 868 collected, 854 passed, 14 skipped, 0 failed, 0 errors, approximately 132.8s), 2026-09-14, Python 3.12/uv local workspace with loopback/subprocess permissions.
- Artifact and fixture hashes; evidence locations: JUnit report `/tmp/dal-obscura-after-oidc-validation.xml`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: the 14 skips are explicit benchmark, loopback-consumer, PostgreSQL race/recovery opt-ins; clean Node install/image/advisory, hostile live transport counters, real OIDC/browser/accessibility, consumer wheels, recovery, capacity, and independent security/UX/release review remain VERIFY under N01/N04–N16. Next action is continue live qualification while retaining release HOLD.
- Atomic implementation commits: `ad87f37`, `2b8f0d3`; verification is documentation only.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N05 complete OIDC settings form

- Scope: expose every validated OIDC provider setting in the authenticated Settings view.
- Observable behavior delivered; FR/NFR and B/G subcases: administrators can edit subject claims, group claims, attribute mappings, algorithms, clock leeway, JWKS refresh interval, and key limits alongside issuer, audience, and JWKS URL. Attribute mappings use explicit `name=claim.path` entries; server validation remains authoritative and secrets stay redacted.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/components/SettingsView.tsx`; no backend, serializer, serialized class, payload, import path, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: +48 UI logical lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing UI TypeScript, Node, and Vite checks plus backend auth-provider validation; no tests deleted. Browser keyboard/error/accessibility and live OIDC evidence remain N05/N16 owners.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node_modules/.bin/tsc -p tsconfig.json --noEmit` (exit 0), `node --experimental-strip-types --test tests/*.test.mjs` (13 passed), and `node_modules/.bin/vite build` (exit 0; 327.49 kB JS / 98.65 kB gzip), 2026-09-14, Node 24 local workspace. Commit hooks passed with repository baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic implementation commit `5dfa3ae`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: live OIDC code/PKCE login, session expiry and role freshness, multi-process revocation, hostile transport counters, consumer/recovery/capacity evidence, and independent security/UX/release review remain VERIFY under N04–N16. Next action is continue live qualification while retaining release HOLD.
- Atomic implementation commits: `5dfa3ae`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N14 path-aware pre-commit fast lane

- Scope: stop broad type and non-heavy test hooks from running on documentation-only commits.
- Observable behavior delivered; FR/NFR and B/G subcases: Ty now runs only for Python source/package changes and the non-heavy pytest hook only for source, package, or test Python changes. Ruff already remains Python-file scoped. Documentation commits complete with no test or type-check process, while code changes retain the existing hooks.
- Changed and deleted paths; old callers removed; protected pickle check: `.pre-commit-config.yaml`; no production modules, serializers, serialized classes/import paths, payloads, migrations, dependencies, or tests changed.
- Production/test logical SLOC delta; dependencies added/removed and reason: +2 configuration lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing CI full/release lanes and package-specific checks remain owners; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pre-commit validate-config` (exit 0) and `uv run --no-sync pre-commit run --files docs/plugin-platform/STATUS.md` (exit 0; all hooks skipped as intended), 2026-09-14, Python 3.12/uv local workspace. Commit hooks passed with baseline `ty` diagnostics skipped (`SKIP=ty`).
- Artifact and fixture hashes; evidence locations: atomic implementation commit `50c642f`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: N14 warm/cold timing and capacity/resource measurements, path-aware CI matrix, live transport/consumer/recovery/OIDC/browser evidence, and independent security/UX/release review remain VERIFY. Next action is continue measured qualification while retaining release HOLD.
- Atomic implementation commits: `50c642f`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N14 acceptance coverage map

- Scope: add an explicit behavioral-test owner map for G01–G04 and B01–B22.
- Observable behavior delivered; FR/NFR and B/G subcases: `docs/plugin-platform/COVERAGE_MAP.md` now distinguishes executable local evidence from live browser, multi-process, clean-wheel, consumer, capacity, recovery, and independent-review proof. It records the three removed prose/source guards as non-invariant checks and keeps their behavioral owners.
- Changed and deleted paths; old callers removed; protected pickle check: new documentation `docs/plugin-platform/COVERAGE_MAP.md` and this ledger entry; no source, serializer, serialized class, payload, import path, migration, dependency, or test changes.
- Production/test logical SLOC delta; dependencies added/removed and reason: documentation only; no production/test SLOC or dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: map references the existing focused suites and executable benchmark/conformance runners; no tests deleted in this slice.
- Exact commands, exit codes, UTC date, runtime versions, environment: `git diff --check` (exit 0), path and owner references verified against the current repository, 2026-09-14, local workspace.
- Artifact and fixture hashes; evidence locations: documentation commit records the map; no external artifact generated.
- Remaining subcases; blocker and next concrete action: every block with live or independent evidence remains VERIFY; next action is execute the highest-value N04/N05/N12/N13/N14 qualification lanes while retaining release HOLD.
- Atomic implementation commits: documentation-only map; no runtime commit.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N14 clean five-run capacity harness qualification

- Scope: qualify the repaired capacity harness on a clean committed tree and retain repeatable per-run JSON evidence.
- Observable behavior delivered; FR/NFR and B/G subcases: `scripts/run_capacity_benchmarks.sh` now derives stable result filenames from each suite basename, so five-run masking, Iceberg multifile, and ticket-to-response suites complete without creating nonexistent nested directories. The ticket benchmark streamed 25 million masked rows successfully in each run. This proves local repeatability only; it does not close fixed-runner, discovery/audit, or 16-consumer mixed-load acceptance. Pickle serializers, serialized classes/import paths, and payload semantics remain untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `scripts/run_capacity_benchmarks.sh`; no production modules, serializers, serialized classes/import paths, payloads, migrations, dependencies, or tests changed in the evidence slice.
- Production/test logical SLOC delta; dependencies added/removed and reason: +2 shell lines in the harness; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: executable benchmark suites under `tests/benchmarks/`; no tests deleted. B19 fixed-runner, discovery/audit, and mixed-load owners remain outstanding.
- Exact commands, exit codes, UTC date, runtime versions, environment: `DAL_OBSCURA_CAPACITY_RUNS=5 ./scripts/run_capacity_benchmarks.sh /tmp/dal-obscura-capacity-c84219f-local-20260914` (exit 0; 5 runs per suite, all benchmark cases passed, 2 ticket-streaming benchmark cases plus 2 explicit skips per ticket run), 2026-09-14, Darwin arm64, Python 3.12.10, clean commit `c84219f`, uv lock SHA `f816873a7b98908d0e2a9990318b8014eaa71c6543b6d56551bad535d1e4029e`.
- Measured result summary from `/tmp/dal-obscura-capacity-c84219f-local-20260914`: Iceberg multifile median per run 1.869–1.898 ms (mean 1.913–1.953 ms); row-filter-only median 3.993–4.101 ms; row-filter plus nested masks median 7.050–7.124 ms; row-filter plus top-level masks median 8.197–8.283 ms; mask-only median 12.244–12.871 ms; complex-schema ticket planning median 33.325–33.754 ms; 25M-row masked stream 41.058–41.328 s.
- Artifact and fixture hashes; evidence locations: clean metadata and 15 per-run JSON files in `/tmp/dal-obscura-capacity-c84219f-local-20260914`; metadata records `git_dirty=false`, commit `c84219f723c4cf178127678d3a17cc71094cacd0`, and the lock hash.
- Remaining subcases; blocker and next concrete action: B19 fixed-runner/discovery/audit/mixed-load capacity, live hostile transport counters, real OIDC/browser/accessibility, PostgreSQL two-process races, clean wheels and Spark/consumer cells, deployment/recovery/artifact checks, and independent security/UX/release review remain VERIFY under N04–N16. Next action is continue the highest-value live and cross-process qualification while retaining release HOLD.
- Atomic implementation commits: `c84219f`; this entry records evidence for that harness repair.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N11 activity pagination rendering correction

- Scope: make the Activity page honor the bounded audit API page it has loaded.
- Observable behavior delivered; FR/NFR and B/G subcases: every audit event returned across keyset pages is now rendered, so Load more activity reveals the fetched records. Empty filtered results now say that no activity matches the filters instead of showing unrelated policy history. Server-side scope, redaction, limits, and cursor semantics are unchanged; pickle logic is untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/components/ManagementViews.tsx`; no backend, serializer, serialized class/import path, payload, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: net -3 UI lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing UI Node suite and TypeScript/Vite build; no tests deleted. Rendered Activity accessibility and live permission journeys remain N11/N16 evidence owners.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node --experimental-strip-types --test tests/*.test.mjs` (13 passed), `node_modules/.bin/tsc -p tsconfig.json --noEmit` (exit 0), `node_modules/.bin/vite build` (exit 0; 327.13 kB JavaScript / 98.64 kB gzip), and `git diff --check` (exit 0), 2026-09-14, Node 24 local workspace.
- Artifact and fixture hashes; evidence locations: implementation commit `36a8e75`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: rendered browser/axe evidence, forbidden-user and last-admin journeys, live OIDC freshness/revocation, cross-process races, clean wheels/real consumers, recovery and mixed-load capacity, and independent review remain VERIFY under N05–N16. Next action is continue the highest-value release qualification while retaining release HOLD.
- Atomic implementation commits: `36a8e75`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N05 optional identity-setting clearing

- Scope: make optional OIDC provider settings editable through their full lifecycle.
- Observable behavior delivered; FR/NFR and B/G subcases: clearing audience or JWKS URL removes the optional staged field; clearing numeric overrides removes the override so validated defaults apply; clearing claim mappings persists an empty mapping. Required issuer edits remain visible and are rejected by authoritative server validation when empty. Secrets and pickle logic remain untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/components/SettingsView.tsx`; no backend, serializer, serialized class/import path, payload, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: +9 UI lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: backend auth-provider validation remains the contract owner; UI TypeScript, Node, and Vite checks pass. Browser error/accessibility and live OIDC journeys remain N05/N16 owners.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node_modules/.bin/tsc -p tsconfig.json --noEmit` (exit 0), `node_modules/.bin/vite build` (exit 0; 327.23 kB JavaScript / 98.68 kB gzip), `node --experimental-strip-types --test tests/*.test.mjs` (13 passed), and `git diff --check` (exit 0), 2026-09-14, Node 24 local workspace.
- Artifact and fixture hashes; evidence locations: implementation commit `b883e02`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: live OIDC/browser login, session freshness/revocation, cross-process races, clean wheels/real consumers, hostile transport, recovery/mixed-load capacity, deployment integrity, and independent review remain VERIFY under N04–N16. Next action is continue release qualification while retaining release HOLD.
- Atomic implementation commits: `b883e02`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N14 deterministic capacity summary artifact

- Scope: make the existing benchmark harness produce reviewable aggregate evidence without adding a second runner.
- Observable behavior delivered; FR/NFR and B/G subcases: `scripts/run_capacity_benchmarks.sh` now invokes `scripts/summarize_capacity_benchmarks.py` after all suites. The summarizer requires metadata, every expected suite/run file, and valid benchmark timing records, then writes stable `summary.json` with run counts and mean/median/min/max milliseconds. Incomplete evidence fails closed; raw per-run pytest JSON remains available. Pickle logic is untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `scripts/run_capacity_benchmarks.sh`, new `scripts/summarize_capacity_benchmarks.py`; no production modules, serializers, serialized classes/import paths, payloads, migrations, dependencies, or tests deleted.
- Production/test logical SLOC delta; dependencies added/removed and reason: +123 benchmark-tooling lines; standard library only, no dependency changes.
- Primary invariant test owners; tests consolidated/deleted: existing pytest benchmark suites remain execution owners; the summarizer is aggregation/validation only and does not execute tests.
- Exact commands, exit codes, UTC date, runtime versions, environment: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync python scripts/summarize_capacity_benchmarks.py /tmp/dal-obscura-capacity-c84219f-local-20260914` (exit 0), metadata/run-count/25M-stream assertions (pass), `python3 -m py_compile scripts/summarize_capacity_benchmarks.py` (exit 0), `sh -n scripts/run_capacity_benchmarks.sh` (exit 0), and `git diff --check` (exit 0), 2026-09-14, Python 3.12/uv local workspace.
- Artifact and fixture hashes; evidence locations: aggregate `/tmp/dal-obscura-capacity-c84219f-local-20260914/summary.json`; implementation commit `be44ceb`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: B19 fixed-runner/discovery/audit/mixed-load resource thresholds, live hostile transport, OIDC/browser/freshness, PostgreSQL process races, clean wheels/real consumers, deployment/recovery/integrity, and independent review remain VERIFY under N04–N16. Next action is continue cross-process and deployment qualification while retaining release HOLD.
- Atomic implementation commits: `be44ceb`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N05 local browser origin parity

- Scope: keep local authenticated browser mutations usable while retaining CSRF and origin checks.
- Observable behavior delivered; FR/NFR and B/G subcases: when the local profile has no explicit CORS setting, the control-plane CLI now allows only the supported Vite origins `http://127.0.0.1:5173` and `http://localhost:5173`. Bootstrap sessions can therefore sign out and perform protected mutations from the local UI. Production behavior is unchanged and still requires explicit HTTPS CORS origins; pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `src/dal_obscura/control_plane/interfaces/control_plane_cli.py`, `tests/interfaces/control_plane/test_control_plane_cli.py`; no serializers, serialized classes/import paths, payloads, migrations, dependencies, or tests deleted.
- Production/test logical SLOC delta; dependencies added/removed and reason: +7 production/test lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: control-plane CLI origin propagation and browser bootstrap/session suites; no tests deleted. Live HTTPS OIDC, cookie, expiry, freshness, and cross-process revocation remain N05/N16 owners.
- Exact commands, exit codes, UTC date, runtime versions, environment: focused CLI/bootstrap pytest selection (2 passed), full control-plane CLI plus actor-auth selection (all passed), changed-path Ruff (exit 0), 2026-09-14, Python 3.12/uv local workspace. Commit hooks retained Ruff/format/pytest and skipped only the documented repository ty baseline diagnostics.
- Artifact and fixture hashes; evidence locations: implementation commit `646fb53`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: restart of the already-running local process is required to observe this code change in the browser; live OIDC/browser/accessibility, freshness/revocation, PostgreSQL races, clean wheels/consumers, hostile transport, recovery/mixed-load capacity, deployment integrity, and independent review remain VERIFY under N04–N16. Next action is apply the change in the next controlled service restart and continue release qualification while retaining HOLD.
- Atomic implementation commits: `646fb53`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N05 local browser authentication verification

- Scope: verify the local UI against the restarted control-plane process carrying the CSRF-origin fix.
- Observable behavior delivered; FR/NFR and B/G subcases: the browser authenticated with the local bootstrap token, rendered the connected workspace state, successfully signed out after the controlled restart, hid private workspace content, and authenticated again. No demo data or demo-login UI was used.
- Changed and deleted paths; old callers removed; protected pickle check: no additional source changes; this entry records the runtime evidence for `646fb53`.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: local browser sign-in/sign-out journey plus existing CLI/bootstrap actor tests; no tests deleted. Real OIDC code/PKCE, expiry/freshness, revocation, and production cookie evidence remain open.
- Exact commands, exit codes, UTC date, runtime versions, environment: restarted `dal-obscura-control-plane` on `127.0.0.1:8821` (PID 37393) with SQLite local profile, UI at `http://127.0.0.1:5173/`; CUA accessibility checks observed Connected → Signed out → Connected states, 2026-09-14.
- Artifact and fixture hashes; evidence locations: browser session was manual CUA evidence; no external artifact committed.
- Remaining subcases; blocker and next concrete action: live OIDC/browser PKCE and session-expiry/revocation timing, PostgreSQL races, hostile transport counters, clean wheels/real consumers, recovery/mixed-load capacity, deployment integrity, and independent review remain VERIFY under N04–N16. Next action is continue cross-process and deployment qualification while retaining release HOLD.
- Atomic implementation commits: `646fb53`; runtime evidence recorded separately.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N15 UI readiness gate

- Scope: ensure deployment startup waits for a serving UI before exposing the secure browser edge.
- Observable behavior delivered; FR/NFR and B/G subcases: the production UI container now has an unprivileged nginx healthcheck on `/`, and the secure-local edge waits for `service_healthy` instead of merely `service_started`. Existing read-only filesystem, dropped capabilities, private network, and TLS boundaries remain. Pickle logic is untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `deployment/production/compose.yaml`, `deployment/local-secure/compose.yaml`, deployment contract tests; no application modules, serializers, serialized classes/import paths, payloads, migrations, dependencies, or tests deleted.
- Production/test logical SLOC delta; dependencies added/removed and reason: +9 deployment/test lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: `tests/architecture/test_production_deployment_contract.py` and `tests/production/test_local_parity.py`; no tests deleted. Docker/TLS/OIDC runtime and recovery drills remain N15 evidence owners.
- Exact commands, exit codes, UTC date, runtime versions, environment: focused deployment/parity pytest selection (4 passed), changed-path Ruff and format (exit 0), `git diff --check` (exit 0), 2026-09-14, Python 3.12/uv local workspace.
- Artifact and fixture hashes; evidence locations: implementation commit `0243c17`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: full Compose/TLS/OIDC/recovery execution, artifact digests/SBOM, PostgreSQL races, clean consumer wheels, hostile transport, mixed-load capacity, and independent security/UX review remain VERIFY under N04/N05/N12–N16. Next action is execute the deployment drill with pinned images and retain release HOLD until all evidence is candidate-bound.
- Atomic implementation commits: `0243c17`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N05 identity-provider draft validation

- Scope: prevent the Settings form from submitting stale provider values when visible input is malformed.
- Observable behavior delivered; FR/NFR and B/G subcases: malformed `name=claim.path` mappings and non-finite numeric overrides now produce visible field errors and disable Save identity providers. Corrected input clears the error and updates the staged payload; server-side validation remains authoritative. Secrets and pickle logic remain untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/components/SettingsView.tsx`; no backend, serializer, serialized class/import path, payload, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: +20 UI lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: backend auth-provider validation plus UI TypeScript/Node/Vite checks; no tests deleted. Browser accessibility and live OIDC/error journeys remain N05/N16 owners.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node_modules/.bin/tsc -p tsconfig.json --noEmit` (exit 0), `node_modules/.bin/vite build` (exit 0; 324.17 kB JavaScript / 97.89 kB gzip), `node --experimental-strip-types --test tests/*.test.mjs` (13 passed), and `git diff --check` (exit 0), 2026-09-14, Node 24 local workspace.
- Artifact and fixture hashes; evidence locations: implementation commit `2ea4dd7`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: rendered browser/axe coverage of error states, live OIDC PKCE/freshness/revocation, PostgreSQL races, hostile transport, clean wheels/real consumers, recovery/mixed-load capacity, deployment integrity, and independent review remain VERIFY under N04–N16. Next action is continue live release qualification while retaining HOLD.
- Atomic implementation commits: `2ea4dd7`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — authoritative full-suite validation after local auth and deployment gates

- Scope: validate the accumulated implementation after the local browser CSRF-origin and UI readiness changes.
- Observable behavior delivered; FR/NFR and B/G subcases: the complete repository suite passed with no failures or errors; Arrow Flight bind, subprocess streaming, nested masking, Iceberg integration, control-plane auth, and deployment contract coverage all execute successfully when local socket binding is available. Pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: no source changes in this validation entry; no serializers, serialized classes/import paths, payloads, migrations, dependencies, or tests deleted.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: the full pytest suite remains the invariant owner; no tests deleted. Skips are explicit benchmark-only skips, consumer opt-in, PostgreSQL race/recovery opt-in, and the documented benchmark lane.
- Exact commands, exit codes, UTC date, runtime versions, environment: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest --durations=20 --junitxml=/tmp/dal-obscura-after-local-auth-deploy-escalated.xml -q -rs` (exit 0; 875 collected, 861 passed, 14 skipped, 130.55 seconds), Python 3.12.10/uv local workspace with local socket binding enabled, 2026-09-14.
- Artifact and fixture hashes; evidence locations: JUnit report `/tmp/dal-obscura-after-local-auth-deploy-escalated.xml`; implementation commits under validation `2ea4dd7`, `0243c17`, `646fb53`, `b883e02`, `be44ceb`, `36a8e756`.
- Remaining subcases; blocker and next concrete action: live OIDC PKCE/freshness/revocation, hostile transport counters, PostgreSQL two-process races/recovery, clean wheels and real DuckDB/Spark consumers, mixed-load capacity/resource thresholds, pinned deployment artifact/SBOM/recovery evidence, and independent review remain VERIFY under N04–N16. Next action is continue candidate-bound live release qualification while retaining HOLD.
- Atomic implementation commits: validation only; no source commit.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N05 rendered identity-provider management

- Scope: make the authenticated Settings view a complete OIDC provider management surface.
- Observable behavior delivered; FR/NFR and B/G subcases: administrators can now add a new OIDC provider, remove a staged provider, enable or disable providers, and edit every validated issuer, audience, JWKS, claim, algorithm, and numeric freshness/key-limit setting. Inputs expose inline validation semantics and accessible error relationships; provider order remains visible. Static secrets and JWKS material are never rendered. Pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/components/SettingsView.tsx`, `apps/governance-ui/src/styles.css`; no backend, serializer, serialized class/import path, payload, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: +65 UI lines and +20 CSS lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: server-side auth-provider validation remains authoritative; UI TypeScript, Vite build, Node lifecycle/query/schema tests, and manual browser accessibility inspection cover this slice; no tests deleted. Browser OIDC/PKCE, freshness/revocation, keyboard and screen-reader evidence remain N05/N16 owners.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node_modules/.bin/tsc -p tsconfig.json --noEmit` (exit 0), `node_modules/.bin/vite build` (exit 0; 327.78 kB JavaScript / 99.11 kB gzip), `node --experimental-strip-types --test tests/*.test.mjs` (13 passed), and `git diff --check` (exit 0), 2026-09-14, Node 24. Manual CUA state verified Settings initially showed no providers; Add OIDC provider rendered all ten editable fields and an enabled toggle; Remove restored the empty state without persistence.
- Artifact and fixture hashes; evidence locations: implementation commit `9c0027a`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: rendered keyboard/axe/screen-reader journeys on populated providers, path-rule editor, live OIDC PKCE/freshness/revocation, PostgreSQL races, hostile transport, clean wheels/real consumers, recovery/mixed-load capacity, deployment integrity, and independent review remain VERIFY under N04–N16. Next action is continue the highest-value rendered and live qualification while retaining release HOLD.
- Atomic implementation commits: `9c0027a`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N04 structured storage-root editor

- Scope: remove raw JSON authoring from the runtime path-boundary settings flow.
- Observable behavior delivered; FR/NFR and B/G subcases: administrators now add, edit, and remove individual storage roots through constrained text rows. Save trims each root and emits only the server contract `{root: string}`; blank rows fail closed with a visible message. The server remains authoritative for URI and private-destination policy, and pickle logic is untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/components/SettingsView.tsx`, `apps/governance-ui/src/styles.css`; no backend, serializer, serialized class/import path, payload, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: +17 UI lines and +13 CSS lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: runtime settings API and path-rule adapter tests remain security owners; UI TypeScript/Vite/Node checks and manual CUA accessibility inspection cover this authoring surface; no tests deleted. Live hostile transport and cancellation evidence remain N04/N16 owners.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node_modules/.bin/tsc -p tsconfig.json --noEmit` (exit 0), `node_modules/.bin/vite build` (exit 0; 328.12 kB JavaScript / 99.12 kB gzip), `node --experimental-strip-types --test tests/*.test.mjs` (13 passed), and `git diff --check` (exit 0), 2026-09-14, Node 24. Manual CUA inspection verified the existing root rendered as an accessible field; Add storage root rendered a second labeled field; Remove restored the original state without persistence.
- Artifact and fixture hashes; evidence locations: implementation commit `c444be7`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: live hostile transport counters and provider request interception, DNS/redirect/private-address qualification, browser axe/screen-reader evidence, OIDC freshness/revocation, PostgreSQL races, clean wheels/consumers, recovery/mixed-load capacity, deployment integrity, and independent review remain VERIFY under N04–N16. Next action is continue candidate-bound transport and browser qualification while retaining release HOLD.
- Atomic implementation commits: `c444be7`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N04 runtime settings serialization contract

- Scope: give the structured path-root editor a pure, directly tested payload boundary.
- Observable behavior delivered; FR/NFR and B/G subcases: `serializePathRules` trims every root, preserves an empty allowlist for explicitly local profiles, and returns no payload when any row is blank. Settings save uses this helper before calling the control plane, while server URI/private-address validation remains authoritative. Pickle logic is untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/runtime_settings.ts`, `apps/governance-ui/src/components/SettingsView.tsx`, `apps/governance-ui/tests/lifecycle.test.mjs`; no production backend, serializer, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: +19 UI/test lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: UI Node test now covers trimming, empty local profiles, and blank-row rejection; runtime settings API and path-rule adapter tests remain server security owners; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node_modules/.bin/tsc -p tsconfig.json --noEmit` (exit 0), `node_modules/.bin/vite build` (exit 0; 328.17 kB JavaScript / 99.14 kB gzip), `node --experimental-strip-types --test tests/*.test.mjs` (14 passed), and `git diff --check` (exit 0), 2026-09-14, Node 24.
- Artifact and fixture hashes; evidence locations: implementation commit `07719f4`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: live transport counters/cancellation, browser axe/screen-reader coverage, OIDC freshness/revocation, PostgreSQL races, clean wheels/consumers, recovery/mixed-load capacity, deployment integrity, and independent review remain VERIFY under N04–N16. Next action is continue live qualification while retaining release HOLD.
- Atomic implementation commits: `07719f4`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N05 provider validation message refinement

- Scope: keep inline provider validation concise and unambiguous.
- Observable behavior delivered; FR/NFR and B/G subcases: when an attribute mapping is malformed, the field now shows the validation error once instead of repeating the instructional help text. The Save action remains disabled until correction; no security or payload behavior changed, and pickle logic is untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/components/SettingsView.tsx`; no backend, serializer, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: -1 UI line; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing UI TypeScript, Vite, Node, and browser validation checks; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node_modules/.bin/tsc -p tsconfig.json --noEmit` (exit 0), `node_modules/.bin/vite build` (exit 0; 328.17 kB JavaScript / 99.16 kB gzip), `node --experimental-strip-types --test tests/*.test.mjs` (14 passed), and `git diff --check` (exit 0), 2026-09-14, Node 24.
- Artifact and fixture hashes; evidence locations: implementation commit `1d45f16`; manual CUA validation exercised malformed attribute mapping and disabled Save state.
- Remaining subcases; blocker and next concrete action: live OIDC/transport/revocation, browser axe and screen-reader coverage, PostgreSQL races, consumers, recovery/capacity, deployment integrity, and independent review remain VERIFY under N04–N16. Next action is continue candidate-bound qualification while retaining release HOLD.
- Atomic implementation commits: `1d45f16`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N05 provider chain ordering controls

- Scope: make the ordered OIDC provider chain fully editable in the Settings UI.
- Observable behavior delivered; FR/NFR and B/G subcases: each staged provider now exposes Move up and Move down controls. Reordering rewrites ordinals to match evaluation order before the existing revision-checked save; boundary buttons are disabled at the first and last rows. No authentication payload or pickle behavior changed.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/components/SettingsView.tsx`, `apps/governance-ui/src/styles.css`; no backend, serializer, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: +11 UI/CSS lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: backend provider-chain validation and revision tests remain authority owners; UI TypeScript, Vite, Node, and browser control inspection cover the rendered surface; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node_modules/.bin/tsc -p tsconfig.json --noEmit` (exit 0), `node_modules/.bin/vite build` (exit 0; 328.63 kB JavaScript / 99.31 kB gzip), `node --experimental-strip-types --test tests/*.test.mjs` (14 passed), and `git diff --check` (exit 0), 2026-09-14, Node 24.
- Artifact and fixture hashes; evidence locations: implementation commit `2789236`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: populated-chain browser keyboard/axe/screen-reader evidence, live OIDC freshness/revocation, hostile transport, PostgreSQL races, clean wheels/consumers, recovery/capacity, deployment integrity, and independent review remain VERIFY under N04–N16. Next action is continue rendered and live qualification while retaining release HOLD.
- Atomic implementation commits: `2789236`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N13 opt-in governed consumer qualification

- Scope: execute the repository's opt-in governed consumer lane with the current candidate.
- Observable behavior delivered; FR/NFR and B/G subcases: the consumer qualification test completed successfully against the governed Arrow/Flight path, exercising the checked-in Python/Arrow consumer fixture and its DuckDB-facing integration. No source behavior changed and pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: documentation-only ledger entry; no source, serializer, serialized class/import path, payload, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: `tests/consumers/test_governed_reads.py` remains the consumer-lane owner; no tests deleted. Real clean-wheel, TLS/OIDC, Spark/JVM, and three external catalog/format pair evidence remain open.
- Exact commands, exit codes, UTC date, runtime versions, environment: `DAL_OBSCURA_RUN_CONSUMER_TESTS=1 UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/consumers/test_governed_reads.py -q -rs --junitxml=/tmp/dal-obscura-consumer-qualification.xml` (exit 0; 1 passed, 0 skipped, 0 failures/errors, 0.405 seconds), 2026-09-14, Python 3.12/uv with loopback permissions.
- Artifact and fixture hashes; evidence locations: JUnit report `/tmp/dal-obscura-consumer-qualification.xml`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: clean installed wheels and real DuckDB/Spark/JVM cells across SQL-Iceberg, REST-Iceberg, and manifest/Parquet, live OIDC/transport/revocation, PostgreSQL races/recovery, capacity mixed-load, deployment integrity, browser accessibility, and independent review remain VERIFY under N04–N16. Next action is continue the next runnable qualification lane while retaining release HOLD.
- Atomic implementation commits: verification only; no source commit.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N13 real SQL-Iceberg consumer lane

- Scope: extend the opt-in consumer qualification beyond the in-memory stub to the executable SQL Iceberg catalog and table-format adapter.
- Observable behavior delivered; FR/NFR and B/G subcases: the consumer suite now creates a disposable SQL catalog, nested Iceberg metadata and parquet data, then reads the governed table through Python/Arrow and DuckDB. Both consumers observe the same two rows and the nested theme mask; the DuckDB reader is closed explicitly. Pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `tests/consumers/test_governed_reads.py`; no production serializers, serialized classes/import paths, payloads, migrations, dependencies, or tests deleted.
- Production/test logical SLOC delta; dependencies added/removed and reason: +92 test lines and -7 lines from reader lifecycle cleanup; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: `tests/consumers/test_governed_reads.py` remains the owner for the opt-in Python/Arrow and DuckDB lane; no tests deleted. REST-Iceberg, manifest/Parquet, Spark/JVM in this lane, clean-wheel, TLS/OIDC, deletes/snapshots, cancellation/failure, PostgreSQL race/recovery, capacity, and release evidence remain open.
- Exact commands, exit codes, UTC date, runtime versions, environment: `DAL_OBSCURA_RUN_CONSUMER_TESTS=1 UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/consumers/test_governed_reads.py -q -rs --junitxml=/tmp/dal-obscura-consumer-sql-iceberg.xml` (exit 0; 2 passed, 0 skipped, 0 failures/errors), `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync ruff format --check tests/consumers/test_governed_reads.py` (exit 0), and `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync ruff check tests/consumers/test_governed_reads.py` (exit 0), 2026-09-14, Python 3.12/uv with loopback permissions.
- Artifact and fixture hashes; evidence locations: implementation commit `6daa5c2`; JUnit report `/tmp/dal-obscura-consumer-sql-iceberg.xml`; disposable catalog and parquet fixture are created under pytest `tmp_path` and removed at teardown.
- Remaining subcases; blocker and next concrete action: three-pair × three-consumer clean-artifact qualification, live OIDC/transport/revocation, PostgreSQL process races/recovery, mixed-load capacity, deployment integrity/SBOM, browser accessibility, and independent review remain VERIFY under N04–N16. Next action is qualify the next runnable provider/consumer pair while retaining release HOLD.
- Atomic implementation commits: `6daa5c2`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N13 real manifest/Parquet consumer lane

- Scope: qualify the public manifest catalog and Parquet table-format pair through the governed consumer path.
- Observable behavior delivered; FR/NFR and B/G subcases: the consumer suite now creates a pinned nested manifest with two Parquet row groups, admits the public catalog/format through the plugin registry, and reads it through Python/Arrow and DuckDB. Both consumers observe the same complete row set and nested email mask; bounded ticket planning is configured for the two row groups. Pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `tests/consumers/test_governed_reads.py`; no production serializers, serialized classes/import paths, payloads, migrations, dependencies, or tests deleted.
- Production/test logical SLOC delta; dependencies added/removed and reason: +110 test lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: `tests/consumers/test_governed_reads.py` remains the owner for the opt-in manifest/Parquet Python/Arrow and DuckDB lane; no tests deleted. REST-Iceberg, Spark in this lane, clean-wheel, TLS/OIDC, deletes/snapshots, cancellation/failure, PostgreSQL race/recovery, capacity, and release evidence remain open.
- Exact commands, exit codes, UTC date, runtime versions, environment: `DAL_OBSCURA_RUN_CONSUMER_TESTS=1 UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/consumers/test_governed_reads.py -q -rs --junitxml=/tmp/dal-obscura-consumer-sql-manifest.xml` (exit 0; 3 passed, 0 skipped, 0 failures/errors), `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync ruff format --check tests/consumers/test_governed_reads.py` (exit 0), and `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync ruff check tests/consumers/test_governed_reads.py` (exit 0), 2026-09-14, Python 3.12/uv with loopback permissions.
- Artifact and fixture hashes; evidence locations: implementation commit `e286a1e`; JUnit report `/tmp/dal-obscura-consumer-sql-manifest.xml`; disposable manifest and Parquet files are created under pytest `tmp_path` and removed at teardown.
- Remaining subcases; blocker and next concrete action: three-pair × three-consumer clean-artifact qualification, live OIDC/transport/revocation, PostgreSQL process races/recovery, mixed-load capacity, deployment integrity/SBOM, browser accessibility, and independent review remain VERIFY under N04–N16. Next action is qualify the remaining REST-Iceberg consumer pair while retaining release HOLD.
- Atomic implementation commits: `e286a1e`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N13 clean installed-wheel plugin admission

- Scope: qualify the exact server and independent plugin wheels in an isolated environment and fix static descriptor loading uncovered by that lane.
- Observable behavior delivered; FR/NFR and B/G subcases: the registry now reads a package-local `dal-obscura-plugin.json` only when that exact path is listed by the installed distribution and resolves it through the distribution locator. Clean wheels import without checkout paths, plugin conformance and both independent plugin suites pass, and the admission lock is generated from installed entry points. Pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `src/dal_obscura/common/plugin_api/registry.py`, `tests/plugin_platform/test_registry.py`; no serializer, serialized class/import path, payload, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: +17 production lines and +33 test lines; no dependencies changed. Generated local build/egg-info artifacts were removed after wheel construction.
- Primary invariant test owners; tests consolidated/deleted: registry package-data test and existing plugin conformance/public-adapter suites remain owners; no tests deleted. Clean server integration, REST provider I/O, three-pair consumer cells, TLS/OIDC, PostgreSQL race/recovery, capacity, deployment and independent review remain open.
- Exact commands, exit codes, UTC date, runtime versions, environment: `uv build --wheel --out-dir /tmp/dal-obscura-wheel-qualification .` plus the four package wheel builds (exit 0), `uv venv /tmp/dal-obscura-wheel-env` and `uv pip install --python /tmp/dal-obscura-wheel-env/bin/python pytest /tmp/dal-obscura-wheel-qualification/*.whl` (exit 0), `env PYTHONPATH= /tmp/dal-obscura-wheel-env/bin/python -c 'import dal_obscura, dal_obscura_plugin_api, dal_obscura_plugin_conformance, dal_obscura_manifest_parquet, dal_obscura_iceberg_rest'` (exit 0), isolated plugin suites with `-o addopts=''` (58 passed), and `PYTHONPATH=src /tmp/dal-obscura-wheel-env/bin/python scripts/build_plugin_lock.py ...` (exit 0; 3 entries), 2026-09-14, Python 3.12/uv.
- Artifact and fixture hashes; evidence locations: implementation commit `aa013e1`; generated wheel directory `/tmp/dal-obscura-wheel-qualification`; lock artifact `/tmp/dal-obscura-plugin-lock.json` SHA-256 `b071ca5fb2c5469c27eb4d61232df493b5242903356448387b6e96ce1907ddce`.
- Remaining subcases; blocker and next concrete action: clean-wheel core routing, REST-Iceberg live provider and all nine consumer/provider/version cells, OIDC/transport/revocation, PostgreSQL races/recovery, mixed-load capacity, deployment integrity/SBOM, browser accessibility, and independent review remain VERIFY under N04–N16. Next action is continue installed-wheel routing and candidate-bound release qualification while retaining release HOLD.
- Atomic implementation commits: `aa013e1`; wheel qualification evidence only otherwise.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N13 clean-wheel core routing

- Scope: run the core public-plugin routing checks entirely from installed wheels with repository source roots disabled.
- Observable behavior delivered; FR/NFR and B/G subcases: the server wheel's declared server extras were installed into the isolated environment, and the public manifest adapter plus built-in plugin routing tests passed from that environment. The registry now keeps an explicit trusted built-in when the same installed wheel entry point is present, while still rejecting duplicate external IDs. Pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `src/dal_obscura/common/plugin_api/registry.py`, `tests/plugin_platform/test_registry.py`; no serializer, serialized class/import path, payload, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: +6 production lines and +21 test lines; no dependency changes.
- Primary invariant test owners; tests consolidated/deleted: `tests/plugin_platform/test_public_plugin_adapter.py` and `test_builtin_plugins.py` remain the clean routing owners; no tests deleted. REST live provider, three-pair consumer cells, TLS/OIDC, PostgreSQL race/recovery, capacity, deployment and independent review remain open.
- Exact commands, exit codes, UTC date, runtime versions, environment: `uv pip install --python /tmp/dal-obscura-wheel-env/bin/python --reinstall --no-deps /tmp/dal-obscura-wheel-qualification/dal_obscura-0.1.0-py3-none-any.whl` (exit 0), and `env PYTHONPATH= /tmp/dal-obscura-wheel-env/bin/python -m pytest -o addopts='' tests/plugin_platform/test_public_plugin_adapter.py tests/plugin_platform/test_builtin_plugins.py -q` (exit 0; 18 passed), 2026-09-14, Python 3.12/uv, wheel-only imports.
- Artifact and fixture hashes; evidence locations: implementation commit `179650a`; wheel set `/tmp/dal-obscura-wheel-qualification`; clean environment `/tmp/dal-obscura-wheel-env`; plugin lock SHA-256 `b071ca5fb2c5469c27eb4d61232df493b5242903356448387b6e96ce1907ddce`.
- Remaining subcases; blocker and next concrete action: REST-Iceberg live provider and all nine consumer/provider/version cells, OIDC/transport/revocation, PostgreSQL races/recovery, mixed-load capacity, deployment integrity/SBOM, browser accessibility, and independent review remain VERIFY under N04–N16. Next action is continue the next runnable candidate-bound qualification while retaining release HOLD.
- Atomic implementation commits: `179650a`; wheel qualification evidence only otherwise.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N01 full-suite verification after wheel fixes

- Scope: run the complete repository suite after the plugin descriptor and built-in precedence fixes.
- Observable behavior delivered; FR/NFR and B/G subcases: all collected tests completed successfully; the known opt-in lanes remain explicit skips when their environment variables/services are absent. Pickle fixtures and serializer tests remain covered and unchanged.
- Changed and deleted paths; old callers removed; protected pickle check: verification only; no source, serializer, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: existing full suite remains authoritative; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest -q` (exit 0; 100% collected tests passed; explicit integration/benchmark/PostgreSQL opt-ins remained skipped), and `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest --collect-only -q` (exit 0), 2026-09-14, Python 3.12/uv with loopback permissions.
- Artifact and fixture hashes; evidence locations: candidate commits `aa013e1`, `179650a`, `6daa5c2`, `e286a1e`; no external artifact committed.
- Remaining subcases; blocker and next concrete action: live REST provider, nine required consumer/provider/version cells with TLS/OIDC, PostgreSQL races/recovery, hostile transport counters, mixed-load capacity, deployment integrity/SBOM, browser accessibility and independent review remain VERIFY under N04–N16. Release remains HOLD.
- Atomic implementation commits: verification only; no source commit.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N13 real REST-Iceberg consumer lane

- Scope: qualify the REST Iceberg catalog plugin against a real Iceberg table and governed Python/Arrow and DuckDB consumers.
- Observable behavior delivered; FR/NFR and B/G subcases: the consumer suite now creates an actual Iceberg table with PyIceberg SQL, serves its metadata through a deterministic local REST catalog, resolves the table through the installed REST plugin, and reads the governed parquet data through both consumers. Both surfaces observe two rows with the email mask; REST config and table metadata requests are asserted. PyIceberg's callable `current_snapshot()` shape is normalized. Pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `packages/iceberg-rest-plugin/src/dal_obscura_iceberg_rest/catalog.py`, `packages/iceberg-rest-plugin/tests/test_rest_plugin.py`, and `tests/consumers/test_governed_reads.py`; no serializers, serialized classes/import paths, payloads, migrations, dependencies, or tests deleted.
- Production/test logical SLOC delta; dependencies added/removed and reason: +2 production lines and +118 test lines (net +119 after one-line replacement); no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: the REST plugin regression and `tests/consumers/test_governed_reads.py` remain owners for callable snapshot compatibility and end-to-end REST-Iceberg Python/Arrow/DuckDB parity; no tests deleted. Spark/JVM consumer cells, TLS/OIDC, hostile transport counters, PostgreSQL races/recovery, mixed-load capacity, deployment integrity/SBOM, browser accessibility, and independent review remain open.
- Exact commands, exit codes, UTC date, runtime versions, environment: `DAL_OBSCURA_RUN_CONSUMER_TESTS=1 UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest packages/iceberg-rest-plugin/tests/test_rest_plugin.py tests/consumers/test_governed_reads.py -q -rs` (exit 0; 23 passed), `DAL_OBSCURA_RUN_CONSUMER_TESTS=1 UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/consumers/test_governed_reads.py -q -rs --junitxml=/tmp/dal-obscura-consumer-rest-iceberg.xml` (exit 0; 4 passed, 0 skipped, 0 failures/errors, 1.091 seconds), and targeted Ruff check/format (exit 0), 2026-09-14, Python 3.12/uv with loopback permissions.
- Artifact and fixture hashes; evidence locations: implementation commit `aa6217f`; JUnit report `/tmp/dal-obscura-consumer-rest-iceberg.xml`; local REST server, Iceberg metadata, and parquet files are disposable pytest fixtures.
- Remaining subcases; blocker and next concrete action: all nine required consumer/provider/version cells are not yet complete because Spark/JVM and clean-artifact REST cells remain; live OIDC/transport/revocation, PostgreSQL process races/recovery, mixed-load capacity, deployment integrity/SBOM, browser accessibility, and independent review remain VERIFY under N04–N16. Next action is execute the next runnable candidate-bound consumer or security qualification while retaining release HOLD.
- Atomic implementation commits: `aa6217f`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N04 real REST redirect transport qualification

- Scope: add an actual transport-boundary check for REST catalog redirects and denied destination counters.
- Observable behavior delivered; FR/NFR and B/G subcases: a real PyIceberg REST session is pointed at a local endpoint that returns a redirect to a separate local destination. The source request fails closed with redirects disabled, the redirected destination receives zero requests, and all existing path/URI boundary checks remain green. Pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `tests/integration/test_io_boundary.py`; no production serializers, serialized classes/import paths, payloads, migrations, dependencies, or tests deleted.
- Production/test logical SLOC delta; dependencies added/removed and reason: +57 test lines; no production or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: `tests/integration/test_io_boundary.py` remains the owner for REST transport/path boundary checks; no tests deleted. DNS/private-address swaps, metadata/data/delete destination counters, cancellation cleanup, credential redaction, deployment network policy, and live TLS/OIDC remain open.
- Exact commands, exit codes, UTC date, runtime versions, environment: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/integration/test_io_boundary.py -q -rs --junitxml=/tmp/dal-obscura-io-boundary-rest-redirect.xml` (exit 0; 10 passed, 0 skipped, 0 failures/errors, 1.335 seconds), `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync ruff check tests/integration/test_io_boundary.py` (exit 0), and `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync ruff format --check tests/integration/test_io_boundary.py` (exit 0), 2026-09-14, Python 3.12/uv with loopback permissions.
- Artifact and fixture hashes; evidence locations: implementation commit `93d109a`; JUnit report `/tmp/dal-obscura-io-boundary-rest-redirect.xml`; both HTTP servers are disposable in-test fixtures.
- Remaining subcases; blocker and next concrete action: complete N04 hostile DNS/private-address and cancellation/resource probes, then N05 live OIDC/browser evidence; N12 PostgreSQL process races, N13 Spark/JVM and TLS/OIDC consumer cells, N14 capacity, N15 deployment integrity/recovery, and N16 independent review remain VERIFY. Release remains HOLD.
- Atomic implementation commits: `93d109a`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N13 JVM/Spark connector qualification

- Scope: run the complete JVM connector reactor with loopback-enabled fixture execution.
- Observable behavior delivered; FR/NFR and B/G subcases: Java client, Spark 3 datasource, connector testkit, and Spark integration modules all compile and pass. The integration cells exercise nested projection, pushed and residual filters, top-level and nested masks, broad versus selective multi-ticket planning, explicit authorization headers, and a missing-token authorization failure. This is governed local Flight fixture evidence; pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: verification only; no source, serializer, serialized class/import path, payload, migration, dependency, or test deletion.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: `connectors/jvm` unit and integration suites remain owners; no tests deleted. Clean installed-wheel provider pairs, TLS/OIDC transport, and independent external artifact identity remain open.
- Exact commands, exit codes, UTC date, runtime versions, environment: `mvn -f connectors/jvm/pom.xml verify` (exit 0; 44 tests: 7 Java client, 29 Spark datasource, 2 testkit, 6 Spark integration; 0 failures/errors/skips), 2026-09-14, Java 17.0.20.1, Spark 3.5.6, macOS aarch64, loopback/subprocess permissions enabled.
- Artifact and fixture hashes; evidence locations: reactor outputs under `connectors/jvm/*/target`; the fixture runner creates disposable nested data and local service state. No release artifact was published.
- Remaining subcases; blocker and next concrete action: N13 still requires clean exact wheels, real SQL/REST/manifest datasets through TLS/OIDC Flight, and every advertised Python/Arrow, DuckDB, and Spark/JVM cell; N04 hostile transport/cancellation, N05 live OIDC, N12 process races, N14 capacity, N15 recovery/integrity, and N16 independent review remain VERIFY. Release remains HOLD.
- Atomic implementation commits: verification only; no source commit.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N01 full-suite verification after REST and JVM qualification

- Scope: run the complete Python repository suite after the callable REST snapshot, real REST consumer, hostile redirect, and JVM/Spark qualification work.
- Observable behavior delivered; FR/NFR and B/G subcases: all collected Python tests passed; the new REST transport test executes with loopback access, while the documented benchmark, opt-in consumer, PostgreSQL race, and recovery lanes remain explicit skips when their required environments are not enabled. Pickle fixtures and serializer tests remain covered and unchanged.
- Changed and deleted paths; old callers removed; protected pickle check: verification only; no source, serializer, migration, dependency, or test deletion.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: existing full suite remains authoritative; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest -q -rs --junitxml=/tmp/dal-obscura-post-rest-full.xml` (exit 0; 882 tests, 865 passed, 17 explicit skips, 0 failures/errors, 131.676 seconds), 2026-09-14, Python 3.12/uv with loopback permissions.
- Artifact and fixture hashes; evidence locations: JUnit report `/tmp/dal-obscura-post-rest-full.xml`; candidate implementation commits `aa6217f` and `93d109a`; no external artifact published.
- Remaining subcases; blocker and next concrete action: clean Node 24/image/advisory evidence, live OIDC/browser/accessibility, DNS/private-address and cancellation transport, PostgreSQL process races/recovery, clean provider wheels and TLS/OIDC consumer cells, capacity, deployment integrity, and independent review remain VERIFY under N01 and N04–N16. Release remains HOLD.
- Atomic implementation commits: verification only; no source commit.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N06 shell accessibility semantics

- Scope: close a small semantic shell gap in the authenticated governance UI.
- Observable behavior delivered; FR/NFR and B/G subcases: the active primary destination now exposes `aria-current="page"`, and the sidebar workspace connectivity state is a polite live status region. Keyboard and screen-reader users can identify the selected route and hear readiness changes without relying on color. Pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/main.tsx`; no backend, session, serializer, payload, migration, dependency, or test deletion.
- Production/test logical SLOC delta; dependencies added/removed and reason: +2 UI lines net; no dependency changes.
- Primary invariant test owners; tests consolidated/deleted: existing UI lifecycle/query/navigation tests remain owners; no tests deleted. Full B09 browser/axe/screen-reader and responsive evidence remains open.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node_modules/.bin/tsc -p tsconfig.json --noEmit` (exit 0), `node --experimental-strip-types --test tests/*.test.mjs` (exit 0; 14 passed), `node_modules/.bin/vite build` (exit 0; 328.70 kB JavaScript / 99.33 kB gzip), and `git diff --check` (exit 0), 2026-09-14, Node 26.8.2 local runtime (package policy remains Node 24).
- Artifact and fixture hashes; evidence locations: implementation commit `b21cb2d`; current build output in `apps/governance-ui/dist` is ignored/generated and not published.
- Remaining subcases; blocker and next concrete action: complete rendered keyboard/axe/screen-reader journeys, CSP browser inspection, live OIDC, PostgreSQL races/recovery, clean wheel/TLS/OIDC consumers, capacity, deployment integrity, and independent review. Release remains HOLD.
- Atomic implementation commits: `b21cb2d`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N04 manifest cancellation and cleanup boundary

- Scope: close the remaining local-file adapter gap where a Parquet row-group read could finish after cancellation and leave the reader to garbage collection.
- Observable behavior delivered; FR/NFR and B/G subcases: `ParquetDatasetFormat.execute` now checks the execution context after decoding, propagates cancellation/deadline errors without converting them into generic task failures, and closes the `ParquetFile` in a `finally` block. A regression test uses an actual fixture Parquet file and a tracking reader to prove cancellation is surfaced and cleanup runs. Pickle logic is untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `packages/manifest-parquet-plugin/src/dal_obscura_manifest_parquet/format.py`, `packages/manifest-parquet-plugin/tests/test_manifest_plugin.py`; no serializers, serialized classes/import paths, payloads, migrations, dependencies, or tests deleted.
- Production/test logical SLOC delta; dependencies added/removed and reason: +10 production lines and +51 test lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: `packages/manifest-parquet-plugin/tests/test_manifest_plugin.py::test_parquet_execute_propagates_cancellation_and_closes_reader` owns the post-read cancellation/cleanup invariant; existing plugin conformance cancellation checks remain the cross-plugin owner; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest packages/manifest-parquet-plugin/tests/test_manifest_plugin.py -q` (exit 0; 16 passed), targeted Ruff check and format (exit 0), and commit hooks Ruff/pytest non-heavy (exit 0; repository `ty` hook skipped because the documented baseline still reports 72 unrelated diagnostics), 2026-09-14, Python 3.12/uv.
- Artifact and fixture hashes; evidence locations: implementation commit `93004cd`; disposable fixture Parquet file created and removed by pytest; no external artifact committed.
- Remaining subcases; blocker and next concrete action: REST DNS/private-address swaps, live OIDC, browser axe/screen-reader and responsive coverage, PostgreSQL process races/recovery, clean TLS/OIDC consumer cells, capacity, deployment integrity/SBOM, and independent review remain VERIFY under N04–N16. Next action is continue candidate-bound qualification while retaining release HOLD.
- Atomic implementation commits: `93004cd`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N01 full-suite verification after manifest cancellation fix

- Scope: run the complete Python repository suite after the manifest Parquet cancellation and reader-cleanup boundary change.
- Observable behavior delivered; FR/NFR and B/G subcases: all collected tests passed; the new manifest cancellation regression is included, while benchmark, opt-in consumer, PostgreSQL race, and recovery lanes remain explicit skips when their required environments are absent. Pickle fixtures and serializer tests remain covered and unchanged.
- Changed and deleted paths; old callers removed; protected pickle check: verification only; no source, serializer, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta for this verification entry.
- Primary invariant test owners; tests consolidated/deleted: existing full suite remains authoritative; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest -q -rs --junitxml=/tmp/dal-obscura-post-manifest-cancel-full.xml` (exit 0; 882 tests, 865 passed, 17 explicit skips, 0 failures/errors, approximately 136 seconds), 2026-09-14, Python 3.12/uv with loopback permissions.
- Artifact and fixture hashes; evidence locations: JUnit report `/tmp/dal-obscura-post-manifest-cancel-full.xml`; no external artifact published.
- Remaining subcases; blocker and next concrete action: live OIDC and hostile DNS/private-address transport, browser axe/screen-reader and responsive evidence, PostgreSQL process races/recovery, clean TLS/OIDC consumer cells, mixed-load capacity, deployment integrity/SBOM, and independent review remain VERIFY under N04–N16. Release remains HOLD.
- Atomic implementation commits: verification only; implementation commit `93004cd` and ledger commit `7502a60` remain the source of truth.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N05/N06 authenticated local browser session boundary

- Scope: manually inspect the running governance UI through its accessibility tree after the shell and session changes.
- Observable behavior delivered; FR/NFR and B/G subcases: the local disposable profile accepted its configured control-plane token, rendered a connected workspace with `platform:admin` capabilities and an Assets route, then Sign out immediately cleared private state, disabled protected navigation, and rendered the login panel with a retry path. The browser URL retained only the same-origin route; no token or policy data appeared in the route. This is local bootstrap evidence only and does not claim live OIDC/PKCE qualification. Pickle logic is untouched.
- Changed and deleted paths; old callers removed; protected pickle check: verification-only ledger entry; no source, serializer, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: CUA accessibility-tree inspection covers the rendered login, authenticated shell, logout, and disabled-navigation states; browser B08 OIDC and full B09 axe/screen-reader/responsive evidence remain separate acceptance owners; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: authenticated and signed-out states inspected in the Codex in-app browser at `http://127.0.0.1:5173/#assets` against the local control plane on `127.0.0.1:8821`; source/UI services remained running; 2026-09-14, Vite UI and Python 3.12 control plane.
- Artifact and fixture hashes; evidence locations: disposable local SQLite profile `/private/tmp/dal-obscura-ui-dev-8821.db`; no external artifact published.
- Remaining subcases; blocker and next concrete action: production OIDC/PKCE, upstream revocation freshness, hostile DNS/private-address transport, browser automated/manual accessibility coverage, PostgreSQL process races/recovery, clean TLS/OIDC consumers, capacity, deployment integrity/SBOM, and independent review remain VERIFY under N04–N16. Release remains HOLD.
- Atomic implementation commits: verification-only ledger entry; prior UI commits `b21cb2d` and `0f25cbe` remain the source of truth.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — Secure local service readiness probe

- Scope: verify the manually inspectable local deployment remains reachable after the UI/session and adapter changes.
- Observable behavior delivered; FR/NFR and B/G subcases: the control plane `/readyz` returned HTTP 200 with `{"status":"ready","checks":{"database":"ok"}}` and security headers including CSP, `X-Frame-Options: DENY`, `nosniff`, same-origin opener policy, and a restrictive permissions policy. The Vite UI returned HTTP 200 with a no-cache HTML shell. This is disposable local evidence, not production deployment or OIDC evidence. Pickle logic is untouched.
- Changed and deleted paths; old callers removed; protected pickle check: verification-only ledger entry; no source, serializer, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: service readiness and header integration tests remain authoritative; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `curl -sS -i http://127.0.0.1:8821/readyz` and `curl -sS -I http://127.0.0.1:5173/` (exit 0), local Python control plane on port 8821 and Vite UI on port 5173, 2026-09-14.
- Artifact and fixture hashes; evidence locations: disposable SQLite profile `/private/tmp/dal-obscura-ui-dev-8821.db`; no external artifact published.
- Remaining subcases; blocker and next concrete action: production OIDC/PKCE, hostile DNS/private-address transport, browser axe/screen-reader/responsive proof, PostgreSQL races/recovery, clean TLS/OIDC consumers, capacity, deployment integrity/SBOM, and independent review remain VERIFY under N04–N16. Release remains HOLD.
- Atomic implementation commits: verification-only ledger entry; no source commit.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N14 benchmark lane after manifest cancellation fix

- Scope: execute the repository-owned benchmark suite against the current candidate.
- Observable behavior delivered; FR/NFR and B/G subcases: all six runnable benchmarks completed. The 10M-style multifile scan baseline measured 1.9115 ms mean (443 rounds); row-filter/mask means ranged from 4.1698 ms to 12.1853 ms; the complex ticket-to-response case measured 32.3026 ms mean; the 25-million-row masked stream completed in 40.622 seconds. No benchmark assertion failed and no source or pickle behavior changed.
- Changed and deleted paths; old callers removed; protected pickle check: verification-only ledger entry; no source, serializer, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: existing `tests/benchmarks` remain the benchmark owners; no tests deleted. This run is only a single local machine series and does not close the five-run mixed-load B19 requirement.
- Exact commands, exit codes, UTC date, runtime versions, environment: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/benchmarks --benchmark-only --benchmark-json /tmp/dal-obscura-benchmarks-post-manifest.json -q` (exit 0; 7 benchmark tests passed, 2 explicit benchmark skips), 2026-09-14, Python 3.12/uv, loopback permissions enabled for Flight fixtures.
- Artifact and fixture hashes; evidence locations: benchmark JSON `/tmp/dal-obscura-benchmarks-post-manifest.json`; disposable benchmark fixtures under pytest temporary directories.
- Remaining subcases; blocker and next concrete action: five-run throughput comparison, 16-consumer/metadata mixed-load RSS and cancellation cleanup, 10M schema/discovery deadlines, audit-load p95, and browser interaction measurements remain VERIFY under B19. Production OIDC/transport, PostgreSQL races/recovery, clean TLS/OIDC consumers, deployment integrity/SBOM, and independent review remain open. Release remains HOLD.
- Atomic implementation commits: verification-only ledger entry; manifest implementation commit `93004cd` remains the source of truth.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N06 generated API contract check

- Scope: verify the governance UI still matches the checked-in OpenAPI-derived DTO contract after the current candidate changes.
- Observable behavior delivered; FR/NFR and B/G subcases: the UI API type generator ran in check mode with no diff, so the typed client remains synchronized with the control-plane contract. Pickle logic is untouched.
- Changed and deleted paths; old callers removed; protected pickle check: verification-only ledger entry; no source, serializer, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: `apps/governance-ui/scripts/generate-api-types.mjs --check` remains the generated-contract owner; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node scripts/generate-api-types.mjs --check` (exit 0) from `apps/governance-ui`, 2026-09-14, Node 26.8.2 local runtime (package policy remains Node 24).
- Artifact and fixture hashes; evidence locations: generated API types under `apps/governance-ui/src/api.generated.ts`; no external artifact published.
- Remaining subcases; blocker and next concrete action: live OIDC/PKCE, hostile DNS/private-address transport, browser axe/screen-reader/responsive evidence, PostgreSQL races/recovery, clean TLS/OIDC consumers, five-run mixed-load capacity, deployment integrity/SBOM, and independent review remain VERIFY under N04–N16. Release remains HOLD.
- Atomic implementation commits: verification-only ledger entry; no source commit.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N02/N15 deployment topology and boundary checks

- Scope: run the production/local-secure topology and package-boundary tests against the current candidate.
- Observable behavior delivered; FR/NFR and B/G subcases: all seven targeted checks passed. The secure-local profile still layers production services, loopback exposure, read-only containers, dropped capabilities, internal backend networking, placeholder/digest validation, and fail-closed startup ordering. Package-boundary tests continue to reject removed legacy root packages. Pickle logic is untouched.
- Changed and deleted paths; old callers removed; protected pickle check: verification-only ledger entry; no source, serializer, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: `tests/production/test_local_parity.py` and architecture boundary suites remain owners; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/production tests/architecture/test_package_boundaries.py tests/architecture/test_secure_local_profile.py -q` (exit 0; 7 passed), 2026-09-14, Python 3.12/uv.
- Artifact and fixture hashes; evidence locations: checked-in `deployment/production/compose.yaml`, `deployment/local-secure/compose.yaml`, and local-secure runbook; no external artifact published.
- Remaining subcases; blocker and next concrete action: actual Compose/TLS/OIDC startup, artifact signatures/SBOM, encrypted recovery, PostgreSQL races, hostile transport, clean consumer wheels, mixed-load capacity, browser accessibility, and independent review remain VERIFY under N04–N16. Release remains HOLD.
- Atomic implementation commits: verification-only ledger entry; no source commit.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N06 responsive shell browser qualification

- Scope: inspect the authenticated shell at the required narrow and desktop viewport widths.
- Observable behavior delivered; FR/NFR and B/G subcases: at 390×844 the menu drawer exposes an accessible named close control, updates `aria-expanded`, and keeps the account/sign-out controls visible. At both 390px and 1440px, DOM measurements report `scrollWidth === clientWidth`, so there is no page-level horizontal overflow. The desktop shell exposes the persistent navigation rail. This is partial B09 evidence; axe, 200% zoom, populated editor, and manual screen-reader review remain open. Pickle logic is untouched.
- Changed and deleted paths; old callers removed; protected pickle check: verification-only ledger entry; no source, serializer, migration, dependency, or test deletions.
- Production/test logical SLOC delta; dependencies added/removed and reason: no source or dependency delta.
- Primary invariant test owners; tests consolidated/deleted: CUA accessibility and DOM viewport inspection cover responsive shell semantics; full B09 browser journey remains the primary acceptance owner; no tests deleted.
- Exact commands, exit codes, UTC date, runtime versions, environment: authenticated CUA inspection of `http://127.0.0.1:5173/#assets` at 390×844 and 1440×900, including drawer open/close; DOM overflow checks returned equal widths; 2026-09-14, Vite UI and local control plane.
- Artifact and fixture hashes; evidence locations: running built/dev UI on port 5173; no external artifact published.
- Remaining subcases; blocker and next concrete action: populated policy/editor/review states, axe and screen-reader checks, 200% zoom, live OIDC/PKCE, hostile DNS/private-address transport, PostgreSQL races/recovery, clean TLS/OIDC consumers, capacity, deployment integrity/SBOM, and independent review remain VERIFY under N04–N16. Release remains HOLD.
- Atomic implementation commits: verification-only ledger entry; no source commit.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N06/N07 management-form unsaved-change guard

- Scope: prevent runtime, identity-provider, and storage-root edits in the authenticated Settings view from being silently discarded by navigation, refresh, or logout.
- Observable behavior delivered; FR/NFR and B/G subcases: Settings now reports dirty state to the application shell for every editable configuration control. Route changes and logout use the existing unsaved-change confirmation; Settings Refresh has its own discard confirmation; successful saves and explicit discard clear the marker. Accepted navigation cannot leave a stale management-dirty flag that causes a later unrelated prompt. Pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/main.tsx`, `apps/governance-ui/src/components/ManagementViews.tsx`, and `apps/governance-ui/src/components/SettingsView.tsx`; one stale unused `SettingsView` import was removed. No backend, serializer, serialized class/import path, payload, migration, dependency, or test was deleted.
- Production/test logical SLOC delta; dependencies added/removed and reason: +37 UI production lines and -11 obsolete lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing UI navigation/lifecycle tests remain owners; no tests deleted. Full rendered deferred-response matrix and Playwright B09/B10 evidence remain open.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node_modules/.bin/tsc -p tsconfig.json --noEmit` (exit 0), `node_modules/.bin/vite build` (exit 0; 329.13 kB JavaScript / 99.49 kB gzip), `node --experimental-strip-types --test tests/*.test.mjs` (exit 0; 14 passed), and `git diff --check` (exit 0), 2026-09-14, Node 26.8.2 local runtime (package policy remains Node 24). Commit hook used `SKIP=ty`; the documented repository `ty` baseline still reports 72 unrelated diagnostics.
- Browser evidence: authenticated CUA inspection of the local disposable profile changed Ticket TTL from 900 to 901, attempted navigation to Connections, observed the confirmation text `You have unsaved changes. Leave this editor?`, dismissed it and retained the edit, then accepted it and navigated with the edit discarded. The shell remained authenticated and no private value entered the URL.
- Artifact and fixture hashes; evidence locations: implementation commit `434ced24`; running disposable UI/control-plane profile on `127.0.0.1:5173`/`127.0.0.1:8821`; SQLite database `/private/tmp/dal-obscura-ui-dev-8821.db`; no external artifact published.
- Remaining subcases; blocker and next concrete action: live OIDC/PKCE, hostile DNS/private-address transport, browser axe/screen-reader/200% zoom, full deferred-response isolation, PostgreSQL process races/recovery, clean TLS/OIDC consumer cells, five-run mixed-load capacity, deployment integrity/SBOM, and independent review remain VERIFY under N04–N16. Release remains HOLD; continue with the next runnable qualification packet without changing pickle behavior.
- Atomic implementation commits: `434ced24`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N07 browser lifecycle guard completion

- Scope: extend Settings dirty-state protection to browser-originated navigation and tab unload.
- Observable behavior delivered; FR/NFR and B/G subcases: the hashchange/popstate guard now captures management edits, and `beforeunload` prompts for runtime/provider/path changes as well as policy drafts. The effect dependencies include the live management-dirty marker, preventing stale closures after an edit. Pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/main.tsx`; no backend, serializer, serialized class/import path, payload, migration, dependency, or test deletion.
- Production/test logical SLOC delta; dependencies added/removed and reason: +3 UI production lines net; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing UI navigation/lifecycle tests remain owners; no tests deleted. A rendered browser back/forward and unload journey remains required for complete B10 evidence.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node_modules/.bin/tsc -p tsconfig.json --noEmit` (exit 0), `node_modules/.bin/vite build` (exit 0; 329.14 kB JavaScript / 99.50 kB gzip), `node --experimental-strip-types --test tests/*.test.mjs` (exit 0; 14 passed), and `git diff --check` (exit 0), 2026-09-14, Node 26.8.2 local runtime (package policy remains Node 24). Commit hook used `SKIP=ty`; the documented repository `ty` baseline still reports 72 unrelated diagnostics.
- Artifact and fixture hashes; evidence locations: implementation commit `eec4d9b0`; local disposable UI/control-plane profile on `127.0.0.1:5173`/`127.0.0.1:8821`; no external artifact published.
- Remaining subcases; blocker and next concrete action: rendered browser back/forward/unload and full deferred-response matrix, live OIDC/PKCE and revocation, hostile transport, PostgreSQL races/recovery, clean TLS/OIDC consumer cells, capacity, deployment integrity/SBOM, and independent review remain VERIFY under N04–N16. Release remains HOLD; continue with the next runnable qualification packet without changing pickle behavior.
- Atomic implementation commits: `eec4d9b0`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N08 access editor unsaved-change guard

- Scope: protect owner and delegated-capability authoring in the asset Access tab from silent discard.
- Observable behavior delivered; FR/NFR and B/G subcases: owner text edits, grant principal/capability edits, grant add/remove, tab changes, history/version links, Refresh, browser history, asset navigation, logout, and unload now participate in the shared unsaved-change guard. Explicit discard and successful saves clear the marker; failed saves preserve the edits. Access data remains server-authorized and no source rows are exposed. Pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/components/AssetWorkspace.tsx` and `apps/governance-ui/src/main.tsx`; no backend, serializer, serialized class/import path, payload, migration, dependency, or test deletion.
- Production/test logical SLOC delta; dependencies added/removed and reason: +32 UI production lines and -9 obsolete lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing UI lifecycle/query tests remain owners; no tests deleted. Rendered owner/grant navigation and deferred-response journeys remain required B10/B15 evidence.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node_modules/.bin/tsc -p tsconfig.json --noEmit` (exit 0), `node_modules/.bin/vite build` (exit 0; 329.85 kB JavaScript / 99.55 kB gzip), `node --experimental-strip-types --test tests/*.test.mjs` (exit 0; 14 passed), and `git diff --check` (exit 0), 2026-09-14, Node 26.8.2 local runtime (package policy remains Node 24). Commit hook used `SKIP=ty`; the documented repository `ty` baseline still reports 72 unrelated diagnostics.
- Artifact and fixture hashes; evidence locations: implementation commit `b4c66d19`; local disposable UI/control-plane profile on `127.0.0.1:5173`/`127.0.0.1:8821`; no external artifact published.
- Remaining subcases; blocker and next concrete action: full rendered browser guard/race matrix, live OIDC/PKCE and revocation, hostile transport, PostgreSQL races/recovery, clean TLS/OIDC consumer cells, capacity, deployment integrity/SBOM, and independent review remain VERIFY under N04–N16. Release remains HOLD; continue with the next runnable qualification packet without changing pickle behavior.
- Atomic implementation commits: `b4c66d19`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N10 connection draft unsaved-change guard

- Scope: prevent catalog draft, typed connection fields, ambiguous format selection, and plugin lifecycle edits from being silently abandoned.
- Observable behavior delivered; FR/NFR and B/G subcases: all connection form controls report dirty state to the application shell. Refresh, switching to another catalog editor, route changes, logout, browser history, and unload require explicit discard; Cancel edit is an explicit discard action. Successful catalog saves clear the marker while rejected saves keep input visible. Lifecycle choices remain staged until Apply and are included in the guard. Secret fields continue to hold only scoped references. Pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/components/ConnectionsView.tsx` and `apps/governance-ui/src/components/ManagementViews.tsx`; no backend, serializer, serialized class/import path, payload, migration, dependency, or test deletion.
- Production/test logical SLOC delta; dependencies added/removed and reason: +25 UI production lines and -5 obsolete lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing UI query/lifecycle tests remain owners; no tests deleted. Rendered connection lifecycle, stale response, and forbidden-state journeys remain required B10/B14 evidence.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node_modules/.bin/tsc -p tsconfig.json --noEmit` (exit 0), `node_modules/.bin/vite build` (exit 0; 330.23 kB JavaScript / 99.67 kB gzip), `node --experimental-strip-types --test tests/*.test.mjs` (exit 0; 14 passed), and `git diff --check` (exit 0), 2026-09-14, Node 26.8.2 local runtime (package policy remains Node 24). Commit hook used `SKIP=ty`; the documented repository `ty` baseline still reports 72 unrelated diagnostics.
- Artifact and fixture hashes; evidence locations: implementation commit `0f22baa6`; local disposable UI/control-plane profile on `127.0.0.1:5173`/`127.0.0.1:8821`; no external artifact published.
- Remaining subcases; blocker and next concrete action: rendered browser confirmation and deferred-response matrix, live OIDC/PKCE/revocation, hostile transport, PostgreSQL races/recovery, clean TLS/OIDC consumer cells, capacity, deployment integrity/SBOM, and independent review remain VERIFY under N04–N16. Release remains HOLD; continue with the next runnable qualification packet without changing pickle behavior.
- Atomic implementation commits: `0f22baa6`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N07 form mutation response fencing

- Scope: prevent late connection and access save responses from overwriting newer user edits.
- Observable behavior delivered; FR/NFR and B/G subcases: connection and owner/grant saves capture an edit generation before issuing the request. Any subsequent edit, context change, explicit discard, reload, or session transition invalidates that generation; a late success is ignored and cannot clear newer input or trigger an obsolete reload. Pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/components/ConnectionsView.tsx` and `apps/governance-ui/src/components/AssetWorkspace.tsx`; no backend, serializer, serialized class/import path, payload, migration, dependency, or test deletion.
- Production/test logical SLOC delta; dependencies added/removed and reason: +26 UI production lines and -8 obsolete lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing UI lifecycle/query tests remain owners; no tests deleted. Deferred rendered mutation races remain required B10 evidence.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node_modules/.bin/tsc -p tsconfig.json --noEmit` (exit 0), `node_modules/.bin/vite build` (exit 0; 330.47 kB JavaScript / 99.75 kB gzip), `node --experimental-strip-types --test tests/*.test.mjs` (exit 0; 14 passed), and `git diff --check` (exit 0), 2026-09-14, Node 26.8.2 local runtime (package policy remains Node 24). Commit hook used `SKIP=ty`; the documented repository `ty` baseline still reports 72 unrelated diagnostics.
- Artifact and fixture hashes; evidence locations: implementation commit `a86bc1fa`; local disposable UI/control-plane profile on `127.0.0.1:5173`/`127.0.0.1:8821`; no external artifact published.
- Remaining subcases; blocker and next concrete action: rendered deferred-response proof, live OIDC/PKCE/revocation, hostile transport, PostgreSQL races/recovery, clean TLS/OIDC consumer cells, capacity, deployment integrity/SBOM, and independent review remain VERIFY under N04–N16. Release remains HOLD; continue with the next runnable qualification packet without changing pickle behavior.
- Atomic implementation commits: `a86bc1fa`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.

## 2026-09-14 — N07 Settings mutation response fencing

- Scope: prevent slow runtime or identity-provider saves from overwriting newer Settings edits.
- Observable behavior delivered; FR/NFR and B/G subcases: Settings captures an edit generation before each save. A newer control edit, server reload, explicit discard, or session transition invalidates the generation; late success is ignored and cannot clear current dirty state or trigger an obsolete reload. Successful saves still clear the marker only when no newer edit exists. Pickle logic remains untouched.
- Changed and deleted paths; old callers removed; protected pickle check: `apps/governance-ui/src/components/SettingsView.tsx`; no backend, serializer, serialized class/import path, payload, migration, dependency, or test deletion.
- Production/test logical SLOC delta; dependencies added/removed and reason: +24 UI production lines and -13 obsolete lines; no dependencies changed.
- Primary invariant test owners; tests consolidated/deleted: existing UI lifecycle/query tests remain owners; no tests deleted. Rendered deferred Settings response races remain required B10 evidence.
- Exact commands, exit codes, UTC date, runtime versions, environment: `node_modules/.bin/tsc -p tsconfig.json --noEmit` (exit 0), `node_modules/.bin/vite build` (exit 0; 330.61 kB JavaScript / 99.78 kB gzip), `node --experimental-strip-types --test tests/*.test.mjs` (exit 0; 14 passed), and `git diff --check` (exit 0), 2026-09-14, Node 26.8.2 local runtime (package policy remains Node 24). Commit hook used `SKIP=ty`; the documented repository `ty` baseline still reports 72 unrelated diagnostics.
- Artifact and fixture hashes; evidence locations: implementation commit `b8406bdb`; local disposable UI/control-plane profile on `127.0.0.1:5173`/`127.0.0.1:8821`; no external artifact published.
- Remaining subcases; blocker and next concrete action: rendered deferred-response proof, live OIDC/PKCE/revocation, hostile transport, PostgreSQL races/recovery, clean TLS/OIDC consumer cells, capacity, deployment integrity/SBOM, and independent review remain VERIFY under N04–N16. Release remains HOLD; continue with the next runnable qualification packet without changing pickle behavior.
- Atomic implementation commits: `b8406bdb`.
- Human acceptance, if required: independent security/UX/release review remains VERIFY; release remains HOLD.
