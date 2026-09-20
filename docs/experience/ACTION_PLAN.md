# Remaining action plan — R01–R12

Baseline eed27772; 2026-09-20. Planning only. Read [review](REVIEW.md),
[local access](LOCAL_ACCESS.md), [designbook](DESIGN_SYSTEM.md) and
[acceptance](ACCEPTANCE.md). This replaces the N execution order; completed
work is excluded. No packet may weaken the protected pickle or governance boundary.

## Working rules

For each slice: identify existing owner/callers → add the smallest missing
behavioral case → implement → delete replaced code/tests → run focused checks →
record evidence → atomic Conventional Commit. Do not implement this plan until
the owner requests implementation. Do not deploy, purchase services, create DNS/
Access resources or contact participants merely because this plan names them.

Use one concise evidence record per packet: candidate and artifact hashes,
FR/NFR and case IDs, exact commands/results, environment, deleted/added code and
dependencies, remaining evidence. Evidence in /tmp is not a durable release dossier.
OPEN = work remains; VERIFY = implementation exists but proof is missing;
DONE = every owned criterion passes. Never replace missing integration evidence
with a mock, a skip, an assertion about source text or an old green run.

Preserve existing framework choices unless a replacement removes specific code
or meets a named requirement. No rewrite of publication, SDK, policy evaluator
or sessions merely to reorganize files. No runtime compatibility shims. Update
obsolete tests in the same commit that removes their behavior.

Use uv, existing pnpm scripts and existing JVM/CI harnesses. R05 adds storybook,
build-storybook and test:stories scripts; these are planned command names, not
currently available commands. Keep browser tests against built artifacts for
release proof; the Vite development server is not production-image evidence.

## R01 — Establish current ownership and close remaining contract ambiguity

**Dependencies:** none. **Acceptance:** E01; inherited B01–B05/B18.
**Files:** current manifests/locks, common/plugin_api, public SDK, generated DTOs,
tests/acceptance, CI, docs/plugin-platform/BASELINE_20260914.md.

**FR:** capture resolved toolchain/artifact/test map at the new baseline. Verify
canonical SDK ownership, explicit pair admission, mandatory revision errors,
retired public routes and migrated callers; retain passing implementation.
Inventory actual leftovers before deleting anything. Select maintained stable
versions compatible with Mantine/Storybook/Vite/React/Node 24, pin the tested set, and move
build tooling to devDependencies. Update generated DTOs only from the canonical API.

**NFR:** no new runner/service, no “latest” release tag, no source-string assertions
pinning obsolete versions, no unintended serialized-class changes. Record logical
SLOC/test/runtime deltas; minimize code by deleting replaced owners, not minifying.

**Proof:** existing package/route/admission tests, clean locked install/build,
advisory report and exact source/test inventory. No broad duplicate matrix.
**Done:** E01 passes; unresolved live proof is assigned R10/R11, not declared done.

## R02 — Define and implement the trusted HTTPS/OIDC boundary

**Dependencies:** R01. **Acceptance:** E02/E03; inherited B07/B08.
**Files:** session_api.py, routes/session.py/deps.py, API/CLI profile validation,
ui/nginx.conf, existing secure-local/production profiles.

**FR:** a canonical external origin drives callback/logout/CSRF/host checks.
Implement an exact trusted-proxy contract and safe client attribution for login
rate limiting. Normal local login uses existing OIDC, not bootstrap. Preserve
app authorization independently of Access. Handle edge HTML/challenge expiry
without parsing it as API success or replaying mutations.

**NFR:** no arbitrary forwarded-header trust, no disabled TLS verification,
no bearer/session data in URLs/logs/browser storage. Local-only and tunnel modes
have identical app authority/session invariants. No new password database.

**Proof:** parameterized origin/header/rate/cookie tests, existing session tests,
real local-IdP journey in R09. Do not claim live SSO from mocked code exchange.
**Done:** E02/E03 local contracts pass; exact upstream client support documented.

## R03 — Add one optional named-tunnel development profile

**Dependencies:** R02. **Acceptance:** E04/E05/E06.
**Files:** existing local launcher and deployment profiles; proposed cloudflared
overlay/configuration and runbook, no runtime tunnel SDK dependency.

**FR:** implement the selected topology in LOCAL_ACCESS.md. Start/doctor/stop reuse
existing setup; validate hostname, Access team/audience, OIDC callback, TLS CA/SNI
and readiness before ingress. Connector validates Access assertions. Bind origin
privately, require explicit profile selection and operator-provided credentials.
Retain local-only HTTPS/OIDC mode and separate Flight endpoint.

**NFR:** idempotent process management, credentials in restricted secret files,
redacted diagnostics, no automatic DNS/policy creation or reseeding, no public
dev server/DB/Flight exposure. Stop preserves data and only stops owned processes.

**Proof:** configuration failure cases and loopback process lifecycle; E07 later
executes authorized external setup. No fake “Access enforced” green check.
**Done:** E04–E06 pass locally; external gate remains VERIFY until R09.

## R04 — Implement the selected visual foundation

**Dependencies:** none; align build pins with R01 before delivery.
**Acceptance:** E08/E09.
**Files:** styles.css, existing Icon/AppShell, proposed src/design and shared ui components.

**FR:** implement the exact semantic palette, IBM Plex type scale, density, spacing,
icons and theme resolution in DESIGN_SYSTEM.md. Self-host licensed font assets.
Remove hardcoded competing palettes/serif headings; resolve System-light behavior.
Use Mantine core/hooks directly through the shared theme/provider defined in
DESIGN_SYSTEM.md; do not build a custom primitive layer. First qualify a small
real production-CSP/bundle slice before broad adoption. Map existing controls to
the documented Mantine components, then delete replaced CSS/focus/theme code.

**NFR:** one Mantine theme/token source, one icon family, no second component library, no remote font
dependency, no inline CSS exception to production CSP. Existing working screens
continue using the shared layer while being converted; no second app/theme system.

**Proof:** library license/version/peer checks, real CSP and bundle checks,
measured contrast/token checks and a representative real component
composition. E09 validates both actual CSS themes, not only hex arithmetic.
**Done:** E08/E09 foundations pass, replaced values removed and visual board
translated into actual components; this does not accept all screen workflows.

## R05 — Build Storybook as the executable designbook

**Dependencies:** R01/R04. **Acceptance:** E10/E11.
**Files:** proposed .storybook, colocated stories, designbook MDX, UI package scripts/CI.

**FR:** wire the actual Mantine theme, styles, fonts, components and provider fixtures into
React/Vite Storybook; add Docs/a11y/Vitest integration. Cover required foundations,
components, patterns and synthetic workflows. Document keyboard behavior/content/
states. Stories provide primary component behavior tests where appropriate.

**NFR:** no production API/IdP calls, no secrets, no Storybook-only replicas, no
second design framework. Mantine core/hooks are runtime dependencies; Storybook
tooling remains dev-only and absent from app bundle. Test our configured behavior,
not the entire upstream library's internals.
Freeze time/data, wait for fonts and disable motion in representative visual checks.

**Proof:** static build, deterministic play/a11y checks, unexpected-network failure
case and import/reuse evidence. Keep Playwright for real app boundaries.
**Done:** E10/E11 pass, contribution rules prevent unreviewed design drift.

## R06 — Adopt shared shell and simplify async UI ownership

**Dependencies:** R01/R04/R05. **Acceptance:** E12; inherited B09/B10.
**Files:** main.tsx, AppShell, LoginPanel, CommandPalette, query_scope/lifecycle,
api.ts, navigation and actual feature hooks.

**FR:** implement workbench layout and keyboard navigation. Use structured session
query identities and session generation. Keep server data in the existing query
client, forms in their owning feature, mutation reconciliation in one explicit
flow. Preserve scope checks after every await/finally, including draft-reference
changes. Replace custom focus trap and misleading listbox behavior using Mantine
Modal/Combobox; use its shell/menu controls and one color-scheme manager.

**NFR:** no parallel caches, private persistence, Redux or generic event bus.
Delete replaced epoch/arithmetic tests only when a rendered story owns the fault.
Keep sign-out usable at every width and clear private state immediately.

**Proof:** one parameterized deferred-response component/story matrix, shell
keyboard/theme checks and actual API login wiring. Role matrices live at the API.
**Done:** E12 passes and no replaced state owner has production callers.

## R07 — Complete the policy author-to-publisher vertical journey

**Dependencies:** R06; backend contracts from R01.
**Acceptance:** E13; inherited B11–B13/B16.
**Files:** AssetWorkspace, schema tree, draft/review/publication services,
typed navigation, policy stories and existing browser/API tests.

**FR:** real asset inventory, clear nested authoring layout, lossless existing masks/
conditions/filters, saved/active status and semantic diff. Bind review links to
asset/draft/revision; stale links never silently load current content. Clipboard
failure has visible recovery. Remove index-based schema identity fallback.
Support separate editor/publisher, deny-all, uncertain publish reconciliation,
history and restore-to-saved-draft.

**NFR:** no new policy language, unsupported OR/type widgets or browser evaluator.
Existing canonical policy/schema services remain authoritative. Preserve 10k-node
tree performance and keyboard behavior; do not trade safety for visual success.

**Proof:** one real-backend author→review→publish→read→restore journey, referenced
draft race in R10, focused story cases for invalid/empty/conflict states.
**Done:** E13 passes and owner can explain exactly what is saved versus active.

## R08 — Complete connections, administration and audit with shared patterns

**Dependencies:** R06/R01. **Acceptance:** E14; inherited B14/B15.
**Files:** ConnectionsView, SettingsView, ManagementViews, AccessView and owning APIs.

**FR:** migrate typed config forms, capability explanations, lifecycle activation,
last-admin-safe settings, audit filters/details and consumer handoff to shared
components. Reuse existing audit pagination backend. Show real health/diagnostic
states, field errors and request IDs. Explain web versus Flight endpoints.

**NFR:** no direct secrets, default settings on GET failure, fake health or provider
hardcoding. Keep management and data privileges separate. No widget-by-widget
duplication of the backend permission matrix.

**Proof:** one settings/access/audit journey plus parameterized connection lifecycle
and typed-value story cases. Preserve current unique negative API oracles.
**Done:** E14 passes; all visible actions have a verified authorized backend effect.

## R09 — Qualify real local and Cloudflare SSO experience

**Dependencies:** R02/R03/R06. **Acceptance:** E07; inherited B08/B20.
**Files:** existing browser harness, profile/runbook, tunnel configuration.

**FR:** execute clean local-only and named-mode setup; exact callback login,
unauthorized edge denial, browser SSO reuse, app-role denial, logout, expiry,
restart, network outage and recovery. Test foreign Host/forwarded headers and
actual verified origin TLS. Preserve in-progress publication reconciliation.

**NFR:** record authorization for account/exposure actions separately; do not
fabricate credentials or approvals. Provider failure never enables a fallback
token login. Keep Flight read proof on its supported local/private TLS route.

**Proof:** real IdP and Access logs plus redacted app/browser evidence, from an
allowed and denied principal. Remote proof cannot be replaced by header mocks.
**Done:** E07 passes or remains VERIFY with the exact missing external prerequisite.

## R10 — Finish security, process and consumer qualification

**Dependencies:** R01/R07/R08; local auth from R02.
**Acceptance:** E15; inherited B06/B07/B16/B17.
**Files:** existing integration/conformance/consumer suites and JVM harness.

**FR:** two API processes with PostgreSQL barriers, publication/revocation/author
revision/termination races; hostile actual destinations and cancellation; exact
installed plugin wheels and three pairs × Python/DuckDB/Spark through TLS/OIDC.
Retain real fixtures and reject invalid plugins without adding a second harness.

**NFR:** no source-checkout substitution, SQLite-for-races, stub-for-provider or
count-only expected output. Protected pickle fixtures remain unchanged.
**Proof:** current B06/B07/B16/B17 cases, full nested schema/row multiset and
artifact identities. **Done:** E15 passes for every advertised support cell.

## R11 — Bound cost, performance and release operations

**Dependencies:** R07/R08/R09/R10. **Acceptance:** E16/E17; inherited B18–B21.
**Files:** existing capacity/CI/image/provenance/backup/restore tooling.

**FR:** consolidate redundant tests with invariant ownership, measure UI/font/
bundle/API/resource budgets, run real encrypted PostgreSQL backup and timed
restore, invalidate old access before reopening, and bind release evidence to
exact wheels/images. Retain path-aware fast checks and a complete release lane.

**NFR:** no global test-count/coverage target, no automatic threshold relaxation,
no new backup framework. Real RPO <=15min/RTO <=60min and all inherited resource
gates remain. Add no production Storybook dependencies or external font requests.
**Proof:** E16/E17 and original candidate-integrity fault cases.
**Done:** complete candidate dossier; every required live gate executed without
silent skips, documents refer to supported commands/versions.

## R12 — Accept the experience and security outcome

**Dependencies:** R01–R11. **Acceptance:** E18 and all inherited guarantees.
**FR:** owner approves actual light/dark screens and designbook, five representative
users complete governed workflows, independent security review covers app and
optional edge/proxy boundary. Fix and retest material findings.
**NFR:** no invented scores or review signoff; contact participants only when
authorized. A private local product must remain usable without Cloudflare.
**Proof:** observed task success/timing, owner visual decision, security report
and exact candidate/runbook. **Done:** all gates pass; otherwise release HOLD.

## Execution ledger

All R packets start OPEN at this planning baseline. R01/R04 are ready to begin;
the remaining packets await their dependencies. Existing functionality is reused,
not relabeled as absent. Replace an entry with ACTIVE/VERIFY/DONE and one concise
evidence record when implementation resumes. Do not append another thousands-line
progress narrative or report component existence as a finished user journey.
