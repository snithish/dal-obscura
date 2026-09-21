# Execution ledger

Implementation authorized 2026-09-20. Plan: [R01–R12](ACTION_PLAN.md).
No packet is DONE until all of its acceptance criteria pass.

## R06 — ACTIVE: exact identity cache isolation

Replaced delimiter-concatenated issuer/subject cache scope with an unambiguous
serialized tuple. The existing QueryClient test now exercises a collision pair
against actual cached inventory: failed before the fix, passes afterward.
Each accepted session load now also uses its load generation in cache keys and
clears the old cache before loading inventory. A second real QueryClient test
proves same-identity reauthentication cannot reuse prior inventory, even if its
cache lifetime has not expired. Both cases failed before their fixes.
Added a built-app Playwright journey that holds a pre-logout asset response,
signs out, reauthenticates the same principal with changed capabilities, and
releases the late response. The new session remains on its own asset and the
old response cannot repopulate the workspace. No authentication or pickle
changes.

Validation: all 16 existing UI unit tests, TypeScript check, and 9 built-shell
Playwright tests pass on bundled Node 24.19.0. Initial collision-only
validation also passed on Node 26.8.2. Commit identities are available from
this file's history. R06 remains ACTIVE pending the full parameterized
workflow matrix and live identity/provider qualification.

## R01 — ACTIVE: build tooling ownership

Moved Vite and its React build plugin from runtime to development dependencies,
retaining exact versions/integrities. Frozen offline install passed with the
cached pnpm 12.3.4 binary and Node 24.19.0. Production build passed: JS 101.38 kB
gzip, CSS 5.28 kB gzip. Existing package-boundary, route-inventory, plugin-contract
and registry tests: 38 passed. Full support/advisory and clean-artifact evidence
remain open; these focused checks do not close E01.

## Remaining packets

R04: ACTIVE; Mantine production-CSP foundation qualified below.
R05: ACTIVE; initial executable designbook below.
R02/R03/R07–R12: OPEN. No production readiness or live Cloudflare claim.

## R04/R06 — ACTIVE: Mantine shell and CSP foundation (2026-09-21)

Pinned MIT-licensed Mantine core/hooks 9.6.1 with compatible React peers. Added
self-hosted IBM Plex Sans/Mono and a shared Mantine provider/theme. Login, shell,
mobile navigation and command search now use real library components. Removed
their replaced CSS and custom focus/theme management. Disabled navigation keeps
explicit capability guards. Authentication and pickle behavior are unchanged.

NGINX generates a per-response style nonce, injects it into the shell and CSP,
and prevents shell caching/304 reuse. Mantine and scroll-lock styles receive that
nonce. Scripts remain self-only; style attributes remain forbidden. Built Vite
preview uses the same CSP contract for browser regression checks.

Proof: 8 browser tests pass against the actual NGINX 1.27.5 image, including
wrong/missing nonce rejection, fresh nonces, keyboard focus, OS theme changes,
narrow viewport and signed-out axe checks. API responses in these tests are
synthetic: this is not SSO or authenticated workflow evidence. All 16 UI unit
tests, TypeScript and 10 focused deployment/UI-shell Python tests pass. Production
build: JS 165.60 kB gzip, CSS 39.59 kB gzip; WOFF2 fonts 85.73 kB. These meet size
budgets only; no latency/LCP claim. Frozen offline install passes.

Registry blocker resolved using a temporary loopback curl relay to the official
npm registry, with TLS and package integrity checks retained. No relay URL is
stored in the lockfile or runtime configuration.

Remaining: convert feature controls and competing legacy CSS values, measure both
themes across authenticated screens, finish workbench/query ownership, Storybook,
and live identity/provider qualification. R04/R06 are not DONE.

## R05 — ACTIVE: executable designbook foundation (2026-09-21)

Storybook 10.6.0 React/Vite, Docs, accessibility and Vitest integration are pinned
as development dependencies. Vitest/browser provider 4.1.11 satisfies the addon
peer range (the newer Vitest 5 is not supported by that range). Storybook imports
the actual AppProviders, theme, styles, LoginPanel and AppShell. Ten stories cover
semantic palette/type/icons, login unavailable/rejected/pending states, reader
and administrator navigation, and representative dark themes. Contribution MDX
documents ownership, limitations and remaining workflow coverage.

Ten browser story tests pass, including axe and actual retry/submission/capability
behavior. They first exposed two real contrast failures: dark primary-button text
and the connected badge. The shared filled-button default and semantic status
tokens now fix those failures. No accessibility rule was disabled. Static
Storybook and production application builds pass; app JS 165.63 kB gzip, CSS
39.63 kB gzip. TypeScript and frozen offline install pass. CI now builds and tests
stories using its existing Chromium install. Storybook has a separate Vite config
without production API/auth proxies, loopback binding and telemetry disabled.

Commands from apps/governance-ui: `pnpm storybook`, `pnpm build-storybook`,
`pnpm test:stories`. Story tests use Playwright Chromium; the existing
DAL_OBSCURA_E2E_EXECUTABLE_PATH override also works for a local browser install.

Still OPEN before E10/E11 can pass: network interception with fail-on-unexpected
requests (do not add networked stories before this), complete documented component/
pattern/workflow coverage, contrast matrix, reviewed responsive visual baselines,
manual keyboard/screen-reader/zoom checks and owner visual approval. Current
stories are synthetic and do not prove SSO or backend authorization. Production
Playwright retains distinct CSP, focus and OS-theme boundary checks; no existing
security tests were removed in favor of stories. R05 is not DONE.

## R05 — ACTIVE: Storybook network boundary (2026-09-21)

Added a preview-level network guard and a contribution story that proves
synthetic stories fail immediately when they attempt to call `/v1/*`, `/auth/*`,
an identity provider, or another origin. The guard wraps fetch, XHR and beacon
requests and returns Storybook's cleanup callback so one story cannot leak its
boundary into the next. The designbook now documents this rule and keeps real
API/IdP/customer-data journeys in application browser suites. The story uses
Mantine primitives for its explanation; it does not duplicate product UI.

Proof: `pnpm check`, `pnpm build`, `pnpm test`, `pnpm build-storybook`, and
`pnpm test:stories` all pass from `apps/governance-ui`; the browser suite now
reports 4 files and 11 tests passing with the pinned Playwright Chromium. No
backend, authentication, or pickle code changed.

Still OPEN before E10/E11 can pass: complete documented component/pattern/
workflow coverage, contrast matrix, reviewed responsive visual baselines,
manual keyboard/screen-reader/zoom checks, owner visual approval, and the
remaining R04 feature-CSS migration. R05 remains ACTIVE.

## R05 — ACTIVE: command palette coverage and search correctness (2026-09-21)

Added executable stories for the production `CommandPalette` component. The
stories use deterministic assets and verify both scoped search results and the
real selection callback through the Mantine modal portal. While covering this
component, fixed a functional gap: asset search now matches id, name, catalog,
and table identifier instead of showing every authorized asset for any query.

Proof: `pnpm check` and `pnpm test:stories` pass from `apps/governance-ui`;
the browser suite reports 5 files and 13 tests passing. No networked fixture,
backend, authentication, or pickle behavior was changed. R05 remains ACTIVE;
the remaining coverage, visual review, manual accessibility, and owner approval
gates are unchanged.

## R05 — ACTIVE: connections management workflow coverage (2026-09-21)

Added executable stories for the production `ConnectionsView` using admitted
Iceberg catalog metadata, plugin lifecycle state, an active configuration
generation, and the empty state. The stories verify rendered management
headings and controls without invoking mutating callbacks or making network
requests. Vitest now pre-optimizes React Query as well as the existing icon
dependency, preventing browser-story reload races when management components
are imported.

Proof: `pnpm check`, `pnpm build`, `pnpm test`, and `pnpm test:stories` pass;
the Storybook browser suite reports 6 files and 15 tests passing. R05 remains
ACTIVE because component/pattern/workflow breadth, contrast and responsive
review, manual accessibility checks, and owner approval are still open.

## R04 — ACTIVE: explicit Mantine scheme boundary (2026-09-21)

Removed the legacy `prefers-color-scheme` fallback that treated a missing
Mantine scheme attribute as dark. Remaining feature overrides now require
`data-mantine-color-scheme="dark"`, matching the provider's explicit light,
dark, and auto resolution and preventing system/light sessions from inheriting
dark feature styles.

Proof: all 8 existing design-system and shell browser tests pass, including OS
theme switching, CSP, keyboard focus, narrow viewport, and signed-out axe
checks. `pnpm check`, `pnpm build`, and `pnpm test:stories` also pass; the
Storybook suite remains 7 files and 17 tests. R04 remains ACTIVE because the
broader legacy CSS/token migration and authenticated visual review are open.

## R04 — ACTIVE: Mantine semantic palette ownership (2026-09-21)

Removed the duplicate root palette and font declarations from the legacy
stylesheet. Mantine's shared CSS variable resolver is now the single source for
canvas, surfaces, text, borders, focus, status colors, and typography tokens;
the feature stylesheet retains layout and behavior rules only.

Proof: all 8 design-system and shell browser tests, 17 Storybook browser tests,
TypeScript, and the production build pass. The production bundle remains
within the recorded budgets at 165.29 kB JS gzip and 39.36 kB CSS gzip. R04
remains ACTIVE because hardcoded feature colors and authenticated visual review
still require migration and qualification.

## R05 — ACTIVE: runtime and identity workflow coverage (2026-09-21)

Added executable stories for the production `SettingsView` with configured
runtime limits, governed storage roots, deployment-managed OIDC metadata, an
active generation, and the no-provider state. Fixtures contain no credentials;
the stories only inspect rendered controls and do not invoke save or identity
provider actions.

Proof: `pnpm check`, `pnpm test`, and `pnpm test:stories` pass from
`apps/governance-ui`; the Storybook browser suite reports 7 files and 17 tests
passing. R05 remains ACTIVE pending broader pattern/workflow coverage, visual
review, manual accessibility checks, and owner approval.

## R04/R05 — ACTIVE: nested policy workspace accessibility (2026-09-21)

Added an executable story for the production `AssetWorkspace` with a nested
struct schema (`customer.email`), field-level grant state, DuckDB row filter,
email mask, reviewable draft, and administrator capabilities. The story uses
only deterministic fixtures. Its axe run exposed and fixed an invalid
`aria-setsize` on the tree container and contrast failures for selected field
types and rule principals; metadata now uses the shared semantic text token.

Proof: TypeScript, production build, all 8 design-system/shell E2E tests, and
the Storybook browser suite pass; Storybook now reports 8 files and 18 tests.
R04/R05 remain ACTIVE pending broader token migration, authenticated visual
review, manual accessibility checks, and owner approval.

Static Storybook verification also passes after the nested workflow addition:
`pnpm build-storybook` completed successfully with the 8-story-file inventory.

## R07 — ACTIVE: authoritative schema identity guard (2026-09-21)

Removed `AssetWorkspace`'s index-based fallback for legacy flat schema
summaries. When the authoritative nested schema response is missing, the UI now
shows an explicit unavailable state and does not create or select fabricated
field identities. Added a Storybook edge-case story beside the nested policy
workflow story.

Proof: `pnpm check`, `pnpm build`, `pnpm test`, and `pnpm test:stories` pass;
the Storybook browser suite reports 8 files and 19 tests. R07 remains ACTIVE:
real author/reviewer/publisher, stale-link, restore, and process-race journeys
still require backend qualification.

## R07 — ACTIVE: review-link clipboard recovery (2026-09-21)

`AssetWorkspace` no longer silently ignores a failed clipboard write. It keeps
the exact asset/draft URL in a read-only field, explains that clipboard access
is unavailable, and offers a retry action. Storybook covers the real component
failure path by forcing the browser clipboard write to reject.

Proof: `pnpm check`, `pnpm build`, `pnpm test`, and `pnpm test:stories` pass;
the Storybook browser suite reports 8 files and 20 tests. R07 remains ACTIVE:
the real author/reviewer/publisher journey, stale-link qualification, restore,
and process races still require backend evidence.

## R07 — ACTIVE: exact draft-revision review links (2026-09-21)

Review links now include `draft_revision`. Navigation preserves the typed
revision, and asset loading compares it with the saved draft before rendering
review content. A mismatch renders an explicit stale-link notice with no newer
policy substituted, while links without the new parameter retain the existing
draft-id behavior for compatibility.

Proof: `pnpm check`, `pnpm build`, `pnpm test`, and `pnpm test:stories` pass;
the navigation unit test covers the revision parameter and the Storybook suite
remains 8 files and 20 tests. R07 remains ACTIVE pending real author/reviewer/
publisher, restore, and process-race qualification.

Static Storybook verification also passes after revision-bound links were added:
`pnpm build-storybook` completed successfully with the 8-story-file inventory.

## R11 — ACTIVE: production UI budget gate (2026-09-21)

Added `check:budgets` and wired it into the normal UI production build. The
gate measures the built asset directory rather than trusting Vite's display
rounding: JavaScript and CSS are summed after gzip, while initial WOFF2 fonts
are measured as raw bytes. It fails the build when E16's 200 KiB JS, 50 KiB
CSS, or 120 KiB font limits are exceeded.

Proof: `pnpm build` passes with 161.74 KiB JS gzip, 38.44 KiB CSS gzip, and
83.72 KiB WOFF2 fonts; `pnpm check`, `pnpm test`, and `pnpm test:stories` also
pass. R11 remains ACTIVE because route/LCP/CLS, release provenance, and real
backup/restore qualification are still open.

## R08 — ACTIVE: activity and audit workflow coverage (2026-09-21)

Added executable stories for the production `ManagementView` activity surface.
The connected state renders server summary counts, measured data-plane health,
active generation metadata, a redacted audit event, and filter controls. The
unknown state distinguishes unavailable observations from an empty audit log.
No story invokes a mutation or network request.

Proof: `pnpm check`, `pnpm build`, `pnpm test`, and `pnpm test:stories` pass;
the Storybook browser suite reports 9 files and 22 tests. R08 remains ACTIVE:
real settings/access/audit journeys and authorized backend effects still need
qualification.

## R08 — ACTIVE: access and grant-management workflow coverage (2026-09-21)

Added an executable `AssetWorkspace` access story using the real production
component. It verifies effective capability cards, owner principals, delegated
grant principal/capability values, and the enabled save affordance with
deterministic fixtures. The story performs no mutation or network request, so
backend authorization effects remain unverified.

Proof: `pnpm check`, `pnpm build`, `pnpm test`, `pnpm test:stories`, and
`pnpm build-storybook` pass; the Storybook browser suite reports 9 files and
23 tests. R08 remains ACTIVE: live settings/access/audit journeys and
authorized backend effects still need qualification.

## R08 — ACTIVE: delegated authorization state coverage (2026-09-21)

Added a second access story for a non-admin actor with delegated grant
capability. It verifies the production workspace disables owner management,
keeps delegated grant editing available, and explains the capability reason
from the control-plane fixture. This covers UI authorization state only; it
does not substitute for an authorized backend mutation journey.

Proof: `pnpm check`, `pnpm build`, `pnpm test:stories`, and
`pnpm build-storybook` pass; the Storybook browser suite reports 9 files and
24 tests. R08 remains ACTIVE pending live settings/access/audit requests and
their backend authorization effects.

## R08 — ACTIVE: catalog secret-reference editing coverage (2026-09-21)

Added a real `ConnectionsView` story for editing a catalog with a scoped secret
reference. It verifies the password control remains masked, preserves the
deployment-managed reference for editing, and never renders a credential value.
The story uses the production form and does not issue a save or network call.

Proof: `pnpm check`, `pnpm build`, `pnpm test:stories`, and
`pnpm build-storybook` pass; the Storybook browser suite reports 9 files and
25 tests. R08 remains ACTIVE pending live settings/access/audit requests and
their authorized backend effects.

## R08 — ACTIVE: typed catalog configuration coverage (2026-09-21)

Added a typed plugin fixture and executable connection story covering integer,
boolean, enum, and URI fields. The story verifies production form controls
preserve typed values and improves acronym presentation for TLS fields. No
configuration mutation or network request runs in Storybook.

Proof: `pnpm check`, `pnpm build`, `pnpm test:stories`, and
`pnpm build-storybook` pass; the Storybook browser suite reports 9 files and
26 tests. R08 remains ACTIVE pending live settings/access/audit requests and
authorized backend effects.

## R08 — ACTIVE: identity-provider validation coverage (2026-09-21)

Added an executable settings story for malformed attribute-claim mappings. It
verifies the production form reports the actionable field error and disables
identity-provider save until the mapping is corrected. No provider mutation or
network request runs in Storybook.

Proof: `pnpm check`, `pnpm build`, `pnpm test:stories`, and
`pnpm build-storybook` pass; the Storybook browser suite reports 9 files and
27 tests. R08 remains ACTIVE pending live settings/access/audit requests and
authorized backend effects.

## R08 — ACTIVE: audit filter and pagination coverage (2026-09-21)

Added an executable activity story with an audit cursor. It verifies applying a
server-facing actor filter calls the owning callback with the typed filter and
that the production “Load more activity” control invokes the pagination
callback. Fixtures remain deterministic and mutation-free.

Proof: `pnpm check`, `pnpm build`, `pnpm test:stories`, and
`pnpm build-storybook` pass; the Storybook browser suite reports 9 files and
28 tests. R08 remains ACTIVE pending live settings/access/audit requests and
authorized backend effects.

## R08 — ACTIVE: published-history pagination coverage (2026-09-21)

Added an executable changes story for an active published policy version with a
continuation cursor. It verifies production history rendering and invokes the
owner pagination callback. Backend history retrieval and restore authorization
remain integration work, so R07/R08 stay ACTIVE.

Proof: `pnpm check`, `pnpm build`, `pnpm test:stories`, and
`pnpm build-storybook` pass; the Storybook browser suite reports 9 files and
29 tests. R08 remains ACTIVE pending live settings/access/audit requests and
authorized backend effects.

## R07 — ACTIVE: deny-all draft state coverage (2026-09-21)

Added an executable policy-workspace story for an intentional deny-all draft.
It verifies the real editor explains the empty rule set, keeps deny-all save
available, requires review before publish, and offers the first-rule action.
No policy mutation runs in Storybook; author/reviewer/publisher backend races
remain open.

Proof: `pnpm check`, `pnpm build`, `pnpm test:stories`, and
`pnpm build-storybook` pass; the Storybook browser suite reports 9 files and
30 tests. R07 remains ACTIVE pending real author/reviewer/publisher and
restore qualification.

## R07 — ACTIVE: review-only publisher workflow coverage (2026-09-21)

Added a policy story for a separate review-only publisher. It verifies editing
controls stay disabled, asset switching stays restricted, the exact reviewed
draft can be inspected, and a permitted publisher can see the publish action.
Storybook performs no publication request; author/reviewer/publisher race and
backend authorization evidence remain open.

Proof: `pnpm check`, `pnpm build`, `pnpm test:stories`, and
`pnpm build-storybook` pass; the Storybook browser suite reports 9 files and
31 tests. R07 remains ACTIVE pending real author/reviewer/publisher and
restore qualification.

## R04/R05/R06 — ACTIVE: final shell regression evidence (2026-09-21)

Re-ran the UI unit suite and built-shell browser boundary checks after the
workflow coverage slices. The unit suite reports 16 passing tests. Playwright
reports 8 passing auth-shell/design-system tests, including CSP nonce
enforcement, OS/theme override behavior, signed-out deep-link gating, keyboard
focus, narrow viewport sign-in, and accessibility checks. These checks do not
qualify live OIDC, Cloudflare Access, backend mutations, or production
operations.

## R01 — ACTIVE: production UI advisory check (2026-09-21)

The production dependency graph reports no known vulnerabilities with
`pnpm audit --prod`. This closes the current production-UI advisory check only;
the full E01 toolchain/support inventory and repository-wide typing diagnostics
remain open.

## R04 — ACTIVE: feature CSS semantic-token consolidation (2026-09-21)

Migrated the remaining feature stylesheet onto the Mantine semantic variables
for surfaces, text, borders, controls, focus, selection, success, warning, and
danger states. Removed the competing hardcoded dark palette and legacy green/
cream color literals, and routed feature typography through the shared IBM Plex
font variables. This keeps authenticated screens on the same light/dark token
source as the shell and Storybook.

Proof: `pnpm check`, 16 UI unit tests, 31 Storybook tests, `pnpm build` with
161.75 KiB JS gzip, 37.89 KiB CSS gzip, and 83.72 KiB fonts, 9 built-browser
tests, and `pnpm build-storybook` all pass. R04 remains ACTIVE pending the
contrast matrix, responsive/zoom/forced-colors review, and owner visual
approval.

Follow-up story `97da75b1` adds the real nested policy workspace under the dark
theme. The Storybook suite now reports 32 passing tests and the static
designbook rebuild passes; this improves theme coverage but does not replace
manual contrast, zoom, forced-colors, or owner review.

## R02 — ACTIVE: trusted proxy login attribution (2026-09-21)

Added an explicit trusted-proxy contract for browser login rate limiting. The
control plane accepts a sanitized `X-Forwarded-For` client address only when
the direct peer matches an operator-configured IP or CIDR. It also maintains a
separate aggregate gateway budget so one tunnel user cannot exhaust every
client bucket, while untrusted or malformed forwarding headers fall back to
the direct peer. The UI NGINX boundary overwrites forwarding metadata rather
than appending caller-controlled values. Production and secure-local examples
document both settings, and architecture tests protect the forwarding and
deployment contracts.

Proof: focused OIDC/CLI tests (39 passed), the full control-plane suite,
secure-local/production architecture tests (6 passed), `ruff check`,
`ruff format --check`, and the non-heavy pre-commit suite pass. The repository
`ty` hook remains blocked by 73 pre-existing diagnostics in unrelated demo,
plugin, and application typing paths. R02 remains ACTIVE pending the full
origin/header matrix and live local/Cloudflare SSO verification.

Follow-up regression `dec4a450` proves a successful callback clears only the
forwarded client bucket; it cannot reset the shared trusted-gateway budget.
Configuration regressions in `cf8b8a00` prove malformed or hostname-based proxy
peer values are rejected at app construction rather than silently trusted.

Follow-up `97254480` closes a host-boundary gap for browser cookie mutations:
the request Host must match a configured browser origin before CSRF-valid state
changes proceed, while forwarded host metadata remains ignored. Regression tests
cover forged and configured hosts with and without an Origin header.

Proof: the full `tests/interfaces/control_plane` suite and focused OIDC/auth
tests pass; Ruff passes. R02 remains ACTIVE pending the complete origin/header
matrix and live local/Cloudflare SSO verification.

Follow-up parameterized auth coverage exercises the cookie-mutation matrix for
implicit configured host, explicit configured origin/host, forged Host,
attacker Origin, and caller-supplied `X-Forwarded-Host`. The forwarded host is
ignored and only the configured browser boundary can succeed.

Proof: `uv run pytest tests/interfaces/control_plane/test_actor_auth.py -q`
passes (including the six matrix cases). R02 remains ACTIVE pending live local
and Cloudflare SSO verification.

## R03 — ACTIVE: bounded secure-local doctor (2026-09-21)

Added `deployment/local-secure/run doctor` after profile validation and Compose
rendering. It reports only redacted local state: HTTPS callback/origin shape,
bootstrap mode, browser OIDC configuration presence, loopback Flight exposure,
UI certificate trust/SAN checks, and current Compose service state. The runbook
explicitly marks Cloudflare Access as unverified; the command makes no external
edge, identity-provider, connector, consumer, or production-readiness claim.

Proof: `sh -n deployment/local-secure/run`, `git diff --check`, Ruff, and
`tests/architecture/test_secure_local_profile.py` plus
`tests/production/test_local_parity.py` (4 passed). R03 remains ACTIVE pending
the named-tunnel profile and authorized real local/edge SSO qualification.

## R03 — ACTIVE: named-tunnel development overlay (2026-09-21)

Added `deployment/named-tunnel` as an explicit optional Compose overlay over the
production and secure-local profiles. The connector has no published port; the
origin remains loopback-bound and uses operator-provided hostname certificates,
while Flight stays on the private local TLS endpoint. The runner validates a
stable DNS hostname, exact HTTPS callback/logout/CORS origins, disabled
bootstrap, required owner-readable secret files, and immutable image digests.
It never creates DNS, tunnel, or Access resources and its doctor output marks
Cloudflare Access as unverified.

Follow-up `98ea7223` explicitly ignores generated secret material for all three
deployment profiles and protects that contract with an architecture test.

Proof: shell syntax, YAML parsing, Ruff, and six architecture/production parity
tests pass. Compose runtime rendering could not be executed because the local
Podman socket was unavailable. R03 remains ACTIVE pending an authorized named
tunnel, real Access allow/deny probes, and local/edge SSO evidence.

## R05/R08 — ACTIVE: consumer handoff clipboard recovery (2026-09-21)

Consumer snippets in the real `AssetWorkspace` now expose a visible retry and
manual-selection message when browser clipboard access fails. Added an
executable Storybook journey that forces the browser write to reject and checks
the recovery state without changing the generated Python, DuckDB, Spark, or
Arrow examples.

Proof: `pnpm check`, 16 UI unit tests, 33 Storybook browser tests, and the
production build budget gate pass (161.79 KiB JS gzip, 37.89 KiB CSS gzip,
83.72 KiB fonts); `pnpm build-storybook` also passes. R05/R08 remain ACTIVE pending real consumer endpoint runs,
the complete administration journey, and live authorization evidence.

## R08 — ACTIVE: complete audit resource filters (2026-09-21)

Aligned the Activity view's resource selector with the backend's authoritative
audit vocabulary: asset, catalog, plugin, publication, and workspace. The
Storybook filter journey now asserts the previously missing management scopes;
filter submission still delegates to the existing paginated API query.

Proof: `pnpm check` and 33 Storybook browser tests pass. R08 remains ACTIVE
pending the complete settings/access/audit management journey and authorized
backend mutation evidence.

## R04 — ACTIVE: focus and forced-colors fallback (2026-09-21)

Aligned the legacy feature focus ring with the design system's 2px semantic
focus token and added a forced-colors media fallback for controls, selected
rows, status chips, notices, and selection contrast. This is a browser fallback
for system high-contrast modes; it does not replace manual OS review.

Proof: `pnpm check`, 33 Storybook tests, the production budget build (161.79 KiB
JS gzip, 38.00 KiB CSS gzip, 83.72 KiB fonts), and 9 built-browser auth/design
system tests pass. R04 remains ACTIVE pending rendered contrast matrix,
responsive/zoom/forced-colors review, and owner visual approval.

Follow-up browser proof adds a forced-colors signed-out journey. It verifies the
browser media mode, keyboard focus, explicit two-pixel focus outline, and local
sign-in visibility against the production build. The forced-colors fallback now
sets the complete outline because Mantine inputs may reset the base outline.
The built browser suite reports 10 passing tests; R04 remains ACTIVE pending
manual OS contrast/zoom review and owner visual approval.

The built browser suite now also verifies `prefers-reduced-motion: reduce` and
the production shell's animation/transition durations. It reports 11 passing
tests; R04 remains ACTIVE pending manual OS contrast/zoom review and owner
visual approval.

The signed-out browser journey now checks the real production shell at
390×844, 768×1024, and 1440×900, plus a 200% page zoom. Login controls remain
visible and inside the viewport at each size. The full built browser suite
reports 12 passing tests; R04 remains ACTIVE pending manual visual, screen-reader,
and authenticated workflow review.

The reauthentication journey now also exercises the actual authenticated policy
workspace in explicit dark and light themes. It verifies the Mantine scheme
attribute and rendered `.studio` surface in both modes before releasing the
held stale response. The full built browser suite remains at 12 passing tests;
R04/R06 remain ACTIVE pending manual contrast/screen-reader review and live
identity qualification.

R05 workflow coverage also adds deterministic management loading and retry-error
stories. They exercise the real `ManagementView` boundary, retain the retry
callback, and keep server failure copy in an alert region. Storybook now reports
36 passing browser tests; R05 remains ACTIVE pending broader workflow breadth,
manual accessibility review, and authenticated backend evidence.

## R02/R07 — ACTIVE: encoded asset route segments (2026-09-21)

Centralized browser API construction for asset-scoped routes through an encoded
path-segment helper. Asset IDs containing slashes, query delimiters, or dot
segments can no longer change the request route shape; ordinary UUIDs are
unchanged. The helper is covered by unit cases for normal, traversal-shaped,
and query-shaped identifiers.

Proof: `pnpm check` and 17 UI unit tests pass. This hardens the client boundary;
server authorization and live hostile-destination qualification remain open in
R02/R10.

## R08 — ACTIVE: typed numeric configuration validation (2026-09-21)

Catalog forms now reject non-finite numeric values and fractional values for
integer fields before constructing the mutation payload. The error names the
typed field and preserves the editor contents. A Storybook workflow proves an
invalid integer is blocked without contacting the control plane.

Proof: `pnpm check` and 37 Storybook browser tests pass. R08 remains ACTIVE
pending live settings/access/audit journeys and authorized backend mutation
evidence.

## R07 — ACTIVE: semantic history diff (2026-09-21)

The history workspace now compares the saved draft with a selected immutable
policy revision. The diff is keyed by rule ordinal, normalizes set-like
principals, columns, masks, and condition values, and reports added, removed,
and changed rules without evaluating policy in the browser. The pure diff
algorithm is isolated from JSX for direct unit coverage, while Storybook seeds
the real history query client and renders the workflow without network access.

Proof: `pnpm check`, 19 UI unit tests, 38 Storybook browser tests, `pnpm build`,
and `pnpm build-storybook` pass. R07 remains ACTIVE pending the real
author/reviewer/publisher, restore, and process-race journeys.

R08 workflow coverage also adds a seeded catalog diagnostic failure. It invokes
the real `ConnectionsView` check action and renders the measured unavailable
state with its transport error, without contacting an external endpoint.

Proof: `pnpm check` and 39 Storybook browser tests pass. R08 remains ACTIVE
pending live settings/access/audit journeys and authorized backend mutation
evidence.

R05/R08 workflow coverage adds a runtime-settings validation story. It edits
the real ticket TTL field to an invalid non-positive value, verifies the
actionable status message, and stops before any mutation request is created.

Proof: `pnpm check` and 40 Storybook browser tests pass. R05/R08 remain ACTIVE
pending broader authenticated workflow coverage and live backend mutation
evidence.

OIDC provider numeric fields now reject fractional, negative, and non-finite
values before the identity-provider payload is built. A Storybook workflow
drives the real Maximum JWKS keys control, verifies the field alert, and keeps
the save action disabled.

Proof: `pnpm check`, `pnpm test:stories` (41 browser tests), and `pnpm build`
pass; the production budget gate measures 162.40 KiB JS gzip, 38.01 KiB CSS
gzip, and 83.72 KiB fonts. R05/R08 remain ACTIVE pending live
settings/access/audit journeys and authorized backend evidence.

## R08 — ACTIVE: platform-admin lifecycle affordance boundary (2026-09-21)

Disabled plugin lifecycle selectors and apply actions for non-platform-admin
sessions. The control plane already requires platform-admin authorization for
these mutations; the UI now reflects that boundary before a request can be
attempted. Added a read-only Storybook workflow asserting both controls are
disabled while admitted plugin status remains visible.

Proof: `pnpm check`, 34 Storybook browser tests, and the production budget build
(161.83 KiB JS gzip, 38.00 KiB CSS gzip, 83.72 KiB fonts) pass. R08 remains
ACTIVE pending the complete live settings/access/audit journey and authorized
backend mutation evidence.

## R04/R05/R06 — ACTIVE: authenticated responsive shell proof (2026-09-21)

Added a reusable synthetic authenticated browser fixture and a production-build
journey at 390×844. It loads the real policy workspace, opens the Mantine mobile
navigation Drawer, verifies the reader cannot activate the admin-only Settings
destination, confirms Escape closes the Drawer and restores focus to its trigger,
and confirms an enabled Activity navigation action closes the Drawer and changes
the rendered page. The fixture is explicitly synthetic and does not qualify an
identity provider, Cloudflare Access, or a live backend.

Proof: `pnpm check` and the full built browser suite pass with 13 tests. Atomic
implementation commit: `8e787f77 test(ui): cover authenticated mobile navigation`.
R04/R05/R06 remain ACTIVE pending manual screen-reader/visual review, live
identity qualification, and the broader authenticated feature journeys.

## R08 — ACTIVE: identity-provider lockout confirmation (2026-09-21)

Settings now requires an explicit browser confirmation before staging a provider
chain with no enabled identity provider. Cancelling leaves the staged form
unsaved and reports the recovery action; the server-side revision and publication
readiness rules remain authoritative. A Storybook workflow removes the only
provider, cancels the confirmation, and verifies the visible status message.

Proof: `pnpm check`, `pnpm test:stories` (42 browser tests), and `pnpm build`
pass; the budget gate measures 162.48 KiB JS gzip, 38.01 KiB CSS gzip, and
83.72 KiB fonts. Atomic implementation commit: `fea21100 fix(ui): confirm
identity provider lockout`. R08 remains ACTIVE pending live settings/access/
audit journeys and authorized backend mutation evidence.

R08 settings validation now also rejects empty issuer URLs, malformed or
credential-bearing HTTP(S) issuer/JWKS URLs, empty signing-algorithm lists, and
OIDC cache values outside the backend's documented bounds before a mutation
payload is created. The new issuer-error workflow keeps Save disabled while the
field is invalid; server validation remains authoritative for all other cases.

Proof: `pnpm check`, `pnpm test:stories` (43 browser tests), and `pnpm build`
pass; the budget gate measures 162.70 KiB JS gzip, 38.01 KiB CSS gzip, and
83.72 KiB fonts. Atomic implementation commit: `e928562a fix(ui): validate
oidc fields before save`. R08 remains ACTIVE pending live settings/access/audit
journeys and authorized backend mutation evidence.

## R07 — ACTIVE: stale review-link browser proof (2026-09-21)

The built authenticated shell suite now opens a review URL carrying an older
`draft_revision`, serves a newer saved draft from the synthetic API boundary,
and verifies the real application displays its explicit stale-link notice. The
workspace remains read-only: the deny-all save action and asset selector are
disabled, so a stale link cannot silently substitute or edit newer content.
This is deterministic browser integration evidence only; it does not qualify a
live author/reviewer/publisher backend or a concurrent revision race.

Proof: `pnpm check` and the full built-browser suite pass with 14 tests. Atomic
implementation commit: `b4775319 test(ui): prove stale review links stay
read-only`. R07 remains ACTIVE pending the real author/reviewer/publisher,
restore, and process-race journey.

## R06 — ACTIVE: deferred management-response race proof (2026-09-21)

Added a built-browser race journey over the real management query ownership. The
first Activity audit response is held, navigation moves to Changes and back to
Activity, the newer response renders, and only then is the stale response
released. The current Activity view retains the fresh event and never renders
the stale event, exercising page epochs, query cancellation, and session-scoped
management state together. The fixture is synthetic and does not qualify a live
identity provider or backend process race.

Proof: `pnpm check` and the full built-browser suite pass with 15 tests. Atomic
implementation commit: `7e7f4eda test(ui): cover deferred management responses`.
R06 remains ACTIVE pending the broader parameterized workflow matrix and live
identity/provider qualification.

## R02/R06/R08 — ACTIVE: fail-closed reader admin deep links (2026-09-21)

Authenticated readers who open `#connections` or `#settings` directly are now
redirected to the Assets workspace before admin management requests are issued.
The loader also guards the route independently, so a transient direct-link
state cannot start a settings or connection fetch without the
`workspace:admin` capability. A built-browser reader fixture proves the hash
redirect and asserts that no `/v1/settings/*` request was sent; server
authorization remains authoritative.

Proof: `pnpm check` and the full built-browser suite pass with 16 tests. Atomic
implementation commit: `0dd4754e fix(ui): fail closed on reader admin deep links`.
R02/R06/R08 remain ACTIVE pending live identity qualification and authorized
backend settings/access/audit journeys.

## R08 — ACTIVE: last-owner UI safeguard (2026-09-21)

The asset access editor now blocks an empty owner list before creating a
mutation request and explains that a replacement owner must be assigned first.
This mirrors the control-plane last-owner invariant while preserving server
authorization and revision checks. A real `AssetWorkspace` Storybook workflow
clears the owner field, submits the form, and verifies the actionable status
message without contacting the API.

Proof: `pnpm check`, `pnpm test:stories` (44 browser tests), and `pnpm build`
pass; the budget gate measures 162.82 KiB JS gzip, 38.01 KiB CSS gzip, and
83.72 KiB fonts. Atomic implementation commit: `590c627f fix(ui): prevent
empty asset ownership`. R08 remains ACTIVE pending live settings/access/audit
journeys and authorized backend mutation evidence.

## R05 — ACTIVE: designbook coverage synchronization (2026-09-21)

Updated `designbook/Guide.mdx` to document the workflows now covered by the
actual production components: command search, authenticated shell states,
nested policy and stale-link handling, consumer handoff, access/grants,
connections, settings, audit, lifecycle guards, validation, and deferred
response recovery. The guide continues to separate synthetic Storybook proof
from live identity/backend qualification and manual visual review.

Proof: `pnpm build-storybook` completes successfully. Atomic documentation
commit: `23ea70ba docs(ui): refresh designbook coverage`. R05 remains ACTIVE
pending complete visual/manual review and live authenticated workflow evidence.

## R07 — ACTIVE: policy-history restore affordance coverage (2026-09-21)

Added a real `AssetWorkspace` Storybook workflow for an immutable published
revision. It selects the revision's restore action and verifies the production
component invokes the owning `onRestore(4)` callback; mutation and reconciliation
remain outside Storybook. This complements the semantic diff and stale-link
proofs without claiming a backend restore transaction.

Proof: `pnpm check`, `pnpm test:stories` (45 browser tests), and
`pnpm build-storybook` pass. Atomic implementation commit: `57238433 test(ui):
cover policy history restore`. R07 remains ACTIVE pending the real
author/reviewer/publisher, restore, and process-race journey.

## E03/R02 — ACTIVE: HTML edge authentication challenge recovery (2026-09-21)

The API boundary now treats redirected responses, HTML responses, and followed
SSO login URLs as authentication challenges before attempting JSON parsing. It
dispatches the existing auth-expired event, clears the private workspace through
the normal recovery path, and exposes a specific sign-in-again message. This
prevents an edge or identity-provider login document from being accepted as a
successful API response. Unit coverage verifies the recovery wording, and the
built shell journey serves a `200 text/html` challenge for `/v1/session` and
proves the UI remains signed out without demo data or authenticated asset
controls.

Proof: `tsc -b`, the UI node suite (20 tests), Storybook browser tests (45
tests), production build plus budget gate (162.98 KiB JS gzip, 38.01 KiB CSS
gzip, 83.72 KiB fonts), and the full built-browser suite (17 tests) pass.
`pnpm` itself did not return in this runner, so equivalent direct binaries were
used for the TypeScript, Vitest, Vite, and Playwright checks. Atomic
implementation commits: `d80327fd fix(ui): fail closed on html auth challenges`
and `74b07f8e fix(ui): preserve auth challenge recovery state`. The follow-up
keeps the actionable edge-session message visible after private state is cleared.
Follow-up browser matrix `192e4ca4 test(ui): cover forbidden and redirected edge
challenges` adds 403-HTML and followed-redirect cases; the full built-browser
suite now passes 19 tests. Mutation coverage `22caa646 test(ui): prove mutation
challenges clear private state` exercises a challenged deny-all draft save and
the full built-browser suite now passes 20 tests.
The shared challenge fixture contract is explicit in `cd742c6e
refactor(ui-tests): type edge challenge fixture`; TypeScript and the focused
four-case edge/mutation matrix pass.
E03/R02 remains ACTIVE pending live local/Cloudflare SSO allow/deny and
re-authentication evidence, plus the production deployment qualification.

## R06 — ACTIVE: deferred policy-history response race (2026-09-21)

Extended the authenticated browser boundary with a held Published Changes
history response. The test navigates from Changes to Activity and back to
Changes, verifies the fresh history entry, then releases the original response
and proves the stale entry cannot replace the current page. This complements
the existing Activity audit race and exercises the same epoch, cancellation, and
session-scoped query ownership through a second management resource.

Proof: `tsc -b`, the focused history-race browser test, and the full built
browser suite (21 tests) pass. Atomic implementation commit: `5b954e62
test(ui): cover deferred history responses`. R06 remains ACTIVE pending the
broader parameterized workflow matrix and live identity/provider qualification.

## R02/R06/R08 — ACTIVE: reader connection deep-link guard (2026-09-21)

Added the companion built-browser proof for a reader opening `#connections`.
The application redirects to Assets before issuing catalog, plugin, publication,
or settings requests. This complements the existing `#settings` proof and
keeps the server-side capability check authoritative for both administrator
routes.

Proof: TypeScript and the full built-browser suite (22 tests) pass; the focused
reader deep-link matrix passes 2/2. Atomic implementation commit: `24e1ea4c
test(ui): cover reader connection deep links`. R02/R06/R08 remain ACTIVE
pending live identity qualification and authorized backend management journeys.

## R08 — ACTIVE: administrator management route composition (2026-09-21)

Added a capability-scoped authenticated browser fixture and exercised the real
application route composition for an administrator. Connections loads its
empty-state management view, then Settings loads runtime and identity state;
the fixture exposes `workspace:admin` and returns only synthetic, non-secret
management data. This proves positive UI capability presentation and route
loading, while live backend authorization and mutation effects remain separate
acceptance gates.

Proof: TypeScript, the focused administrator journey, and the full built-browser
suite (23 tests) pass. Atomic implementation commit: `08598994 test(ui): cover
administrator management routes`. R08 remains ACTIVE pending live settings,
access, audit, and authorized backend mutation evidence.

## R04/R06 — ACTIVE: authenticated shell accessibility scan (2026-09-21)

Added an axe scan after the real built application loads an authenticated reader
asset workspace. Serious and critical violations fail the browser test, covering
the populated shell state that the signed-out scan cannot exercise.

Proof: TypeScript, the focused authenticated axe test, and the full built-browser
suite (24 tests) pass. Atomic implementation commit: `440856a7 test(ui): scan
authenticated shell accessibility`. R04/R06 remain ACTIVE pending manual
screen-reader/visual review, contrast and zoom review, and live identity
qualification. The Storybook browser regression suite remains green at 45
tests after this shell coverage was added.

Follow-up `5783b78e test(ui): scan administrator settings accessibility` adds
axe coverage for the capability-gated Settings form; the full built-browser
suite remains green at 24 tests. Automated checks do not replace manual
screen-reader, visual, contrast, or owner review.

## R06 — ACTIVE: deferred Settings response race (2026-09-21)

Extended the authenticated browser boundary with a held administrator runtime
settings response. The test navigates from Settings to Assets and back to
Settings, verifies the fresh ticket TTL, then releases the original response
and proves the fresh value remains. This complements the Activity and Changes
race proofs across three management resources.

Proof: TypeScript, the focused Settings-race browser test, and the full built
browser suite (25 tests) pass. Atomic implementation commit: `ed83e65e
test(ui): cover deferred settings responses`. R06 remains ACTIVE pending the
remaining parameterized workflow matrix and live identity/provider qualification.

## R06 — ACTIVE: deferred Connections response race (2026-09-21)

Extended the authenticated administrator boundary with a held catalog response.
The test navigates from Connections to Assets and back, verifies the fresh
catalog card, then releases the original response and proves the stale card
cannot replace the current page. Activity, Changes, Settings, and Connections
now each have explicit deferred-response coverage.

Proof: TypeScript, the focused Connections-race browser test, and the full built
browser suite (26 tests) pass. Atomic implementation commit: `db290e8a
test(ui): cover deferred connection responses`. R06 remains ACTIVE pending the
remaining parameterized workflow matrix and live identity/provider qualification.

## R06 — ACTIVE: deferred asset lookup response race (2026-09-21)

Added a held asset-inventory search response. The test starts a stale lookup,
navigates away and back, issues a newer lookup, verifies the fresh inventory,
then releases the stale response and proves it cannot replace the current
selection. This found and fixed a real state-ownership defect: management-route
loads replaced asset-scoped access metadata, so returning to the still-mounted
editor temporarily became read-only. Management results now merge into shared
state while preserving asset access, grants, and history metadata.

Proof: `tsc -b`, the focused lookup-race browser test, the full built-browser
suite (27 tests), the UI node suite (20 tests), Storybook browser tests (45
tests), production build, and bundle-budget gate pass. Measured budgets are
163.03 KiB JS gzip, 38.01 KiB CSS gzip, and 83.72 KiB fonts. Atomic
implementation commit: `09a828b2 fix(ui): preserve asset access across
management navigation`. `pnpm` remains unavailable in this runner; direct
tool binaries provide equivalent evidence. R06 remains ACTIVE pending the
remaining load/save/evaluate/review/publish/restore matrix, live identity and
provider qualification, and production process-race evidence.

## R06/R07 — ACTIVE: deferred publish response race (2026-09-21)

Added a publisher-capable synthetic fixture and held publication response. The
browser completes server review, starts publish, edits locally, then releases
the old response. The new draft remains unsaved; stale publication success does
not apply, and rule edits clear review authority so the old token cannot be
reused. This closes the local save/evaluate/review/publish/lookup/restore race
matrix for the policy authoring path.

Proof: `tsc -b`, the focused publish-race browser test, the full built browser
suite (33 tests), the UI node suite (20 tests), Storybook browser tests (45
tests), production build, and bundle-budget gate pass. Measured budgets are
163.03 KiB JS gzip, 38.01 KiB CSS gzip, and 83.72 KiB fonts. Atomic
implementation commit: `9c968683 fix(ui): invalidate review authority on new
edits`. R06/R07 remain ACTIVE pending live author/reviewer/publisher identity,
backend publication, real consumer, manual owner, and production process-race
qualification.

## R06 — ACTIVE: deferred policy-review response race (2026-09-21)

Added a held server review response. The browser requests publish authority,
edits the draft while review is pending, then releases the old response. The
newer draft remains unsaved and no stale review-current message or authority is
attached to it.

Proof: `tsc -b`, the focused review-race browser test, the full built browser
suite (32 tests), the UI node suite (20 tests), Storybook browser tests (45
tests), production build, and bundle-budget gate pass. Measured budgets remain
163.03 KiB JS gzip, 38.01 KiB CSS gzip, and 83.72 KiB fonts. Atomic
implementation commit: `484a1757 test(ui): cover deferred policy reviews`.
R06 remains ACTIVE pending publish races, live identity and provider
qualification, and production process-race evidence.

## R06 — ACTIVE: deferred policy-restore response race (2026-09-21)

Added a held restore-to-draft response. The browser starts restoring an
immutable revision, switches back to Policy, creates a newer local rule, then
releases the restore response. The editor remains unsaved and never applies
the stale restored rules or success notice.

Proof: `tsc -b`, the focused restore-race browser test, the full built browser
suite (31 tests), the UI node suite (20 tests), Storybook browser tests (45
tests), production build, and bundle-budget gate pass. Measured budgets remain
163.03 KiB JS gzip, 38.01 KiB CSS gzip, and 83.72 KiB fonts. Atomic
implementation commit: `b98c74f1 test(ui): cover deferred policy restores`.
R06 remains ACTIVE pending review/publish races, live identity and provider
qualification, and production process-race evidence.

## R06 — ACTIVE: deferred policy-evaluation response race (2026-09-21)

Added a held server-side policy evaluation. The browser starts the evaluation,
adds a new local rule while it is pending, then releases the old response. The
newer draft stays unsaved and no stale preview or completion notice appears.
This covers evaluation ownership across a draft edit-epoch change.

Proof: `tsc -b`, the focused evaluation-race browser test, the full built
browser suite (30 tests), the UI node suite (20 tests), Storybook browser tests
(45 tests), production build, and bundle-budget gate pass. Measured budgets
remain 163.03 KiB JS gzip, 38.01 KiB CSS gzip, and 83.72 KiB fonts. Atomic
implementation commit: `ff34d4bc test(ui): cover deferred policy evaluations`.
R06 remains ACTIVE pending review/publish/restore races, live identity and
provider qualification, and production process-race evidence.

## R06 — ACTIVE: deferred policy-version detail lookup race (2026-09-21)

Added two synthetic immutable revisions to the authenticated asset workflow and
held revision 1 detail while selecting revision 2. The browser proof confirms
the selected revision 2 snapshot remains visible after the late revision 1
response is released; stale rules never replace the newer detail panel.

Proof: `tsc -b`, the focused policy-version lookup browser test, the full built
browser suite (28 tests), the UI node suite (20 tests), Storybook browser tests
(45 tests), production build, and bundle-budget gate pass. Measured budgets
remain 163.03 KiB JS gzip, 38.01 KiB CSS gzip, and 83.72 KiB fonts. Atomic
implementation commit: `0f129afd test(ui): cover deferred policy version
lookups`. R06 remains ACTIVE pending the remaining load/save/evaluate/review/
publish/restore matrix, live identity and provider qualification, and
production process-race evidence.

## R06 — ACTIVE: deferred draft-save response race (2026-09-21)

Added a held draft-save response. The browser starts saving the deny-all draft,
adds a new rule while the request is pending, then releases the old response.
The editor remains unsaved, retains the newer rule editor, and never reports
the stale save as current. This covers mutation ownership across a draft
identity/edit-epoch change.

Proof: `tsc -b`, the focused draft-save browser test, the full built browser
suite (29 tests), the UI node suite (20 tests), Storybook browser tests (45
tests), production build, and bundle-budget gate pass. Measured budgets remain
163.03 KiB JS gzip, 38.01 KiB CSS gzip, and 83.72 KiB fonts. Atomic
implementation commit: `7eb0e6cf test(ui): cover deferred draft saves`. R06
remains ACTIVE pending evaluate/review/publish/restore races, live identity and
provider qualification, and production process-race evidence.

## R08 — ACTIVE: authenticated access mutation journey (2026-09-21)

Added a capability-gated browser fixture for the production access workspace.
The administrator journey edits owner principals, saves them, adds a delegated
grant, and saves the grant through typed owner APIs. This proves rendered
control wiring, capability presentation, mutation feedback, and reload
composition in the built app; the fixture remains synthetic and does not prove
live backend authorization or persistence.

Proof: `tsc -b`, the focused access-mutation browser test, the full built browser
suite (34 tests), the UI node suite (20 tests), Storybook browser tests (45
tests), production build, and bundle-budget gate pass. Measured budgets remain
163.03 KiB JS gzip, 38.01 KiB CSS gzip, and 83.72 KiB fonts. Atomic
implementation commit: `31a37305 test(ui): cover administrator access
mutations`. R08 remains ACTIVE pending live settings/access/audit journeys,
backend authorization effects, and production qualification.

## R08 — ACTIVE: authenticated runtime-settings mutation journey (2026-09-21)

Added a configured administrator settings fixture and exercised the production
runtime form through a staged TTL update. The journey exposed a real refresh
ownership bug: `ManagementView` unmounted loaded children whenever a reload
started, dropping success feedback. Loaded management children now remain
mounted during refresh; initial loads still show the loading state.

Proof: `tsc -b`, the focused settings-mutation browser test, the full built
browser suite (35 tests), the UI node suite (20 tests), Storybook browser tests
(45 tests), production build, and bundle-budget gate pass. Measured budgets are
163.10 KiB JS gzip, 38.01 KiB CSS gzip, and 83.72 KiB fonts. Atomic
implementation commit: `c528c944 fix(ui): preserve management views during
refresh`. R08 remains ACTIVE pending live settings/access/audit requests,
backend authorization effects, and production qualification.

## R08 — ACTIVE: admitted catalog workflow journey (2026-09-21)

Added a typed admitted catalog/table-format fixture and exercised the production
Connections view through catalog save, connection diagnostics, and table
discovery. The journey uses scoped secret-reference input and plugin-pair
metadata; it proves rendered control wiring and bounded discovery behavior while
remaining synthetic and credential-free.

Proof: `tsc -b`, the focused catalog-workflow browser test, the full built
browser suite (36 tests), the UI node suite (20 tests), Storybook browser tests
(45 tests), production build, and bundle-budget gate pass. Measured budgets are
163.10 KiB JS gzip, 38.01 KiB CSS gzip, and 83.72 KiB fonts. Atomic
implementation commit: `81462532 test(ui): cover admitted catalog workflow`.
R08 remains ACTIVE pending live catalog credentials, backend mutation effects,
publication activation, and production qualification.

## R02/R03/R08/R10 — ACTIVE: local backend regression qualification (2026-09-21)

Re-ran the repository's local end-to-end smoke, control-plane authentication,
UI shell, secure-local parity, secure-local architecture, and named-tunnel
architecture suites. The full Python suite also completed successfully; skipped
cases remain reported by pytest and are not treated as proof of live providers,
Cloudflare Access, PostgreSQL process races, or consumer qualification.

Proof: `uv run pytest tests/test_e2e_smoke.py -q` passes; the focused auth/local
profile selection passes; and `uv run pytest -q` completes at 100% with exit 0.
This strengthens local contract evidence only. R02/R03/R08/R10 remain ACTIVE
pending real IdP/edge allow-deny, live backend mutation, PostgreSQL/process,
consumer, and production operations evidence.

## R08 — ACTIVE: workspace publication lifecycle journey (2026-09-21)

Added a stateful administrator fixture for workspace publication generations and
exercised the production Connections view through snapshot creation and explicit
activation. The journey verifies the staged generation is rendered after create,
the activation request carries the current-generation precondition, the new
generation becomes the sole serving snapshot, and the previous generation stays
available as staged history. The fixture is synthetic and does not prove live
publication persistence, authorization, data-plane reload, or process safety.

Proof: `tsc -b`, the focused publication-lifecycle browser test, the full built
browser suite (37 tests), the UI node suite (20 tests), Storybook browser tests
(45 tests), production build, and bundle-budget gate pass. Measured budgets are
163.10 KiB JS gzip, 38.01 KiB CSS gzip, and 83.72 KiB fonts. Atomic
implementation commit: `33350015 test(ui): cover workspace publication
lifecycle`. R08 remains ACTIVE pending live catalog/settings/access/audit and
publication effects, real plugin lifecycle qualification, and production
operations evidence.

## R08 — ACTIVE: admitted plugin lifecycle journey (2026-09-21)

Added a stateful plugin fixture and exercised the production Connections view
through an administrator disabling a catalog adapter, revoking a table-format
adapter, and confirming its retirement. The journey verifies lifecycle options
follow the admitted state machine, every transition issues the typed PATCH
request, refreshed cards show the returned state, and retirement requires an
explicit confirmation. This remains synthetic browser evidence; it does not
prove live plugin registry persistence, in-flight drain behavior, catalog
retirement safety, or production process coordination.

Proof: `tsc -b`, the focused plugin-lifecycle browser test, the full built
browser suite (38 tests), the UI node suite (20 tests), Storybook browser tests
(45 tests), production build, and bundle-budget gate pass. Measured budgets are
163.10 KiB JS gzip, 38.01 KiB CSS gzip, and 83.72 KiB fonts. Atomic
implementation commit: `8b8d830c test(ui): cover plugin lifecycle
management`. R08 remains ACTIVE pending live settings/access/audit and
publication effects, real plugin registry qualification, and production
operations evidence.

## R07 — ACTIVE: successful publisher policy journey (2026-09-21)

Added a successful-path browser journey for a publisher-capable session. The
production AssetWorkspace saves the deny-all draft, obtains server review for
the saved revision, and publishes only after the review token is current. This
is synthetic control-plane evidence and does not qualify independent author and
reviewer identities, cross-process revision races, or a live data-plane read of
the resulting publication.

Proof: the focused publisher browser test passes against the built shell, and
the existing deferred save/review/publish race tests remain green. Atomic
implementation commit: `51004419 test(ui): cover publisher policy journey`.
R07 remains ACTIVE pending live author/reviewer/publisher qualification,
publication reconciliation, consumer reads, and production process-race
evidence.

## R07 — ACTIVE: review-only publisher link journey (2026-09-21)

Added a built-browser journey for an exact saved-draft review link. The
publisher can inspect the saved revision while asset navigation and authoring
remain disabled, then can complete server review and publish only with the
session's explicit publish capability. This remains synthetic and does not
qualify separate live author/reviewer identities, cross-process revisions, or a
live data-plane read.

Proof: the focused review-only publisher browser test passes against the built
shell. Atomic implementation commit: `a8eac89e test(ui): cover review-only
publisher links`. R07 remains ACTIVE pending live author/reviewer/publisher
qualification, publication reconciliation, consumer reads, and production
process-race evidence.

## R08 — ACTIVE: audit filtering and cursor journey (2026-09-21)

Added a stateful audit fixture and exercised the production Activity view through
an action filter followed by cursor pagination. The journey verifies the first
page contains only the requested action, unrelated records are absent, and the
next permitted page appends without replacing the filtered result set. This is
synthetic browser evidence and does not qualify live audit authorization,
retention, redaction, or database cursor behavior.

Proof: the focused audit-filter browser test passes against the built shell;
existing Activity loading/error/empty and deferred-response Storybook cases
remain green. Atomic implementation commit: `aa0bf254 test(ui): cover audit
filtering and pagination`. R08 remains ACTIVE pending live settings/access/
audit and publication effects, real plugin registry qualification, and
production operations evidence.

## R06/R08 — ACTIVE: stale catalog discovery race (2026-09-21)

Added a held table-discovery response and exercised the production Connections
view across navigation. The browser starts discovery, leaves the management
page, performs a fresh discovery after returning, and then releases the older
response. The fresh table inventory remains visible and the stale table never
replaces it, covering the existing discovery epoch and management-query
cancellation boundary. The fixture is synthetic and does not qualify live
catalog latency, cancellation, or backend authorization.

Proof: TypeScript, the focused stale-discovery browser test, the full built
browser suite (41 tests), the UI node suite (20 tests), Storybook browser tests
(45 tests), production build, and bundle-budget gate pass. Measured budgets are
163.10 KiB JS gzip, 38.01 KiB CSS gzip, and 83.72 KiB fonts. Atomic
implementation commit: `bc1928c4 test(ui): cover stale catalog discovery`.
R06/R08 remain ACTIVE pending the remaining live identity, backend mutation,
plugin, consumer, and production process evidence.

## R10 — VERIFY: local consumer qualification lane (2026-09-21)

Executed the opt-in local consumer suites instead of counting their default
skips as proof. The Python lane passed four nested-data tests covering the
Python/Arrow SDK, DuckDB reader, SQL Iceberg, manifest/Parquet, and REST Iceberg
paths. The JVM reactor passed the Spark integration suite (6 tests), Java Flight
client suite (7), Spark datasource suite (29), and connector testkit suite (2),
with zero failures, errors, or skips. These are loopback/local fixtures with
local authentication and do not qualify E15's TLS/OIDC, two-process PostgreSQL
publication races, exact installed plugin artifacts, or every advertised
consumer pair.

Proof: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache
DAL_OBSCURA_RUN_CONSUMER_TESTS=1 uv run pytest
tests/consumers/test_governed_reads.py -q` reports 4 passed; `mvn -f
connectors/jvm/pom.xml verify` reports all reactor tests green. R10 remains
VERIFY pending the real boundary matrix and production process evidence.

## R11 — VERIFY: local performance baselines (2026-09-21)

Executed the repository-owned focused benchmark lanes. The nested ticket-to-
response benchmark averaged 40.4818 ms over 23 rounds; row-filter and masking
benchmarks averaged 5.2612 ms (row filter only), 8.8987 ms (nested masks),
10.1326 ms (top-level masks), and 15.2330 ms (mask only); the multi-file
Iceberg fixture averaged 2.4605 ms over 365 rounds. These measurements are
repeatable local fixture baselines, not production capacity, LCP, p95 tree
interaction, encrypted-PostgreSQL, or 25-million-row release evidence.

Proof: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run pytest
tests/benchmarks/test_masking_row_filter_benchmarks.py
tests/benchmarks/test_iceberg_multifile_benchmark.py --benchmark-only -q` and
the focused complex-schema ticket benchmark both pass. R11 remains VERIFY
pending the complete capacity, restore, provenance, and release lanes.

## R06/R08 — ACTIVE: management refresh error retention (2026-09-21)

Fixed the production management shell so a refresh failure does not discard a
successfully loaded page. Connections, activity, settings, and changes retain
their current state and show an actionable recovery alert with a retry control;
initial load failures still use the full-page recovery path. This protects
operator context during transient control-plane outages while keeping the
existing fail-closed entry behavior.

Proof: the focused refresh-failure browser test and the full built browser
suite (43 tests), TypeScript, UI node suite (20 tests), Storybook browser tests
(45 tests), production build, and bundle-budget gate pass. Measured budgets are
163.18 KiB JS gzip, 38.01 KiB CSS gzip, and 83.72 KiB fonts. Atomic
implementation commit: `e4b068ca fix(ui): preserve management state on refresh
errors`. R06/R08 remain ACTIVE pending live identity, backend mutation,
authorization, plugin, consumer, and production process evidence.

## R05/R06 — ACTIVE: refresh-error designbook contract (2026-09-21)

Added a production-component Storybook play case for the management refresh
failure state. The story keeps the loaded audit view visible, exposes the
server error as an alert, and verifies that the retry action is wired to the
owning reload callback. This documents the state in the executable designbook
without introducing a Storybook-only component.

Proof: the complete Storybook behavior suite reports 46 passed tests. Atomic
implementation commit: `1430ab62 test(ui): document refresh error retention`.
R05/R06 remain ACTIVE pending the remaining live workflow, accessibility,
identity, and production evidence.

## R06/R11 — ACTIVE: route-level UI code splitting (2026-09-21)

Moved the nested policy workspace and management views behind React lazy
boundaries with accessible loading states. The authenticated shell now loads a
smaller initial entry while preserving the existing application components and
route behavior; route chunks remain same-origin production assets under the
existing CSP.

Proof: the production build reports a 147.46 KiB gzip initial entry,
10.94 KiB policy-workspace chunk, and 11.07 KiB management chunk; the summed
JavaScript budget remains 165.50 KiB gzip under the 200 KiB limit. TypeScript,
43 built-browser tests, 46 Storybook behavior tests, and 20 Node tests pass.
Atomic implementation commit: `2b54edb5 perf(ui): split policy and management
routes`. R06/R11 remain ACTIVE pending LCP/CLS measurement, complete capacity
evidence, and the remaining live identity and production gates.

## R05 — ACTIVE: designbook guidance synchronization (2026-09-21)

Corrected the contribution guide so it reflects the implemented preview network
guard. The guide now treats the guard as enforced and keeps reviewed visual
baselines as the remaining designbook evidence; it no longer tells contributors
to wait for a boundary that already exists.

Proof: `storybook build --disable-telemetry` completes successfully after the
documentation change. Atomic implementation commit: `1c8800db docs(ui): align
designbook network guidance`. R05 remains ACTIVE pending the contrast matrix,
responsive visual review, manual keyboard/screen-reader/zoom checks, and owner
approval.

## R11 — VERIFY: local browser web-vitals baseline (2026-09-21)

Added `measure:web-vitals`, a Playwright-backed command that measures LCP, CLS,
and navigation timing against the built preview and fails when LCP is absent or
CLS exceeds the E16 threshold. The local 1440×900 signed-out fixture reports
LCP 100 ms, CLS 0.00094, and navigation 56 ms. These are repeatable local
browser observations only; they do not qualify authenticated LCP, network
latency, production capacity, or the complete E16/E17 release lane.

Proof: `./node_modules/.bin/tsc -b --pretty false && node
scripts/measure-web-vitals.mjs` passes against the built preview. Atomic
implementation commit: `ae7eba5c test(ui): add web vitals measurement command`.
R11 remains VERIFY pending authenticated and fixed-runner performance evidence,
restore/provenance checks, and the remaining live gates.

## R04 — VERIFY: rendered light/dark contrast check (2026-09-21)

Added a built-browser contrast assertion over the actual login shell and the
authenticated nested policy workspace. It walks rendered headings, supporting
text, buttons, inputs, selects, and textareas in both light and dark schemes,
resolves transparent elements to their rendered ancestor background, and
enforces E09's 4.5:1 text threshold. This complements the existing axe and
theme-state checks without claiming manual screen-reader or visual approval.

Proof: the complete built browser suite reports 45 passed tests, including the
signed-out and authenticated contrast cases; TypeScript also passes. Atomic
implementation commits: `b9d58b92 test(ui): verify rendered theme contrast`
and `8f7c5c1f test(ui): cover authenticated theme contrast`. R04 remains VERIFY
pending the broader authenticated contrast matrix, responsive/zoom/forced-colors
manual review, and owner approval.

## R04/R06 — VERIFY: authenticated responsive bounds (2026-09-21)

Added a built-browser assertion over the real policy workspace at 390×844,
768×1024, and 1440×900, followed by 200% browser zoom. The check verifies the
document itself does not overflow the viewport while preserving named internal
scroll regions. It complements the mobile navigation, signed-out viewport, and
forced-colors checks; it is not a substitute for manual screen-reader or visual
review.

Proof: the complete built browser suite reports 46 passed tests and TypeScript
passes. Atomic implementation commit: `9f3222db test(ui): cover authenticated
responsive bounds`. R04/R06 remain VERIFY pending manual contrast, keyboard,
screen-reader, visual, and owner review plus live identity qualification.

## R04/R08 — VERIFY: authenticated management contrast matrix (2026-09-21)

Extended the rendered contrast oracle to the production activity, published
history, catalog connections, and runtime/identity settings screens. The matrix
runs each screen in explicit light and dark themes and checks its actual
headings, copy, buttons, inputs, selects, and textareas against the 4.5:1 text
threshold. It uses synthetic management data and does not qualify live
authorization or manual review.

Proof: the complete built browser suite reports 50 passed tests and TypeScript
passes; each management screen/theme pair also completes a serious/critical
axe scan. Atomic implementation commits: `7642fe07 test(ui): cover management
theme contrast` and `219197b2 test(ui): scan management accessibility matrix`.
R04/R08 remain VERIFY pending live management authorization, manual contrast/
keyboard/screen-reader review, and owner approval.

## R04/R06/R08 — ACTIVE: administrator tablet overflow fix (2026-09-21)

The new administrator responsive check found a real 768 px defect: the
activity metric grid required five fixed minimum columns and filter inputs kept
their native 210 px minimum, producing 23 px of page overflow. Production CSS
now uses auto-fitting metric columns and zero minimum widths for form labels and
inputs. The browser regression covers activity, history, connections, and
settings at 390×844, 768×1024, and 1440×900, plus the activity screen at 200%
zoom. It now exercises all four management screens at 200% as well.

Proof: the focused regression and complete built browser suite (49 tests), plus
TypeScript, the Storybook behavior suite (46 tests), Node tests (20), and the
production build/budget gate pass after the fix. The build measures 147.46 KiB
initial JS gzip, 165.50 KiB total JS gzip, 38.03 KiB CSS gzip, and 83.72 KiB
fonts. Atomic implementation commits: `fa0c65a0 fix(ui): prevent management
grid overflow` and `7976ca26 test(ui): cover management zoom matrix`.
The static Storybook build also completes successfully. R04/R06/R08 remain
ACTIVE pending manual visual/keyboard/screen-reader review and live
authorization evidence.

## R06/R11 — VERIFY: authenticated local web-vitals check (2026-09-21)

Added a built-browser LCP/CLS assertion for the authenticated nested policy
workspace using the existing synthetic API boundary. The test waits for the
production route and self-hosted fonts, requires an LCP event, and enforces
LCP ≤2.5 seconds and CLS ≤0.1. It is deliberately scoped to local fixture
evidence and does not replace real-network, fixed-runner, or production tests.

Proof: the complete built browser suite reports 47 passed tests and TypeScript
passes. Atomic implementation commit: `15e716d9 test(ui): measure authenticated
web vitals`. R06/R11 remain VERIFY pending authenticated fixed-runner LCP/CLS,
capacity, restore/provenance, and live identity evidence.

## R06/R08 — VERIFY: administrator management keyboard order (2026-09-21)

Added a production browser focus regression for the administrator Activity and
Settings screens. It focuses the first audit filter and runtime limit, advances
with Tab, and verifies the next labeled controls receive focus in document
order. This supplements the command-palette and mobile-navigation focus tests;
it does not replace manual keyboard or screen-reader review.

Proof: the complete built browser suite reports 50 passed tests and TypeScript
passes. Atomic implementation commit: `815de8b5 test(ui): verify management
keyboard order`. R06/R08 remain VERIFY pending broader manual keyboard and
screen-reader review plus live authorization evidence.

## R04/R06/R08 — VERIFY: authenticated forced-colors coverage (2026-09-22)

Extended the built browser accessibility lane beyond the signed-out shell. The
authenticated policy workspace now verifies focus visibility in forced-colors
mode, while Activity, Connections, and Settings verify discoverable headings and
no page-level overflow at the same media setting. This is rendered local
coverage only; it does not replace manual operating-system review or live
identity qualification.

Proof: the focused forced-colors check and the complete built browser suite
(51 tests) pass; TypeScript also passes. Atomic implementation commit:
`831c9be2 test(ui): cover authenticated forced colors`. R04/R06/R08 remain
VERIFY pending manual forced-colors/keyboard/screen-reader review, owner
approval, and live management authorization.

## R04/R06 — ACTIVE: named scroll regions for data tables (2026-09-22)

Named every production horizontally scrollable table wrapper in policy history,
published history, workspace generations, discovered catalog tables, and
immutable rule details. Assistive technology can now identify the bounded
scrolling context instead of encountering an unnamed overflow container.

Proof: the focused authenticated management contrast/axe check and TypeScript
pass after the markup change. Atomic implementation commit: `79f090e3
fix(ui): name scrollable data regions`. R04/R06 remain ACTIVE pending the full
browser matrix, manual screen-reader review, and live workflow evidence.

Follow-up axe review found duplicate landmark names when policy history and its
immutable rule details were shown together. The regions now have distinct
accessible names, and the complete Storybook behavior suite reports 46 passed
tests. Atomic correction commit: `16cfcb9f fix(ui): disambiguate table
landmarks`.

## R08 — ACTIVE: discovered table identity preservation (2026-09-22)

Hardened the catalog workflow to accept the canonical `table_identifier` and
common `identifier`/`target` aliases from admitted discovery adapters. Governing
now sends the complete stable identifier to the control plane and uses it for
row keys, avoiding namespace truncation and duplicate-name reconciliation bugs.

Proof: the focused administrator catalog journey captures the PUT request and
asserts `table_identifier: demo.orders`; TypeScript passes. Atomic implementation
commit: `38225652 fix(ui): preserve discovered table identifiers`. R08 remains
ACTIVE pending live catalog credentials, backend mutation effects, and the full
administration qualification journey.

Follow-up verification: the complete built browser suite remains green at 51
tests after this change.

## R08 — VERIFY: explicit admitted table-format selection (2026-09-22)

Added a capability-scoped administrator journey where one catalog advertises two
admitted table formats. The UI refuses to govern until the operator chooses a
format, then sends the selected plugin id and complete table identifier to the
control plane. This preserves the plugin-pair boundary instead of guessing from
the catalog or display name.

Proof: the focused built browser journey passes and asserts the selected Delta
backend in the governing PUT request; TypeScript passes. Atomic implementation
commit: `7573e0e9 test(ui): qualify table format selection`. R08 remains VERIFY
pending live plugin-pair persistence, backend mutation effects, and the complete
administration qualification journey.

## R06/R07/R11 — VERIFY: large nested schema tree qualification (2026-09-22)

Added a built application regression with 10,000 authoritative scalar fields.
The real virtual schema tree stays at or below 200 mounted rows and its Arrow-
style keyboard navigation remains below the 200 ms E16 p95 interaction budget
over 20 movements. The test uses the production tree and focus path; it does
not replace manual screen-reader review or fixed-runner performance evidence.

Proof: the focused browser test passes in 3.0 seconds. Atomic implementation
commit: `04995eed test(ui): qualify large schema tree performance`. R06/R07/R11
remain VERIFY pending fixed-runner measurements, manual accessibility review,
and the live authoring workflow.
