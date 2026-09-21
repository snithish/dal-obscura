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
Rendered interleaving coverage remains open. No authentication or pickle changes.

Validation: all 16 existing UI unit tests and TypeScript check pass on bundled
Node 24.19.0. Initial collision-only validation also passed on Node 26.8.2.
Commit identities are available from this file's history.

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
