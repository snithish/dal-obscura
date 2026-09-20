# Implementation review

Reviewed 2026-09-20 at `eed27772`, against the N01–N16 plan last revised at
`4869bf2`. Git reports 222 changed files between those commits. The review read
source, tests, deployment configuration and the progress ledger. It did not run
the application suite, benchmarks or an authenticated UI. No UI server answered
on 127.0.0.1:5173 and no application browser tab was available. Visual findings
below are source-based and incorporate the owner's direct assessment.

## What to retain

The implementation now has meaningful foundations: SDK-owned contracts, stricter
plugin admission, exact federated identity helpers, revisioned mutation contracts,
referenced drafts, paginated audit queries, OIDC hardening, split UI components,
Lucide icons, theme tokens, query caching, a condition editor and undo/redo.
Rebuilding these would add risk and duplicated code. The remaining work is
integration, visual coherence, actual workflow tests and operational qualification.

The historical ledger reports 911 passing Python tests with 13 explicit skips
at its final full run, local Python/DuckDB reads through real catalog fixtures,
a JVM fixture run and isolated package/build checks. These are prior claims,
not fresh results from this review. They do not establish the complete real
TLS/OIDC/Spark/plugin matrix, two-process publication behavior or timed recovery.
Retrieve exact historical evidence with:

`git show eed27772:docs/plugin-platform/STATUS.md`

## N01–N16 reconciliation

- **N01:** Python/Node metadata, baseline inventory, narrowed pre-commit paths and
  pinned UI packages exist. Remove their creation tasks. R01 verifies exact
  supported toolchains/advisories; R11 retains artifact qualification.
- **N02:** SDK imports, strict lock/admission paths, retired demo/password and old
  policy endpoints, paginated-only history/audit helpers and several dead helpers
  have been addressed. Remove blanket rewrite tasks. R01 performs a focused
  remaining-caller inventory; R06/R07 remove residual UI machinery and fallbacks.
- **N03:** descriptors declare output formats/handle versions; routes use DTOs and
  draft references, and generated TypeScript exists. Remove initial construction.
  R01 checks remaining contract edges; R07/R10 prove exact-reference workflows.
- **N04:** cancellation/cleanup, strict URI/secret validation and a real REST
  redirect test exist. Remove “validators only” as a blanket finding. R10 still
  owns hostile DNS/metadata/storage transport and actual cancellation coverage.
- **N05:** exact identity encoding, bounded identities, normal OIDC-first login,
  endpoint/redirect hardening and CSRF/session controls exist. Remove initial
  identity/session implementation. R02/R03/R09 own stable HTTPS/SSO integration,
  proxy trust and live parity; authority freshness remains a live gate.
- **N06:** AppShell, LoginPanel, Icon, CommandPalette, mobile account controls
  and theme selection exist. Remove scaffold creation. R04–R06 replace fragmented
  styling and custom focus behavior, add real design-system enforcement.
- **N07:** QueryClient and many captured-scope/duplicate-click fences exist.
  Remove “add query client” tasks. R06 consolidates ownership and tests rendered
  mutations; do not discard working guards until their replacement is proven.
- **N08:** rule duplication/reordering, six masks, conditions with validation,
  virtual tree and undo/redo exist. Remove their initial implementation. R07
  finishes a coherent lossless editor and removes fabricated schema fallback.
- **N09:** review-link and referenced-draft backend paths, operation lookup and
  history exist. R07 finishes exact-revision links, clear semantic diff and
  editor/publisher journeys; R10 proves their process races.
- **N10:** generic descriptor/config/lifecycle forms and discovery scope guards
  exist. R08 closes typed editing, selection and lifecycle usability against real APIs.
- **N11:** settings/access forms, consumer examples and database-scoped audit
  pagination/filtering exist. The old “audit backend missing” finding is closed.
  R08 handles consistency and full journeys; R11 retains load/recovery gates.
- **N12:** PostgreSQL tests still use ThreadPoolExecutor and repository sessions.
  R10 retains independent API-process races and kill/restart evidence.
- **N13:** local real-provider fixtures supplement the stub read. R10 retains
  clean-artifact, actual TLS/OIDC and all advertised Spark/JVM cells.
- **N14:** prose guards and broad documentation hooks were reduced; recorded
  capacity evidence exists. R05/R06 replace helper-heavy UI checks economically;
  R11 measures all required resource and latency gates.
- **N15:** deployment/backup/provenance scaffolding exists. Recovery test helpers
  still fake pg_dump/age in relevant tests. R11 retains real timed restore and
  candidate-bound image/consumer evidence.
- **N16:** independent security, owner visual acceptance and user study remain
  unverified. R12 owns them. Build success cannot close this packet.

## Actionable findings

### V01 — Visual tokens do not control the product (high UX priority)

`apps/governance-ui/src/styles.css` contains 91 distinct hex literals. Root
indigo tokens coexist with hardcoded green sidebar/buttons, warm editor surfaces
and duplicated dark overrides. Global h1/h2 use Georgia; body uses Avenir/Segoe,
while schema/status text drops to 10–12px. This explains inconsistency; installing
another theme selector will not solve it. R04/R06 must replace these competing
values with one semantic token and typography system.

At lines 170–173, `:root:not([data-theme="light"])` applies dark capability-card
colors outside a dark-media query. System mode under a light OS preference can
therefore receive those dark colors. This is a concrete theme-selection defect.

### V02 — No reusable component designbook or representative story tests (high UX priority)

No Storybook configuration or stories exist. Only the command palette has a CSS
module; core views still use the dense global stylesheet. Playwright's suite titled
“authenticated governance shell” exercises the signed-out shell and local token
form. It does not prove authenticated policy, review, settings or SSO workflows.
R05 introduces stories of actual production components and R07–R09 add a small
number of real-backend journeys. Retain useful existing checks, rename their scope honestly.

### V03 — Component extraction has not removed state complexity (medium)

`main.tsx` remains 938 lines; AssetWorkspace has a large inline prop contract.
QueryClient coexists with many manual state/epoch paths; lifecycle.ts still exposes
increment/equality helpers. R06 uses feature-owned hooks and explicit draft/mutation
state, removing old owners in the same slice. This is not a mandate for Redux,
a generic form engine or an arbitrary line-count target.

`query_scope.ts::sessionQueryScope` concatenates issuer and subject with an
unescaped delimiter. Unlike the corrected server identity helpers, distinct pairs
can produce the same key. Logout clears caches, which reduces exposure; this
review does not claim a demonstrated data leak. R06 uses structured query identity
plus a session generation and tests the collision/reauth interleavings.

### V04 — Palette accessibility is hand-maintained (medium)

CommandPalette manually traps Tab, renders a listbox of option buttons, and lacks
the complete selection/arrow-key contract. R05/R06 replace custom dialog focus
handling with one accessible primitive set and implement an appropriate combobox
pattern, or use ordinary navigation buttons without claiming listbox semantics.
Keep Escape, focus restoration and permission-filtered search.

### V05 — Exact draft and schema evidence can be lost in presentation (high)

`AssetWorkspace.tsx::copyReviewLink` includes asset/draft IDs but no expected
revision and silently ignores clipboard failure. Its schema fallback fabricates
field IDs from array indices when the canonical schema is absent. R07 requires
revision-bound links, truthful copy feedback and an unavailable-schema state.
Never invent selectable identities to make a failed load look usable.

### V06 — Cloudflare is not a drop-in identity or Flight transport (high integration priority)

`session_api.py::exchange_authorization_code` currently implements a public OIDC
client with PKCE, not confidential-client secret authentication. The app's login
rate key intentionally ignores forwarded headers; a tunnel puts many users behind
one peer. Existing NGINX sets forwarded scheme from its local connection. R02
must define trusted proxy/origin semantics instead of enabling arbitrary forwarded
headers. R03 adds a supported named-tunnel profile; R09 proves it live.

Quick Tunnel HTTPS does not itself configure application SSO. Cloudflare Access
can protect the edge while the current app performs its own OIDC/session checks.
Public tunnel hostname gRPC is unsupported, so Flight remains TLS local/private.
Research and exact decisions are in [LOCAL_ACCESS.md](LOCAL_ACCESS.md).

## Review outcome

Proceed with the existing backend and a disciplined presentation rebuild.
The strongest next investment is one polished vertical journey backed by shared
components and live OIDC, rather than more isolated helper fixes or a second UI.
R01–R12 is the remaining queue. All evidence must identify the exact candidate.
