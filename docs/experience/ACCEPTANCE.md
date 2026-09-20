# Acceptance specifications — implementation, local access and designbook

These are future executable test specifications, not executed tests. Owners are
[R01–R12](ACTION_PLAN.md). Baseline eed27772; reviewed 2026-09-20.

## Inherited obligations and evidence

Retain [G01–G05 and B01–B22](../plugin-platform/ACCEPTANCE.md) as security,
functional and release guarantees, with these explicit amendments:

- R packets replace N execution ownership. The reconciliation in REVIEW.md
  removes completed construction tasks, not unique regression coverage.
- DESIGN_SYSTEM.md replaces the old palette/type choices. Current supported
  AND/equals/one-of conditions must remain lossless; do not add an unapproved
  policy grammar merely to match an earlier hypothetical builder description.
- Storybook play tests replace duplicate component test scenarios where they own
  the same behavior. They do not replace live auth, database or consumer proof.
- E02–E07 add optional Cloudflare mode. All security/local-parity gates still run
  in local-only mode. Claiming Cloudflare support additionally requires E07.
- Code/runtime compatibility may deliberately break; protected pickle definitions,
  import paths and semantics may not. No new state service or multi-customer hosting.

Every PASS records candidate commit, command/test node, exit code, UTC date,
environment/tool versions, artifact/fixture hashes and durable redacted evidence.
Missing account/environment/reviewer = VERIFY. No silent skips, arbitrary sleeps,
old pass reuse, mocked provider qualification or automatic screenshot approval.

## E01 — Current contract and support inventory (R01)

Collect exact runtime/lock/image versions, test ownership/durations and code/
dependency/bundle baseline. Map every retained B/G case to an existing owner or
one planned boundary test. Confirm SDK single ownership, explicit format/handle
pair admission, mandatory write revisions, typed DTOs and retired route rejection.
Only actual remaining failures create implementation tasks. No unresolved
high/critical runtime advisory; support claims match tested versions.
Owner: existing package/API/plugin CI; no new prose-count tests.

## E02 — Same application security under local and named origins (R02)

Run the same app/session matrix under local HTTPS and a configured named HTTPS
origin. Real IdP code/PKCE login binds state, nonce, callback, issuer and audience;
bad/replayed values fail. Secure/HttpOnly/SameSite host-bound cookies, CSRF, logout,
privileged freshness and grant revocation follow B07/B08. No bootstrap or demo
login in the exposed profile. Local-only mode has no Cloudflare dependency.
Owner: existing session API cases plus E07's real browser journey.

## E03 — Proxy trust, rate attribution and challenge recovery (R02/R06)

Send spoofed Host/Forwarded/X-Forwarded-Proto/CF-Connecting-IP and Access identity
headers directly and through untrusted peers: no origin change, privilege or
rate-limit bypass. Only the configured gateway can supply sanitized client identity.
Two legitimate clients behind the tunnel retain separate login budgets; a bounded
aggregate limit still exists. Bad app TLS/CA/SNI prevents forwarding.

Inject Access 302/403 and HTML responses into metadata and mutation fetches:
UI shows edge-sign-in/unavailable, never data/success/empty defaults. Lost publish
response keeps the operation key; after explicit login reconcile before any new
mutation. No automatic replay. Owner: parameterized API and rendered story case;
real challenge behavior is exercised in E07.

## E04 — Named profile startup and lifecycle (R03)

With valid inputs, existing stack plus connector starts at one stable hostname.
Repeat start: no duplicate process or reseed. Restart connector: callback remains
valid. Stop: only owned processes stop; data remains. Doctor output is redacted and
distinguishes local checks from externally verified Access protection.

Missing Access audience, wildcard callback, bootstrap enabled, unknown hostname,
wrong CA/SNI, unexpected origin listener or missing credential must fail before
public ingress. Backend/DB/dev-server ports are not exposed. Connector cannot
forward a missing/wrong/expired Access assertion. Owner: profile contract/lifecycle
tests plus real E07. Never use noTLSVerify.

## E05 — Quick preview is explicitly restricted (R03)

Runbook and any optional preview command identify random URL, no SLA, 200 concurrent
request limit and no SSE. Preview serves synthetic static content only, with no
credentials or customer data. Do not claim Quick Tunnel SSO parity or stable OAuth
callback support. No wildcard redirect registrations. No Quick Tunnel command is
required to pass named-profile acceptance.

## E06 — Web and Flight separation (R03/R08)

Every UI/snippet/runbook distinguishes HTTPS web hostname from local/private TLS
Flight endpoint. A Python/DuckDB local governed read works while web UI uses the
tunnel. No public hostname gRPC success claim or Access-JWT-as-Flight-token alias.
Optional private routing must qualify real client enrollment/TLS/JWT separately.
Owner: existing consumer fixture and profile UI/browser checks.

## E07 — Real local/Cloudflare SSO qualification (R09)

On a disposable approved account/hostname: execute local-only and named setup,
allowed-user login, denied-user edge policy, allowed-edge/denied-app-role, normal
IdP SSO reuse, app logout, edge expiry, upstream revocation, invalid callback,
connector restart and outage. Verify denied edge traffic does not reach the app.
Origin TLS validation failure blocks access; no auth/secret bytes in logs.
Confirm app logout revokes app sessions even when upstream SSO remains valid;
document/test shared-browser logout separately. Restore a valid session and
reconcile a previously unknown publication without duplicate publish.

After one-time account/IdP setup and images cached, warm start-to-ready <=60s;
network/provider failures have bounded actionable diagnostics. Three fresh local
install rehearsals follow the runbook without editing source or bypassing TLS.
Measure credential prompts/redirects rather than promising zero prompts.
External setup requires authorization; no live result can be inferred from mocks.

## E08 — Single design source and fonts (R04)

App and Storybook import identical semantic tokens, licensed self-hosted fonts and
actual shared primitives. Required color/font literals are confined to the design
source; no old green/indigo palette overrides remain in feature styles. No remote
font requests or Storybook code in the production app bundle. Core text remains
readable with fonts blocked/offline; no layout shift beyond E16.
Owner: style lint/build plus one loaded-app network check.

## E09 — Correct themes, contrast and layout (R04/R06)

Test explicit Light, explicit Dark, System+light OS and System+dark OS, including
live OS changes. Capability cards and all normal inputs follow the effective theme.
Text contrast >=4.5:1; large text/meaningful control boundaries >=3:1. Verify actual
hover/focus/selected/error states and translucent composites. Essential labels
meet the specified type scale. Status is understandable without color.

390x844, 768x1024, 1440x900 and 200% zoom: no clipped actions/page-level overflow,
account/sign-out reachable, panels adapt, tables scroll in named regions. Include
forced-colors and reduced-motion checks. Owner: representative story/browser states
plus manual review, not a screenshot for every prop combination.

## E10 — Storybook documentation and behavior (R05)

Static Storybook builds with Foundations/Components/Patterns/Workflows/Contribution
sections. Each shipped shared component documents purpose, variants, tokens,
keyboard contract and limitations, with meaningful state stories. App and stories
import the same component module. A token change visibly updates both.

Play tests exercise validation, pending, failure/conflict, dialog/menu focus and
unknown-outcome recovery. Deliberately broken labels/focus must fail the relevant
test. Automated axe has zero serious/critical violations; manual screen-reader
and keyboard checks cover representative workflows. Owner: Storybook Vitest/a11y.

## E11 — Story isolation and economical regression coverage (R05)

Story fixtures are synthetic and deterministic; unexpected external fetch fails.
No real session cookie, IdP client secret, tunnel credential or source row in static
output. Product CSP/frame restrictions unchanged by Storybook embedding.
Only authorized static designbook sharing is allowed; dev servers stay private.

Record each migrated test's unique invariant and replacement. One primary story/
unit oracle per behavior; live Playwright only for distinct application boundaries.
Keep pure schema/tree algorithm cases. Remove increment/equality and duplicated
markup-only tests after behavioral replacement, not unique negative security cases.
Visual baselines require deliberate review; never auto-accept diffs in CI.

## E12 — Scope isolation and accessible shell (R06)

Use colliding delimiter identity examples and same-principal reauthentication with
changed grants: private caches remain distinct by structured identity/session
generation. Delay success/error/finally for load/save/evaluate/review/publish/lookup/
restore/discovery/settings; change actor/asset/draft/revision mid-flight. No stale
result may overwrite new state or clear a newer pending operation.

Palette/dialog: correct roles, labels, arrow selection if listbox, Escape, Tab/
Shift+Tab, inert background and focus return. Mobile menu has equivalent behavior.
Navigation back/refresh/deep links and unsaved edits remain correct. Owner:
parameterized rendered story tests plus one app-shell integration journey.

## E13 — Complete policy workflow (R07)

Use the inherited nested fixture, masks, filter/condition semantics and 10k-node
tree. Author A edits, saves, copies an exact-revision review link; publisher B
reads it without edit authority, compares semantic changes and publishes. Change
the draft after link creation: opening/reviewing it detects staleness, never silently
substitutes current content. Wrong-asset and forbidden references are concealed.
Clipboard failure displays a selectable link or retry explanation.

No schema response/error can create index-based field IDs or selectable fabricated
fields. Deny-all is intentional; unknown publication is reconciled; restore creates
a saved draft; actual governed reads match the active version. Preserve typed
values, undo/redo and keyboard focus. Owner: one real workflow, focused story edge
cases and existing backend policy goldens; process races in E15.

## E14 — Complete administration workflow (R08)

Qualified plugin-pair configuration, validation, discovery, explicit format choice,
govern, edit, activate, disable and retire succeed with authoritative revisions.
Source tables/history are never deleted by retirement. Settings failure cannot
show editable defaults; last-admin replacement is verified. Existing API role
matrix remains authoritative; grant management never implies Flight data access.

Audit filters/cursors produce exact permitted records; UI exposes loading/error/
empty distinctly. Health is measured or unknown. Copied consumer examples run
against supported endpoints. Every visible action has an actual backend result.
Owner: one management journey, parameterized lifecycle fixture, existing API tests.

## E15 — Real boundary qualification (R10)

Pass inherited B06/B07/B16/B17 with actual providers, two API processes, PostgreSQL
barriers and built artifacts. Include cross-author draft edits, revocation and
kill-before/after-commit; no mixed generation or duplicate operation. Three pairs
times Python/Arrow, DuckDB and Spark/JVM must match full nested schemas/row
multisets through TLS/OIDC. Denied transport destinations receive zero requests;
actual canceled provider work closes within deadline+2s. Preserve pickle fixtures.

## E16 — Cost and performance (R11)

Retain B18/B19 fixed-runner budgets: fast checks warm <=60s/cold <=120s, docs <=10s,
PR critical path <=15min, schema/discovery <=10s, evaluation <=5s, tree interaction
p95 <=200ms/<=200 mounted rows. Initial app JS <=200KiB gzip, route increment
<=150KiB, CSS <=50KiB; initial fonts <=120KiB. Storybook is measured separately and
does not inflate production budgets. LCP <=2.5s/CLS <=0.1 under the inherited fixture.
Preserve five-run/60-minute/resource/audit-load gates. No security-check removal
or formatting compression to meet metrics.

## E17 — Real restore and candidate integrity (R11)

Pass inherited B20/B21: actual encrypted PostgreSQL backup/restore, recorded
RPO <=15min/RTO <=60min, old-access invalidation before ingress, rotation/drain
and one compatible artifact set. Exact UI/server/plugin hashes, SBOM/advisories,
provenance and executed mandatory-lane evidence bind to the candidate. Fake
pg_dump/age helper tests alone cannot close this. Fail promotion on missing or
tampered evidence/artifacts. Cloudflare outage must not prevent local recovery.

## E18 — Product acceptance (R12)

Owner approves actual light/dark app screens and Storybook, not just the illustrative
board. Five representative users perform connect/govern, nested authoring,
review/publish, revoke and audit diagnosis. >=4/5 complete each unaided, median
simple policy change <=5min, all distinguish draft/active and deny-all, mean
usability >=4/5. Record observed errors/time and changes made.
Independent security review has no unresolved high/critical issue. External/user
participation awaits explicit authorization; missing evidence keeps release HOLD.
