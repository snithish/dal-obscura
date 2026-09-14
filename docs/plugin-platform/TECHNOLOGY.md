# Technology decisions and upgrade procedure

Planning snapshot: 2026-09-13; implementation owner N01. Lock exact versions when
N01 runs and verify official release/security metadata again then. A published
new version is not evidence that this repository works with it.

## Retain the architecture that already fits

Keep Python, FastAPI, SQLAlchemy/Alembic, PostgreSQL, Arrow Flight, PyArrow,
PyIceberg and DuckDB; use uv for Python environments and locking. Keep the
React/TypeScript/Vite SPA, served with same-origin authenticated backend access.
Do not introduce SSR, a second backend language, Redis, a new durable state service,
a general plugin marketplace or a custom browser RPC protocol. Preserve trusted
pickle functions/classes/import paths and semantics.

N01 records exact Python/database/Arrow/DuckDB/Iceberg/JDK/Spark versions actually
resolved and exercised. Select one production Python minor with maintained security
support and compatible binary wheels, test the documented minimum separately if
it differs, and bound package metadata honestly. The selected support line is Python 3.12;
evaluate a newer supported minor using clean installs and the protected fixtures,
then choose the newest passing supported minor. Do not claim or advertise an
untested interpreter. Apply the same rule to PostgreSQL and consumer versions.

## Frontend baseline and selected replacements

- **React:** evaluate stable 19.3 and pin matching react/react-dom/types. The
  official release is dated September 9, 2026. New animation APIs are optional;
  do not enable them as part of authentication or private-state removal.
  [React 19.3 release](https://react.dev/blog/2026/09/09/react-19-3)
- **Vite:** target the current stable patched line; the official page lists 8.3
  for regular patches at this review. Upgrade through its migration instructions,
  verifying build plugins and Node requirements; pin the exact passing patch.
  [Vite release policy](https://vite.dev/releases)
- **Node:** select active LTS (24 at this planning snapshot), not the current
  non-LTS line merely for its higher number. Match local, CI and image tooling.
  Verify phase and supported engines at implementation.
  [Node release schedule](https://nodejs.org/en/about/previous-releases)
- **TypeScript/pnpm:** use verified current stable compatible releases, exact
  package-manager pin and frozen lockfile. TypeScript 6.0 has published migration
  notes; inspect newer stable releases when N01 runs instead of assuming this
  reference is the latest. Do not suppress new errors to force an upgrade.
  [TypeScript 6.0 notes](https://www.typescriptlang.org/docs/handbook/release-notes/typescript-6-0.html)
- **Server state:** one TanStack Query client and a small fetch transport. Replace
  manual repeated loading/cache/retry state. Query keys include session identity
  and resource; supply AbortSignal. Mutations still need explicit captured scope
  and idempotency reconciliation. Do not add Redux/Zustand for the same state.
  [TanStack React quick start](https://github.com/TanStack/query/blob/main/docs/framework/react/quick-start.md)
- **Navigation:** use one maintained typed router, default React Router, replacing
  hand-written hash parsing. Configure same-origin deep-link fallback. Keep
  route/search state in the router, query results in Query, drafts in components.
  Verify the selected stable router supports the chosen React version in N01.
- **Accessible primitives:** use only needed Radix primitives for dialogs, menus,
  tabs, tooltips and focus handling. Style with tokens/CSS modules. Do not retain a
  second primitive library or copy a custom focus-trap implementation.
  [Radix accessibility](https://www.radix-ui.com/primitives/docs/overview/accessibility)
- **Icons:** Lucide React, named tree-shaken imports. No full icon font, remote icon
  CDN or parallel hand-drawn navigation set.
  [Lucide React guide](https://lucide.dev/guide/react)
- **Types/forms:** generate TS API types from strict OpenAPI responses; retain one
  handwritten transport. Native controls plus shared typed field components first.
  Add a form/validation library only after naming duplicated behavior it removes;
  it must not become a second authoritative domain validator.
- **Tests:** use Vitest + Testing Library for rendered behavior, Playwright for a
  small real-browser workflow suite, axe for automated accessibility. Remove
  replaced node arithmetic/source-text tests. Use user-facing roles/names and
  controlled deferred responses, not brittle CSS selectors or arbitrary sleeps.
  [Playwright locators](https://playwright.dev/docs/locators),
  [accessibility testing](https://playwright.dev/docs/accessibility-testing).

These choices are bounded replacements, not a dependency shopping list. Bundle
budgets in B19 apply to their combined output. If a dependency breaks the budget,
measure the cause and simplify imports/routes before adding another framework.
Use one existing virtualization implementation if it passes B12; replace it with
one maintained primitive only when measured behavior requires it.

## Required upgrade record

Include the UI base image and its web server in the same support/advisory matrix.
The current Dockerfile uses Node 24 and nginx-unprivileged 1.27; verify maintained
target tags and digests at N01, rather than preserving a tag because a test quotes
it. Build tools (Vite and its React plugin) belong in devDependencies; only actual
browser runtime packages belong in dependencies. Preserve the production CSP,
same-origin proxy and hashed-asset cache behavior through one live image test.

Rechecked the official Vite and Node release pages on the second review: the
8.3 stable patch line and Node 24 LTS direction above remain applicable. Exact
passing patch/digest selection remains implementation work, not a guessed pin.

For each changed dependency/toolchain, record current resolved version, target
exact version, official release/support/advisory source and check date, affected
runtime/consumer matrix, removed dependency/code, and executed install/build/type/
behavioral result. Commit lockfiles and package metadata together. Image tags must
be immutable digests in release manifests; no unbounded latest tag.

Run dependency/advisory and license checks against exact distributed artifacts,
including JVM and transitive browser/runtime libraries. No unresolved high/critical
runtime vulnerability may ship. Registry availability alone is not evidence of
maintenance or security. A dependency update must preserve governance/Arrow
semantics and the protected pickle fixtures; incompatible experimental upgrades
are not a reason to weaken the security contract.

Produce one short supported-version policy for local/CI/images/docs. Re-run changed
boundary tests and the full candidate qualification before claiming support.
N01/B02 close the toolchain work; N13/B17 establish consumer compatibility; N15/B21
establish tested artifact integrity. No runtime dependency was changed in this review.
