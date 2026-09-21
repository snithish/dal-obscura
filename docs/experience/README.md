# Current review and execution handoff — 2026-09-20

Source baseline: `eed277728a856801af7ef794d73d99f0b28c42ea`.
**Planning only. No application implementation, tunnel exposure, account changes or application test runs.**
Paid-production release remains HOLD. Implementation is now authorized; see
[execution status](STATUS.md) for completed slices and current blockers. The
planning-only statement above describes the original review deliverable.

Component-library amendment: Mantine is the selected maintained MIT-licensed
library. R01/R04/R05 and E08/E10 now require direct reuse, one shared theme and
Storybook integration, with early CSP/size qualification. Mantine shell/login
foundation is installed and qualified; see the execution ledger for remaining work.
The initial executable designbook is available with
`pnpm --dir apps/governance-ui storybook`; it uses the real application components.

Read [implementation review](REVIEW.md), [action plan](ACTION_PLAN.md),
[local HTTPS/SSO design](LOCAL_ACCESS.md), [design system and Storybook](DESIGN_SYSTEM.md),
then [acceptance specifications](ACCEPTANCE.md).
[Visual direction](design-direction.html) is an illustrative review artifact,
not an implemented application or Storybook instance.

R01–R12 below replace the previous N execution queue. Completed N implementation
is reconciled in REVIEW.md; its B/G acceptance requirements remain regression or
qualification obligations, with explicit amendments in ACCEPTANCE.md. Do not
restart X or N packets or create a second implementation for completed behavior.

Latest owner constraints: strong security, secure local operation, rich management
UI, nested schemas, multiple admitted catalog/format plugins, Python/DuckDB/Spark,
deliberate breaking changes without shims, minimal code and meaningful tests.
Preserve the specifically protected pickle logic/classes/import paths/semantics.
Use the existing database; no new state service. Keep one deployment per customer.

Next implementation starts at R01; R04 design foundation can proceed independently.
Record one concise current status per R packet and link exact execution artifacts.
Missing live/human evidence is VERIFY, never a passing claim.

Planning validation: local Markdown links, packet/acceptance IDs and required
packet fields checked; illustrative light/dark workbench inspected in a browser.
Contrast values are calculated pairs, not an accessibility audit of the app.
