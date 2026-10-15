# Working in Dal Obscura

Dal Obscura serves governed Arrow Flight reads with row filters, column selection
and masking. Changes must preserve authorization, exact Arrow schemas and bounded
stream execution.

## Scope and decisions

Follow this guide throughout the repository. Read any nearer `AGENTS.md` before
editing its subtree. Explicit user instructions take precedence over repository
guidance, subject to the agent's system and safety constraints.

- Inspect `git status --short` before editing. Preserve unrelated work and never
  reset, discard or overwrite changes you did not make.
- Resolve routine choices from the owning code and tests. Ask only when a missing
  decision materially changes behavior, scope or risk. Continue independent work.
- Before a project-wide redesign or a new persistent subsystem, present a concrete
  plan and tradeoffs and obtain approval once. Existing approval remains valid.
- Complete authorized implementation and verification. Commit, push, publish or
  change shared services only when authorized. Use Conventional Commits when
  committing, and push only the authorized branch.

## Read the relevant guidance

Load the documents needed for the affected boundary, rather than reading the
entire repository upfront.

- Read [architecture](docs/architecture.md) before changing ownership, read
  execution, authorization or plugin boundaries. Put behavior with its owner.
- Read [testing](docs/testing.md) before changing code or tests. It defines test
  ownership, installed-wheel checks and mandatory qualification.
- For providers and SDK changes, read the [SDK](packages/plugin-api/README.md) and
  [conformance guide](packages/plugin-conformance/README.md). Packages must remain
  independent of service internals and core test support.
- For UI or identity changes, read [governance UI](docs/governance-ui.md). Before
  starting services or changing databases, read [configuration](docs/configuration.md)
  and [deployment](docs/deployment.md). Use disposable resources for tests.

## Tools and commands

Run commands from the repository root. Use Python 3.14 with `uv`. Use the Node and
pnpm versions declared in [the UI package](apps/governance-ui/package.json) and
[the reader's Rust toolchain](packages/iceberg-reader/rust-toolchain.toml).

Install workspace wheels, not editable packages. After Python source changes,
reinstall affected distributions before testing so tests exercise current code.

```sh
uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen
uv run --no-sync pytest tests packages -o pythonpath= -m 'not heavy and not integration and not socket'
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync ty check
```

For focused Python checks, replace `tests packages` with the owning test path or
pytest node ID and retain `-o pythonpath=`. The command above excludes integration,
socket and heavy tests; it does not establish full qualification.

For UI changes:

```sh
pnpm --dir apps/governance-ui install --frozen-lockfile
pnpm --dir apps/governance-ui test
pnpm --dir apps/governance-ui check
pnpm --dir apps/governance-ui check:api-types
pnpm --dir apps/governance-ui build
```

Run browser, story, JVM, native-provider and live SSO checks when their contracts
change. Follow [testing](docs/testing.md) for prerequisites and full qualification;
use [CI](.github/workflows/ci.yml) to locate the corresponding automated checks.
Do not replace required evidence with a narrower passing suite.

## Implement at the owning boundary

- Start new behavior or a bug fix with a failing behavioral test in the owning
  suite. Assert observable results, including exact schemas and metadata where
  relevant. Avoid tests that merely repeat implementation details.
- Prefer a complete change to the owning contract over compatibility wrappers.
  The project has no production users or external plugin consumers. Update
  affected callers together and remove obsolete code, configuration and docs.
- Keep feature code, state and tests together. Share behavior only where multiple
  callers need it. Do not add forwarding modules, duplicate contracts or unrelated
  refactors. Trace callbacks and plugin entry points before deleting apparent
  dead code.
- Regenerate OpenAPI, client types and protobuf outputs from their sources.
  Never hand-edit generated output. Keep release artifacts outside Git, and
  update lockfiles through the owning tools when dependencies change.

## Preserve the critical contracts

Read the detailed execution rules in [architecture](docs/architecture.md) when
working on these paths.

- Capture one immutable configuration and release database transactions before
  provider IO. Validate every task and output schema before issuing tickets.
  Existing tickets retain captured policy until expiry or revocation.
- Fetch trusts signed tickets. Check identity, expiry, revocation and deadlines
  before every batch. Filter original values before masking, preserving SQL NULL
  behavior, parent masks, nested collections, field identity and Arrow metadata.
  SQL and declared output schemas use the same projection compiler.
- Bound work and retained buffers. Close resources on success, failure,
  cancellation and early stop without replacing the primary error. Tasks contain
  passive JSON; distributed assignments cover disjoint work exactly once and
  preserve pinned snapshots and native deletes.
- Admit plugins through SDK contracts, installed entry points and artifact locks.
  Keep credentials out of tickets, handles, logs and committed files. Core
  authorizes secret references through scope grants to startup-selected backends.
  Identity attributes require verified claims; previews remain synthetic.
- Preserve UI drafts, revision fences, session-scoped caches and stale-response
  guards. Do not weaken concurrency or authorization checks to make a test pass.

## Finish with evidence

Run focused checks first, then the required checks for affected contracts. For
documentation-only changes, verify links and command definitions. Do not run the
application suites unless executable behavior also changed.

Review the final diff for accidental changes, obsolete paths and duplicated
behavior. Measure relevant performance changes using comparable workloads and
report measured limits. Do not claim performance gains from inspection alone.

Report what changed, checks run and their results, and any remaining limitations.
Distinguish failed checks from checks that could not run. If blocked, name the
missing prerequisite and exact remaining check; never silently skip it or claim
completion. A separate senior review is not a completion gate.

## Session preferences

Use the `caveman` skill at level full when available. Write code, commit messages,
PR text, warnings and clarifications normally. Respect "normal mode" and "stop
caveman".

Use `rg` for exact text and file discovery. Use the `ccc` skill for conceptual
code lookup when exact search is insufficient. Preserve
`COCOINDEX_DISABLE_USAGE_TRACKING=1` for every CocoIndex command, and refresh its
index after meaningful code changes. If the skill or tool is unavailable, state
the limitation and continue with available search tools.

Keep this file focused on durable instructions that change agent behavior.
Update stale commands and links when workflows change. Keep detailed contracts
in their owning docs instead of duplicating inventories or session history here.
