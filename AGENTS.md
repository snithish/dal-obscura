# Agent guide

Use the `caveman` skill by default at level full. Write code, commit messages,
PR descriptions, safety warnings and clarifications normally. Respect "normal
mode" and "stop caveman". Use the `ccc` skill for code exploration and semantic
search. Preserve `COCOINDEX_DISABLE_USAGE_TRACKING=1`; refresh after code changes.

## Working agreement

- Before a major project-wide redesign, present a concrete plan and tradeoffs and
  wait for approval once. Finish already approved work without repeating approval.
- Preserve unrelated changes. Prefer clean breaking contracts over compatibility
  scaffolding; the project has no production users or external plugin consumers.
- Remove obsolete code, configuration and documents. Share behavior at its owner.
  Keep functions small and names concrete. Avoid forwarding modules and duplicate
  contracts. Add persistent subsystems only with explicit approval.
- Complete dead-code audits and relevant performance measurements during
  implementation. Record findings, fixes and measured limits. A separate senior
  review is not a completion or approval gate.
- Use Python 3.14 and uv. Use the UI's pinned Node and pnpm versions. Release output
  belongs outside Git. When asked to commit and push, use Conventional Commits,
  finish relevant checks, and push the authorized branch.

## Ownership

Follow [architecture](docs/architecture.md). Policy owns authorization and the
shared SQL projection compiler. Read owns planning, tickets, streams and execution.
Sources own published source resolution and SDK adapters. Plugins own installed
artifact admission. Runtime owns startup configuration and secret authorization.
Identity owns verified claims and browser sessions. Storage owns SQL and revisions.
Control owns administrative commands. Interfaces translate HTTP, Flight and CLI.
Provider implementations and the independent SDK belong in `packages/`.
Group feature code with its owning state and tests. Read tickets and execution
have separate folders; administrative features live under control. UI screens,
stories and unit tests belong together under features. Shared test support is
grouped by owner and contains only cross-suite helpers. Do not recreate the old
flat components folder, forwarding modules or compatibility imports.

## Contracts that must survive changes

- Capture one immutable configuration; release transactions before provider IO.
  Validate every task and output schema before issuing any tickets.
- Fetch trusts signed tickets. Check identity, expiry, revocation and deadlines
  before each batch. Existing tickets retain their policy until expiry or revocation.
- Apply full filters to original values before masking. Preserve SQL NULL behavior,
  parent masks, nested collections, field identity and Arrow metadata. SQL and
  declared output schemas use the same projection compiler.
- Bound work and retained buffers. Close resources on success, failure, cancellation
  and early stop without replacing the primary error. Passive tasks contain JSON.
- Distributed scans cover disjoint work exactly once across separate workers.
  Preserve pinned snapshots and native deletes. Keep credentials out of handles.
- All plugins use SDK contracts, installed entry points and artifact locks. Secrets
  are startup-selected backends; core authorizes every reference using scope grants.
- Preserve drafts, revision fences, session-scoped caches and stale-response guards.
  Identity attributes come from verified claims; previews remain explicitly synthetic.

## Qualification

Follow [testing](docs/testing.md). Start new behavior with an owning behavioral
failure, then implement and run focused checks. Assert exact schemas and values.
Keep package tests independent of core test support. Use native writers for real
mutations; hand edits belong only in malformed-artifact tests. Use provider signals
for races. Qualify installed wheels without source paths, disposable PostgreSQL,
real consumers and separate S3 workers. Run UI, JVM and live SSO lanes when their
contracts change. Documentation changes need link and command checks.
