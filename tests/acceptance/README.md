# Acceptance case ownership registry

The active specification is [E01–E18 with inherited G/B gates](../../docs/experience/ACCEPTANCE.md).
The active queue is [R01–R12](../../docs/experience/ACTION_PLAN.md).
The N assignments below are historical test-location references, not task order.
This Markdown registry defines future executable ownership; it is not a pass report.
Old A01–A23 cases remain in the [archived specification](../../docs/plugin-platform/ACCEPTANCE_ARCHIVE_20260913.md)
and are grouped under G guarantees. Do not create a second runner per packet.

## Primary locations and evidence

Current E ownership: E01 uses existing package/API/plugin suites; E02/E03 existing
session/profile API suites; E04–E07 the existing local launcher/profile and real
browser/consumer harnesses with the proposed tunnel overlay; E08–E12 proposed
colocated Storybook stories and shared UI design sources; E13/E14 real policy/
management browser journeys plus existing API suites; E15 integration/conformance/
consumers/JVM; E16/E17 existing capacity/CI/recovery; E18 human candidate dossier.
No executable Storybook, Cloudflare or acceptance test is added by this plan.

- **B01/B02, N01:** existing package/import/build CI and baseline report. Inventory
  resolved toolchains/collection/durations; no executable test of packet prose.
- **B03, N02:** tests/plugin_platform and tests/architecture plus installed-wheel
  CI. Strict obsolete-input rejection, canonical imports, offline migration,
  protected serialized fixtures. Replace retention-only prose checks in N14.
- **B04/B05, N03:** tests/interfaces/control_plane and tests/control_plane.
  Pair declarations/returned handles, authoritative DTOs and revisions.
  Include cross-author saved draft references and reviewer-bound evidence.
- **B06, N04:** tests/integration/test_io_boundary.py extended with real local
  transports/counters and provider cancellation. Validators alone are insufficient.
- **B07/B08, N05:** existing actor/session API tests plus real OIDC browser fixture.
  Exact typed identity and two-process authority revocation.
- **B09, N06:** planned apps/governance-ui/e2e/shell.spec.ts using the built app;
  theme/responsive/keyboard/axe plus manual assistive-technology evidence.
- **B10, N07:** planned apps/governance-ui/src/features/session/async-scope.test.tsx
  mounts actual feature components with deferred responses; one live wiring case.
  Do not recreate tests of epoch increment/equality.
- **B11/B12, N08:** planned policy editor component tests and
  apps/governance-ui/e2e/policy.spec.ts; existing backend nested goldens; 10k-node
  measured browser fixture. Typed mask/condition roundtrip and keyboard navigation.
- **B13, N09:** extend the same policy browser journey and existing operation API
  tests. Saved semantic diff/lost-response/history/restore and separate
  editor-to-publisher handoff without copying draft ownership.
- **B14, N10:** planned apps/governance-ui/e2e/connections.spec.ts parameterized by
  the three pairs, backed by catalog/activation API tests.
- **B15, N11:** planned apps/governance-ui/e2e/management.spec.ts and existing actor,
  grant, settings and audit API tests. One explicit capability matrix.
  Add database-filtered cursor traversal with timestamp ties/revocation; reuse
  its fixture for UI coverage. Management capabilities do not imply Flight access.
- **B16, N12:** tests/integration/control_plane/test_publication_races.py extended
  to real PostgreSQL and two API processes, plus actual Flight output.
  Include author A changing the selected draft while publisher B commits it.
- **B17, N13:** tests/plugin_conformance/test_pairs.py,
  tests/consumers/test_governed_reads.py and connectors/jvm integration tests.
  Reuse the wheel admission/conformance runner and fixture data for nine live
  pair/consumer cells. Stubs remain fast tests only.
- **B18/B19, N14:** existing benchmarks/capacity runner, UI browser measurements,
  CI duration/collection artifacts and resource metrics.
- **B20/B21, N15:** tests/production, tests/integration/test_recovery_upgrade.py,
  existing secure-local/production Compose and release manifest lane.
  Real encrypted restore, exact hashes, TLS/OIDC and failure evidence.
- **B22, N16:** candidate dossier, owner visual acceptance, authorized user study
  and independent security review. These cannot be generated as unit-test passes.

Planned paths are instructions for where to add the smallest missing test, not
claims that files exist. Reuse/rename the nearest owning suite if that produces
less duplication; update this registry in the same slice. Keep G01–G05 owning
regression tests mapped to their original A cases and new B boundary proof.

## Rules for economical coverage

One primary behavioral oracle per invariant. Add integration coverage only when
it crosses a real distinct boundary: serialization, process/database, browser,
provider/network, installed artifact or consumer. Parameterize identical semantics;
retain distinct failure checks. Mocking is valid for deterministic UI ordering but
not for qualification of real auth/providers/restore. Never replace negative
security tests with line coverage or generic snapshots.

Record commands/results and candidate/artifact identities in
[STATUS.md](../../docs/plugin-platform/STATUS.md). Missing mandatory environment
means VERIFY; skipped or unexecuted tests never establish acceptance.
