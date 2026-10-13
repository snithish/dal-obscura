# Whole project test audit

This audit starts at `a34de5f0aef1bf0d7c4d7d88d8e4ea9300bb6174`.
It applies the audit-tests and write-tests principles to every tracked test
declaration, including parameter rows, component stories, browser journeys,
live harnesses, review captures, benchmarks and JVM tests.

The aim is independent proof with clear ownership. A smaller suite is not a
success criterion. Parser, transport, admission, storage, consumer and resource
boundaries retain separate coverage when they can fail independently.

## Evidence

[Summary](summary.json) owns the inventory comparison, before and after sizes,
verification results and remaining qualification limits. Each declaration has
a decision, the assertions it actually protects, source and history evidence,
and an owning validation command:

- [Core contracts](core.json)
- [Storage and plugins](plugins.json)
- [UI and JVM](ui-jvm.json)
- [Operations, clients and benchmarks](operations.json)

These are dated audit records, not a second registry of current code. Original
names and locations preserve the reviewed starting state. Transfer decisions
identify surviving owners. New resource and batch regressions have separate
records. [Fault controls](controls.json) record deliberate defects that failed
for the intended reason and passed after restoration.

## Decisions

Actual IO denial replaces copied DuckDB configuration expectations. Actual
filtered values replace captured SQL text. Independent ticket bytes, artifact
digests, row values and complete schemas replace circular expectations and
count-only checks.

Browser race fixtures capture each response before waiting. Tests start the
request they claim to race and await its cancellation or completed operation.
Provider events replace cancellation call counts and timing luck. Keyboard
timing starts after the complete search state is rendered.

Private helper coverage moves only after the public owner preserves every
unique case. Independent review restored positive two-ticket persistence beside
rollback coverage and retained actual extension loading denial. Owner and grant
lock-order tests remain because sequential HTTP checks do not prove the same
concurrent authorization contract.

Fixtures own and release their database, HTTP, Flight, Spark and subprocess
resources. JVM fixture construction has a deadline, bounded diagnostics and
cleanup on interruption and failure. Synthetic HTTP fixtures demonstrate UI
behavior; they do not establish live federated authentication correctness.

## Qualification limits

The benchmark lane checks local streaming and resource bounds. Its native
planner may split files into row groups, so ticket counts follow parallelism
and task-budget bounds. File count does not define an exact ticket count.
The RSS probes measure read-time process growth after fixture setup. They do
not establish global capacity or a distributed memory SLA.

Live SSO, production cloud IAM and TLS, image publication and the Spark 4 profile
remain separate qualification lanes. This audit does not substitute mocks or
silent skips for them. Release artifacts and raw execution logs remain outside
Git; the checked-in records retain their conclusions and limits.
