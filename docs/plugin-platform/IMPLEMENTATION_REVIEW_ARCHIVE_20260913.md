# Implementation review

> Historical snapshot archived 2026-09-13. Do not execute this queue or interpret
> its status as current. Use the [current handoff](README.md) and N01–N16 plan.
> Current acceptance explicitly supersedes old compatibility requirements.

Baseline: `5208eee`, reviewed 2026-09-12. Scope: control-plane workflows, UI state,
catalog/format boundaries, schema/evaluation, publication, deployment, and tests.
This is a targeted code review with local probes, not a complete security audit.

## Result

The repository has useful foundations: separate application and infrastructure
layers, executable table-format contracts, canonical policy resolution, signed
tickets, durable publication/draft/session records, nested schema support, and a
working set of focused tests. A total rewrite would discard useful behavior.
Repair the security and correctness gaps first, then extract plugin seams with
compatibility tests. Current evidence does not justify serving paying customers.

Evidence labels used below:

- **Reproduced** means a local test or probe demonstrated the behavior at this
  commit. Its environment limits are stated.
- **Code-confirmed** means the cited execution path contains the gap, but the
  complete deployed workflow was not run during this review.
- **Unverified** means release evidence is missing or a risk still needs a
  reproducer. Do not report it as an exploited vulnerability.

## R01 — First publication activates unrelated asset drafts

**Priority: release blocker. Evidence: reproduced in SQLite/TestClient. Repair: X01.**

In [policy_version_service.py](../../src/dal_obscura/control_plane/application/policy_version_service.py),
`create_asset_policy_version()` loads and compiles the entire workspace draft
when no active publication exists. Replacing the selected asset's rules does not
remove the other assets. Later publications use a different, selected-asset path.

Probe: provision `default.users`, add `default.other` with a separate allow rule,
give both owners, and publish only `default.users`. The HTTP result was 200;
publication history contained **both** targets. This probe used the non-review
test app; the same initial-publication branch also follows strict review checks.
No live external-reader exploit is claimed.

Expected: publishing A activates only A and the required reviewed configuration.
B must remain unavailable, even if B has complete draft rules. Readiness checks
must not require unrelated unfinished assets to be publishable.

## R02 — Review can authorize content other than the evaluated draft

**Priority: release blocker. Evidence: reproduced; additional races code-confirmed.
Repair: X02 and X03.**

[review_service.py](../../src/dal_obscura/control_plane/application/review_service.py)
accepts the absence of a personal draft as revision 0/content hash null.
[policies.py](../../src/dal_obscura/control_plane/interfaces/routes/policies.py)
still permits revisionless shared `PUT policy-rules` updates. Verification checks
the absent personal draft again, not a digest of the changed shared rules.

Probe used `create_app(require_review=True, bootstrap_enabled=True)` with separate
review secret, SQLite, and a fake authoritative catalog. Review for `user1` returned
200. Shared rules were then replaced with unmasked access for `different-reader`
(200). Publishing with the old review token returned **200**. Bootstrap supplied
an authorized test actor; this is a review-integrity failure, not an authentication
bypass or evidence of a live OIDC exploit.

Further gaps: evaluation resolves policy, then reads draft metadata separately;
token issuance reads the draft again. It does not assert that evaluation evidence
equals the token's current draft hash. Publication verifies, then reloads content.
Publication row locking alone does not serialize draft, grant, asset-binding, and
connection changes. Reproduce these interleavings on PostgreSQL before fixing.

Expected: strict publication requires an explicit immutable saved-draft snapshot.
Bind evaluation, review, authorization generation, asset/connection binding,
schema, plugin configuration, and committed publication to that same snapshot.
Any relevant change must conflict or require fresh review. Keep deny-all supported.

## R03 — Synthetic evaluation reconstructs the wrong mask parameters

**Priority: high. Evidence: code-confirmed. Repair: X04.**

[policy_service.py](../../src/dal_obscura/control_plane/application/policy_service.py)
returns mask types but drops their resolved values from preview.
[evaluation_service.py](../../src/dal_obscura/control_plane/application/evaluation_service.py)
reconstructs values with `setdefault()` over all raw rules, including rules that
do not match the persona. This can disagree with canonical mask merging, such as
the most restrictive `keep_last` value. Synthetic evidence can misrepresent the
data-plane result used to justify publication.

Expected: return/reuse the full canonical resolved policy internally. A rule for
another principal must never determine this persona's mask value. Golden tests
must compare actual values and Arrow schemas between evaluation and Flight.

## R04 — Nested evaluation paths and fixture generation are incomplete

**Priority: high. Evidence: reproduced path collision; other gaps code-confirmed.
Repair: X04 and X05.**

`evaluation_service._leaf_paths()` concatenates field names with dots. A literal
top-level field `a.b` and nested `a -> b` both yielded `a.b` in a probe. Top-level
list/map paths do not follow the same recursion as collection children of structs.
Use the existing typed `FieldPath` contract, including escaping and collection
segments, throughout evaluation.

`_sample_value()` makes string map keys even for non-string map key types and
returns strings for several non-string scalar types. Arrow table construction is
outside the redacted transform error handler. Empty input is replaced with a sample
through `rows or [...]`; empty transform output falls back to a schema-less table.
Evidence has no synthetic fixture digest. Per-request adapter construction also
does not establish a process-wide concurrency budget.

Expected: type-correct nested fixtures, explicit omitted-versus-empty input
semantics, preserved empty output schema, bounded/redacted failures, fixture-bound
review, and shared resource admission. Unsupported types must produce an explicit
safe capability error, never invented “successful” data.

## R05 — Schema fingerprint and resource limits do not protect every path

**Priority: high. Evidence: reproduced digest collision; other gaps code-confirmed.
Repair: X05 and X06.**

[schema_service.py](../../src/dal_obscura/control_plane/application/schema_service.py)
hashes `str(schema)`. In a probe, otherwise identical Iceberg list schemas with
element IDs 2 and 99 produced the same digest. API responses fingerprint Iceberg
objects while evaluation/review fingerprint Arrow objects; this is not one shared
versioned schema identity contract. `schema_version: 1` is a response version, not
the authoritative Iceberg schema ID.

The 10,000-node/64-depth checks run in `get_asset_schema()`, but direct
`load_asset_iceberg_schema()` callers in review/evaluation bypass those checks.
They also lack total metadata byte and operation deadline enforcement.

Persisted policy approval does not capture a complete admitted stable field-ID
set. The effect of parent/wildcard grants under field additions/rebinding requires
a consumer-level regression test; do not assume existing path validation secures
schema evolution. Define and test rename, add, drop/re-add, list/map ID, nullability,
and type changes before adding non-Iceberg schemas.

## R06 — Provider configuration can reach nested dynamic loaders

**Priority: release blocker for broader plugin exposure. Evidence: reproduced
validator acceptance and installed-dependency inspection. Repair: X07.**

[catalog_service.py](../../src/dal_obscura/control_plane/application/catalog_service.py)
accepts arbitrary option keys. `validate_catalog_options()` accepted both
`py-catalog-impl` and `py-io-impl` set to a class-name string, even with an egress
allowlist. The installed PyIceberg `catalog/__init__.py` and `io/__init__.py`
interpret these properties via `importlib`. This is a path to selecting installed
implementation classes outside the apparent top-level catalog allowlist; this
review did not load a malicious class or demonstrate remote code installation.

Checking only the catalog's outer module string is insufficient. Use per-provider
typed option allowlists and reject implementation-loader settings from API input,
stored config, and returned metadata unless a specific operator-owned adapter
supplies a fixed, audited value. Never convert this into “allow any import path.”

## R07 — Secret resolution and egress are inconsistent across control/data planes

**Priority: high. Evidence: code-confirmed. Repair: X07 and X13.**

Schema loading calls `pyiceberg.load_catalog()` directly with stored options.
Discovery validates options but forwards secret-reference dictionaries unresolved.
The data plane has separate resolution in
[published_config.py](../../src/dal_obscura/data_plane/infrastructure/adapters/published_config.py).
The UI's secret-reference guidance therefore lacks a verified end-to-end connection
path. Do not assume SQL provider credentials work as arbitrary separate options.

Egress validation inspects strings containing `://` and initial hostnames. It does
not establish bounds for redirects, DNS changes, file paths, manifest/delete-file
locations, or object-store endpoints returned by a catalog. Exact secret-key
matching is also not a provider-specific sensitive-field policy.

Expected: one configured connection resolver for diagnostics, discovery, schemas,
review, and reads; scoped secret references; safe connection builders; enforced IO
policy across all external resources; sanitized response, logs, traces, and audit.

## R08 — Discovery caps are applied after unbounded materialization

**Priority: high for availability. Evidence: code-confirmed. Repair: X08.**

[catalog_discovery.py](../../src/dal_obscura/control_plane/infrastructure/catalog_discovery.py)
calls `list(registry.list_tables())` before applying its table cap. A separate
bounded Iceberg discovery helper is not the path used by this entry point.
[catalog_registry.py](../../src/dal_obscura/data_plane/infrastructure/adapters/catalog_registry.py)
walks/materializes namespaces and tables without a total deadline/page budget.
Its namespace queue uses `pop(0)`, adding avoidable repeated list shifts.

Registry reload updates shared configuration before completing a rebuild.
Expected: bounded incremental traversal and immutable registry generations built
off to the side, with a single successful swap. Failed reload preserves the prior
generation; explicit revocation must not silently preserve revoked access.

## R09 — Existing catalog/format ports do not form an installable plugin system

**Priority: architectural requirement. Evidence: code-confirmed. Repair: X09–X17.**

[catalog ports](../../src/dal_obscura/common/catalog/ports.py) and
[format ports](../../src/dal_obscura/common/table_format/ports.py) are useful seams,
but the registry constructs Iceberg directly. The compiler, API schemas, published
config adapter, schema service, and UI each repeat Iceberg/class-path knowledge.
There is no versioned SDK, installed-plugin admission, compatibility contract,
generic schema service, conformance kit, or independently packaged second format.

Adding another branch to each switch is not completion. See
[the architecture contract](ARCHITECTURE.md) for separate catalog and format
factories, operator trust, typed configuration, and migration requirements.

## R10 — Serialized scan objects constrain extraction and upgrades

**Priority: compatibility/security constraint. Evidence: code-confirmed. Repair: X00,
X12, X20. Serialization changes are prohibited.**

[plan_access.py](../../src/dal_obscura/data_plane/application/use_cases/plan_access.py)
pickles scan tasks containing executable format objects;
[fetch_stream.py](../../src/dal_obscura/data_plane/application/use_cases/fetch_stream.py)
unpickles them after ticket checks. Iceberg also serializes trusted internal tasks.
Moving classes into new wheels can break outstanding tickets or alter trusted
execution. Database/ticket storage and installed packages are part of the trusted
computing base. A Python Protocol cannot sandbox a malicious plugin.

Expected: preserve exact serializer behavior and legacy import paths; maintain
golden old-ticket compatibility tests; qualify package combinations; drain or
explicitly invalidate tickets before incompatible deployment. Record unresolved
pickle-boundary risk honestly rather than silently declaring it fixed.

## R11 — UI lifecycle still admits stale results and a stuck loading state

**Priority: high. Evidence: code-confirmed; browser reproduction required. Repair: X09.**

In [main.tsx](../../apps/governance-ui/src/main.tsx), `loadInitialWorkspace()` captures
an epoch and calls `loadAsset()`, which increments the same epoch. The caller's
post-load equality check then returns before setting the workspace ready.

Save, preview, review, restore, and history pagination lack consistent operation
scope checks. An old save can mark newer edits saved; late responses can update a
different asset/session. Persona changes and add/remove operations do not uniformly
invalidate review evidence. Logout clears private state after awaiting its request,
and management rendering has a separate path from the workspace auth gate.

Expected: explicit session/asset/draft/persona operation identities, immediate local
logout fencing, ignored stale success AND failure responses, and recovery actions.
Server authorization remains mandatory even when the UI disables an action.

## R12 — UI and management workflows are not feature complete

**Priority: high. Evidence: code-confirmed; full journeys unverified. Repair: X10,
X14, X18.**

The empty-rule UI hides Save/Test/Review/Publish, preventing an intentional deny-all
workflow even though backend support exists. Rule reordering and `when` editing are
missing; typed mask values are incomplete. The schema summary unions all rules
while presenting selected-rule context. Large trees lack measured virtualization
and complete keyboard/focus behavior.

Connection creation assumes SQL Iceberg. Management fetch failures can render as
empty data. Configuration writes can update workspace drafts without an explicit
UI/backend activation workflow: subsequent policy publication keeps active runtime
settings and may retain an existing catalog configuration. Full-list grant updates
lack conflict protection. Consumer snippets lack complete verified TLS and dependency
instructions. Disable/repair/retirement semantics need a backend contract first.

Expected: each visible control has an authorized backend action and clear draft vs
active status, including deny-all, conflict handling, connection activation, and
consumer handoff. Unsupported features explain why and cannot be submitted.

## R13 — Identity and privilege lifecycle require production evidence

**Priority: release gate. Evidence: code-confirmed details; deployment effects
unverified. Repair: X03 and X19.**

[access.py](../../src/dal_obscura/control_plane/application/access.py) scopes identities
using an issuer/subject string and strips trailing issuer slashes. Define a
collision-free identity representation without silently merging distinct exact
OIDC issuers. Production ownership must use stable subjects, not mutable usernames.
Migrate existing owner/grant/draft records explicitly; never rewrite them on restart.

Stored browser sessions contain role/group snapshots; evidence for upstream role
removal, privilege freshness, logout/reauthentication, bootstrap closure, and
multi-process revocation is incomplete. Decide grant-manager delegation scope and
enforce it consistently. Authorize asset visibility without leaking existence
through different inaccessible-object responses. This review does not claim an
unauthorized cross-tenant exploit; shared-tenant hosting is out of scope.

## R14 — Production deployment and operational closure are incomplete

**Priority: release gate. Evidence: code-confirmed config risks and missing live
evidence. Repair: X19–X23.**

[production Compose](../../deployment/production/compose.yaml) passes one database
URL to migration, control-plane, and data-plane services despite distinct privilege
needs. Its example Flight location uses an external hostname/443 while the service
exposes 8815; verify separate bind and advertised endpoint configuration. Test TLS
file ownership with actual unprivileged containers. Readiness must prove useful
dependencies, not merely HTTP liveness.

Local fixtures share application security code, but HTTP/development IdP shortcuts
do not demonstrate production TLS, cookie, proxy, and restart parity. Backup/PITR,
restore invalidation, upgrade drains, rotation, aggregate capacity, and alerts lack
complete release evidence. Sanitizing HTTP errors is not enough if provider exception
tracebacks log secrets: [data-plane health](../../src/dal_obscura/data_plane/interfaces/health.py)
uses `LOGGER.exception` for readiness failures. Scan logs/traces as well as responses
with sentinel credentials.

## R15 — Test coverage, efficiency, and cleanup need behavioral ownership

**Priority: medium; missing security/live tests remain release gates. Repair: X00,
X09, X21, X22.**

[UI package scripts](../../apps/governance-ui/package.json) contain build/type checks
but no behavioral test command. There is no complete browser/real-IdP acceptance
lane tied to the production artifacts. Focused SQLite success is not PostgreSQL
race evidence. Existing release CI must prove that the exact promoted server and
UI images, including every advertised architecture, are those tested and scanned.

The workspace test helper defines `_client()` twice. Several compatibility objects
and discovery helpers appear redundant, but reference and serialization analysis
must precede deletion. Do not call a module useless merely because one search finds
no direct import. Replace fixed waits with bounded readiness polling where present;
profile test duration before consolidating fixtures. Avoid repeated complete-suite
runs for unrelated documentation or UI styling changes.

## Follow-up reconciliation

The review above is intentionally retained as the historical 2026-09-12
baseline. Subsequent implementation slices addressed the reproduced local
probes as follows:

| Historical probe | Current evidence | Current status |
| --- | --- | --- |
| Initial publication activated unrelated drafts | `tests/control_plane/test_policy_version_service.py` and selected-asset publication API tests; X01 ledger entries | Local regression fixed; PostgreSQL/Flight publication evidence remains open. |
| Review token authorized changed shared rules | Immutable draft/review hash and policy-version tests; X02/X03 ledger entries | Local snapshot binding and lock ordering fixed; multi-process race/recovery evidence remains open. |
| Literal dotted field collided with a nested path | `tests/control_plane/test_evaluation_service.py`, `tests/common/query_planning/test_field_paths.py`, and DuckDB transform tests | Canonical typed paths now preserve the distinction. |
| Collection field IDs collided in schema digest | `tests/control_plane/test_schema_service.py` and published-config schema-admission tests | Canonical nested/collection identity digest now includes IDs and shape. |
| Provider class-loader options were admitted | `tests/interfaces/control_plane/test_catalogs_api.py::test_workspace_catalog_rejects_nested_dynamic_loader_options` | Nested loader keys are rejected before provider construction. |

These tests prove the local code paths at the current commit; they do not prove
the live PostgreSQL, provider, TLS/OIDC, browser, consumer, recovery, capacity,
artifact, or independent-review gates. The unresolved pickle constraint is
deliberate and remains covered by the X00 compatibility fixtures.

## Evidence recorded during this review

The following focused command completed successfully at the baseline:

```sh
UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest \
  tests/interfaces/control_plane/test_schema_api.py \
  tests/interfaces/control_plane/test_actor_auth.py \
  tests/control_plane/test_schema_service.py \
  tests/examples/test_demo_initialization.py -q
```

Separate in-memory probes produced:

```json
{
  "first_publication_status": 200,
  "published_targets": ["default.other", "default.users"],
  "literal_dot_paths": ["a.b", "a.b"],
  "collection_id_change_same_schema_digest": true,
  "review_before_shared_rule_change": 200,
  "shared_rule_change": 200,
  "publication_with_old_review": 200,
  "unrestricted_import_options_accepted": ["py-catalog-impl", "py-io-impl"]
}
```

Reproduction ingredients are existing `workspace_helpers._client`,
`workspace_helpers._provision_draft`, `_EvaluationCatalog` in `test_schema_api.py`,
`schema_service.schema_fingerprint`, and `evaluation_service._leaf_paths`.
[Acceptance cases](ACCEPTANCE.md) specify the complete failing expectations to turn
these probes into durable regressions. The first strict probe omitted bootstrap
enablement and failed during fixture setup; the corrected strict probe above passed
setup and demonstrated the stale review result. This is not hidden test success.

Not executed for this review: complete Python suite, browser journeys, PostgreSQL
race suite, live IdP, production Compose/TLS, consumer matrix, benchmarks, recovery
drill, or independent security review. CocoIndex refresh failed because its daemon
log was outside the writable sandbox; exact-text/source exploration used `rg`.
