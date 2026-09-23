# Governed Iceberg gateway status ledger

> **Historical progress record.** Its publication-generation, draft/review,
> and bundle requirements were superseded by the [live-configuration decision](../decisions/2026-09-live-configuration.md).
> Do not use its packet states as the current implementation checklist.

Updated: 2026-09-12. This ledger follows the ordered work packages in
[WORK_PACKAGES.md](WORK_PACKAGES.md). A committed partial fix does not mark a
package complete unless its stated evidence exists.

| Package | State | Notes |
| --- | --- | --- |
| W00 | complete | Baseline and removal inventory recorded in `evaluation/baseline/`. |
| W01 | implementing | Focused regressions exist for parent masking, invalid masks, grant removal, and asset binding; streaming and mask-conflict regressions remain. |
| W02 | implementing | Immutable request models reject ambiguous projections; versioned typed paths resolve struct/list/map nodes and distinguish literal dotted names. Python Flight and Java client encode typed protobuf paths; Java tests cover literal dotted, list, map, and wildcard discovery paths. End-to-end field-ID binding and cross-language golden fixture coverage remain. |
| W03 | implementing | Planning and row-filter schema checks share canonical paths; DuckDB prunes and masks map-value and list-element struct leaves while preserving keys and list/null shape. Map-value projections now require key authorization; redact preserves null/empty semantics and malformed email masks become null. Nested grants, compound masks and collection predicates remain. |
| W04 | implementing | Fetch reauthorization plus issuer/subject/identity-expiry ticket binding are present. JWKS refreshes are rate-limited and accepted signing keys are bounded; exact Flight endpoint evidence and stream expiry checks remain. |
| W05 | implementing | Asset binding is fixed; published authorization versions bind policy revision to immutable publication UUID, invalidating tickets after every activation. Request-scoped publication snapshots and stream generation freshness remain. |
| W06 | pending | Existing ticket serialization remains by owner direction. |
| W07 | implementing | Format v3 now fails closed; native multi-file tasks stay pinned across append. Delete, schema-evolution and REST-catalog behavior remain unproven; native nested field-ID preservation is covered. |
| W08 | implementing | Python SDK provides a managed sequential Flight stream, opt-in typed path transport, and explicit Polars materialization; DuckDB remains adapter-only. It accepts a token-refresh callback and resolves it for every schema, plan, and ticket-stream RPC. Spark driver and partitions use credential references, resolved from environment variables or JVM properties. Spark partition readers clean up streams and clients after failed construction and repeated close. Real Flight socket, retry/speculation/cancellation, and production token-refresh evidence remain. |
| W09 | implementing | DuckDB streams have configured non-blocking admission, one execution thread, memory and input-batch limits; planned scan payloads have a byte limit. Identity, ticket expiry, active-generation freshness, and a configured delivery deadline are checked before each emitted batch; DuckDB output batches have a byte limit. Audit events and per-value limits remain. |
| W10 | implementing | Asset admission is Iceberg-only and the operator CLI supports validation, offline preview, CAS publish, and status. Delta/file/Unity runtime paths are removed. The React governance UI now has OIDC/PKCE sessions, scoped owners/grants, nested schema, drafts, review/publication, history/restore, activity, catalog discovery, asset onboarding, and local/production packaging. Browser and real consumer evidence remain. |
| W11 | implementing | The default wheel now depends only on PyArrow and protobuf; DuckDB, Polars, and server dependencies are explicit extras, and the SDK lazy-loads DuckDB. Separate distributable artifacts and deployment privilege separation remain. |
| W12 | implementing | CI separates deterministic contract/security tests from a bounded integration lane, a named native-Iceberg conformance lane, default-client wheel smoke, privileged package smoke, and JVM gates. Spark unit/client checks pass; the full JVM `mvn verify` lane currently stops at fixture tests that need a socket, so no green full-lane claim is retained. PostgreSQL race, capacity, and cross-consumer lanes remain. |
| W13 | pending | Independent evaluation has not begun. |
| W14 | implementing | A design-partner decision packet exists; external discovery has not begun. |
| W15 | pending | Release/hold/pivot decision awaits evaluation. |

## Current implementation commits

- `7ff6776`: nested child projections honour direct parent masks.
- `5d722c7`: publication rejects invalid mask definitions.
- `e79af83`: fetch reauthorizes current principal grants.
- `624dadf`: published assets bind their stored backend/table/options.
- `7cbf2c9`: control plane accepts Iceberg assets only.
- `d8cb6ba`: malformed and ambiguous projection requests reject at the model boundary.
- `65ea54d`: canonical typed paths distinguish quoted literal names and resolve nested Arrow nodes.
- `482663b`: row-filter dependency validation uses canonical paths.
- `ad18421`: catalog construction uses a single captured publication generation.
- `e5e5b66`: Flight accepts canonical typed paths and fetch reauthorization preserves client filter dependencies.
- `dfc0fe3`: Python client exposes a managed, cancellable Flight batch stream.
- `21c4b77`: DuckDB stream admission and per-connection resource limits are enforced.
- `3289af5`: Arrow input batches have a configured byte limit.
- `72ef40d`: planned ticket scan payloads have a configured byte limit.
- `e255761`: CI separates contract/security and bounded integration test lanes.
- `pending`: fetch rejects same-subject tickets issued by another issuer before reservation.
- `pending`: Iceberg v3 admission now fails closed pending native conformance evidence; capability inventory records W07 blockers.
- `pending`: native two-file plans remain on their original snapshot after a later append.
- `pending`: native nested schema loading preserves PyIceberg Arrow field IDs.
- `pending`: DuckDB preserves map keys while pruning and masking selected map-value struct leaves.
- `pending`: DuckDB prunes and masks canonical list-element struct leaves without flattening lists.
- `pending`: fetch stops streaming before handing over a batch after identity expiry.
- `pending`: DuckDB rejects transformed output batches above configured byte limits.
- `pending`: authorization versions bind ticket checks to active publication UUIDs, including identical-policy republishes.
- `10f9437`: Python SDK exposes explicit governed Arrow-to-Polars materialization; masked primitive and nested values are covered.
- `2d03d0a`: streams recheck the effective active-publication policy version before each emitted batch.
- `f1a5a19`: map-value planning adds map keys as an authorization dependency and emitted structural path.
- `9094a2e`: redact null/empty semantics and malformed-email masking are enforced at publication and execution.
- `7ce68d0`: Spark integration fixture uses an RS256 token and static JWKS through the OIDC-only production runtime.
- `c72491e` and `5426f48`: parent requests are pruned to nested grants, including Flight schema/data evidence.
- `4165ffd`: Java plans encode canonical typed protobuf paths, including quoted and collection segments.
- `555fef1`: Spark partitions and serializable reader factories exclude credentials; executor token references resolve at execution time.
- `86df7ae`: default Python client wheel excludes server dependencies and lazy-loads DuckDB.
- `01ceb39`: configured stream delivery deadline stops output before a late batch is handed to Flight.
- `edba5e5`: Spark driver and executor token references avoid raw credentials in datasource options and serialized work.
- Spark reader cleanup is idempotent and closes its client when ticket-stream construction fails.
- Python Flight RPCs resolve an explicit bearer-token provider per request and reject empty refresh results.

These are W01/W04/W05/W10 inputs, not completion evidence for those packages.
