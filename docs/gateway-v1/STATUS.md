# Governed Iceberg gateway status ledger

Updated: 2026-09-10. This ledger follows the ordered work packages in
[WORK_PACKAGES.md](WORK_PACKAGES.md). A committed partial fix does not mark a
package complete unless its stated evidence exists.

| Package | State | Notes |
| --- | --- | --- |
| W00 | complete | Baseline and removal inventory recorded in `evaluation/baseline/`. |
| W01 | implementing | Focused regressions exist for parent masking, invalid masks, grant removal, and asset binding; streaming and mask-conflict regressions remain. |
| W02 | implementing | Immutable request models reject ambiguous projections; versioned typed paths resolve struct/list/map nodes and distinguish literal dotted names. Field IDs and protobuf/Java fixtures remain. |
| W03 | implementing | Planning and row-filter schema checks share canonical paths; DuckDB now prunes and masks map-value and list-element struct leaves while preserving keys and list/null shape. Nested grants, compound masks and collection predicates remain. |
| W04 | implementing | Fetch reauthorization plus issuer/subject/identity-expiry ticket binding are present. JWKS cache bounds, exact Flight endpoint evidence and stream expiry checks remain. |
| W05 | implementing | Asset binding is fixed and catalog construction reads asset/catalog from one captured generation; immutable plan generations and stream freshness remain. |
| W06 | pending | Existing ticket serialization remains by owner direction. |
| W07 | implementing | Format v3 now fails closed; native multi-file tasks stay pinned across append. Delete, schema-evolution and REST-catalog behavior remain unproven; native nested field-ID preservation is covered. |
| W08 | implementing | Python SDK provides a managed sequential Flight stream and opt-in typed path transport; DuckDB remains adapter-only and Spark/distributed attempt support remain. |
| W09 | implementing | DuckDB streams have configured non-blocking admission, one execution thread, memory and input-batch limits; planned scan payloads have a byte limit. Generation freshness, deadlines, audit events, and output/value limits remain. |
| W10 | implementing | Asset admission is Iceberg-only; deletion inventory is recorded but removal waits for replacement paths. |
| W11 | pending | Client package/deployment privilege split has not begun. |
| W12 | implementing | CI separates deterministic contract/security tests from a bounded integration lane while retaining package and JVM gates. PostgreSQL race, native Iceberg conformance, capacity, and cross-consumer lanes remain. |
| W13 | pending | Independent evaluation has not begun. |
| W14 | pending | Partner discovery has not begun. |
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

These are W01/W04/W05/W10 inputs, not completion evidence for those packages.
