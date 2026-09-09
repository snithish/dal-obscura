# Governed Iceberg gateway status ledger

Updated: 2026-09-09. This ledger follows the ordered work packages in
[WORK_PACKAGES.md](WORK_PACKAGES.md). A committed partial fix does not mark a
package complete unless its stated evidence exists.

| Package | State | Notes |
| --- | --- | --- |
| W00 | complete | Baseline and removal inventory recorded in `evaluation/baseline/`. |
| W01 | implementing | Focused regressions exist for parent masking, invalid masks, grant removal, and asset binding; streaming and mask-conflict regressions remain. |
| W02 | pending | Strict immutable models and typed field paths are not implemented. |
| W03 | pending | Existing nested projection remains transitional. |
| W04 | implementing | Fetch reauthorization is present; issuer/subject/expiry binding is not. |
| W05 | implementing | Asset binding is fixed; immutable publication generations are not. |
| W06 | pending | Existing ticket serialization remains by owner direction. |
| W07 | pending | Iceberg pinned scan specification has not begun. |
| W08 | pending | DuckDB/Spark consumer contract has not begun. |
| W09 | pending | Stream lifecycle and resource limits have not begun. |
| W10 | implementing | Asset admission is Iceberg-only; deletion inventory is recorded but removal waits for replacement paths. |
| W11 | pending | Client package/deployment privilege split has not begun. |
| W12 | pending | CI/test reorganization has not begun. |
| W13 | pending | Independent evaluation has not begun. |
| W14 | pending | Partner discovery has not begun. |
| W15 | pending | Release/hold/pivot decision awaits evaluation. |

## Current implementation commits

- `7ff6776`: nested child projections honour direct parent masks.
- `5d722c7`: publication rejects invalid mask definitions.
- `e79af83`: fetch reauthorizes current principal grants.
- `624dadf`: published assets bind their stored backend/table/options.
- `7cbf2c9`: control plane accepts Iceberg assets only.

These are W01/W04/W05/W10 inputs, not completion evidence for those packages.

