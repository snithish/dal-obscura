# Catalog discovery review

The initial improvement-plan delivery replaces complete inventories with 50-row
pages, visible navigation, an explicit empty state, and recoverable page failures.
Mobile rows retain the table action without horizontal scrolling; adapter and
governed status move beneath the exact table name. Desktop retains separate
columns and a bounded scroll viewport with a sticky header.

Captures use a synthetic 150-table inventory and the same browser viewport/data.
The baseline is the actual UI at commit `0c1d89f0`, built in a temporary source
copy. After captures use the current implementation. These images establish
rendered layout, not provider correctness or live-cloud performance.

| Viewport | Before | After |
| --- | --- | --- |
| 1440 × 1000 | [Desktop](before-1440.png) | [Desktop](after-1440.png) |
| 390 × 1000 | [Mobile](before-390.png) | [Mobile](after-390.png) |

The owning correctness lane is
`apps/governance-ui/e2e/catalog-discovery.spec.ts`: paging replaces rather than
accumulates rows; retry preserves the current page; empty discovery is explicit;
320/390-pixel layouts keep actions in view and support keyboard focus. Automated
WCAG rules are scoped to the discovery section. Existing management/race tests
remain separate. Manual screen-reader qualification remains in the broader plan.

Reproduce current captures with pinned Node 24 and pnpm 12.3.4:

```sh
pnpm --dir apps/governance-ui exec playwright test \
  --config playwright.review.config.ts review/catalog-discovery-review.spec.ts
```

Baseline captures additionally set `DAL_OBSCURA_REVIEW_PHASE=before` and
`DAL_OBSCURA_REVIEW_OUTPUT` to this directory in the temporary baseline copy.
