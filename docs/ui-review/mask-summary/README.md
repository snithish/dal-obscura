# Bounded mask summaries

The previous mask cards mounted every path, making large groups fill many screens
and hiding the surrounding rule context. Masked and original-value summaries now
show at most five exact-path previews and a native keyboard disclosure for read-only
inspection. Search reports its scope and exact match count; pages mount at most
50 paths in a keyboard-focusable 288 px scroll area. Navigation reports the visible
range. Review remains available without edit permission and never applies a mask
or dirties a policy draft. Full paths wrap in review; preview truncation retains a
full title. Exemption lists and the number of distinct mask groups remain separate
scaling concerns.

This reuses existing inputs and native disclosure/list semantics. Inspection has
no selection or mutation actions, so controls remain understandable in read-only
states. Viewing fields and editing masks retain distinct actions.

Captures use the same 1,000-field hash-mask fixture, Chromium, 900 px height,
1440/390 px widths, and loaded fonts. Before captures use the committed
`1af26816` BulkMaskEditor; after captures use the final implementation. No live
provider data or secrets appear. The page is aligned to the same mask-card context.

- [Before desktop](before-1440.png) → [After desktop](after-1440.png)
- [Before mobile](before-390.png) → [After mobile](after-390.png)
- [Open desktop review](review-1440.png), [open mobile review](review-390.png)

```bash
pnpm --dir apps/governance-ui capture:review mask-summary-review.spec.ts
```

Behavioral checks are separate from captures. A 5,000-field fixture verifies bounded
DOM, exact search/page values, empty search results, keyboard close/focus, no HTTP
writes and axe checks at 320/390/1440 px. A read-only story verifies disabled mask
editing while inspecting an exact long Unicode literal path. Original-value review
uses the same inspection boundary. Existing local wide-form typing remains under
its unchanged 200 ms p95 gate. Full-schema transfer/search allocation and live
operator/field performance qualification remain open.
