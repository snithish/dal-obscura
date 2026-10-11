# Wide selection review

The matched synthetic fixture contains 1,000 selected fields. Before captures use
`fee62039`; after captures use the bounded selection implementation. The fixture
uses authenticated HTTP responses with no live provider or policy publication.

- [Before desktop](before-1440.png) and [after desktop](after-1440.png).
- [Before mobile](before-390.png) and [after mobile](after-390.png).

The rule now shows at most five chips and an explicit review action. Selection
review has its own search, count, bounded row window, and removal buttons. Mobile
operators switch between browsing available fields and reviewing selections;
the old mobile layout hid the review panel. Full names remain in accessible button
labels and titles; the schema reference provides exact path inspection.

The available-fields picker retains its existing combobox/listbox contract. The
review uses ordinary removal buttons rather than interactive controls inside
listbox options, following the [W3C listbox guidance](https://www.w3.org/WAI/ARIA/apg/patterns/listbox/).
The modal header is presentation, while the dialog retains its accessible title.

A separate behavioral fixture exercises 10,000 selections at 1440/390/320 pixels,
keyboard focus, draft-only removal, unavailable selections in component stories,
zero automated accessibility violations, and local search p95 below 200 ms.
This browser fixture does not increase backend policy, schema, Flight descriptor,
or consumer limits, or qualify a live provider. No claim of unbounded schemas is
made. The browser still receives the complete admitted schema.

Capture with `DAL_OBSCURA_REVIEW_PHASE=before|after pnpm --dir apps/governance-ui capture:review wide-selection-review.spec.ts`.
