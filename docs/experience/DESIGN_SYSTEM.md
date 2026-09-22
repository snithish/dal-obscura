# Obscura Workbench — design system and Storybook specification

Owner: R04/R05; adoption: R06–R08. This replaces the old indigo/green/Georgia
visual direction in docs/plugin-platform/UX_REQUIREMENTS.md. Its functional
workflows remain required. The owner has rejected the current visual result;
existing theme tokens and icons alone do not constitute acceptance.

## Research and selected direction

Choose a precise, calm data workbench: neutral surfaces, one cobalt action accent,
clear row hierarchy, compact productive typography and visible governance state.
Avoid oversized editorial headlines, green branding mixed with purple controls,
decorative KPI cards and repeated bordered cards around every field.

Carbon distinguishes productive typography for task-focused interfaces from
expressive editorial typography. Use that principle, not the entire Carbon runtime.
Select IBM Plex Sans for interface text and IBM Plex Mono for SQL/IDs.
[Carbon typography](https://carbondesignsystem.com/elements/typography/overview/),
[IBM Plex source and OFL license](https://github.com/IBM/plex)

Radix Colors assigns different roles to backgrounds, interactive states, borders
and readable text. Apply that separation through semantic tokens instead of
allowing arbitrary color literals in components. Our concrete palette below is
a product choice, not a claim that it is an official Radix palette.
[Radix scale roles](https://www.radix-ui.com/colors/docs/palette-composition/understanding-the-scale)

## Component library decision — Mantine

Use **Mantine** as the single production component library. This supersedes the
previous Radix-plus-custom-primitives proposal. Mantine packages are MIT-licensed,
have published maintenance releases, documented component APIs and an official
Storybook integration. Support here means maintained open-source documentation,
issues and releases, not a contracted vendor SLA.
[License and setup](https://mantine.dev/getting-started/),
[releases](https://github.com/mantinedev/mantine/releases),
[Storybook integration](https://mantine.dev/guides/storybook/)

Start with @mantine/core and @mantine/hooks on the same exact supported stable
version. R01 verifies React/Vite/TypeScript/Storybook peer compatibility, license
notices and advisories before pinning; do not install a floating latest tag.
Keep Lucide named imports. Do not add Radix, shadcn, MUI, Carbon components,
Tailwind or a parallel homegrown primitive library. Optional Mantine packages
require a concrete current workflow that core cannot cover; no speculative installs.

Use these existing components directly:

- Shell/navigation: AppShell, NavLink, Breadcrumbs, Burger and Drawer.
- Forms: TextInput, Textarea, NativeSelect/Select, Checkbox, Switch and NumberInput
  only where its value semantics match the API. Keep schema-driven validation.
- Actions/status: Button, ActionIcon, Badge, Alert, Loader and Skeleton.
- Overlays/review: Modal, Menu, Tooltip, Tabs and Accordion. Use Modal, not the
  non-modal Dialog, for trapped focus and confirmation boundaries.
- Inventory/audit: Table and Pagination with existing server queries. Do not
  introduce an additional grid package for already supported table requirements.
- Command search: Modal plus Combobox with documented keyboard semantics.

Keep custom code for domain behavior: nested virtual schema, policy rules,
semantic diff and publication reconciliation. Do not replace the proven virtual
tree with Mantine Tree unless the 10k-node performance/keyboard gates pass.
No wrapper around every Mantine component and no generic re-export facade:
shared wrappers must encode a repeated product behavior, not rename props.
[Component catalog and shell](https://mantine.dev/core/app-shell/)

R04 starts with a small built-app compatibility slice: themed Button/TextInput,
Modal, Select popup and responsive shell under the real production CSP. Measure
JS/CSS and inspect CSP violations before migrating all screens. Prefer CSS Modules
and supported Styles API overrides. Mantine can generate style elements and inline
styles; a nonce on style elements does not authorize style attributes. Verify
the selected release's actual output. Use a server-generated per-response nonce
only where needed and supported; never add unsafe-inline/unsafe-eval, disable CSP,
fork the library or silently relax budgets. If a required component cannot meet
these gates through supported APIs, record the blocker before broad migration.
[MantineProvider and nonce support](https://mantine.dev/theming/mantine-provider/),
[Styles API](https://mantine.dev/styles/styles-api/)

## Exact starting tokens

The visual board shows these candidates. Values may change only with a measured
contrast check and an intentional visual-review update, not per-page exceptions.

- Canvas: light #F6F7F9; dark #10151E.
- Surface: light #FFFFFF; dark #171F2C.
- Raised/subtle surface: light #EDF1F5; dark #202C3D.
- Primary text: light #182230; dark #E7ECF4.
- Secondary text: light #526076; dark #ADB9CA.
- Decorative divider: light #D9E0E8; dark #334155.
- Control boundary: light #7A8799; dark #7D8FA7. Decorative dividers must not
  serve as the only visible boundary of an input.
- Primary action/link: light #175CD3; dark #85B4FF.
- Primary action foreground: light #FFFFFF; dark #0C1525.
- Primary hover: light #144CB0; dark #A8CAFF.
- Selection: light #E8F0FF with #184A9B text; dark #203D68 with #D7E7FF text.
- Focus: light #175CD3; dark #A8CAFF; 2px outline with 2px offset, visible even
  beside the primary fill. Use a contrasting surface gap where needed.
- Success: light #157347 on #E8F5ED; dark #8BDEB0 on #163627.
- Warning: light #8A4B00 on #FFF3D6; dark #FFD084 on #3B2B12.
- Danger: light #B42318 on #FEECEB; dark #FFB4AB on #42201F.
- Informational/draft: use the cobalt selection pair; unknown/pending uses neutral
  tokens and explicit wording, not green success.

Calculated opaque-pair contrast in this review: primary text/surface 16.03:1
(light), 13.95:1 (dark); secondary text/surface 6.38:1 and 8.33:1;
primary button foreground/fill 5.99:1 and 8.67:1; light control boundary/white
3.65:1. Light success/warning/danger text pairs are 5.23/6.17/5.76:1.
These arithmetic checks do not establish rendered accessibility: opacity,
hover/focus, overlays, disabled states and forced colors still need E09/E10.
[WCAG contrast criterion](https://www.w3.org/WAI/WCAG22/Understanding/contrast-minimum.html)

One Mantine theme module and its CSS-variable resolver define light/dark values.
Map the semantic palette through supported theme/component defaults; do not copy
the same constants into independently maintained CSS. Reuse Mantine's color-scheme
manager rather than keeping a second theme state machine. System preference resolves to an
effective light/dark theme; components consume only semantic values. Remove
selectors treating “not explicit light” as equivalent to dark. Browser storage
may persist theme/density preferences, never private policy/session data.

## Typography, density and iconography

- IBM Plex Sans: labels/table cells 14px/20px at 400–500; body/help 16px/24px;
  section headings 20px/28px at 600; page heading 28px/36px at 600.
- IBM Plex Mono: SQL/schema types/IDs 13px/20px, with 14px where edited. Tabular
  numerals for counts, times and revisions. No 10px essential status labels.
- Self-host versioned WOFF2 assets; preserve OFL notices. Ship only required
  language subsets and weights, retain fallback glyph coverage, font-display: swap.
  Preload only the primary face. No Google Fonts/CDN requirement in the product.
  Initial font transfer <=120 KiB; verify fallback layout and offline loading.
- Spacing scale: 4, 8, 12, 16, 24, 32, 48px. Controls 40px desktop; important
  mobile targets 44px. Default data rows 44px, optional compact desktop rows 36px.
  Corners 6px controls/8px panels; pill corners reserved for small status labels.
- Sidebar 224px; app header 64px; page padding 24–32px desktop/16px mobile.
  No permanent 76px horizontal margins inside the data workspace.
- Lucide 16px inline and 20px controls/navigation, consistent stroke width.
  Database/Plug/Activity/Settings/History/ShieldCheck remain consistent meanings.
  Pair destructive/security actions with text. Icon-only buttons require an
  accessible name; tooltips are supplementary.

## Interaction and information architecture

Primary navigation: Assets, Connections, Activity, Settings. Move Changes into
the selected asset's workspace and retain a global recent-changes filter within
Activity if useful. Avoid two apparently different publish/review concepts.
Header: breadcrumbs, command search, environment label and account menu.
Show the selected asset and Saved draft / Active policy separately at all times.

**Assets:** real searchable inventory table with catalog/format, governance status,
owner and last publication. Empty state explains Connect → Govern → Author →
Publish with a permission-aware next action. Search-empty, forbidden and failed
load states are distinct. Do not begin with a small select box as the entire inventory.

**Policy workspace:** schema navigator on the left, rule editor in the center,
impact/test inspector in an optional right panel. Use clear tabs for Policy,
Test access, Review, History and Consume. At narrower widths switch between these
panels explicitly; never compress three unusable columns. Sticky action footer:
Save draft, Test access, Review changes; Publish appears in reviewed context.

**Authoring:** preserve all existing masks, filters, conditions, rule order,
undo/redo and deny-all. Show claim conditions as human-readable rows. The current
backend supports AND between claims and equals/one-of text values; do not draw
unsupported OR/type controls or silently coerce them. Advanced JSON is a lossless
alternative. Expose SQL as an advanced validated expression with useful errors.
Schema unavailable means unavailable, never index-derived field identities.

**Review:** semantic before/after changes, actor/author, saved revision, schema
and activation impact. Changes are explained as rows, not a raw JSON dump.
Unknown publication outcome has a persistent Reconcile action. An editor's review
link pins exact revision; publishers cannot accidentally review later content.
Restore clearly creates a saved draft and never activates it.

**Connections/settings:** descriptor-driven typed forms grouped by identity,
connectivity and credential references. Prefill safe values. Validate and Activate
are separate actions, showing scope and active generation. Last-admin changes have
explicit continuity checks; secrets never appear as normal text fields.

**Activity/consume:** readable scoped timeline/table, real filters and pagination,
redacted details and request IDs. Flight health and control-plane reachability are
separate. Show exact consumer endpoint/version support and copy feedback; do not
advertise the web hostname as a Flight endpoint.

**Login/local setup:** normal SSO action, environment name, clear unavailable/
expired/denied states and a compact diagnostic link. Do not show bootstrap tokens
as the normal local experience. Explain app sign-out versus shared-browser SSO
without exposing protocol internals on the main happy path.

All forms retain input after validation errors, explain conflicts and warn before
discarding edits. Keyboard shortcuts never publish silently. Every mutation shows
pending/succeeded/failed/unknown truthfully. Reduced motion removes transitions;
default transitions <=150ms. Use no celebratory animation around permission changes.

## Storybook is the designbook

Implement one Storybook inside apps/governance-ui. Use the React/Vite integration,
Docs, accessibility and Vitest addons at verified compatible stable versions.
Storybook's Vitest integration can execute stories in a real browser; the
accessibility addon can participate in those checks. This avoids a second set of
near-identical component tests.
[Storybook Vitest integration](https://storybook.js.org/docs/writing-tests/integrations/vitest-addon),
[Storybook accessibility tests](https://storybook.js.org/docs/writing-tests/accessibility-testing)

Proposed locations (not created by this plan):

- src/design/theme.ts and AppProviders.tsx, typography.css and licensed font assets:
  shared Mantine theme, semantic variable resolver and provider configuration.
- Feature components: direct Mantine imports; shared components contain only
  repeated domain compositions, never a replacement Button/Input/Modal library.
- .storybook/main.ts and preview.tsx: same Mantine styles/theme/fonts as production,
  theme and viewport toolbar, synthetic session/query fixtures.
- Colocated *.stories.tsx: import the actual production component.
- designbook/*.mdx: foundations, patterns, copy, accessibility and contribution
  guidance, with live stories embedded rather than copied HTML.

Navigation within Storybook:

1. Foundations: color roles, full contrast matrix, typography samples, spacing,
   density, icons, light/dark/System behavior and motion.
2. Components: the Mantine components actually used, with configured product
   variants and representative states; link upstream APIs instead of copying
   their entire documentation or recreating upstream component tests.
3. Patterns: field validation, async panel, permission explanation, policy status,
   semantic diff, confirmation, empty state and unknown-outcome recovery.
4. Workflows: synthetic Login, Asset inventory, Nested policy editor, Review,
   Connections and Administration/Activity compositions.
5. Contribution: token ownership, story checklist, test ownership and visual approval.

For each component record purpose, supported props/variants, keyboard behavior,
content guidance, token use and known limitations. Mock only network boundaries
with deterministic fixtures. Storybook must never fetch a real IdP, source data
or customer API; fail tests on unexpected outbound requests.

Storybook's isolated iframe needs its own compatible framing policy; never weaken
the production application's frame restrictions or CSP to embed it. Serve a
separate static storybook build locally; optional sharing uses its own Access policy.
[Storybook assets](https://storybook.js.org/docs/configure/integration/images-and-assets)

## Consistency and economical testing rules

The app imports the components shown in Storybook. A visually identical copied
storybook-only component fails acceptance. New color/font/spacing values outside
the token layer require an explicit design decision; enforce simple style linting,
not snapshot tests of source code text.

Prefer one story play test as the primary component behavior oracle. Keep separate
unit tests for real algorithms and one real-backend Playwright journey per distinct
workflow boundary. Do not run a full role × viewport × theme × field-type Cartesian
browser matrix. Use representative compositions for visual snapshots, both themes,
390px/1440px widths and explicit state checks. Manual screen-reader, keyboard,
200% zoom and owner review remain necessary.

Command search follows a complete combobox/listbox keyboard contract if using those
roles; use accessible primitives for its modal boundary. Test arrow navigation,
selection, Escape, focus restoration and inert background. [WAI-ARIA combobox](https://www.w3.org/WAI/ARIA/apg/patterns/combobox/)

No screen is complete until the real API workflow and the component/story visual
contract both pass. “We added icons” or “axe passed on login” is insufficient.
