# Governance UI

Authenticated administrative application for dal-obscura. It manages live
configuration and policies; administrative permissions do not grant Flight data access.

## Workflows

- Search and browse governed assets through bounded inventory requests.
- Edit named policy rules, nested columns, row restrictions, and column masks.
- Save the complete live policy against its expected revision; discard local
  changes or reconcile a concurrent edit.
- Test the saved policy with a synthetic principal, groups, and attributes.
- Revoke outstanding asset tickets explicitly, or as part of a policy save.
- Manage catalog connections, owners, grants, authentication providers, and runtime settings.
- Obtain Python/DuckDB, Spark, and Arrow consumer instructions.

The policy workspace shows audience, column, mask, and revision metadata as tags.
Use **Browse columns** to search names or types, review selected paths, and select
or deselect only matching results. **Select all** uses every schema leaf;
**Select all except…** previews multiple field or subtree exclusions before
applying them; **Add by prefix** previews matches and keeps existing selections.
These controls change the local rule; **Save policy** persists the complete policy.

**Show schema** opens a searchable, collapsible reference beside the rules on wide
screens and above them on narrow screens. It shows nested paths, types, and
nullability without changing access. The rules toolbar has one **New rule** action
that stays visible while scrolling on desktop and mobile. It appends a rule,
opens its editor, and focuses its name. Rule footers contain only duplicate and
delete actions for that rule. Inventory owner tags open
the complete principal identity, including its issuer, without stretching rows.

See the [visual before/after review](../../docs/ui-review/review.html) for the
design rationale and representative browser captures.

Edits remain local until saved. Successful policy saves immediately affect new
plans; issued tickets retain captured access until expiry unless revoked.
Runtime and authentication startup settings require worker restarts. See
[policy authoring](../../docs/policy-authoring.md) and
[live configuration](../../docs/live-configuration.md).

## Development

From the repository root:

```bash
pnpm --dir apps/governance-ui install --frozen-lockfile
pnpm --dir apps/governance-ui dev
```

Vite proxies `/v1` and `/auth` to `http://127.0.0.1:8821`. The normal route starts
signed out. OIDC uses the authorization-code flow with PKCE. Disposable local
profiles can enable the bootstrap token form with
`DAL_OBSCURA_CONTROL_PLANE_BOOTSTRAP_ENABLED=true`; it exchanges the configured
admin token for an HttpOnly session and CSRF cookie. Production disables bootstrap
and requires SSO. The development proxy is not a production authorization boundary.

**Sign out** clears private UI state, revokes the app session, and sends the browser
to the configured provider logout endpoint. Confirm sign-out on the provider's page
when prompted; a subsequent SSO sign-in should ask for credentials again. The app
does not retain ID tokens. Bootstrap-only profiles clear only the local session.
Configure `DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_END_SESSION_ENDPOINT` for providers
whose logout endpoint differs from the default Keycloak path. Register the exact
`DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGOUT_REDIRECT_URI` with the provider.

## Checks

```bash
pnpm --dir apps/governance-ui check
pnpm --dir apps/governance-ui test
pnpm --dir apps/governance-ui test:stories
pnpm --dir apps/governance-ui test:e2e
pnpm --dir apps/governance-ui build
```

Storybook is available with `pnpm --dir apps/governance-ui storybook`. Browser tests
need their configured services and browser runtime; use the
[Keycloak demo](../../examples/demo/keycloak/README.md) for live OIDC checks.
See [control-plane API](../../docs/control-plane-api.md) for generated-contract updates
and [production deployment](../../deployment/production/README.md) for serving the built app.
