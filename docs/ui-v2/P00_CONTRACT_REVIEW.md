# P00 control-plane contract review packet

This packet freezes the observable administrative API before session, grant, or
durable-workflow changes. It is preparation for the review required by
[the execution handoff](EXECUTION_HANDOFF.md), not approval to introduce new
persistent state or select a new authorization model.

`tests/architecture/test_control_plane_route_inventory.py` checks the current
OpenAPI path set and two method sets. It does not verify this document's complete
method or authorization inventory. Endpoint changes must update this document;
P00 must add complete method/contract coverage and P03 must test permissions.

See [PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) for the current release
blockers, per-workflow backend requirements, and production gates P13–P16.

## Current route and authorization inventory

The labels in this table describe current code, not the target authorization
contract. `Actor` means any bearer-authenticated or current cookie-authenticated
identity. `Admin` means the static bootstrap token or an OIDC identity in the
configured administrator group. A row marked `public` has no authentication
dependency.

| Method | Path | Current gate | Current purpose | P00 finding |
| --- | --- | --- | --- | --- |
| GET | `/healthz` | public | process liveness | retain public |
| GET | `/readyz` | public | database readiness | retain public; do not expose internals |
| GET | `/v1/ui-auth-config` | public when configured | browser OIDC configuration | retain public safe metadata only |
| POST | `/v1/demo-login` | public when configured | password-grant demo shortcut | remove from default supported path in P02 |
| POST | `/v1/logout` | Actor + CSRF for cookie client | expires current browser cookies | replace with revocable-session logout in P02 |
| GET | `/v1/session` | Actor | actor metadata | replace with safe session/capability response |
| GET | `/v1/workspace/summary` | Actor | workspace inventory summary | scope by workspace capability |
| GET | `/v1/catalogs` | Actor | catalog list | scope by workspace and connection visibility |
| PUT | `/v1/catalogs/{name}` | Admin | catalog registration | connection-operator capability, scoped |
| GET | `/v1/catalogs/{name}/tables` | Actor | catalog discovery | connection diagnostic/read capability, scoped |
| GET | `/v1/assets` | Actor | asset list | metadata-viewer capability and list filtering |
| GET | `/v1/assets/{asset_id}` | Actor | asset detail | metadata-viewer capability and direct-ID scope check |
| PUT | `/v1/assets/{catalog}/{target}` | Admin | asset registration | connection-operator capability, scoped |
| PUT | `/v1/assets/{asset_id}/owners` | Admin | owner replacement | grant-administrator capability, scoped |
| PUT | `/v1/assets/{asset_id}/schema-fields` | Admin | schema-field replacement | replace with revisioned asset/schema workflow after P05/P06 review |
| GET | `/v1/assets/{asset_id}/policy-rules` | Actor | policy rules | asset policy-read capability; no policy body disclosure to metadata viewer |
| PUT | `/v1/assets/{asset_id}/policy-rules` | Actor, service checks owner/editor | draft rule replacement | replace with revision-preconditioned draft mutation |
| POST | `/v1/assets/{asset_id}/policy-preview` | Actor | policy preview | synthetic-evaluate capability; bind to exact draft/revision |
| POST | `/v1/assets/{asset_id}/policy-versions` | Actor, service checks owner/editor | create and activate policy version | replace with exact-review then explicit publish capability |
| GET | `/v1/policy-versions` | Actor | version history | auditor/editor scoped history visibility |
| GET | `/v1/settings/runtime` | Actor | runtime settings | dedicated workspace settings-read capability |
| PUT | `/v1/settings/runtime` | Admin | runtime settings mutation | dedicated workspace settings-write capability |
| GET | `/v1/settings/auth-providers` | Actor | authentication-provider configuration | sensitive settings-read capability |
| PUT | `/v1/settings/auth-providers` | Admin | authentication-provider configuration | dedicated workspace security-settings capability |

## Required contract decisions

The capable owner/reviewer must explicitly accept or revise these proposals
before P02 or P03 changes application-session, grant, or migration behavior.

1. **Browser session.** Use an opaque, random HttpOnly cookie whose server-side
   record stores only a hash of its identifier, the subject, expiry timestamps,
   and minimal protected provider-token material. Enforce idle and absolute
   expiry, revoke on logout, and prefer reauthentication to first-release token
   refresh. Keep Flight workers stateless.
2. **Persistence.** Add only additive control-plane migrations for sessions,
   grants, drafts, evaluations, operations, and audit records. Define retention,
   backup/restore behavior, and migration rollback/recovery tests before writing
   those migrations.
3. **Capabilities.** Deny unspecified actions. Authorize every list, count,
   cursor, direct ID, history, export, preview, and publication-impact request by
   workspace and asset or connection scope. The proposed personas are reader,
   metadata viewer, asset owner/editor, publisher, connection operator, grant
   administrator, and auditor, with the separation in the execution handoff.
4. **Publication.** No second-person approval workflow is required. Publishing
   requires an explicit scoped publish grant and review of the exact immutable
   revision; editor/owner status alone is insufficient.
5. **Local parity.** The supported local path uses the same OIDC,
   server-session, CSRF, authorization, validation, publication, and read
   enforcement code as deployment. Demo impersonation cannot satisfy acceptance
   tests.

## Proposed session and boundary values

These are concrete proposals for review, not implemented defaults. They allow
P02 to begin with tests that have an unambiguous expected outcome.

| Concern | Proposal | Acceptance consequence |
| --- | --- | --- |
| Session identifier | Generate at least 256 random bits. Store only a keyed hash, never the raw value. | A database dump cannot be replayed as a browser cookie. |
| Expiry | Absolute expiry: 8 hours. Idle expiry: 30 minutes. Reauthentication creates a new session. | A request after either boundary returns unauthenticated and private response headers prevent caching. |
| Revocation | Recheck the server session record on every administrative request; effective no later than the next request. | Logout or a grant/session revocation takes effect across two API processes without a cache-delay exception. |
| Cookie | Production and supported local TLS use `__Host-` prefix, `Secure`, `HttpOnly`, `SameSite=Lax`, `Path=/`, and no `Domain`. | A non-TLS demo shortcut cannot be mistaken for the supported path. |
| CSRF and origin | Bind a per-session CSRF secret to the server session; require it for cookie-authenticated unsafe methods and require an exact allowed `Origin`. Bearer CLI calls remain a separate path. | Missing, stale, cross-session, or foreign-origin state changes return 403. |
| Proxy headers | Trust forwarded host/proto/client headers only from explicitly configured proxy networks. | Forged public headers on a direct connection cannot relax redirect, cookie, origin, or rate-limit checks. |
| Login abuse | Apply a bounded per-client and per-subject callback/login failure limit at the administrative boundary, with generic failures and audit events. | Repeated failed state/code exchanges do not produce an oracle or unlimited provider traffic. |
| Provider tokens | Encrypt retained provider material with a deployment-provided key; retain only what logout/re-authentication requires. Do not implement refresh in the first release. | Tokens never appear in cookies, JSON, browser storage, logs, traces, or error responses. |
| Bootstrap credential | Keep the static admin token only as a one-time installation/bootstrap mechanism. Require an explicit enabled flag, document rotation, audit its use, and disable it after initial grant administration. | Browser sessions can never become privileged by presenting the bootstrap credential. |

## Proposed additive persistence plan

The existing control-plane database remains the only state store. Each table and
index below is additive and requires its own Alembic migration, downgrade/recovery
decision, retention rule, and PostgreSQL backup/restore test before implementation.

| Record | Minimum fields | Retention and integrity proposal |
| --- | --- | --- |
| Administrative session | keyed session hash, subject, issuer, created/idle/absolute expiry, revoked timestamp, protected provider material reference | delete expired/revoked records after 30 days; index session hash and active expiry |
| Scoped grant | principal selector, capability, workspace scope, optional asset/connection scope, grant revision, revoked timestamp | retain revocations for audit; unique live grant by scope/capability selector |
| Personal draft | author, resource scope, base publication, schema fingerprint, canonical content digest, revision, timestamps | retain until explicit discard or configured cleanup; compare-and-swap revision index |
| Evaluation | actor, exact draft/revision/digest, synthetic input digest, result digest, timestamps | short retention (14 days by default); no source rows or provider tokens |
| Operation and publication | immutable requested revision digest, idempotency key, actor, outcome, timestamps | retain as audit history; uniqueness on resource plus idempotency key |
| Audit event | actor, action, scope, safe before/after digests, request correlation, timestamp | append-only retention configured by deployment; never store secrets or source rows |

The migration review must include: an upgrade from an existing workspace with
authoring rows; a failed-migration recovery path; a backup/restore test; and a
mixed-version rollout decision. No migration may reinterpret or delete existing
policy data, and none touches the preserved pickle task payload logic.

## Proposed API-family replacement map

The names below are candidates to freeze during review. They prevent P02/P03
from silently expanding old endpoints; they are not routes yet.

| Candidate family | Replaces or supplements | Contract boundary |
| --- | --- | --- |
| `GET /v1/auth/login`, `GET /v1/auth/callback` | public demo login | start/complete authorization-code flow with bounded return location |
| `POST /v1/session/logout`, `GET /v1/session` | current cookie logout/session | revocable opaque-session state and safe capability metadata |
| `GET/PUT /v1/workspaces/{workspace_id}/grants` | no equivalent | grant-administrator-only scoped assignment and revocation |
| `GET/POST/PATCH/DELETE /v1/assets/{asset_id}/drafts` | mutable rule replacement | personal revisioned drafts with `If-Match`/expected revision |
| `POST /v1/assets/{asset_id}/evaluations` | current unbound preview | exact draft/revision synthetic evaluation |
| `POST /v1/assets/{asset_id}/publications` | current create-and-activate endpoint | exact revision review and idempotent publication operation |

All private/session responses use `Cache-Control: no-store`. Error bodies expose
safe machine-readable codes and a correlation ID, never provider, grant, or
resource detail that the caller is not entitled to see.

## Required corrections before contract acceptance

The proposal above is not implementation-ready until these ambiguities are
resolved in the final typed contracts and migration plan:

1. Add short-lived, browser-bound, single-use OIDC login transactions containing
   state/nonce/PKCE verifier and an allowlisted return location. Store sensitive
   transaction material protected in the existing control-plane store; expire
   and atomically consume it across API processes. A session table alone does
   not provide this replay protection. Use library implementations of OIDC/PKCE.
2. Bind every session/grant to issuer plus subject, and define group-grant
   freshness. App-grant revocation can take effect next request; removal of an
   IdP group embedded in a previously issued token cannot promise the same bound
   without an implemented notification/revalidation mechanism. Specify that
   bound and its failure behavior; do not imply IdP-wide logout.
3. Retain provider tokens only when an implemented feature requires them. If no
   refresh/provider logout is supported, prefer storing none after callback.
   Provider token exchanges necessarily contain tokens; the prohibition covers
   app/browser responses, cookies, logs and artifacts, not the secure IdP protocol.
4. Specify how the browser obtains a session-bound CSRF token without exposing
   the HttpOnly session identifier. Logout after session expiry still clears
   cookies with origin/CSRF handling defined for that case. Protect login/logout
   and their error paths against caching, redirect abuse, and information leaks.
5. Define bounded, multi-process login limiting without enabling account lockout
   through an unauthenticated arbitrary subject name. Separate callback attempts,
   trusted client-IP derivation, and authenticated subject limits. Document
   concrete thresholds, cleanup, safe errors, and recovery under saturation.
6. Replace the unconditional static-admin bypass with explicit bootstrap state
   and a narrow bootstrap command/service. Prevent ordinary API/CLI clients from
   regaining bootstrap authority after it is closed; document audited emergency
   access. Direct database administrative access cannot be constrained by API RBAC.
7. Specify collection and item routes separately: draft creation/listing under
   `/v1/assets/{asset_id}/drafts`; read/update/discard under
   `/v1/assets/{asset_id}/drafts/{draft_id}`. Define review and operation lookup
   endpoints, not just publish. Operations must be scoped by workspace, asset,
   actor and idempotency key; payload mismatches conflict. Include safe 401/403/
   404/409/412/413/422/429/503 contracts and their UI recovery behavior.
8. Keep grants of management capabilities separate from data-plane policy
   authoring. An administrator allowed to assign itself editor and publisher
   capabilities can indirectly change read policy; document this trusted power
   instead of claiming impossible prevention. Test that lesser roles cannot
   self-assign or cross scope. Handle final-admin removal and emergency recovery.
9. Model shared-table constraints, unique indexes, foreign keys, timestamps,
   cleanup and atomic update boundaries precisely. Session identifiers/key
   versions/CSRF material and per-draft evaluation/review/operation references need
   explicit columns or a reviewed equivalent. Tables are grouped into coherent
   migrations by dependency; do not require an arbitrary one-table-per-migration
   split. Session/ticket invalidation after backup restore must prevent replay of
   access revoked after the recovery point.
10. Confirm first-customer isolation and production targets from P13–P16. A single
    workspace deployment is the planning assumption; shared tenancy is a new
    isolation requirement. Fix operation-size/time/concurrency limits, supported
    protocol/library versions and deployment topology before load acceptance.

## Failing examples to implement after approval

These scenarios become route-level tests when the above decisions are approved:

- A metadata viewer cannot retrieve policy bodies, source-row previews, settings,
  another workspace's list count, or a guessed asset ID.
- An editor can save a draft only for an assigned asset, cannot publish, and gets
  a conflict response when its revision is stale.
- A publisher can review and publish only an exact revision inside its assigned
  asset scope, without editing rules or bundling unrelated drafts.
- A revoked browser session fails immediately; expired idle and absolute sessions
  fail; logout is idempotent and invalidates the browser cookie and server record.
- A cookie state mutation without the current CSRF token fails, while a bearer
  client follows the documented non-browser API path.
- A connection operator cannot read policies or source data; a grant
  administrator cannot obtain source rows merely by assigning grants.

## Evidence and next step

The inventory test passed against the generated OpenAPI document on the current
control-plane app. This packet has no migration and makes no authentication or
authorization behavior change. Review the five decisions above with the capable
owner before P02/P03; P01 and P11 verification work can continue independently.
