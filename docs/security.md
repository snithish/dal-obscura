# Security Guide

dal-obscura authenticates every read and enforces row filters and masks inside
the data plane before Arrow batches leave the service. Asset policy edits write
directly to live configuration. Tickets issued before an edit retain the
permissions captured at planning until expiry unless the asset owner revokes
them.

## Contents

- [Security Model](#security-model)
- [Trust Boundaries](#trust-boundaries)
- [Identity Providers](#identity-providers)
- [Ticket Lifecycle](#ticket-lifecycle)
- [Policy Enforcement](#policy-enforcement)
- [Secret Handling](#secret-handling)
- [Live Configuration And Ticket Revocation](#live-configuration-and-ticket-revocation)
- [Operator Checklist](#operator-checklist)

## Security Model

```mermaid
flowchart TD
    request["Client request"] --> auth["Authenticate"]
    auth --> policy["Authorize current live asset policy"]
    policy --> ticket["Mint or verify opaque ticket"]
    ticket --> execute["Execute trusted scan task"]
    execute --> transform["Apply DuckDB row filters and masks"]
    transform --> response["Return Arrow batches"]
```

The data plane re-authenticates on `do_get`; a ticket alone is not enough to
stream data.

## Trust Boundaries

| Boundary | Rule |
| --- | --- |
| Client request | Treat descriptor and ticket inputs as untrusted until parsed and verified. |
| Ticket payload | Client receives an opaque reference, not an editable scan plan. |
| Config database | Stores live policy revisions and trusted internal scan payloads. |
| Secret values | Stay in runtime secret providers, not in config records. |

Tickets persist bounded passive JSON scan envelopes server-side. Envelopes bind
the admitted plugin artifact, immutable handle, schema and task data; decoding
cannot import a module selected by the payload. Native Iceberg tasks capture admitted file membership, projected columns,
bounded parallelism and an optional validated SQL hint; native libraries own deletes. No runtime
ticket path uses pickle. The stored envelope is internal, outside the connector
wire contract; payload integrity, identity binding and revocation still apply.

## Identity Providers

| Provider | Best for | Notes |
| --- | --- | --- |
| OIDC/JWKS | Gateway and local Keycloak demos | Required identity provider. |

The gateway accepts one configured OIDC/JWKS provider. API keys, trusted headers,
mTLS identity mapping, shared-secret JWTs, and composite auth are unsupported.
Remote JWKS refreshes are rate-limited (30 seconds by default) and accept at
most 256 usable signing keys. The next authentication after that interval refreshes
the complete key set, including known key IDs, so removed keys stop being trusted
and replacements using the same ID take effect. Concurrent requests share one
refresh. A failed refresh denies authentication until a successful retry; failed
attempts and unknown key IDs cannot trigger one network request per authentication.
The retry interval starts when the refresh attempt completes. This intentionally
trades availability during an IdP outage for bounded key freshness. Inline/file
JWKS configurations are static and require reconfiguration to rotate keys.
Operators can tighten both limits when their IdP rotation policy requires it.

Browser sign-out revokes the local session and expires both session and CSRF
cookies before navigating to the provider's RP-initiated logout endpoint. The
request carries the configured client ID and registered post-logout return URI;
provider tokens are not retained or sent to the browser. Without an ID-token
hint, the provider can require confirmation before ending its SSO session.
Deployments using a non-Keycloak provider should set
`DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_END_SESSION_ENDPOINT` to its logout endpoint.
The return URI must match the UI origin and be registered with the provider.

## Ticket Lifecycle

```mermaid
sequenceDiagram
    participant Client
    participant Flight
    participant Store as "Config database"

    Client->>Flight: get_flight_info
    Flight->>Store: Store trusted scan payload
    Flight-->>Client: Return opaque signed ticket reference
    Client->>Flight: do_get(ticket)
    Flight->>Store: Load scan payload by reference
    Flight->>Flight: Verify signature, expiry, principal, exchanges, revocation state
    Flight-->>Client: Stream governed data
```

The data plane verifies ticket expiry, principal, exchange count, and explicit
revocation state before streaming. It does not silently rewrite a ticket's
captured permissions after a policy edit.

## Policy Enforcement

- Default behavior is deny.
- Row filters and masks are DuckDB SQL expressions.
- Engine-provided row filters are validated before planning.
- Backend pushdown is an optimization; the data plane reapplies the effective
  filter during fetch when needed.
- Masking changes must update both DuckDB projection logic and masked schema
  behavior.

## Secret Handling

```mermaid
flowchart LR
    config["Config record"] --> ref["Secret reference"]
    ref --> provider["Runtime secret provider"]
    provider --> value["Secret value in memory"]
```

Store references in configuration. Keep secret values in environment variables,
container secret stores, or another runtime secret provider.

Select the fixed `environment` or `mounted_file` provider with
`DAL_OBSCURA_SECRET_PROVIDER_KIND`. Both accept a `scope_grants` object in
`DAL_OBSCURA_SECRET_PROVIDER_CONFIG`. Each exact caller scope (for example,
`catalog:analytics` or `identity`) must name the secret keys it may resolve.
Secret resolution is denied unless the provider implements scope authorization
and this grant map includes the exact scope and key; a matching reference scope
alone cannot authorize a provider lookup. Production startup requires a
non-empty grant map in both planes.

The environment provider accepts an optional `prefix`. The mounted-file provider
accepts an absolute `directory` and reads flat regular UTF-8 files on each
resolution, without caching values. File names must start with an ASCII letter or
digit, contain only letters, digits, underscores, dots or hyphens, and fit 255
characters. Values must be nonempty and at most 64 KiB of UTF-8 bytes. Whitespace,
including final newlines, is preserved. Unknown configuration keys fail startup.

For example, provision `CATALOG_TOKEN` through deployment secret tooling, then
configure both planes with references only:

```bash
DAL_OBSCURA_SECRET_PROVIDER_KIND=mounted_file
DAL_OBSCURA_SECRET_PROVIDER_CONFIG='{"directory":"/run/dal-secrets","scope_grants":{"catalog:analytics":["CATALOG_TOKEN"]}}'
```

Bind the directory read-only into each plane and restrict its host/runtime access
to trusted provisioning tooling and worker identities. Bind the directory rather
than an individual file so an atomic file replacement is visible to readers.
Rotate by writing a complete new file and atomically replacing the named file in
the same directory; each resolution observes one opened version. Replace files,
not the root directory. The provider checks the original directory's device/inode
identity, refuses symlinks, hard links and nonregular files, and closes descriptors
on rejected reads. It cannot revoke already opened provider sessions or guarantee
nonblocking disk IO. Startup identity configurations and cached clients can retain
resolved values; their reload/restart or provider-specific credential refresh
remains a separate lifecycle.

Projected symlink secret volumes are rejected. Use provisioning tooling that
delivers flat regular files; Vault/cloud-secret-manager adapters and container
rotation qualification remain separate support gates. Local POSIX qualification
does not establish those provider integrations.

This is a breaking startup configuration change: remove
`DAL_OBSCURA_SECRET_PROVIDER_MODULE` and use `DAL_OBSCURA_SECRET_PROVIDER_KIND`.
The old module-path selector and `DAL_OBSCURA_SECRET_PROVIDER_SECRETS` are rejected,
including when set to an empty string. Arbitrary provider imports and inline secret
configuration remain prohibited.

## Live Configuration And Ticket Revocation

An authorized owner saves a policy directly to its asset using the current
`policy_revision`. The control plane validates the complete replacement and
updates it transactionally; stale revisions fail with a conflict. There is no
workspace bundle or separate review/publish stage.

Existing tickets retain their original authorized columns, row filter, and
masks until their normal expiry. This supports predictable in-flight access
through routine policy edits, but owners must revoke tickets when a change must
take effect immediately. The asset Access view can revoke every active ticket,
and the policy editor offers the same revocation as part of a save. The API
records the operation and returns the number of tickets revoked.

Treat ticket revocation as an access-control action: it can interrupt readers
using the asset. Use the explicit owner action for urgent invalidation; for a
routine policy change, leave revocation off unless the existing authorization
must stop immediately.

## Operator Checklist

- Use Postgres for persistent shared state.
- Use the supported OIDC/JWKS provider.
- Bind local examples to `127.0.0.1`.
- Rotate ticket and IAM secrets through the runtime secret provider.
- Decide whether each policy edit must revoke existing asset tickets; otherwise
  they remain usable until expiry.
- Test one allowed persona and one denied persona before exposing an
  environment.
