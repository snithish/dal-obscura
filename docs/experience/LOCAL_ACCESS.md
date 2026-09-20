# Local HTTPS, SSO and Cloudflare integration plan

Researched 2026-09-20. This document proposes configuration and implementation;
no tunnel, DNS record, account, Access application or external exposure was created.
Acceptance owners: R02/R03/R09 and E02–E07.

## Decision

Use a **named Cloudflare Tunnel + stable developer hostname + Cloudflare Access**
for the optional Internet-connected local profile. Keep the application's normal
OIDC/PKCE login, sessions, CSRF, permissions and Flight authorization. Configure
Access and the app against the same existing compatible upstream identity provider
to reuse its SSO session. Two protocol redirects may remain; do not promise a
single redirect or treat the edge identity as an app session.

Keep a fully local HTTPS + local OIDC-provider profile for working without
Cloudflare or an Internet connection. Both profiles use the same built UI/API and
security checks. Named tunnels are optional developer infrastructure, not a new
production dependency or a route to expose customer databases.

Use Quick Tunnels only for disposable previews of a static synthetic designbook.
Any broader test application exposure requires a separate reviewed scope.
They provide a random HTTPS hostname,
not a stable callback origin; their documented limits include 200 in-flight
requests, no SSE and no production SLA. They are unsuitable as the default
repeatable SSO profile. [Quick Tunnels](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/do-more-with-tunnels/trycloudflare/),
[requested product page](https://try.cloudflare.com/)

Do not infer that a random unlisted URL is access control. A Quick Tunnel's
origin becomes reachable through the public hostname; no real data or admin
bootstrap surface belongs in an unprotected preview.

## Supported topology

Browser → HTTPS developer hostname → Access policy → encrypted tunnel connector →
HTTPS local reverse proxy → same-origin built UI, /auth/* and /v1/*.

The proxy/API/DB remain on private local interfaces or isolated container networks.
The only Internet ingress is the explicitly configured hostname. Backend calls to
the upstream IdP use verified HTTPS directly, without browser Access challenges.
Build artifacts and static hashed assets may be cached; API/auth responses must
remain no-store. Local logs and Cloudflare edge logs have separate retention and
privacy controls; neither is proof of application-level policy audit.

Flight clients → local/private TLS Flight endpoint → existing JWT/ticket/policy
checks. **Do not point Arrow Flight at the public tunnel hostname.** Cloudflare
documents gRPC support via private subnet routing, not public-hostname tunnels.
For later remote consumer testing, use an explicitly scoped private route with
Cloudflare One Client and prove TLS/JWT/Spark behavior separately; it is not part
of the minimum local web profile. [Cloudflare gRPC support](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/use-cases/grpc/)

## Three authentication concepts that must remain separate

1. **Tunnel transport:** cloudflared authenticates the connector. Its credential
   does not identify a user or authorize an asset.
2. **Access self-hosted application:** the edge admits authorized users to the
   web hostname. Configure explicit allow rules and default deny, not “everyone.”
   Have cloudflared validate the Access assertion for the exact team/application
   audience before proxying. Do not trust an arbitrary email or JWT header.
   [Self-hosted Access application](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/self-hosted-public-app/),
   [origin Access validation](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/configure-tunnels/origin-parameters/)
3. **Application OIDC and authorization:** continue to verify issuer/audience/
   signature/nonce/state/PKCE and mint the existing opaque app cookie. App grants,
   published data policies, revocation and audit remain authoritative. Passing
   Access never makes the user a platform administrator.

Cloudflare also offers Access as an OIDC provider for a generic SaaS application.
That is a different integration from a self-hosted edge gate. Its published setup
supplies client credentials and per-client issuer/discovery/JWKS endpoints.
The current public-client exchange must not be assumed compatible.
[Cloudflare generic OIDC](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/saas-apps/generic-oidc-saas/)

**Initial supported choice:** same upstream IdP plus Access edge gate.
**Deferred alternative:** Cloudflare as the app's direct OIDC provider. If selected
later, add provider-neutral confidential-client authentication using a server-only
secret reference and discovery metadata, then qualify the actual registered client
auth method, PKCE, claims and logout. No Cloudflare-specific identity bypass, no
secret in public ui-auth-config and no guessing that an Access JWT is a Flight token.
Do not add this second integration merely to complete the first.

## Profile inputs and operator flow

Use the existing profile/Compose/CLI machinery; do not create a second launcher
framework or store account credentials in the control-plane database.

Required operator-supplied inputs for named mode:

- Cloudflare account and controlled DNS zone; unique developer hostname such as
  obscura-alice.dev.example.com. The example is a placeholder, not a requested DNS change.
- Named tunnel identifier and narrowly scoped connector credential in a secret
  file with local owner-only permissions; never a command-line token in logs.
- Access team name, exact application audience and identity-provider allow policy.
- App OIDC issuer/client ID, exact HTTPS callback /auth/callback and approved
  logout/return origins. Prefer an existing external development IdP supporting
  the app's public authorization-code client.
- Verified local proxy certificate/key, CA file and matching origin SNI name.
  TLS verification is mandatory; never set noTLSVerify to make setup pass.
- Existing database, policy/fixture and Flight TLS profile inputs. Bootstrap is
  disabled before public ingress is enabled.

An operator performs account/DNS/Access consent steps once. The future local
command validates inputs, starts the existing private stack, waits for readiness,
checks local OIDC configuration, then starts the connector. No migrations, reseeding
or automatic public exposure occur as an incidental consequence of a UI command.
Reuse the existing explicit migration/init path and disposable fixtures.

Add a bounded “doctor” mode to the existing launcher: report build versions,
ports, configured public origin, CA/SNI validity, callback match, backend readiness,
connector state and whether Access is required. Redact all credentials.
Offline checks do not claim the remote Access policy is correctly enforced; only
E07's real browser allow/deny probes can establish that.

Repeated start is idempotent. Stop terminates only this profile's processes and
connector, retaining data; the runbook includes explicit remote credential/DNS/
Access cleanup for final decommissioning. A stopped connector does not delete
those remote resources. Do not automatically recreate a missing Access policy.

## Trust and failure requirements

- Preserve a configured canonical external origin. Host, Forwarded,
  X-Forwarded-Proto and CF-Connecting-IP from arbitrary callers cannot alter it.
  Set exact trusted proxy peers and overwrite inherited forwarding headers at
  the boundary. Reject unexpected Host/origin before session mutations.
- Solve client rate-limit attribution at that trusted boundary. Current direct
  peer keys will group all tunnel users. Accept a sanitized client address only
  from a configured trusted gateway; keep a bounded aggregate limiter as well.
  Different users cannot lock everyone out, and spoofed headers cannot evade limits.
- Use connector Access JWT enforcement with exact audience and expiry checking.
  The isolated origin still performs app auth. No bypass rule for /v1 or /auth;
  a normal authorized browser callback must pass the edge gate. Keep Access
  credentials/service tokens out of browser JavaScript.
- If a local Keycloak instance supplies the upstream IdP, its browser/discovery/
  token/JWKS endpoints must have a consistent reachable issuer. Do not put that
  IdP behind an Access policy that depends on the same IdP: that creates a login
  loop. Initial named mode uses an external IdP; offline mode keeps Keycloak local.
- A Cloudflare login HTML response or redirect returned to fetch is an edge-session
  expiry state, not JSON data or a successful mutation. Offer explicit sign-in/
  top-level navigation, preserve safe local edits where allowed, then re-read and
  reconcile operations. Never replay publish automatically.
- Distinguish app logout, edge logout and upstream IdP logout. App logout always
  clears/revokes its session; an existing upstream session may permit immediate
  reauthentication. Offer “Sign out of shared browser” with explicit supported
  provider/edge logout behavior, and test it. Do not claim universal single logout.
- Tunnel or IdP outage shows a recoverable offline/unavailable state. Existing app
  privileged-session freshness policy still applies; never fall back to demo token
  login. A connector restart keeps the hostname and callback stable.
- Cloudflare terminates public TLS and processes web traffic; this is not
  end-to-end browser-to-process encryption. Keep sensitive source datasets and
  Flight off this route. Treat Access logs/analytics as an additional vendor
  processing boundary and document applicable plan/retention settings.

## Operational benefits and limitations

The planned benefits are a stable trusted HTTPS browser origin, managed edge
certificate, no router port forwarding, identity-aware front-door access, remote
review on an explicitly admitted device/user, and connector diagnostics. They do
not replace app RBAC, TLS inside the chosen origin boundary, data policy, backups
or production readiness. Cloudflare account/zone/IdP administration and an Internet
connection are prerequisites; a purchased domain may be needed. Verify account
entitlements/limits when implementing; this plan makes no free-tier price promise.

Storybook remains local by default. If shared, expose only a static synthetic build
through its own Access-protected hostname/audience. Do not tunnel Vite or Storybook
development servers, source maps, environment endpoints or a customer-backed UI.
No tunnel needs to be created during this planning task.
