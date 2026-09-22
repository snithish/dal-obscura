# Named tunnel profile

This optional profile layers the production services and secure-local topology
with one operator-managed Cloudflare Tunnel connector. The connector has no
published port. The local Caddy origin stays on `127.0.0.1:8443`; Flight stays
on its private loopback TLS endpoint and is never routed through the browser
tunnel.

## Operator inputs

Copy `.env.example` to `.env`, replace every placeholder with approved values,
and provide owner-readable files at these paths:

- `secrets/origin.crt` and `secrets/origin.key`: certificate/key whose SAN
  contains the stable hostname;
- `secrets/cloudflared.token`: narrowly scoped connector token.

Set `DAL_OBSCURA_CLOUDFLARE_ACCESS_TEAM_NAME` and
`DAL_OBSCURA_CLOUDFLARE_ACCESS_AUD_TAG` to the exact values configured on the
remote tunnel's `originRequest.access` rule. The runner requires these values
to be declared, but never creates or changes remote tunnel configuration;
`./run doctor` still reports Access as unverified until an operator performs
the real allow/deny qualification.

Keep `origin.key`, `cloudflared.token`, and the local Flight private key
owner-only (`0400` or `0600`). The runner rejects weaker permissions and checks
the origin certificate SAN against `DAL_OBSCURA_NAMED_HOST` before starting any
service or connector.

Create the DNS record, tunnel, Access application, default-deny policy, and
exact Access audience through the separately approved Cloudflare change. This
profile never creates or modifies those resources and never treats a configured
token as proof that Access is enforcing a policy.

## Run

```bash
cd deployment/named-tunnel
cp .env.example .env
mkdir -p secrets
chmod 600 secrets/cloudflared.token secrets/origin.key
./run config
./run up
./run doctor
```

`./run doctor` validates the local overlay, stable HTTPS callback, origin
certificate SAN, and connector process state. It emits redacted diagnostics and
marks Cloudflare Access as unverified. `./run up` first waits for the private
stack and only then enables the connector profile, so a connector cannot be
started as the private services are still becoming ready. `./run down` stops
only this Compose project and keeps PostgreSQL data; `./run reset` is
destructive to the local profile. Use `./run logs cloudflared` for connector
diagnostics without passing the token on a command line.
