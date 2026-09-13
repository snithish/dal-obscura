# Secure-local profile

This profile runs the same production services and security settings locally:
PostgreSQL with separate migration/control/data roles, explicit migrations,
OIDC/PKCE browser sessions, HTTPS cookies and CSRF, immutable plugin locks,
Flight TLS with client verification, bounded limits, and readiness-gated startup.
It layers `compose.yaml` over `../production/compose.yaml`; no application source
patching or alternate authorization path is involved.

## Run

```bash
cd deployment/local-secure
cp .env.example .env
./run init
# Edit .env: use immutable image digests, unique secrets, a published cell UUID,
# approved catalog hosts, and a real local OIDC issuer/client.
./run config
./run up
```

Open `https://localhost:8443`. `./run init` creates a disposable local CA plus
separate `localhost` certificates for the browser edge and Flight service; trust
`secrets/ca.crt` in the test browser or client.
The data plane is available only on loopback at `grpc+tls://localhost:8815` and
requires a client certificate signed by that CA. The profile disables bootstrap
login; sign in through the configured OIDC provider.

`./run down` preserves PostgreSQL data. `./run reset` deletes the local volume and
is destructive to this disposable profile. Keep `secrets/`, `.env`, and generated
certificates out of version control. This profile provides the secure-local
topology and commands; real IdP, provider, artifact, consumer, and recovery runs
remain required evidence for paid-production acceptance.
