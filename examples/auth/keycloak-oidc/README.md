# Keycloak OIDC Authentication

This example starts a real Keycloak container and runs `dal-obscura` with
`OidcJwksIdentityProvider`.

## Services

- `keycloak`: imports the `dal-obscura` realm from `keycloak/realm.json`.
- `setup`: creates the table, policy, and OIDC provider config from
  `fixture/fixture.yaml` and `config/auth.yaml`.
- `dal-obscura`: validates bearer tokens against Keycloak issuer, audience, and
  JWKS.
- `client`: obtains a real access token from Keycloak with the password grant,
  performs the startup read, then stays running for interactive reads.

## Run

```bash
docker compose up --build -d --wait
docker compose logs client
docker compose exec client dal-obscura-example-read --target default.users
docker compose down --volumes
```

Expected success:

```text
keycloak-oidc: authenticated as example-user and read 2 rows
```

## Caveats

Keycloak runs in development mode over HTTP inside the Compose network. The
realm uses fixed demo credentials and direct access grants so the example can run
fully unattended. Production deployments should use proper client flows, TLS,
realm lifecycle management, key rotation, and claim mapping aligned to policy.
