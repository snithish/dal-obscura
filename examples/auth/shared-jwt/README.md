# Shared JWT Authentication

This example runs `dal-obscura` with `DefaultIdentityAdapter`, which validates
an HS256 bearer JWT signed with a shared secret.

## Services

- `setup`: creates the Iceberg table, provisions the JWT provider and policy
  through the control-plane API, then publishes the data-plane snapshot.
- `dal-obscura`: validates `Authorization: Bearer <jwt>` with
  `DAL_OBSCURA_JWT_SECRET`.
- `client`: signs a JWT for `sub=example-user`, performs a startup read, then
  stays running for interactive reads.

## Run

```bash
docker compose up --build -d --wait
docker compose logs client
docker compose exec client dal-obscura-example-read --target default.users
docker compose down --volumes
```

Expected success:

```text
shared-jwt: authenticated as example-user and read 2 rows
```

## Caveats

Shared JWT is simple and works in any environment that can distribute a secret,
but all issuers that know the secret can mint tokens. Production deployments
need strong secret storage, rotation, issuer discipline, and short token TTLs.
