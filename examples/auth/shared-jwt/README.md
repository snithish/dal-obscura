# Shared JWT Authentication

This example runs `dal-obscura` with `DefaultIdentityAdapter`, which validates
an HS256 bearer JWT signed with a shared secret.

## Services

- `setup`: creates the Iceberg table, policy, and app config.
- `dal-obscura`: validates `Authorization: Bearer <jwt>` with
  `DAL_OBSCURA_JWT_SECRET`.
- `client`: signs a JWT for `sub=example-user`, performs a Flight read, and
  validates the result.

## Run

```bash
docker compose up --build --abort-on-container-exit --exit-code-from client
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
