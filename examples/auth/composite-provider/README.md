# Composite Provider Authentication

This example runs one `dal-obscura` server with `CompositeIdentityProvider`.
The provider chain accepts both API key and shared JWT credentials.

## Services

- `setup`: provisions two auth providers through the control-plane API in
  order: API key, then JWT.
- `dal-obscura`: authenticates each request by trying the provider chain.
- `client`: performs one startup read with `x-api-key`, another with a signed
  JWT, then stays running for interactive reads.

## Run

```bash
docker compose up --build -d --wait
docker compose logs client
docker compose exec client dal-obscura-example-read --target default.users --credential api-key
docker compose exec client dal-obscura-example-read --target default.users --credential jwt
docker compose down --volumes
```

Expected success:

```text
composite-provider with api-key: authenticated as example-user and read 2 rows
composite-provider with jwt: authenticated as example-user and read 2 rows
```

## Caveats

Composite auth is meant for mixed environments and migrations. Provider order
matters: missing credentials fall through to the next provider, while invalid
credentials stop the chain and reject the request.
