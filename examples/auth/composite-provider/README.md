# Composite Provider Authentication

This example runs one `dal-obscura` server with `CompositeIdentityProvider`.
The provider chain accepts both API key and shared JWT credentials.

## Services

- `setup`: creates a config with two auth providers in order: API key, then JWT.
- `dal-obscura`: authenticates each request by trying the provider chain.
- `client`: performs one successful read with `x-api-key`, then another with a
  signed JWT.

## Run

```bash
docker compose up --build --abort-on-container-exit --exit-code-from client
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
