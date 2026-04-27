# API Key Authentication

This example runs `dal-obscura` with `ApiKeyIdentityProvider`, using a static
service credential sent in the `x-api-key` Flight header.

## Services

- `setup`: creates the table, policy, and API-key provider config.
- `dal-obscura`: maps the configured API key to `example-user`.
- `client`: sends the configured API key and reads through Arrow Flight.

## Run

```bash
docker compose up --build --abort-on-container-exit --exit-code-from client
docker compose down --volumes
```

Expected success:

```text
api-key: authenticated as example-user and read 2 rows
```

## Caveats

API keys are useful for service accounts, batch jobs, and legacy clients that
cannot obtain OIDC tokens. They are bearer secrets, so production use needs
per-client ownership, rotation, revocation, audit logging, and secure transport.
