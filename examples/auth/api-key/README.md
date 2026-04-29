# API Key Authentication

This example runs `dal-obscura` with `ApiKeyIdentityProvider`, using a static
service credential sent in the `x-api-key` Flight header.

## Services

- `setup`: creates the table, provisions the API-key provider and policy through
  the control-plane API, then publishes the data-plane snapshot.
- `dal-obscura`: maps the configured API key to `example-user`.
- `client`: sends the configured API key for the startup read, then stays
  running for interactive reads.

## Run

```bash
docker compose up --build -d --wait
docker compose logs client
docker compose exec client dal-obscura-example-read --target default.users
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
