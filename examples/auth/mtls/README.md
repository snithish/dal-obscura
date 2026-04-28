# Local mTLS Authentication

This example runs `dal-obscura` over TLS with client certificate verification
enabled and maps the verified peer identity through `MtlsIdentityProvider`.

## Services

- `setup`: generates a local CA, server certificate, client certificate, table,
  policy, and mTLS app config from `fixture/fixture.yaml`, `config/auth.yaml`,
  and `config/transport.yaml`.
- `dal-obscura`: starts on `grpc+tls` and requires a client certificate signed by
  the generated CA.
- `client`: presents the generated client certificate for the startup read, then
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
mtls: authenticated as example-user and read 2 rows
```

## Caveats

This is local PKI for demonstration. In production, protect the CA, automate
certificate issuance and rotation, constrain accepted identities, and treat
transport TLS configuration separately from authorization policy.
