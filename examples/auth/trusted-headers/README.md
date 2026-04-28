# Trusted Header Authentication

This example runs `dal-obscura` behind a real Arrow Flight gateway. The client
connects to the gateway, and the gateway injects trusted identity headers before
forwarding calls to the backend.

## Services

- `setup`: creates the table, policy, and trusted-header provider config from
  `fixture/fixture.yaml` and `config/auth.yaml`.
- `dal-obscura`: trusts identity headers only when the proxy shared secret
  header is present and valid.
- `gateway`: forwards `get_schema`, `get_flight_info`, and `do_get` to the
  backend with the trusted headers injected.
- `client`: connects to the gateway, performs the startup read, and then stays
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
trusted-headers: authenticated as example-user and read 2 rows
```

## Caveats

Trusted headers are safe only when direct backend access is blocked and only the
trusted gateway can set identity headers. Production deployments should enforce
network isolation, TLS between gateway and backend, shared-secret rotation, and
careful stripping of user-supplied identity headers at ingress.
