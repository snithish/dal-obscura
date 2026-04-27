# Auth Compose Examples Design

## Goal

Create runnable Docker Compose examples for every supported authentication approach in
`dal-obscura`. Each example must run completely inside Docker Compose, start a real
`dal-obscura` server, authenticate with the selected mechanism, perform a real Arrow
Flight read, and document how the example works.

## Supported Examples

The examples live under `examples/auth/`:

- `shared-jwt`: shared-secret HS256 bearer JWT through `DefaultIdentityAdapter`.
- `keycloak-oidc`: real Keycloak OIDC issuer with `OidcJwksIdentityProvider`.
- `api-key`: static service account API key with `ApiKeyIdentityProvider`.
- `mtls`: client certificate identity with `MtlsIdentityProvider`.
- `trusted-headers`: real gateway/proxy asserting identity headers with
  `TrustedHeaderIdentityProvider`.
- `composite-provider`: one server using `CompositeIdentityProvider` to accept multiple
  credential families in order.

`CompositeIdentityProvider` is included as its own runnable example because it is the
operator-facing mechanism for mixed environments and migrations, not just an internal
implementation detail.

## Non-Goals

- Do not add production deployment manifests for Kubernetes or cloud platforms.
- Do not mock authentication services. The OIDC example must use a real Keycloak
  container, and the trusted-header example must use a real proxy/gateway container.
- Do not build a new application backend. The examples should use the existing
  `dal-obscura` CLI, existing Iceberg backend, and existing Flight API.
- Do not make examples depend on services outside Docker Compose.

## Architecture

Each auth example is a separate Compose project with its own `compose.yaml` and
`README.md`. Shared scripts and Docker assets live in `examples/auth/_shared/` to avoid
copying the same fixture and client code into six directories.

The shared runtime model is:

1. A setup container creates a real Iceberg SQL catalog, SQLite catalog database,
   warehouse data, policy file, and mechanism-specific app config.
2. A `dal-obscura` container starts the real service with the generated app config.
3. Optional supporting containers start when required, such as Keycloak or a trusted
   gRPC gateway.
4. A client container obtains or sends the correct credential, calls
   `get_flight_info`, calls `do_get`, reads the Arrow data, validates the expected
   result, prints a success message, and exits `0`.

The examples use generated sample data. The data is synthetic, but the catalog,
warehouse, policy evaluation, masking, auth provider, Flight transport, and client
read path are real.

## Shared Assets

`examples/auth/_shared/` contains the common implementation pieces:

- `Dockerfile`: builds an image from the local repository with `uv sync`.
- `scripts/build_fixture.py`: creates the Iceberg table, catalog config, policy config,
  app config, and auth-specific credentials for one example.
- `scripts/flight_client.py`: runs a real Arrow Flight read with auth options selected
  by environment variables.
- `scripts/wait_for_flight.py`: waits until `dal-obscura` accepts connections.
- `scripts/generate_certs.py`: creates a local CA, server certificate, and client
  certificate for the mTLS example.
- `scripts/trusted_gateway.py`: runs a real gRPC/Flight forwarding gateway that injects
  trusted identity headers before forwarding requests to `dal-obscura`.
- `keycloak/realm.json`: importable Keycloak realm for the OIDC example.

The shared fixture code should reuse the structure from
`tests/support/build_connector_fixture.py` where practical, but it should be shaped as
example code rather than a test-only script. The generated policy grants the principal
used by each mechanism enough access to prove the read path succeeds.

## Authentication Flows

### Shared JWT

The setup script writes an app config using
`dal_obscura.infrastructure.adapters.identity_default.DefaultIdentityAdapter` and a
secret from `DAL_OBSCURA_JWT_SECRET`. The client signs an HS256 JWT with that secret and
sends `Authorization: Bearer <token>`.

This demonstrates the simplest standalone bearer-token deployment.

### Keycloak OIDC

Compose starts a real Keycloak container with an imported realm, client, user or service
account, and protocol mappers for the claims needed by the policy. The setup script
writes an app config using
`dal_obscura.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider`.

The client obtains a real access token from Keycloak through the token endpoint and sends
it as `Authorization: Bearer <token>`. `dal-obscura` validates the token against the
Keycloak issuer and JWKS endpoint.

This demonstrates the production-style OIDC flow for enterprise and cloud identity.

### API Key

The setup script writes an app config using
`dal_obscura.infrastructure.adapters.identity_api_key.ApiKeyIdentityProvider`. The client
sends `x-api-key: <secret>`.

This demonstrates machine-to-machine or legacy client access where OIDC is unavailable.

### mTLS

The certificate setup script creates a CA, server certificate, and client certificate.
The app config enables `transport.tls.verify_client: true` and uses
`dal_obscura.infrastructure.adapters.identity_mtls.MtlsIdentityProvider`.

The client connects over `grpc+tls`, presents the client certificate, and authenticates
from the verified peer identity.

This demonstrates transport-bound service identity for service mesh or internal PKI
deployments.

### Trusted Headers

Compose starts a real proxy/gateway container in front of `dal-obscura`. The gateway
forwards Flight RPCs to the backend and injects:

- a shared proxy-secret header
- a subject header
- optional group and attribute headers

The backend uses
`dal_obscura.infrastructure.adapters.identity_trusted_headers.TrustedHeaderIdentityProvider`.
The client connects to the gateway, not directly to `dal-obscura`.

This demonstrates deployments where authentication is owned by ingress or an enterprise
gateway, and `dal-obscura` trusts identity asserted by that gateway.

### Composite Provider

The setup script writes an app config with `auth.providers` containing multiple real
providers, for example API key first and shared JWT second. The client performs two
successful reads in one run, one with the API key and one with the JWT, proving that the
same server accepts both mechanisms through the provider chain.

This demonstrates mixed estates and migration windows.

## Compose Shape

Each example directory owns a Compose file with the same basic service names:

- `setup`: creates `/workspace/runtime` files in a named volume.
- `dal-obscura`: runs `uv run dal-obscura --app-config /workspace/runtime/app.yaml`.
- `client`: runs the mechanism-specific client flow and exits.

Examples with support services add:

- `keycloak` for OIDC.
- `gateway` for trusted headers.
- `certs` or setup-integrated cert generation for mTLS.

The standard command is:

```bash
docker compose up --build --abort-on-container-exit --exit-code-from client
```

This command should exit `0` when the auth flow and read succeed.

## Documentation Requirements

Each `README.md` must explain:

- what auth mechanism is being demonstrated
- which services start in Compose
- how credentials are generated or obtained
- how to run the example
- what success output looks like
- how to clean up volumes
- caveats and production notes for that mechanism

The top-level `examples/auth/README.md` links to all examples and explains the shared
runtime shape.

## Error Handling

The client scripts should fail loudly when:

- Keycloak is not ready or token acquisition fails.
- `dal-obscura` is not reachable before the timeout.
- the Flight call is unauthorized.
- the read returns zero rows.
- the returned schema or expected row count check fails.

The setup scripts should fail loudly when required environment variables are missing or
when generated files cannot be written.

## Testing And Verification

The implementation should include lightweight tests for shared Python helper functions
where they can run quickly without Docker. Full end-to-end verification is the Compose
command for each example:

```bash
docker compose -f examples/auth/shared-jwt/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/keycloak-oidc/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/api-key/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/mtls/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/trusted-headers/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/composite-provider/compose.yaml up --build --abort-on-container-exit --exit-code-from client
```

The normal repository verification should also continue to pass:

```bash
uv run pytest
uv run ruff check .
uv run ty check
mvn -f connectors/jvm/pom.xml verify
```

## Security Notes

The examples are intentionally local and developer-oriented. They should not present
their generated secrets, certificates, Keycloak credentials, or proxy shared secrets as
production defaults.

Production caveats to call out:

- Shared JWT secrets need strong rotation and distribution controls.
- API keys need per-client ownership, rotation, and revocation.
- Keycloak examples use local dev credentials and HTTP inside the Compose network.
- Static JWKS is useful offline but shifts key rotation to operators.
- mTLS security depends on CA protection, certificate lifecycle, and client identity
  verification.
- Trusted headers are safe only when direct backend access is blocked and only the
  trusted gateway can set identity headers.
- Composite provider order matters because missing credentials fall through while
  invalid credentials stop the chain.
