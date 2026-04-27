# Authentication Examples

These examples run `dal-obscura` with real authentication mechanisms inside Docker
Compose. Each directory has its own `compose.yaml` and can be run independently.

## Examples

- `shared-jwt`: shared-secret HS256 bearer JWT.
- `keycloak-oidc`: real Keycloak OIDC issuer and JWKS validation.
- `api-key`: static service API key.
- `mtls`: local certificate-authority mTLS.
- `mtls-spiffe`: real SPIRE server and agent issuing SPIFFE X.509-SVIDs.
- `trusted-headers`: real Flight gateway injecting trusted identity headers.
- `composite-provider`: one server accepting API key and shared JWT credentials.

## Run Pattern

Run from an example directory:

```bash
docker compose up --build --abort-on-container-exit --exit-code-from client
```

Clean up from that directory:

```bash
docker compose down --volumes
```
