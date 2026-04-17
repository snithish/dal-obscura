# Provider-Based Authentication Design

## Goal

Support Keycloak as an authentication provider without coupling the service to
Keycloak-specific wiring. The implementation should let operators configure any
identity provider module that implements the service identity protocol, including
future providers such as Okta, Auth0, or an internal provider.

## Current State

`dal-obscura` already keeps authentication behind the application
`IdentityPort`:

```python
class IdentityPort(Protocol):
    def authenticate(self, headers: Mapping[str, str]) -> Principal: ...
```

The current CLI composition root always wires `DefaultIdentityAdapter`, which
validates HS256 bearer JWTs using a shared secret from `auth.jwt_secret`. The
authorization layer consumes only the resulting `Principal`:

```python
Principal(
    id="user1",
    groups=["analyst"],
    attributes={"tenant": "acme"},
)
```

That boundary is already the right place to add provider-based authentication.
The application use cases and policy engine should not change.

## Recommended Approach

Use a module-driven identity provider configuration, matching the existing
`secret_provider` pattern. Core config loading dynamically imports the configured
provider, resolves secret references in its arguments, constructs the provider,
and verifies that the constructed object implements `IdentityPort`.

This approach is preferred over a `provider: keycloak` switch because the core
service stays open to new providers without adding provider-specific branches to
the CLI or config schema. Keycloak becomes the first built-in OIDC/JWKS provider
example, not a special case.

## Auth Config

The `auth` block changes from hard-coded JWT fields to a provider module plus
provider-owned arguments:

```yaml
auth:
  module: dal_obscura.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider
  args:
    issuer: https://keycloak.example.com/realms/acme
    audience: dal-obscura
    subject_claim: sub
    group_claims:
      - groups
      - realm_access.roles
      - resource_access.dal-obscura.roles
    attribute_claims:
      tenant: tenant
      clearance: clearance
```

`auth.module` is a fully qualified class path. `auth.args` is passed to the
provider constructor after secret resolution.

Secret references inside `auth.args` should use the same shape as existing
ticket and JWT secret references:

```yaml
auth:
  module: dal_obscura.infrastructure.adapters.identity_default.DefaultIdentityAdapter
  args:
    jwt_secret:
      key: DAL_OBSCURA_JWT_SECRET
    jwt_issuer: null
    jwt_audience: null
```

The loader should recursively resolve dictionaries shaped exactly like
`{"key": "SECRET_NAME"}` through the configured secret provider before invoking
the provider constructor. This keeps provider constructors free of secret-provider
knowledge while still allowing providers to receive secrets such as client
secrets if future introspection providers need them.

## Provider Protocol

The identity provider protocol is the existing `IdentityPort`. A provider module
must expose a class whose constructor accepts `auth.args` and whose instances
provide:

```python
def authenticate(self, headers: Mapping[str, str]) -> Principal:
    ...
```

The provider owns token validation and maps the validated identity into the
service principal model:

- `Principal.id` is the stable policy identity.
- `Principal.groups` contains group and role strings that policies can match as
  `group:<value>`.
- `Principal.attributes` contains string key/value attributes for ABAC
  conditions in policy `when` blocks.

The core service should reject a configured provider at startup if the module
cannot be imported, the class cannot be found, construction fails, or the
constructed object does not expose a callable `authenticate(headers)` method.

## Built-In OIDC/JWKS Provider

Add a built-in OIDC/JWKS identity provider for Keycloak-compatible access
tokens. The provider should validate bearer access tokens locally using JWKS,
not by calling Keycloak per request.

The provider validates:

- `Authorization: Bearer <token>` is present.
- JWT signature matches a key from the configured JWKS.
- `iss` matches the configured issuer.
- `aud` matches the configured audience when an audience is configured.
- `exp`, `nbf`, and `iat` are honored by the JWT library.
- The token algorithm is one of the configured allowed algorithms, defaulting to
  asymmetric algorithms suitable for OIDC access tokens.
- The configured subject claim exists and is non-empty.

JWKS discovery can use either:

- `issuer` plus OIDC discovery at
  `/.well-known/openid-configuration`, using the returned `jwks_uri`.
- An explicit `jwks_url`, for environments where discovery is unavailable.

The provider should cache JWKS keys in memory. Normal authentication should not
call the identity provider on every request. On an unknown `kid`, the provider
should refresh JWKS once and retry validation before failing closed.

## Claim Mapping

Claim mapping is token-only. Keycloak must be configured with protocol mappers or
client scopes that put policy-relevant roles, groups, and user attributes into
the access token.

The built-in OIDC/JWKS provider accepts:

- `subject_claim`: JSON path for `Principal.id`, defaulting to `sub`.
- `group_claims`: list of JSON paths whose string, list, or object values are
  flattened into `Principal.groups`.
- `attribute_claims`: map from internal attribute name to JSON path. Values are
  converted to strings and stored in `Principal.attributes`.

Example Keycloak-oriented mappings:

```yaml
group_claims:
  - groups
  - realm_access.roles
  - resource_access.dal-obscura.roles
attribute_claims:
  tenant: tenant
  clearance: clearance
```

Missing group and attribute claims are ignored. Missing or empty subject is an
authentication failure. Attribute values that are lists or objects should be
rejected for the first implementation because the existing policy model expects
string attributes.

## Error Handling

Startup configuration errors should be explicit and fail fast:

- Unknown top-level config keys remain invalid.
- Missing `auth.module` is invalid.
- Non-object `auth.args` is invalid.
- Import failures include the missing module path.
- Provider constructor failures include the provider module path.
- Provider objects without `authenticate(headers)` are invalid.

Request-time authentication failures should continue to raise `PermissionError`
from the provider. The Flight interface already maps auth failures to
`FlightUnauthorizedError`. Client-facing errors should remain generic
`Unauthorized`; logs can distinguish missing token, invalid token, issuer
mismatch, audience mismatch, expired token, JWKS fetch failure, and subject
mapping failure.

If OIDC discovery or JWKS fetch fails during request authentication, the provider
must fail closed. It should not accept tokens without signature validation.

## Backwards Compatibility

The current shared-secret JWT behavior should continue to work as a provider
module. Existing `auth.jwt_secret` config can either be migrated in one release
or supported as a compatibility alias that internally constructs the same
provider.

The preferred documented config should be module-based:

```yaml
auth:
  module: dal_obscura.infrastructure.adapters.identity_default.DefaultIdentityAdapter
  args:
    jwt_secret:
      key: DAL_OBSCURA_JWT_SECRET
    jwt_issuer: null
    jwt_audience: null
```

No application use case, policy file, connector option, or Flight client header
contract should change. Clients still send `Authorization: Bearer <token>`.

## Tests

Add coverage for the provider loading boundary:

- Dynamic auth provider loading from `auth.module`.
- Rejection of missing modules, missing classes, constructor failures, and
  objects without `authenticate(headers)`.
- Recursive secret reference resolution inside `auth.args`.
- Backwards-compatible shared-secret JWT provider config.

Add coverage for the OIDC/JWKS provider:

- Valid Keycloak-like access token authenticates to the expected `Principal`.
- Invalid signature is rejected.
- Wrong issuer is rejected.
- Wrong audience is rejected.
- Expired token is rejected.
- Missing bearer token is rejected.
- Missing subject claim is rejected.
- Groups are extracted from `groups`, `realm_access.roles`, and
  `resource_access.<client>.roles`.
- Attributes are extracted from configured scalar token claims.
- Missing optional group and attribute claims are ignored.
- JWKS cache refresh occurs when a token references a new `kid`.

Add documentation checks through README examples for:

- Module-based shared-secret JWT provider.
- Keycloak/OIDC provider with issuer, audience, groups, roles, and attribute
  mappings.

## Documentation

The README should describe authentication as provider-based identity:

- Providers implement `IdentityPort`.
- The service ships with shared-secret JWT and OIDC/JWKS providers.
- Keycloak is configured as an OIDC/JWKS provider.
- Keycloak user metadata is available to `dal-obscura` only when protocol
  mappers or client scopes place those claims into the access token.
- Operators should map only policy-relevant user attributes into access tokens
  to avoid unnecessary token size and data exposure.

## Non-Goals

- No token introspection provider in the first implementation.
- No `/userinfo` enrichment in the first implementation.
- No per-request Keycloak network dependency for validated JWT access tokens.
- No changes to the policy model.
- No changes to Flight client headers or connector option names.
