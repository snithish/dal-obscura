# Authentication Request Boundary Design

## Goal

Keep the new transport-aware authentication boundary, but remove compatibility
code that exists only to preserve the old header-only interface. The service
should use one explicit authentication request model end to end.

## Current State

The branch already introduces `AuthenticationRequest` in
`src/dal_obscura/application/ports/identity.py` and threads it through the
Flight transport and application use cases. The current implementation still
keeps several compatibility-oriented pieces:

- `AuthenticationRequest` implements `Mapping[str, str]`.
- `AuthenticationInput` allows either `AuthenticationRequest` or a raw header
  mapping.
- `coerce_authentication_request(...)` upgrades raw mappings into the new
  request type.
- Existing identity providers still accept the compatibility union and coerce it
  internally.

That works, but it hides the real boundary. The system is no longer
"authenticate from headers." It is "authenticate from request context."

## Recommended Approach

Make `AuthenticationRequest` the only supported authentication input type.

The service should treat authentication as operating on a transport-neutral
request object with explicit fields for headers and peer metadata. Raw header
mappings should stop being accepted by the port, adapters, and tests. This is a
breaking change by design.

This approach is preferred over keeping the compatibility union because it makes
the boundary honest and reduces adapter noise. It also keeps future providers
for API keys, mTLS, and trusted gateways from depending on accidental header-map
behavior.

## Request Model

`AuthenticationRequest` should be an immutable value object with explicit
fields:

- `headers: dict[str, str]`
- `peer_identity: str`
- `peer: str`
- `method: str`

Header names should still be normalized to lowercase at construction time so
providers can rely on case-insensitive lookup without repeating that logic.

The object should no longer implement `Mapping[str, str]`. Header access should
be explicit through either:

- `request.headers["authorization"]`
- a tiny helper such as `request.header("authorization") -> str | None`

The model exists to carry auth-relevant request context, not to masquerade as a
dictionary.

## Port Contract

`IdentityPort` should become:

```python
def authenticate(self, request: AuthenticationRequest) -> Principal: ...
```

Delete:

- `AuthenticationInput`
- `coerce_authentication_request(...)`

All identity providers should accept `AuthenticationRequest` directly.

This affects:

- `DefaultIdentityAdapter`
- `OidcJwksIdentityProvider`
- all future providers added by the authentication plan
- test doubles and fake providers used by application and config tests

## Application and Transport Boundaries

The application use cases should also require `AuthenticationRequest`
explicitly:

- `GetSchemaUseCase.execute(..., auth_request: AuthenticationRequest)`
- `PlanAccessUseCase.execute(..., auth_request: AuthenticationRequest)`
- `FetchStreamUseCase.execute(..., auth_request: AuthenticationRequest)`

The Flight layer remains responsible for translating transport-specific context
into the request model. `authentication_request_from_context(...)` in
`src/dal_obscura/interfaces/flight/contracts.py` is the right boundary for that
translation.

The old public idea of "headers from context" should no longer drive the auth
flow. A raw `headers_from_context(...)` helper may remain as an internal utility
only if some non-auth code still needs it, but authentication code should not
consume raw header mappings anymore.

## Provider Behavior

JWT-oriented providers should read from the explicit request model:

- bearer token from `request.headers.get("authorization")`
- peer information from `request.peer_identity` if relevant
- method from `request.method` if a provider wants method-sensitive behavior in
  the future

This makes provider code simpler than the current union-based design because it
stops each provider from having to normalize or coerce input before reading it.

## Breaking Changes

The following breaking changes are intentional and acceptable:

- Providers no longer accept raw `Mapping[str, str]` input.
- Tests and helper utilities must construct `AuthenticationRequest` directly.
- Any third-party provider module implementing `IdentityPort` must update its
  `authenticate(...)` signature.

This is the correct place to break the API because the new auth platform is
still in active development on this branch, and carrying a compatibility layer
would only make later provider work noisier.

## Testing

Update tests to verify the stricter boundary:

- `AuthenticationRequest` tests should cover normalization and explicit field
  access.
- Remove tests for coercing plain mappings into request objects.
- Built-in identity provider tests should construct `AuthenticationRequest`
  explicitly.
- Application-use-case and Flight tests should pass `AuthenticationRequest`
  instances through the stack.

The existing behavioral coverage for JWT validation, Flight propagation, and the
plan/fetch access flow should remain. Only the auth-input shape changes.

## Scope

This design change is intentionally narrow. It does not change:

- authorization behavior
- policy evaluation
- client header semantics
- the broader authentication plan for provider chaining, API keys, mTLS, or
  trusted proxy headers

It only tightens the contract those later tasks will build on.
