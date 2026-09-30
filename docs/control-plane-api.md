# Control-plane API

The control plane manages configuration and administrative access. Management
permissions do not grant governed Flight data access. Use [connectors](connectors.md)
for the data-plane protocol.

## Authentication and errors

Writes use JSON. Authenticate with a bearer token or a browser session; browser
mutations also require the session's CSRF token. Missing bearer credentials return
`WWW-Authenticate: Bearer`. Invalid bearer headers cannot fall back to a cookie.

Branch on `error.code`, retain `error.request_id` for support, and inspect
`field_errors` and `current_revision` when present. `X-Request-ID` correlates
requests and responses. Validation errors are redacted structured responses;
malformed requests should not be retried as server failures.

## Resource edits

Read the current resource before replacing it and supply the applicable revision.
Asset metadata `revision` and policy `policy_revision` are separate preconditions.
Missing required preconditions return 428; stale preconditions return 409.
Reconcile the current resource before retrying a stale write.

Policy replacement is a direct live write through `PUT /v1/assets/{asset_id}/policy`.
The body includes `expected_revision`, the complete `rules`, and
`revoke_existing_tokens`. Policy edits affect new reads; existing tickets retain
captured permissions unless revoked. Owners can also use
`POST /v1/assets/{asset_id}/tickets/revoke`. See [policy authoring](policy-authoring.md).

Use `/v1/assets/page` and `/v1/audit/events/page` for bounded collection reads.
Avoid unpaged asset inventory for large deployments.

## Integration limits

- Edit-only and grant-only actors may need separate metadata-read authority to
  obtain the revisions needed for writes. Capabilities are independent.
- Authentication-provider contents and the chain revision use separate GETs.
  Do not assume those responses are an atomic snapshot across concurrent edits.
- Some mutations omit the resulting revision; reload before another edit and
  handle a competing writer's changes.
- Flight targets are limited to 512 characters. HTTP path identifiers cannot
  represent decoded slashes; use identifiers supported by both surfaces.
- Owner and grant principal keys are issuer-scoped. Use the canonical identity
  representation rather than an unqualified display name.

## Generated contracts

The checked-in [OpenAPI snapshot](../apps/governance-ui/openapi/control-plane.json)
is the source for generated response types. After changing routes or DTOs, run:

```bash
uv run scripts/export_control_plane_openapi.py
node apps/governance-ui/scripts/generate-api-types.mjs
uv run scripts/export_control_plane_openapi.py --check
node apps/governance-ui/scripts/generate-api-types.mjs --check
```

HTTP contract tests live in `tests/interfaces/control_plane/test_api_contract.py`;
authorization, sessions, and body limits have their own behavioral suites.
